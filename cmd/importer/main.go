package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultStatus    = "待处理"
	ollamaChatURL    = "http://localhost:11434/api/chat"
	structModel      = "qwen2:7b-instruct"
	structReqTimeout = 5 * time.Minute
	workerCount      = 10
)

type jobRecord struct {
	MID      string `json:"mid"`
	Intro    string `json:"intro"`
	Source   string `json:"source"`
	Category string `json:"category"`
	Status   string `json:"status"`
}

type jobTask struct {
	Index int
	Total int
	Job   jobRecord
}

type structuredPayload struct {
	Summary          string          `json:"岗位概述"`
	Responsibilities json.RawMessage `json:"核心职责"`
	Skills           json.RawMessage `json:"技能标签"`
	Industry         string          `json:"行业"`
	Locations        json.RawMessage `json:"工作地点"`
	CategoryHints    json.RawMessage `json:"可能对应的大类"`
}

type StructuringService struct {
	client *http.Client
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
}

type chatResponse struct {
	Message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"message"`
}

func NewStructuringService() *StructuringService {
	return &StructuringService{
		client: &http.Client{Timeout: structReqTimeout},
	}
}

func main() {
	var (
		dbPath string
		limit  int
	)

	flag.StringVar(&dbPath, "db", "data/jobs.db", "SQLite 文件路径")
	flag.IntVar(&limit, "limit", 0, "本次最多处理多少条岗位（0 表示全部）")
	flag.Parse()

	ctx := context.Background()

	db, err := initDB(dbPath)
	if err != nil {
		log.Fatalf("初始化数据库失败: %v", err)
	}
	defer db.Close()

	jobs, err := fetchPendingJobs(ctx, db, limit)
	if err != nil {
		log.Fatalf("查询待处理岗位失败: %v", err)
	}

	if len(jobs) == 0 {
		log.Println("没有需要结构化的岗位，退出。")
		return
	}

	log.Printf("本次待处理 %d 条岗位", len(jobs))

	structSvc := NewStructuringService()

	tasks := make(chan jobTask)
	var wg sync.WaitGroup
	var successCount atomic.Int64
	var failureCount atomic.Int64

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				handleJob(ctx, db, structSvc, task, &successCount, &failureCount)
			}
		}()
	}

	for idx, job := range jobs {
		tasks <- jobTask{Index: idx + 1, Total: len(jobs), Job: job}
	}
	close(tasks)
	wg.Wait()

	log.Printf("[SUMMARY] total=%d success=%d failed=%d", len(jobs), successCount.Load(), failureCount.Load())
}

func initDB(dbPath string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?_cache_size=200000&_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=60000&_foreign_keys=off", dbPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	schema := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
            mid TEXT PRIMARY KEY,
            job_intro TEXT,
            structured_json TEXT,
            source TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT '待处理',
            category TEXT,
            updated_at TEXT NOT NULL,
            structured_summary TEXT,
            structured_responsibilities TEXT,
            structured_skills TEXT,
            structured_industry TEXT,
            structured_locations TEXT,
            structured_category_hints TEXT,
            structured_updated_at TEXT
        );`,
	}

	for _, stmt := range schema {
		if _, err := db.Exec(stmt); err != nil {
			return nil, err
		}
	}

	ensureCols := []string{
		"structured_json",
		"structured_summary",
		"structured_responsibilities",
		"structured_skills",
		"structured_industry",
		"structured_locations",
		"structured_category_hints",
		"structured_updated_at",
	}

	for _, col := range ensureCols {
		alter := fmt.Sprintf("ALTER TABLE jobs ADD COLUMN %s TEXT", col)
		if _, err := db.Exec(alter); err != nil {
			if !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
				return nil, err
			}
		}
	}

	return db, nil
}

type sqlJobRow struct {
	mid      string
	intro    string
	source   string
	category sql.NullString
	status   string
}

func fetchPendingJobs(ctx context.Context, db *sql.DB, limit int) ([]jobRecord, error) {
	baseQuery := `SELECT mid, job_intro, source, IFNULL(category, ''), status
FROM jobs
WHERE job_intro IS NOT NULL AND job_intro != ''
  AND (structured_summary IS NULL OR structured_summary = '')
ORDER BY updated_at`

	var rows *sql.Rows
	var err error
	if limit > 0 {
		baseQuery += " LIMIT ?"
		rows, err = db.QueryContext(ctx, baseQuery, limit)
	} else {
		rows, err = db.QueryContext(ctx, baseQuery)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []jobRecord
	for rows.Next() {
		var row sqlJobRow
		if err := rows.Scan(&row.mid, &row.intro, &row.source, &row.category, &row.status); err != nil {
			return nil, err
		}
		jobs = append(jobs, jobRecord{
			MID:      row.mid,
			Intro:    row.intro,
			Source:   row.source,
			Category: row.category.String,
			Status:   row.status,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return jobs, nil
}

func handleJob(ctx context.Context, db *sql.DB, structSvc *StructuringService, task jobTask, successCount, failureCount *atomic.Int64) {
	job := task.Job
	queryLog := map[string]interface{}{
		"index": task.Index,
		"total": task.Total,
		"job":   job,
	}
	log.Printf("[QUERY] %s", prettyJSON(queryLog))

	payload, rawJSON, err := structSvc.BuildStructured(ctx, job)
	if err != nil {
		log.Printf("[ERROR] 调用模型失败 mid=%s: %v", job.MID, err)
		failureCount.Add(1)
		logJobStatus(job.MID, "failed", successCount.Load(), failureCount.Load(), task.Total, err.Error())
		return
	}
	if payload == nil {
		log.Printf("[WARN] 模型未返回结构化结果 mid=%s", job.MID)
		failureCount.Add(1)
		logJobStatus(job.MID, "failed", successCount.Load(), failureCount.Load(), task.Total, "empty payload")
		return
	}

	if err := updateJob(ctx, db, job.MID, rawJSON, payload); err != nil {
		log.Printf("[ERROR] 更新数据库失败 mid=%s: %v", job.MID, err)
		failureCount.Add(1)
		logJobStatus(job.MID, "failed", successCount.Load(), failureCount.Load(), task.Total, err.Error())
		return
	}

	successCount.Add(1)
	storeLog := map[string]interface{}{
		"mid":              job.MID,
		"stored_at":        time.Now().UTC().Format(time.RFC3339),
		"summary":          payload.Summary,
		"structured_raw":   parseRawJSON(rawJSON),
		"responsibilities": rawToInterface(payload.Responsibilities),
		"skills":           rawToInterface(payload.Skills),
		"industry":         payload.Industry,
		"locations":        rawToInterface(payload.Locations),
		"category_hints":   rawToInterface(payload.CategoryHints),
	}
	log.Printf("[STORE] %s", prettyJSON(storeLog))
	logJobStatus(job.MID, "success", successCount.Load(), failureCount.Load(), task.Total, "")
}

func (s *StructuringService) BuildStructured(ctx context.Context, job jobRecord) (*structuredPayload, string, error) {
	text := strings.TrimSpace(job.Intro)
	if text == "" {
		return nil, "", nil
	}

	categoryInfo := job.Category
	if categoryInfo == "" {
		categoryInfo = "未分类"
	}

	prompt := fmt.Sprintf(`请阅读以下岗位信息，并生成结构化 JSON。要求：
1. 仅输出 JSON，不要添加额外说明。
2. JSON 字段：岗位概述(一句话)、核心职责(数组)、技能标签(数组)、行业(字符串，可为空)、工作地点(数组)、可能对应的大类(数组，元素包含名称与信心0-1之间的小数)。
3. 若文本中找不到对应信息，请使用空字符串或空数组。
岗位ID: %s
来源: %s
已有分类: %s
岗位描述:
%s`, job.MID, job.Source, categoryInfo, text)
	log.Printf("[MODEL-REQ] %s", prettyJSON(map[string]interface{}{
		"mid":    job.MID,
		"model":  structModel,
		"prompt": prompt,
	}))

	reqBody := chatRequest{
		Model: structModel,
		Messages: []chatMessage{
			{Role: "system", Content: "你是一名岗位结构化助手，只能输出 JSON。"},
			{Role: "user", Content: prompt},
		},
		Stream: false,
	}

	buf, err := json.Marshal(reqBody)
	if err != nil {
		return nil, "", err
	}

	ctxReq, cancel := context.WithTimeout(ctx, structReqTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctxReq, http.MethodPost, ollamaChatURL, bytes.NewReader(buf))
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, "", fmt.Errorf("模型返回错误码 %d: %s", resp.StatusCode, string(body))
	}

	var chatResp chatResponse
	if err := json.NewDecoder(resp.Body).Decode(&chatResp); err != nil {
		return nil, "", fmt.Errorf("解析模型响应失败: %w", err)
	}

	content := strings.TrimSpace(chatResp.Message.Content)
	log.Printf("[MODEL-RESP] mid=%s raw=%s", job.MID, content)
	rawJSON, err := extractJSON(content)
	if err != nil {
		return nil, "", err
	}

	var payload structuredPayload
	if err := json.Unmarshal([]byte(rawJSON), &payload); err != nil {
		return nil, "", fmt.Errorf("解析结构化 JSON 失败: %w", err)
	}

	normalized, err := normalizeJSON(rawJSON)
	if err != nil {
		return nil, "", err
	}

	log.Printf("[MODEL-PAYLOAD] %s", prettyJSON(map[string]interface{}{
		"mid":                 job.MID,
		"summary":             payload.Summary,
		"responsibilities":    rawToInterface(payload.Responsibilities),
		"skills":              rawToInterface(payload.Skills),
		"industry":            payload.Industry,
		"locations":           rawToInterface(payload.Locations),
		"category_hints":      rawToInterface(payload.CategoryHints),
		"normalized_response": parseRawJSON(normalized),
	}))

	return &payload, normalized, nil
}

func updateJob(ctx context.Context, db *sql.DB, mid string, rawJSON string, payload *structuredPayload) error {
	now := time.Now().UTC().Format(time.RFC3339)
	resps := rawToJSONString(payload.Responsibilities, "[]")
	skills := rawToJSONString(payload.Skills, "[]")
	locations := rawToJSONString(payload.Locations, "[]")
	categories := rawToJSONString(payload.CategoryHints, "[]")

	_, err := db.ExecContext(ctx, `UPDATE jobs
        SET structured_json = ?,
            structured_summary = ?,
            structured_responsibilities = ?,
            structured_skills = ?,
            structured_industry = ?,
            structured_locations = ?,
            structured_category_hints = ?,
            structured_updated_at = ?
        WHERE mid = ?`,
		nullIfEmpty(rawJSON),
		payload.Summary,
		resps,
		skills,
		payload.Industry,
		locations,
		categories,
		now,
		mid,
	)

	return err
}

func extractJSON(text string) (string, error) {
	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start == -1 || end == -1 || end <= start {
		return "", errors.New("未找到 JSON 内容")
	}
	candidate := strings.TrimSpace(text[start : end+1])
	if !json.Valid([]byte(candidate)) {
		return "", errors.New("返回内容不是合法 JSON")
	}
	return candidate, nil
}

func normalizeJSON(raw string) (string, error) {
	var v interface{}
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return "", err
	}
	buf, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func nullIfEmpty(s string) interface{} {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func rawToJSONString(raw json.RawMessage, defaultVal string) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return defaultVal
	}
	if !json.Valid(trimmed) {
		return defaultVal
	}
	return string(trimmed)
}

func rawToInterface(raw json.RawMessage) interface{} {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil
	}
	var v interface{}
	if err := json.Unmarshal(trimmed, &v); err != nil {
		return string(trimmed)
	}
	return v
}

func prettyJSON(v interface{}) string {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("%+v", v)
	}
	return string(data)
}

func parseRawJSON(raw string) interface{} {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var v interface{}
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return raw
	}
	return v
}

func logJobStatus(mid, state string, success, failed int64, total int, extra string) {
	if strings.TrimSpace(extra) != "" {
		log.Printf("[STATUS] mid=%s status=%s success=%d failed=%d total=%d msg=%s", mid, state, success, failed, total, extra)
		return
	}
	log.Printf("[STATUS] mid=%s status=%s success=%d failed=%d total=%d", mid, state, success, failed, total)
}
