package main

// 单文件整合程序：
// - 扫描 --input 指定的目录（或单个 CSV 文件）
// - 每个 CSV -> output/<csv_basename> 目录
//   - 导入到 SQLite: output/<csv_basename>/origin.db （表结构兼容 cmd/importer/query）
//   - 过滤岗位描述长度 < 5 的记录到 output/<csv_basename>/ignore.csv
//   - 运行与 cmd/query 相同的查询逻辑（向量 + 知识图谱 + ChromaDB）
//   - 输出分类结果到 output/<csv_basename>/result.csv
// - 在 data/<csv_basename>/count.txt 写入统计：原始、处理、过滤三项，保证 处理+过滤=原始
// - 导入与查询可并发（文件粒度并发；文件内查询保持与 cmd/query 相同的 worker 数，写 CSV 加锁）

import (
    "bufio"
    "bytes"
    "context"
    "crypto/sha256"
    "database/sql"
    "encoding/csv"
    "encoding/hex"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "log"
    "math"
    "net/http"
    "os"
    "path/filepath"
    "runtime"
    "sort"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    _ "github.com/mattn/go-sqlite3"
)

// ===== 与 cmd/query 保持一致的常量/配置 =====
const (
    ollamaAPIURL        = "http://localhost:11434/api/embeddings"
    embeddingModel      = "quentinz/bge-large-zh-v1.5"
    chromaDBURL         = "http://localhost:8000"
    collectionName      = "job_classification"
    graphNeighborLimit  = 5
    similarityThreshold = 0.55
)

// ===== CLI 选项 =====
type options struct {
    inputPath   string // 目录或单个 CSV
    fileWorkers int    // 同时处理多少个 CSV
    batchSize   int    // 导入时事务批量
    queryWorkers int   // 查询阶段的并发（默认与 cmd/query 的 10 一致）
    trace       bool   // 逐条记录阶段日志
    limitJobs   int    // 仅处理前 N 条（用于联调验证准确性）
}

func parseFlags() options {
    var opts options
    flag.StringVar(&opts.inputPath, "input", "data/51job", "输入目录或单个CSV（例如 data/51job 或 data/51job/51job_2021.csv）")
    defFW := runtime.NumCPU()
    if defFW < 2 { defFW = 2 }
    flag.IntVar(&opts.fileWorkers, "file-workers", defFW/2, "同时处理的 CSV 文件数量")
    flag.IntVar(&opts.batchSize, "batch", 2000, "导入 SQLite 的提交批量")
    flag.IntVar(&opts.queryWorkers, "query-workers", 10, "每个CSV在查询阶段的并发数")
    flag.BoolVar(&opts.trace, "trace", false, "开启逐条记录阶段日志（用于准确性排查）")
    flag.IntVar(&opts.limitJobs, "limit-jobs", 0, "仅处理前 N 条岗位（0 表示全部）")
    flag.Parse()
    if opts.fileWorkers < 1 { opts.fileWorkers = 1 }
    if opts.batchSize < 1 { opts.batchSize = 1 }
    if opts.queryWorkers < 1 { opts.queryWorkers = 1 }
    return opts
}

// ===== 导入阶段 =====
type importStats struct {
    Total    int64
    Imported int64
    Filtered int64
}

type csvRow struct {
    MID  string
    Intro string
}

func scanCSV(filePath string) (header []string, rows []csvRow, stats importStats, err error) {
    f, err := os.Open(filePath)
    if err != nil { return nil, nil, stats, err }
    defer f.Close()
    r := csv.NewReader(bufio.NewReader(f))
    r.ReuseRecord = true
    r.LazyQuotes = true
    r.FieldsPerRecord = -1

    recs, err := r.ReadAll()
    if err != nil { return nil, nil, stats, err }
    if len(recs) == 0 { return nil, nil, stats, nil }
    header = recs[0]
    for i := 1; i < len(recs); i++ {
        rec := recs[i]
        stats.Total++
        if len(rec) < 2 { // 不完整行计入过滤
            stats.Filtered++
            continue
        }
        mid := strings.TrimSpace(rec[0])
        intro := strings.TrimSpace(rec[1])
        if introRuneLen(intro) < 5 { // 过滤
            stats.Filtered++
            rows = append(rows, csvRow{MID: mid, Intro: intro}) // 仍返回，用于写 ignore.csv
            continue
        }
        stats.Imported++
        rows = append(rows, csvRow{MID: mid, Intro: intro})
    }
    return header, rows, stats, nil
}

func introRuneLen(s string) int { return len([]rune(strings.TrimSpace(s))) }

func ensureDB(dbPath string) (*sql.DB, error) {
    // dsn 参考 cmd/importer 的参数（WAL / NORMAL / 忙等待）
    dsn := fmt.Sprintf("file:%s?_cache_size=200000&_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=60000&_foreign_keys=off", dbPath)
    db, err := sql.Open("sqlite3", dsn)
    if err != nil { return nil, err }
    if err := db.Ping(); err != nil { db.Close(); return nil, err }
    schema := []string{
        `CREATE TABLE IF NOT EXISTS jobs (
            mid TEXT PRIMARY KEY,
            job_intro TEXT,
            source TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT '待处理',
            category TEXT,
            updated_at TEXT NOT NULL,
            structured_json TEXT,
            structured_summary TEXT,
            structured_responsibilities TEXT,
            structured_skills TEXT,
            structured_industry TEXT,
            structured_locations TEXT,
            structured_category_hints TEXT,
            structured_updated_at TEXT
        );`,
        `CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source);`,
        `CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);`,
        `CREATE INDEX IF NOT EXISTS idx_jobs_category ON jobs(category);`,
    }
    for _, stmt := range schema {
        if _, err := db.Exec(stmt); err != nil { db.Close(); return nil, err }
    }
    return db, nil
}

func importRowsToDB(ctx context.Context, db *sql.DB, rows []csvRow, source string, batchSize int, trace bool) error {
    if len(rows) == 0 { return nil }
    now := time.Now().UTC().Format(time.RFC3339)
    // 单 writer 事务批量写，避免并发写锁
    for i := 0; i < len(rows); i += batchSize {
        end := i + batchSize
        if end > len(rows) { end = len(rows) }
        tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
        if err != nil { return err }
        stmt, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO jobs(mid, job_intro, source, status, category, updated_at) VALUES (?, ?, ?, '待处理', '', ?)`)
        if err != nil { tx.Rollback(); return err }
        for idx, r := range rows[i:end] {
            // 跳过 intro < 5 的行（此前已剔除），这里仍容错
            if introRuneLen(r.Intro) < 5 { continue }
            if _, err := stmt.ExecContext(ctx, strings.TrimSpace(r.MID), r.Intro, source, now); err != nil {
                stmt.Close(); tx.Rollback(); return err
            }
            if trace {
                log.Printf("[DB-INSERT] mid=%s batch=%d item=%d", strings.TrimSpace(r.MID), i/batchSize+1, idx+1)
            }
        }
        stmt.Close()
        if err := tx.Commit(); err != nil { return err }
    }
    return nil
}

// ===== 与 cmd/query 等价的查询组件（尽量原样移植，仅对输出路径做最小变更） =====
type JobRecord struct {
    MID        string
    JobIntro   string
    Structured sql.NullString
    Source     string
    Status     string
    Category   string
}

type DatabaseService struct { db *sql.DB }

func NewDatabaseService(dbPath string) (*DatabaseService, error) {
    db, err := sql.Open("sqlite3", dbPath)
    if err != nil { return nil, fmt.Errorf("failed to open database: %w", err) }
    if err := db.Ping(); err != nil { db.Close(); return nil, fmt.Errorf("failed to connect to database: %w", err) }
    return &DatabaseService{db: db}, nil
}
func (s *DatabaseService) Close() error { return s.db.Close() }

// 与 cmd/query 的行规范化逻辑一致
type jobRow struct { rowID int64; mid, intro, src string }

func (s *DatabaseService) NormalizeJobIDs(ctx context.Context) (int, error) {
    rows, err := s.db.QueryContext(ctx, `SELECT rowid, mid, job_intro, source FROM jobs`)
    if err != nil { return 0, err }
    defer rows.Close()
    used := make(map[string]struct{})
    var invalid []jobRow
    for rows.Next() {
        var r jobRow
        if err := rows.Scan(&r.rowID, &r.mid, &r.intro, &r.src); err != nil { return 0, err }
        clean := strings.TrimSpace(r.mid)
        if isValidJobID(clean) { used[clean] = struct{}{}; continue }
        r.mid = clean
        invalid = append(invalid, r)
    }
    if err := rows.Err(); err != nil { return 0, err }
    if len(invalid) == 0 { return 0, nil }
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil { return 0, err }
    stmt, err := tx.PrepareContext(ctx, `UPDATE jobs SET mid = ? WHERE rowid = ?`)
    if err != nil { tx.Rollback(); return 0, err }
    defer stmt.Close()
    for _, row := range invalid {
        newID := generateJobID(row, used)
        if _, err := stmt.ExecContext(ctx, newID, row.rowID); err != nil { tx.Rollback(); return 0, err }
    }
    if err := tx.Commit(); err != nil { return 0, err }
    return len(invalid), nil
}

func isValidJobID(id string) bool {
    if len(id) != 32 { return false }
    for _, ch := range id {
        if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F') { continue }
        return false
    }
    return true
}
func generateJobID(row jobRow, used map[string]struct{}) string {
    base := fmt.Sprintf("%d|%s|%s|%s", row.rowID, row.mid, row.intro, row.src)
    for salt := 0; ; salt++ {
        data := base
        if salt > 0 { data = fmt.Sprintf("%s|%d", base, salt) }
        sum := sha256.Sum256([]byte(data))
        candidate := hex.EncodeToString(sum[:16])
        if _, exists := used[candidate]; exists { continue }
        used[candidate] = struct{}{}
        return candidate
    }
}

func (s *DatabaseService) GetAllJobs() ([]JobRecord, error) {
    rows, err := s.db.Query(`SELECT mid, job_intro, structured_json, source, status, category FROM jobs WHERE job_intro IS NOT NULL AND job_intro != '' ORDER BY mid`)
    if err != nil { return nil, err }
    defer rows.Close()
    var jobs []JobRecord
    for rows.Next() {
        var j JobRecord
        if err := rows.Scan(&j.MID, &j.JobIntro, &j.Structured, &j.Source, &j.Status, &j.Category); err != nil { return nil, err }
        j.MID = strings.TrimSpace(j.MID)
        jobs = append(jobs, j)
    }
    return jobs, rows.Err()
}

// Embedding
type EmbeddingService struct { client *http.Client }
type EmbeddingRequest struct {
    Model  string `json:"model"`
    Prompt string `json:"prompt"`
}
type EmbeddingResponse struct { Embedding []float32 `json:"embedding"` }

func NewEmbeddingService() *EmbeddingService {
    return &EmbeddingService{client: &http.Client{Timeout: 120 * time.Second}}
}
func (s *EmbeddingService) GetEmbedding(text string) ([]float32, error) {
    reqBody := EmbeddingRequest{Model: embeddingModel, Prompt: text}
    data, _ := json.Marshal(reqBody)
    resp, err := s.client.Post(ollamaAPIURL, "application/json", bytes.NewBuffer(data))
    if err != nil { return nil, fmt.Errorf("failed to call ollama API: %w", err) }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK { b, _ := io.ReadAll(resp.Body); return nil, fmt.Errorf("ollama API returned status %d: %s", resp.StatusCode, string(b)) }
    var out EmbeddingResponse
    if err := json.NewDecoder(resp.Body).Decode(&out); err != nil { return nil, fmt.Errorf("failed to decode response: %w", err) }
    return out.Embedding, nil
}

// Graph repo
type GraphRepository struct { db *sql.DB }
type graphNeighbor struct { ID string; Score float64 }
func NewGraphRepository(dbPath string) (*GraphRepository, error) {
    db, err := sql.Open("sqlite3", dbPath)
    if err != nil { return nil, err }
    if err := db.Ping(); err != nil { db.Close(); return nil, err }
    return &GraphRepository{db: db}, nil
}
func (r *GraphRepository) Close() error { if r == nil || r.db == nil { return nil }; return r.db.Close() }
func (r *GraphRepository) GetNeighbors(jobID string, limit int) ([]graphNeighbor, error) {
    rows, err := r.db.Query(`SELECT neighbor_id, score FROM job_neighbors WHERE job_id = ? ORDER BY score DESC LIMIT ?`, jobID, limit)
    if err != nil { return nil, err }
    defer rows.Close()
    var out []graphNeighbor
    for rows.Next() { var n graphNeighbor; if err := rows.Scan(&n.ID, &n.Score); err != nil { return nil, err }; out = append(out, n) }
    return out, rows.Err()
}

// Chroma repo（集合 ID 动态获取，与 cmd/query 一致）
type ChromaRepository struct { httpClient *http.Client; collectionID string }
func NewChromaRepository(ctx context.Context) (*ChromaRepository, error) {
    r := &ChromaRepository{httpClient: &http.Client{Timeout: 30 * time.Second}}
    if err := r.loadCollectionID(ctx); err != nil { return nil, err }
    return r, nil
}
func (r *ChromaRepository) loadCollectionID(ctx context.Context) error {
    url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s", chromaDBURL, collectionName)
    req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
    resp, err := r.httpClient.Do(req)
    if err != nil { return fmt.Errorf("查询集合 %s 失败: %w", collectionName, err) }
    defer resp.Body.Close()
    if resp.StatusCode == http.StatusNotFound { return fmt.Errorf("未在 ChromaDB 中找到集合 %s，请先执行 job-import", collectionName) }
    if resp.StatusCode != http.StatusOK { b, _ := io.ReadAll(resp.Body); return fmt.Errorf("获取集合 %s 失败，状态码 %d: %s", collectionName, resp.StatusCode, string(b)) }
    var info struct{ ID string `json:"id"` }
    if err := json.NewDecoder(resp.Body).Decode(&info); err != nil { return fmt.Errorf("解析集合响应失败: %w", err) }
    if info.ID == "" { return fmt.Errorf("集合 %s 的 ID 为空", collectionName) }
    r.collectionID = info.ID
    return nil
}

type ChromaQueryRequest struct { QueryEmbeddings [][]float32 `json:"query_embeddings"`; NResults int `json:"n_results"`; Include []string `json:"include"` }
type ChromaQueryResponse struct {
    IDs       [][]string                 `json:"ids"`
    Distances [][]float64                `json:"distances"`
    Metadatas [][]map[string]interface{} `json:"metadatas"`
    Documents [][]string                 `json:"documents"`
}
func (r *ChromaRepository) Query(ctx context.Context, embedding []float32) (*ChromaQueryResponse, error) {
    url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s/query", chromaDBURL, r.collectionID)
    reqBody := ChromaQueryRequest{QueryEmbeddings: [][]float32{embedding}, NResults: 2000, Include: []string{"documents", "metadatas", "distances"}}
    data, _ := json.Marshal(reqBody)
    req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
    req.Header.Set("Content-Type", "application/json")
    resp, err := r.httpClient.Do(req)
    if err != nil { return nil, fmt.Errorf("failed to query chroma: %w", err) }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK { b, _ := io.ReadAll(resp.Body); return nil, fmt.Errorf("chroma API returned status %d: %s", resp.StatusCode, string(b)) }
    var out ChromaQueryResponse
    if err := json.NewDecoder(resp.Body).Decode(&out); err != nil { return nil, fmt.Errorf("failed to decode response: %w", err) }
    return &out, nil
}

// CSV 写入（对单文件结果加锁，避免并发问题）
type CSVWriter struct { mu sync.Mutex; file *os.File; w *csv.Writer }
func NewCSVWriterTo(path string) (*CSVWriter, error) {
    if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil { return nil, err }
    f, err := os.Create(path)
    if err != nil { return nil, err }
    w := csv.NewWriter(f)
    headers := []string{"job_id","job_intro","similarity_score","大类","大类含义","中类","中类含义","小类","小类含义","细类职业","细类含义","细类主要工作任务"}
    if err := w.Write(headers); err != nil { f.Close(); return nil, err }
    return &CSVWriter{file: f, w: w}, nil
}
func (c *CSVWriter) WriteRow(job JobRecord, similarity float64, meta map[string]interface{}) error {
    row := []string{
        cleanCell(job.MID),
        truncate(job.JobIntro, 500),
        fmt.Sprintf("%.4f", similarity),
        safeString(meta, "大类"),
        safeString(meta, "大类含义"),
        safeString(meta, "中类"),
        safeString(meta, "中类含义"),
        safeString(meta, "小类"),
        safeString(meta, "小类含义"),
        safeString(meta, "细类职业"),
        safeString(meta, "细类含义"),
        truncate(safeString(meta, "细类主要工作任务"), 500),
    }
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.w.Write(row)
}
func (c *CSVWriter) Flush() error { c.mu.Lock(); defer c.mu.Unlock(); c.w.Flush(); return c.w.Error() }
func (c *CSVWriter) Close() error { _ = c.Flush(); return c.file.Close() }

// 与 cmd/query 的格式化/工具函数一致
type structuredPayload struct {
    Summary string `json:"岗位概述"`
    Responsibilities []string `json:"核心职责"`
    Skills []string `json:"技能标签"`
    Industry string `json:"行业"`
    Locations []string `json:"工作地点"`
    CategoryHints []struct{ Name string `json:"名称"`; Confidence float64 `json:"信心"` } `json:"可能对应的大类"`
}
func formatStructuredText(raw string) string {
    if strings.TrimSpace(raw) == "" { return "" }
    var p structuredPayload
    if err := json.Unmarshal([]byte(raw), &p); err != nil { return raw }
    var sec []string
    if p.Summary != "" { sec = append(sec, fmt.Sprintf("岗位概述: %s", p.Summary)) }
    if len(p.Responsibilities) > 0 { sec = append(sec, fmt.Sprintf("核心职责: %s", strings.Join(p.Responsibilities, "；"))) }
    if len(p.Skills) > 0 { sec = append(sec, fmt.Sprintf("技能标签: %s", strings.Join(p.Skills, "；"))) }
    if p.Industry != "" { sec = append(sec, fmt.Sprintf("行业: %s", p.Industry)) }
    if len(p.Locations) > 0 { sec = append(sec, fmt.Sprintf("工作地点: %s", strings.Join(p.Locations, "；"))) }
    if len(p.CategoryHints) > 0 {
        var hints []string
        for _, h := range p.CategoryHints { if h.Name != "" { hints = append(hints, fmt.Sprintf("%s(信心%.2f)", h.Name, h.Confidence)) } }
        if len(hints) > 0 { sec = append(sec, fmt.Sprintf("大类猜测: %s", strings.Join(hints, "；"))) }
    }
    return strings.Join(sec, "\n")
}
func cleanCell(s string) string { s = strings.ReplaceAll(s, "\r\n", " "); s = strings.ReplaceAll(s, "\n", " "); s = strings.ReplaceAll(s, "\r", " "); s = strings.ReplaceAll(s, "\t", " "); return strings.TrimSpace(s) }
func truncate(s string, max int) string { s = cleanCell(s); r := []rune(s); if len(r) <= max { return s }; return string(r[:max]) + "..." }
func safeString(meta map[string]interface{}, key string) string { if val, ok := meta[key]; ok && val != nil { if str, ok := val.(string); ok { str = strings.ReplaceAll(str, "\r\n", " "); str = strings.ReplaceAll(str, "\n", " "); str = strings.ReplaceAll(str, "\r", " "); str = strings.ReplaceAll(str, "\t", " "); str = strings.ReplaceAll(str, "\"", "\"\""); return str } }; return "" }
func normalizeEmbedding(vec []float32) ([]float32, error) { var sum float64; for _, v := range vec { fv := float64(v); sum += fv*fv }; norm := math.Sqrt(sum); if norm == 0 { return nil, fmt.Errorf("embedding norm is zero") }; out := make([]float32, len(vec)); inv := float32(1/norm); for i, v := range vec { out[i] = v*inv }; return out, nil }
func cosineDistanceToScore(distance float64) float64 { score := 1 - distance; if score < 0 { return 0 }; if score > 1 { return 1 }; return score }

// 查询处理器（核心逻辑保持一致）
type QueryProcessor struct {
    dbService    *DatabaseService
    embeddingSvc *EmbeddingService
    chromaRepo   *ChromaRepository
    graphRepo    *GraphRepository
    csvWriter    *CSVWriter
    workerCount  int
    trace        bool
    limitJobs    int
}
type jobResult struct { job JobRecord; similarity float64; metadata map[string]interface{}; matched bool; err error }
type classificationMatch struct { Metadata map[string]interface{}; Similarity float64 }

func buildMatchIndex(resp *ChromaQueryResponse) map[string]classificationMatch {
    index := make(map[string]classificationMatch)
    if resp == nil || len(resp.Metadatas) == 0 || len(resp.Distances) == 0 { return index }
    metaList := resp.Metadatas[0]
    distList := resp.Distances[0]
    for i := 0; i < len(metaList) && i < len(distList); i++ {
        meta := metaList[i]
        jobID := safeString(meta, "细类职业")
        if jobID == "" { continue }
        similarity := cosineDistanceToScore(distList[i])
        index[jobID] = classificationMatch{Metadata: meta, Similarity: similarity}
    }
    return index
}

func (p *QueryProcessor) applyGraphCorrection(bestJobID string, currentMeta map[string]interface{}, currentSimilarity float64, matches map[string]classificationMatch) (map[string]interface{}, float64, bool) {
    if p.graphRepo == nil || bestJobID == "" || len(matches) == 0 { return currentMeta, currentSimilarity, false }
    neighbors, err := p.graphRepo.GetNeighbors(bestJobID, graphNeighborLimit)
    if err != nil || len(neighbors) == 0 { return currentMeta, currentSimilarity, false }
    bestMeta := currentMeta; bestSim := currentSimilarity; changed := false; bestID := bestJobID
    for _, n := range neighbors {
        if m, ok := matches[n.ID]; ok { if m.Similarity > bestSim { bestSim = m.Similarity; bestMeta = m.Metadata; bestID = n.ID; changed = true } }
    }
    if changed { log.Printf("Graph correction applied: %s -> %s (%.4f -> %.4f)", bestJobID, bestID, currentSimilarity, bestSim) }
    return bestMeta, bestSim, changed
}

func NewQueryProcessor(ctx context.Context, dbPath, resultPath, graphDBPath string, workerCount int, trace bool, limit int) (*QueryProcessor, error) {
    dbService, err := NewDatabaseService(dbPath)
    if err != nil { return nil, err }
    if fixed, err := dbService.NormalizeJobIDs(ctx); err != nil { dbService.Close(); return nil, fmt.Errorf("failed to normalize job IDs: %w", err) } else if fixed > 0 { log.Printf("Normalized %d invalid job IDs", fixed) }
    csvWriter, err := NewCSVWriterTo(resultPath)
    if err != nil { dbService.Close(); return nil, err }
    chromaRepo, err := NewChromaRepository(ctx)
    if err != nil { csvWriter.Close(); dbService.Close(); return nil, err }
    graphRepo, err := NewGraphRepository(graphDBPath)
    if err != nil { csvWriter.Close(); dbService.Close(); return nil, err }
    return &QueryProcessor{dbService: dbService, embeddingSvc: NewEmbeddingService(), chromaRepo: chromaRepo, graphRepo: graphRepo, csvWriter: csvWriter, workerCount: workerCount, trace: trace, limitJobs: limit}, nil
}
func (p *QueryProcessor) Close() { p.dbService.Close(); if p.graphRepo != nil { p.graphRepo.Close() }; p.csvWriter.Close() }

func (p *QueryProcessor) Process(ctx context.Context) error {
    jobs, err := p.dbService.GetAllJobs()
    if err != nil { return err }
    if p.limitJobs > 0 && p.limitJobs < len(jobs) {
        jobs = jobs[:p.limitJobs]
    }
    total := len(jobs)
    log.Printf("Found %d jobs to process", total)
    jobsCh := make(chan JobRecord)
    resultsCh := make(chan jobResult)
    var wg sync.WaitGroup
    for i := 0; i < p.workerCount; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for job := range jobsCh { resultsCh <- p.processSingleJob(ctx, job) }
        }()
    }
    go func() { for _, j := range jobs { jobsCh <- j }; close(jobsCh); wg.Wait(); close(resultsCh) }()
    processed, matched := 0, 0
    start := time.Now()
    for res := range resultsCh {
        processed++
        if processed == 1 || processed%50 == 0 { log.Printf("Progress: %d/%d (%.1f%%)", processed, total, float64(processed)/float64(total)*100) }
        if res.err != nil { log.Println(res.err); continue }
        if res.matched {
            matched++
            if err := p.csvWriter.WriteRow(res.job, res.similarity, res.metadata); err != nil { log.Printf("Warning: write csv row failed: %v", err) }
            if matched%100 == 0 { if err := p.csvWriter.Flush(); err != nil { log.Printf("Warning: csv flush failed: %v", err) } else { log.Printf("Auto-saved CSV after %d matched records", matched) } }
        }
    }
    log.Printf("Query completed! total=%d processed=%d matched(>=%.2f)=%d elapsed=%.2fs", total, processed, similarityThreshold, matched, time.Since(start).Seconds())
    return nil
}

func (p *QueryProcessor) processSingleJob(ctx context.Context, job JobRecord) jobResult {
    structuredText := ""
    if job.Structured.Valid { structuredText = formatStructuredText(job.Structured.String) }
    var desc []string
    if structuredText != "" { desc = append(desc, structuredText) }
    desc = append(desc, fmt.Sprintf("岗位描述: %s", job.JobIntro))
    desc = append(desc, fmt.Sprintf("来源: %s", job.Source))
    desc = append(desc, fmt.Sprintf("状态: %s", job.Status))
    desc = append(desc, fmt.Sprintf("分类: %s", job.Category))
    full := strings.Join(desc, "\n")
    if p.trace {
        log.Printf("[EMB-REQ] mid=%s text_len=%d", job.MID, len([]rune(full)))
    }

    embedding, err := p.embeddingSvc.GetEmbedding(full)
    if err != nil { return jobResult{job: job, err: fmt.Errorf("Warning: Failed to get embedding for job %s: %v", job.MID, err)} }
    if p.trace { log.Printf("[EMB-OK] mid=%s dim=%d", job.MID, len(embedding)) }
    embedding, err = normalizeEmbedding(embedding)
    if err != nil { return jobResult{job: job, err: fmt.Errorf("Warning: Failed to normalize embedding for job %s: %v", job.MID, err)} }
    if p.trace { log.Printf("[CHROMA-REQ] mid=%s n_results=2000", job.MID) }
    qresp, err := p.chromaRepo.Query(ctx, embedding)
    if err != nil { return jobResult{job: job, err: fmt.Errorf("Warning: Failed to query ChromaDB for job %s: %v", job.MID, err)} }
    if len(qresp.Distances) == 0 || len(qresp.Distances[0]) == 0 { return jobResult{job: job} }
    // 取最优候选
    bestSim := 0.0; bestIdx := 0; bestDist := 0.0
    for j := 0; j < len(qresp.Distances[0]); j++ { d := qresp.Distances[0][j]; s := cosineDistanceToScore(d); if s > bestSim { bestSim, bestIdx, bestDist = s, j, d } }
    if p.trace { log.Printf("[CHROMA-BEST] mid=%s similarity=%.4f threshold=%.2f", job.MID, bestSim, similarityThreshold) }
    if bestSim < similarityThreshold { return jobResult{job: job} }
    var meta map[string]interface{}
    if len(qresp.Metadatas) > 0 && len(qresp.Metadatas[0]) > bestIdx { meta = qresp.Metadatas[0][bestIdx] }
    // 图纠偏（与 cmd/query 一致）
    if meta != nil {
        bestID := safeString(meta, "细类职业")
        if idx := buildMatchIndex(qresp); len(idx) > 0 {
            if m2, s2, changed := p.applyGraphCorrection(bestID, meta, bestSim, idx); changed {
                if p.trace { log.Printf("[GRAPH] mid=%s corrected best to similarity=%.4f", job.MID, s2) }
                meta, bestSim = m2, s2
                _ = bestDist
            } else if p.trace {
                log.Printf("[GRAPH] mid=%s no change", job.MID)
            }
        }
    }
    if p.trace { log.Printf("[CSV-WRITE] mid=%s final_similarity=%.4f job_id=%s", job.MID, bestSim, safeString(meta, "细类职业")) }
    return jobResult{job: job, similarity: bestSim, metadata: meta, matched: true}
}

// ===== 主流程 =====
type fileTask struct {
    csvPath    string
    baseName   string
    source     string
    outDir     string
    resultPath string
    ignorePath string
    countPath  string      // legacy path: data/<base>/count.txt
    countPath2 string      // new path:   output/<base>/count.txt
    dbPath     string
}

func discoverCSV(inputPath string) ([]string, error) {
    fi, err := os.Stat(inputPath)
    if err != nil { return nil, err }
    if fi.IsDir() {
        var files []string
        entries, err := os.ReadDir(inputPath)
        if err != nil { return nil, err }
        for _, e := range entries { if !e.IsDir() && strings.HasSuffix(strings.ToLower(e.Name()), ".csv") { files = append(files, filepath.Join(inputPath, e.Name())) } }
        sort.Strings(files)
        return files, nil
    }
    // 单文件
    if strings.HasSuffix(strings.ToLower(inputPath), ".csv") { return []string{inputPath}, nil }
    return nil, fmt.Errorf("input is neither a directory nor a CSV file: %s", inputPath)
}

func buildTasks(csvFiles []string) ([]fileTask, error) {
    var tasks []fileTask
    for _, p := range csvFiles {
        base := strings.TrimSuffix(filepath.Base(p), filepath.Ext(p))
        outDir := filepath.Join("output", base)
        // 如果输出目录已存在，则清空后重建，确保“重新匹配和处理”
        if fi, err := os.Stat(outDir); err == nil && fi.IsDir() {
            log.Printf("[CLEAN] remove existing output folder: %s", outDir)
            if err := os.RemoveAll(outDir); err != nil {
                return nil, fmt.Errorf("failed to clean output folder %s: %w", outDir, err)
            }
        }
        if err := os.MkdirAll(outDir, 0o755); err != nil { return nil, err }
        src := filepath.Base(filepath.Dir(p)) // 上一级目录名作为 source，例如 51job
        resultPath := filepath.Join(outDir, "result.csv")
        ignorePath := filepath.Join(outDir, "ignore.csv")
        dataDir := filepath.Join("data", base)
        if err := os.MkdirAll(dataDir, 0o755); err != nil { return nil, err }
        countPath := filepath.Join(dataDir, "count.txt")
        countPath2 := filepath.Join(outDir, "count.txt")
        dbPath := filepath.Join(outDir, "origin.db")
        tasks = append(tasks, fileTask{csvPath: p, baseName: base, source: src, outDir: outDir, resultPath: resultPath, ignorePath: ignorePath, countPath: countPath, countPath2: countPath2, dbPath: dbPath})
    }
    return tasks, nil
}

func writeIgnoreCSV(path string, header []string, rows []csvRow) error {
    f, err := os.Create(path)
    if err != nil { return err }
    defer f.Close()
    w := csv.NewWriter(f)
    // 强制使用两列标题
    if err := w.Write([]string{"mid", "job_intro"}); err != nil { return err }
    for _, r := range rows { if introRuneLen(r.Intro) < 5 { if err := w.Write([]string{r.MID, r.Intro}); err != nil { return err } } }
    w.Flush(); return w.Error()
}

func writeCountTXT(path string, st importStats) error {
    // 处理+过滤==原始
    ok := st.Imported+st.Filtered == st.Total
    content := fmt.Sprintf("total=%d\nprocessed=%d\nfiltered=%d\nok=%v\n", st.Total, st.Imported, st.Filtered, ok)
    return os.WriteFile(path, []byte(content), 0o644)
}

func processSingleCSV(ctx context.Context, t fileTask, opts options) error {
    log.Printf("[START] %s -> %s", t.csvPath, t.outDir)
    // 读取 & 过滤
    header, allRows, stats, err := scanCSV(t.csvPath)
    if err != nil { return fmt.Errorf("read csv failed: %w", err) }
    // 拆分：过滤行 & 需导入行
    var importRows []csvRow
    for idx, r := range allRows {
        if introRuneLen(r.Intro) >= 5 {
            importRows = append(importRows, r)
        } else if opts.trace {
            log.Printf("[FILTER] line=%d mid=%s intro_len=%d(<5)", idx+2, r.MID, introRuneLen(r.Intro))
        }
    }
    // ignore.csv
    if err := writeIgnoreCSV(t.ignorePath, header, allRows); err != nil { return fmt.Errorf("write ignore.csv failed: %w", err) }
    log.Printf("[IMPORT] total=%d to_import=%d filtered(<5)=%d", stats.Total, len(importRows), stats.Filtered)
    // 建库 & 导入
    db, err := ensureDB(t.dbPath)
    if err != nil { return fmt.Errorf("open sqlite failed: %w", err) }
    if err := importRowsToDB(ctx, db, importRows, t.source, opts.batchSize, opts.trace); err != nil { db.Close(); return fmt.Errorf("import to sqlite failed: %w", err) }
    db.Close()
    // 查询
    qp, err := NewQueryProcessor(ctx, t.dbPath, t.resultPath, "data/job_graph.db", opts.queryWorkers, opts.trace, opts.limitJobs)
    if err != nil { return fmt.Errorf("init query processor failed: %w", err) }
    if err := qp.Process(ctx); err != nil { qp.Close(); return fmt.Errorf("query process failed: %w", err) }
    qp.Close()
    // 统计（写两份：data/<base>/count.txt 与 output/<base>/count.txt）
    if err := writeCountTXT(t.countPath, stats); err != nil { return fmt.Errorf("write count.txt failed: %w", err) }
    if err := writeCountTXT(t.countPath2, stats); err != nil { return fmt.Errorf("write count.txt (output) failed: %w", err) }
    log.Printf("[DONE] %s -> result=%s origin.db=%s count=%s,%s", t.baseName, t.resultPath, t.dbPath, t.countPath, t.countPath2)
    _ = header // 保留变量以防将来扩展
    return nil
}

func main() {
    log.SetFlags(log.LstdFlags | log.Lmicroseconds)
    opts := parseFlags()
    ctx := context.Background()
    files, err := discoverCSV(opts.inputPath)
    if err != nil { log.Fatalf("discover csv failed: %v", err) }
    if len(files) == 0 { log.Println("no csv files found, exit"); return }
    tasks, err := buildTasks(files)
    if err != nil { log.Fatalf("build tasks failed: %v", err) }
    log.Printf("发现 %d 个 CSV，将并发处理（file-workers=%d）", len(tasks), opts.fileWorkers)

    // 并发处理不同 CSV
    type result struct { name string; err error }
    jobs := make(chan fileTask)
    results := make(chan result)
    var wg sync.WaitGroup
    for i := 0; i < opts.fileWorkers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for t := range jobs {
                if err := processSingleCSV(ctx, t, opts); err != nil { results <- result{name: t.baseName, err: err} } else { results <- result{name: t.baseName} }
            }
        }()
    }
    go func() { for _, t := range tasks { jobs <- t }; close(jobs); wg.Wait(); close(results) }()

    var fail atomic.Int32
    for res := range results {
        if res.err != nil { fail.Add(1); log.Printf("[ERROR] %s: %v", res.name, res.err) }
    }
    if fail.Load() > 0 { log.Fatalf("完成，存在 %d 个 CSV 失败", fail.Load()) }
    log.Printf("全部完成，共处理 %d 个 CSV。", len(tasks))
}
