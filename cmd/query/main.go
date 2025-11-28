package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	_ "github.com/mattn/go-sqlite3"
)

const (
	ollamaAPIURL        = "http://localhost:11434/api/embeddings"
	embeddingModel      = "quentinz/bge-large-zh-v1.5"
	chromaDBURL         = "http://localhost:8000"
	collectionName      = "job_classification"
	databasePath        = "data/jobs.db"
	graphDatabasePath   = "data/job_graph.db"
	similarityThreshold = 0.55
	graphNeighborLimit  = 5
)

type JobRecord struct {
	MID        string
	JobIntro   string
	Structured sql.NullString
	Source     string
	Status     string
	Category   string
}

type jobRow struct {
	rowID int64
	mid   string
	intro string
	src   string
}

func (j JobRecord) StructuredJSON() string {
	if j.Structured.Valid {
		return j.Structured.String
	}
	return ""
}

type EmbeddingService struct {
	client *http.Client
}

func NewEmbeddingService() *EmbeddingService {
	return &EmbeddingService{
		client: &http.Client{
			Timeout: 120 * time.Second,
		},
	}
}

type EmbeddingRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type EmbeddingResponse struct {
	Embedding []float32 `json:"embedding"`
}

func (s *EmbeddingService) GetEmbedding(text string) ([]float32, error) {
	reqBody := EmbeddingRequest{
		Model:  embeddingModel,
		Prompt: text,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := s.client.Post(ollamaAPIURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to call ollama API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ollama API returned status %d: %s", resp.StatusCode, string(body))
	}

	var embeddingResp EmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&embeddingResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return embeddingResp.Embedding, nil
}

type DatabaseService struct {
	db *sql.DB
}

func NewDatabaseService(dbPath string) (*DatabaseService, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	log.Printf("Successfully connected to SQLite database: %s", dbPath)
	return &DatabaseService{db: db}, nil
}

func (s *DatabaseService) Close() error {
	return s.db.Close()
}

func (s *DatabaseService) NormalizeJobIDs(ctx context.Context) (int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT rowid, mid, job_intro, source FROM jobs`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	used := make(map[string]struct{})
	var invalid []jobRow

	for rows.Next() {
		var r jobRow
		if err := rows.Scan(&r.rowID, &r.mid, &r.intro, &r.src); err != nil {
			return 0, err
		}
		clean := strings.TrimSpace(r.mid)
		if isValidJobID(clean) {
			used[clean] = struct{}{}
			continue
		}
		r.mid = clean
		invalid = append(invalid, r)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(invalid) == 0 {
		return 0, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	stmt, err := tx.PrepareContext(ctx, `UPDATE jobs SET mid = ? WHERE rowid = ?`)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	defer stmt.Close()

	for _, row := range invalid {
		newID := generateJobID(row, used)
		if _, err := stmt.ExecContext(ctx, newID, row.rowID); err != nil {
			tx.Rollback()
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(invalid), nil
}

func isValidJobID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, ch := range id {
		if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F') {
			continue
		}
		return false
	}
	return true
}

func generateJobID(row jobRow, used map[string]struct{}) string {
	base := fmt.Sprintf("%d|%s|%s|%s", row.rowID, row.mid, row.intro, row.src)
	for salt := 0; ; salt++ {
		data := base
		if salt > 0 {
			data = fmt.Sprintf("%s|%d", base, salt)
		}
		sum := sha256.Sum256([]byte(data))
		candidate := hex.EncodeToString(sum[:16])
		if _, exists := used[candidate]; exists {
			continue
		}
		used[candidate] = struct{}{}
		return candidate
	}
}

func (s *DatabaseService) GetAllJobs() ([]JobRecord, error) {
	query := `
		SELECT mid, job_intro, structured_json, source, status, category
		FROM jobs
		WHERE job_intro IS NOT NULL AND job_intro != ''
		ORDER BY mid
	`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query jobs: %w", err)
	}
	defer rows.Close()

	var jobs []JobRecord
	for rows.Next() {
		var job JobRecord
		err := rows.Scan(&job.MID, &job.JobIntro, &job.Structured, &job.Source, &job.Status, &job.Category)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		job.MID = strings.TrimSpace(job.MID)
		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
}

type GraphRepository struct {
	db *sql.DB
}

type graphNeighbor struct {
	ID    string
	Score float64
}

func NewGraphRepository(dbPath string) (*GraphRepository, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open graph database: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect graph database: %w", err)
	}
	log.Printf("Successfully connected to graph database: %s", dbPath)
	return &GraphRepository{db: db}, nil
}

func (r *GraphRepository) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

func (r *GraphRepository) GetNeighbors(jobID string, limit int) ([]graphNeighbor, error) {
	rows, err := r.db.Query(`
		SELECT neighbor_id, score
		FROM job_neighbors
		WHERE job_id = ?
		ORDER BY score DESC
		LIMIT ?
	`, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var neighbors []graphNeighbor
	for rows.Next() {
		var n graphNeighbor
		if err := rows.Scan(&n.ID, &n.Score); err != nil {
			return nil, err
		}
		neighbors = append(neighbors, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return neighbors, nil
}

type ChromaRepository struct {
	httpClient   *http.Client
	collectionID string
}

func NewChromaRepository(ctx context.Context) (*ChromaRepository, error) {
	repo := &ChromaRepository{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	if err := repo.loadCollectionID(ctx); err != nil {
		return nil, err
	}

	return repo, nil
}

func (r *ChromaRepository) loadCollectionID(ctx context.Context) error {
	collectionURL := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s", chromaDBURL, collectionName)
	req, err := http.NewRequestWithContext(ctx, "GET", collectionURL, nil)
	if err != nil {
		return fmt.Errorf("构造集合查询请求失败: %w", err)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("查询集合 %s 失败: %w", collectionName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("未在 ChromaDB 中找到集合 %s，请先执行 job-import", collectionName)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("获取集合 %s 失败，状态码 %d: %s", collectionName, resp.StatusCode, string(body))
	}

	var info struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return fmt.Errorf("解析集合响应失败: %w", err)
	}
	if info.ID == "" {
		return fmt.Errorf("集合 %s 的 ID 为空", collectionName)
	}

	r.collectionID = info.ID
	return nil
}

type ChromaQueryRequest struct {
	QueryEmbeddings [][]float32 `json:"query_embeddings"`
	NResults        int         `json:"n_results"`
	Include         []string    `json:"include"`
}

type ChromaQueryResponse struct {
	IDs       [][]string                 `json:"ids"`
	Distances [][]float64                `json:"distances"`
	Metadatas [][]map[string]interface{} `json:"metadatas"`
	Documents [][]string                 `json:"documents"`
}

func (r *ChromaRepository) Query(ctx context.Context, embedding []float32) (*ChromaQueryResponse, error) {
	queryURL := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s/query", chromaDBURL, r.collectionID)

	reqBody := ChromaQueryRequest{
		QueryEmbeddings: [][]float32{embedding},
		NResults:        2000, // Query top 2000 to match against all vectors
		Include:         []string{"documents", "metadatas", "distances"},
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query chroma: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("chroma API returned status %d: %s", resp.StatusCode, string(body))
	}

	var queryResp ChromaQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &queryResp, nil
}

func (r *ChromaRepository) GetCount(ctx context.Context) (int, error) {
	countURL := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s/count", chromaDBURL, r.collectionID)

	req, err := http.NewRequestWithContext(ctx, "GET", countURL, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to get count: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("chroma API returned status %d: %s", resp.StatusCode, string(body))
	}

	// ChromaDB returns just a number, not JSON
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read response: %w", err)
	}

	var count int
	if _, err := fmt.Sscanf(string(body), "%d", &count); err != nil {
		return 0, fmt.Errorf("failed to parse count: %w", err)
	}

	return count, nil
}

type CSVWriter struct {
	file   *os.File
	writer *csv.Writer
}

func NewCSVWriter() (*CSVWriter, error) {
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("result/%s_query_results.csv", timestamp)

	if err := os.MkdirAll("result", 0755); err != nil {
		return nil, fmt.Errorf("failed to create result directory: %w", err)
	}

	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV file: %w", err)
	}

	writer := csv.NewWriter(file)

	headers := []string{
		"job_id",
		"job_intro",
		"similarity_score",
		"大类",
		"大类含义",
		"中类",
		"中类含义",
		"小类",
		"小类含义",
		"细类职业",
		"细类含义",
		"细类主要工作任务",
	}

	if err := writer.Write(headers); err != nil {
		return nil, err
	}

	return &CSVWriter{
		file:   file,
		writer: writer,
	}, nil
}

func (w *CSVWriter) WriteRow(job JobRecord, similarity float64, metadata map[string]interface{}) error {
	row := []string{
		cleanCell(job.MID),
		truncate(job.JobIntro, 500),
		fmt.Sprintf("%.4f", similarity),
		safeString(metadata, "大类"),
		safeString(metadata, "大类含义"),
		safeString(metadata, "中类"),
		safeString(metadata, "中类含义"),
		safeString(metadata, "小类"),
		safeString(metadata, "小类含义"),
		safeString(metadata, "细类职业"),
		safeString(metadata, "细类含义"),
		truncate(safeString(metadata, "细类主要工作任务"), 500),
	}

	return w.writer.Write(row)
}

func (w *CSVWriter) Flush() error {
	w.writer.Flush()
	if err := w.writer.Error(); err != nil {
		return err
	}
	return nil
}

func (w *CSVWriter) Close() error {
	w.Flush()
	return w.file.Close()
}

func truncate(s string, maxLen int) string {
	s = cleanCell(s)
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "..."
}

func cleanCell(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	return strings.TrimSpace(s)
}

func safeString(metadata map[string]interface{}, key string) string {
	if val, ok := metadata[key]; ok && val != nil {
		if str, ok := val.(string); ok {
			// 清理换行符，替换为空格
			str = strings.ReplaceAll(str, "\r\n", " ")
			str = strings.ReplaceAll(str, "\n", " ")
			str = strings.ReplaceAll(str, "\r", " ")

			// 清理制表符
			str = strings.ReplaceAll(str, "\t", " ")

			// 转义双引号（CSV 格式要求：双引号内的双引号需要双写）
			str = strings.ReplaceAll(str, "\"", "\"\"")

			return str
		}
	}
	return ""
}

type structuredPayload struct {
	Summary          string         `json:"岗位概述"`
	Responsibilities []string       `json:"核心职责"`
	Skills           []string       `json:"技能标签"`
	Industry         string         `json:"行业"`
	Locations        []string       `json:"工作地点"`
	CategoryHints    []categoryHint `json:"可能对应的大类"`
}

type categoryHint struct {
	Name       string  `json:"名称"`
	Confidence float64 `json:"信心"`
}

func formatStructuredText(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	var payload structuredPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return raw
	}
	var sections []string
	if payload.Summary != "" {
		sections = append(sections, fmt.Sprintf("岗位概述: %s", payload.Summary))
	}
	if len(payload.Responsibilities) > 0 {
		sections = append(sections, fmt.Sprintf("核心职责: %s", strings.Join(payload.Responsibilities, "；")))
	}
	if len(payload.Skills) > 0 {
		sections = append(sections, fmt.Sprintf("技能标签: %s", strings.Join(payload.Skills, "；")))
	}
	if payload.Industry != "" {
		sections = append(sections, fmt.Sprintf("行业: %s", payload.Industry))
	}
	if len(payload.Locations) > 0 {
		sections = append(sections, fmt.Sprintf("工作地点: %s", strings.Join(payload.Locations, "；")))
	}
	if len(payload.CategoryHints) > 0 {
		var hints []string
		for _, hint := range payload.CategoryHints {
			if hint.Name == "" {
				continue
			}
			hints = append(hints, fmt.Sprintf("%s(信心%.2f)", hint.Name, hint.Confidence))
		}
		if len(hints) > 0 {
			sections = append(sections, fmt.Sprintf("大类猜测: %s", strings.Join(hints, "；")))
		}
	}
	return strings.Join(sections, "\n")
}

func normalizeEmbedding(vec []float32) ([]float32, error) {
	var sum float64
	for _, v := range vec {
		fv := float64(v)
		sum += fv * fv
	}

	norm := math.Sqrt(sum)
	if norm == 0 {
		return nil, fmt.Errorf("embedding norm is zero")
	}

	normalized := make([]float32, len(vec))
	inv := float32(1 / norm)
	for i, v := range vec {
		normalized[i] = v * inv
	}

	return normalized, nil
}

func cosineDistanceToScore(distance float64) float64 {
	// Chroma 在 cosine 度量下返回 distance=1-cos_sim，直接映射为可解释的 0-1 分数
	score := 1 - distance
	if score < 0 {
		return 0
	}
	if score > 1 {
		return 1
	}
	return score
}

type QueryProcessor struct {
	dbService    *DatabaseService
	embeddingSvc *EmbeddingService
	chromaRepo   *ChromaRepository
	graphRepo    *GraphRepository
	csvWriter    *CSVWriter
}

type jobResult struct {
	job        JobRecord
	similarity float64
	metadata   map[string]interface{}
	matched    bool
	err        error
}

type classificationMatch struct {
	Metadata   map[string]interface{}
	Similarity float64
}

func buildMatchIndex(resp *ChromaQueryResponse) map[string]classificationMatch {
	index := make(map[string]classificationMatch)
	if resp == nil || len(resp.Metadatas) == 0 || len(resp.Distances) == 0 {
		return index
	}
	metaList := resp.Metadatas[0]
	distList := resp.Distances[0]
	for i := 0; i < len(metaList) && i < len(distList); i++ {
		meta := metaList[i]
		jobID := safeString(meta, "细类职业")
		if jobID == "" {
			continue
		}
		similarity := cosineDistanceToScore(distList[i])
		index[jobID] = classificationMatch{
			Metadata:   meta,
			Similarity: similarity,
		}
	}
	return index
}

func (p *QueryProcessor) applyGraphCorrection(bestJobID string, currentMeta map[string]interface{}, currentSimilarity float64, matches map[string]classificationMatch) (map[string]interface{}, float64, bool) {
	if p.graphRepo == nil || bestJobID == "" || len(matches) == 0 {
		return currentMeta, currentSimilarity, false
	}
	neighbors, err := p.graphRepo.GetNeighbors(bestJobID, graphNeighborLimit)
	if err != nil {
		log.Printf("Warning: failed to read graph neighbors for %s: %v", bestJobID, err)
		return currentMeta, currentSimilarity, false
	}
	if len(neighbors) == 0 {
		return currentMeta, currentSimilarity, false
	}
	bestMeta := currentMeta
	bestSimilarity := currentSimilarity
	bestID := bestJobID
	changed := false
	for _, neighbor := range neighbors {
		match, ok := matches[neighbor.ID]
		if !ok {
			continue
		}
		if match.Similarity > bestSimilarity {
			bestSimilarity = match.Similarity
			bestMeta = match.Metadata
			bestID = neighbor.ID
			changed = true
		}
	}
	if changed {
		log.Printf("Graph correction applied: %s -> %s (%.4f -> %.4f)", bestJobID, bestID, currentSimilarity, bestSimilarity)
	}
	return bestMeta, bestSimilarity, changed
}

func NewQueryProcessor(ctx context.Context, dbPath string) (*QueryProcessor, error) {
	dbService, err := NewDatabaseService(dbPath)
	if err != nil {
		return nil, err
	}

	if fixed, err := dbService.NormalizeJobIDs(ctx); err != nil {
		dbService.Close()
		return nil, fmt.Errorf("failed to normalize job IDs: %w", err)
	} else if fixed > 0 {
		log.Printf("Normalized %d invalid job IDs", fixed)
	}

	csvWriter, err := NewCSVWriter()
	if err != nil {
		dbService.Close()
		return nil, err
	}

	chromaRepo, err := NewChromaRepository(ctx)
	if err != nil {
		csvWriter.Close()
		dbService.Close()
		return nil, err
	}

	graphRepo, err := NewGraphRepository(graphDatabasePath)
	if err != nil {
		csvWriter.Close()
		dbService.Close()
		return nil, err
	}

	return &QueryProcessor{
		dbService:    dbService,
		embeddingSvc: NewEmbeddingService(),
		chromaRepo:   chromaRepo,
		graphRepo:    graphRepo,
		csvWriter:    csvWriter,
	}, nil
}

func (p *QueryProcessor) Close() {
	p.dbService.Close()
	if p.graphRepo != nil {
		p.graphRepo.Close()
	}
	p.csvWriter.Close()
}

const workerCount = 10

func (p *QueryProcessor) Process(ctx context.Context) error {
	recordCount, err := p.chromaRepo.GetCount(ctx)
	if err != nil {
		log.Printf("Warning: Failed to get ChromaDB record count: %v", err)
	} else {
		log.Printf("ChromaDB collection has %d records", recordCount)
	}

	jobs, err := p.dbService.GetAllJobs()
	if err != nil {
		return err
	}

	totalJobs := len(jobs)
	log.Printf("Found %d jobs to process", totalJobs)

	jobsCh := make(chan JobRecord)
	resultsCh := make(chan jobResult)

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobsCh {
				resultsCh <- p.processSingleJob(ctx, job)
			}
		}()
	}

	go func() {
		for _, job := range jobs {
			jobsCh <- job
		}
		close(jobsCh)
		wg.Wait()
		close(resultsCh)
	}()

	processed := 0
	matched := 0
	startTime := time.Now()

	for res := range resultsCh {
		processed++
		if processed == 1 || processed%50 == 0 {
			log.Printf("Progress: %d/%d (%.1f%%)", processed, totalJobs, float64(processed)/float64(totalJobs)*100)
		}

		if res.err != nil {
			log.Println(res.err)
			continue
		}

		if res.matched {
			matched++
			if err := p.csvWriter.WriteRow(res.job, res.similarity, res.metadata); err != nil {
				log.Printf("Warning: Failed to write CSV row for job %s: %v", res.job.MID, err)
				continue
			}
			if matched%100 == 0 {
				if err := p.csvWriter.Flush(); err != nil {
					log.Printf("Warning: Failed to flush CSV writer: %v", err)
				} else {
					log.Printf("Auto-saved CSV after %d matched records", matched)
				}
			}
		}
	}

	elapsed := time.Since(startTime)
	log.Printf("\nQuery completed!")
	log.Printf("Total jobs: %d", totalJobs)
	log.Printf("Processed: %d", processed)
	log.Printf("Matched (similarity ≥ %.2f): %d", similarityThreshold, matched)
	log.Printf("Time elapsed: %.2f seconds", elapsed.Seconds())

	return nil
}

func (p *QueryProcessor) processSingleJob(ctx context.Context, job JobRecord) jobResult {
	structuredText := formatStructuredText(job.StructuredJSON())
	var descParts []string
	if structuredText != "" {
		descParts = append(descParts, structuredText)
	}
	descParts = append(descParts, fmt.Sprintf("岗位描述: %s", job.JobIntro))
	descParts = append(descParts, fmt.Sprintf("来源: %s", job.Source))
	descParts = append(descParts, fmt.Sprintf("状态: %s", job.Status))
	descParts = append(descParts, fmt.Sprintf("分类: %s", job.Category))
	fullDescription := strings.Join(descParts, "\n")

	embedding, err := p.embeddingSvc.GetEmbedding(fullDescription)
	if err != nil {
		return jobResult{job: job, err: fmt.Errorf("Warning: Failed to get embedding for job %s: %v", job.MID, err)}
	}

	embedding, err = normalizeEmbedding(embedding)
	if err != nil {
		return jobResult{job: job, err: fmt.Errorf("Warning: Failed to normalize embedding for job %s: %v", job.MID, err)}
	}

	queryResp, err := p.chromaRepo.Query(ctx, embedding)
	if err != nil {
		return jobResult{job: job, err: fmt.Errorf("Warning: Failed to query ChromaDB for job %s: %v", job.MID, err)}
	}

	if len(queryResp.Distances) == 0 || len(queryResp.Distances[0]) == 0 {
		preview := previewIntro(job.JobIntro)
		log.Printf("Job[%s]: no candidates returned", preview)
		return jobResult{job: job}
	}

	matchIndex := buildMatchIndex(queryResp)

	bestSimilarity := 0.0
	bestDistance := 0.0
	bestIndex := 0

	for j := 0; j < len(queryResp.Distances[0]); j++ {
		distance := queryResp.Distances[0][j]
		similarity := cosineDistanceToScore(distance)
		if similarity > bestSimilarity {
			bestSimilarity = similarity
			bestDistance = distance
			bestIndex = j
		}
	}

	preview := previewIntro(job.JobIntro)
	log.Printf("Job[%s]: distance=%.4f, similarity=%.4f (best among %d results)",
		preview, bestDistance, bestSimilarity, len(queryResp.Distances[0]))

	if bestSimilarity < similarityThreshold {
		return jobResult{job: job}
	}

	var metadata map[string]interface{}
	if len(queryResp.Metadatas) > 0 && len(queryResp.Metadatas[0]) > bestIndex {
		metadata = queryResp.Metadatas[0][bestIndex]
	}

	if metadata != nil {
		bestJobID := safeString(metadata, "细类职业")
		if updatedMeta, updatedSim, changed := p.applyGraphCorrection(bestJobID, metadata, bestSimilarity, matchIndex); changed {
			metadata = updatedMeta
			bestSimilarity = updatedSim
		}
	}

	return jobResult{
		job:        job,
		similarity: bestSimilarity,
		metadata:   metadata,
		matched:    true,
	}
}

func previewIntro(intro string) string {
	intro = strings.TrimSpace(intro)
	const limit = 80
	if utf8.RuneCountInString(intro) <= limit {
		return intro
	}
	runes := []rune(intro)
	return string(runes[:limit]) + "..."
}

func main() {
	ctx := context.Background()

	log.Println("Starting job classification query...")

	processor, err := NewQueryProcessor(ctx, databasePath)
	if err != nil {
		log.Fatalf("Failed to initialize query processor: %v", err)
	}
	defer processor.Close()

	log.Printf("CSV file will be saved to result/ directory")

	if err := processor.Process(ctx); err != nil {
		log.Fatalf("Failed to process jobs: %v", err)
	}

	log.Println("Query completed successfully!")
}
