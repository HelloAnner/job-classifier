package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
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

	_ "github.com/mattn/go-sqlite3"
)

const (
	ollamaAPIURL        = "http://localhost:11434/api/embeddings"
	embeddingModel      = "quentinz/bge-large-zh-v1.5"
	chromaDBURL         = "http://localhost:8000"
	collectionName      = "job_classification"
	databasePath        = "data/jobs.db"
	similarityThreshold = 0.55
)

type JobRecord struct {
	MID        string
	JobIntro   string
	Structured sql.NullString
	Source     string
	Status     string
	Category   string
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
		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
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
		job.MID,
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
	// 清理换行符和回车符，替换为空格
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")

	// 清理制表符，替换为空格
	s = strings.ReplaceAll(s, "\t", " ")

	// 清理多余的连续空格
	for strings.Contains(s, "  ") {
		s = strings.ReplaceAll(s, "  ", " ")
	}

	// 去除开头和结尾的空白
	s = strings.TrimSpace(s)

	// 如果清理后的字符串仍然超过最大长度，则截断
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
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
	csvWriter    *CSVWriter
}

type jobResult struct {
	job        JobRecord
	similarity float64
	metadata   map[string]interface{}
	matched    bool
	err        error
}

func NewQueryProcessor(ctx context.Context, dbPath string) (*QueryProcessor, error) {
	dbService, err := NewDatabaseService(dbPath)
	if err != nil {
		return nil, err
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

	return &QueryProcessor{
		dbService:    dbService,
		embeddingSvc: NewEmbeddingService(),
		chromaRepo:   chromaRepo,
		csvWriter:    csvWriter,
	}, nil
}

func (p *QueryProcessor) Close() {
	p.dbService.Close()
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

	return jobResult{
		job:        job,
		similarity: bestSimilarity,
		metadata:   metadata,
		matched:    true,
	}
}

func previewIntro(intro string) string {
	if len(intro) > 80 {
		return intro[:80] + "..."
	}
	return intro
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
