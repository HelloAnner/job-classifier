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
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultClassificationPath = "classification.json"
	defaultDBPath             = "db/job_graph.db"
	defaultTopN               = 10
	defaultWorkers            = 8
	embeddingModel            = "quentinz/bge-large-zh-v1.5"
	ollamaAPIURL              = "http://localhost:11434/api/embeddings"
	chromaDBURL               = "http://localhost:8000"
	collectionName            = "job_classification"
	chromaPageSize            = 200
)

type options struct {
	classificationPath string
	dbPath             string
	topN               int
	workers            int
	timeout            time.Duration
}

func parseFlags() options {
	var opts options
	flag.StringVar(&opts.classificationPath, "input", defaultClassificationPath, "classification JSON path")
	flag.StringVar(&opts.dbPath, "db", defaultDBPath, "output SQLite path")
	flag.IntVar(&opts.topN, "top", defaultTopN, "neighbor count to store per job")
	flag.IntVar(&opts.workers, "workers", defaultWorkers, "embedding concurrency")
	flag.DurationVar(&opts.timeout, "timeout", 120*time.Second, "embedding HTTP timeout")
	flag.Parse()

	if opts.topN <= 0 {
		opts.topN = defaultTopN
	}
	if opts.workers <= 0 {
		opts.workers = defaultWorkers
	}

	return opts
}

type classificationRecord struct {
	BigCategory      string   `json:"大类"`
	BigMeaning       string   `json:"大类含义"`
	MiddleCategory   string   `json:"中类"`
	MiddleMeaning    string   `json:"中类含义"`
	SmallCategory    string   `json:"小类"`
	SmallMeaning     string   `json:"小类含义"`
	FineCode         string   `json:"细类（职业）"`
	FineMeaning      string   `json:"细类含义"`
	Tasks            string   `json:"细类主要工作任务"`
	IncludedJobs     string   `json:"细类包含工种"`
	Overview         string   `json:"LLM岗位概述"`
	Responsibilities []string `json:"LLM典型职责"`
	Keywords         []string `json:"LLM关联关键词"`
	TypicalTitles    []string `json:"LLM典型岗位"`
}

type jobVector struct {
	ID        string
	Embedding []float32
}

type EmbeddingService struct {
	client *http.Client
}

func NewEmbeddingService(timeout time.Duration) *EmbeddingService {
	return &EmbeddingService{
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

type embeddingRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type embeddingResponse struct {
	Embedding []float32 `json:"embedding"`
}

func (s *EmbeddingService) GetEmbedding(text string) ([]float32, error) {
	reqBody := embeddingRequest{
		Model:  embeddingModel,
		Prompt: text,
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal embedding request failed: %w", err)
	}

	resp, err := s.client.Post(ollamaAPIURL, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return nil, fmt.Errorf("ollama API error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ollama API returned %d: %s", resp.StatusCode, string(body))
	}

	var result embeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode embedding response failed: %w", err)
	}

	if len(result.Embedding) == 0 {
		return nil, errors.New("empty embedding")
	}

	return result.Embedding, nil
}

func main() {
	opts := parseFlags()

	if err := os.MkdirAll(filepath.Dir(opts.dbPath), 0o755); err != nil {
		log.Fatalf("failed to create db directory: %v", err)
	}

	log.Printf("Loading classification data from %s", opts.classificationPath)
	records, err := loadClassification(opts.classificationPath)
	if err != nil {
		log.Fatalf("failed to load classification data: %v", err)
	}
	if len(records) == 0 {
		log.Fatal("classification data is empty")
	}

	log.Printf("Loaded %d classification nodes", len(records))

	ctx := context.Background()

	vectors, err := loadVectorsFromChroma(ctx)
	if err != nil {
		log.Printf("Warning: failed to load embeddings from ChromaDB, will fall back to embedding API: %v", err)
	}

	if len(vectors) == 0 {
		log.Println("ChromaDB returned no embeddings, falling back to embedding service")
		vectors, err = buildEmbeddings(ctx, records, NewEmbeddingService(opts.timeout), opts.workers)
		if err != nil {
			log.Fatalf("failed to build embeddings: %v", err)
		}
	} else {
		if len(vectors) < len(records) {
			missing := findMissing(records, vectors)
			if len(missing) > 0 {
				log.Printf("Detected %d records missing from ChromaDB, generating embeddings via API", len(missing))
				extra, err := buildEmbeddings(ctx, missing, NewEmbeddingService(opts.timeout), opts.workers)
				if err != nil {
					log.Fatalf("failed to backfill embeddings: %v", err)
				}
				vectors = append(vectors, extra...)
			}
		}
	}

	graph := buildGraph(vectors, opts.topN)
	if err := persistGraph(opts.dbPath, graph); err != nil {
		log.Fatalf("failed to store graph: %v", err)
	}

	log.Printf("Graph stored to %s, edges=%d", opts.dbPath, countEdges(graph))
}

func loadClassification(path string) ([]classificationRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var raw []map[string]interface{}
	if err := json.NewDecoder(file).Decode(&raw); err != nil {
		return nil, err
	}

	var records []classificationRecord
	for i, item := range raw {
		rec := classificationRecord{
			BigCategory:      cleanString(getString(item, "大类")),
			BigMeaning:       cleanString(getString(item, "大类含义")),
			MiddleCategory:   cleanString(getString(item, "中类")),
			MiddleMeaning:    cleanString(getString(item, "中类含义")),
			SmallCategory:    cleanString(getString(item, "小类")),
			SmallMeaning:     cleanString(getString(item, "小类含义")),
			FineCode:         cleanString(getString(item, "细类（职业）")),
			FineMeaning:      cleanString(getString(item, "细类含义")),
			Tasks:            cleanString(getString(item, "细类主要工作任务")),
			IncludedJobs:     cleanString(getString(item, "细类包含工种")),
			Overview:         cleanString(fmt.Sprint(item["LLM岗位概述"])),
			Responsibilities: toStringSlice(item["LLM典型职责"]),
			Keywords:         toStringSlice(item["LLM关联关键词"]),
			TypicalTitles:    toStringSlice(item["LLM典型岗位"]),
		}

		if rec.FineCode == "" {
			log.Printf("skip record %d without job id", i)
			continue
		}

		records = append(records, rec)
	}

	return records, nil
}

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok && v != nil {
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprint(v)
	}
	return ""
}

func cleanString(s string) string {
	return strings.TrimSpace(strings.ReplaceAll(s, "**", ""))
}

func toStringSlice(value interface{}) []string {
	switch v := value.(type) {
	case []string:
		return cleanStringSlice(v)
	case []interface{}:
		var result []string
		for _, item := range v {
			if item == nil {
				continue
			}
			result = append(result, fmt.Sprint(item))
		}
		return cleanStringSlice(result)
	case string:
		return cleanStringSlice(parseStringField(v))
	case nil:
		return nil
	default:
		return cleanStringSlice(parseStringField(fmt.Sprint(v)))
	}
}

func cleanStringSlice(values []string) []string {
	var result []string
	for _, v := range values {
		if s := cleanString(v); s != "" {
			result = append(result, s)
		}
	}
	return result
}

func parseStringField(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		var arr []string
		if err := json.Unmarshal([]byte(s), &arr); err == nil {
			return arr
		}
	}
	seps := func(r rune) bool {
		switch r {
		case '；', ';', '，', ',', '\n', '\r':
			return true
		default:
			return false
		}
	}
	parts := strings.FieldsFunc(s, seps)
	if len(parts) > 1 {
		return parts
	}
	return []string{s}
}

func buildEmbeddings(ctx context.Context, records []classificationRecord, svc *EmbeddingService, workers int) ([]jobVector, error) {
	jobs := make(chan classificationRecord)
	results := make(chan jobVectorResult)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rec := range jobs {
				text := buildEmbeddingText(rec)
				vec, err := svc.GetEmbedding(text)
				if err != nil {
					results <- jobVectorResult{err: fmt.Errorf("embedding failed for %s: %w", rec.FineCode, err)}
					continue
				}
				normalized, err := normalize(vec)
				if err != nil {
					results <- jobVectorResult{err: fmt.Errorf("normalization failed for %s: %w", rec.FineCode, err)}
					continue
				}
				results <- jobVectorResult{
					vector: jobVector{
						ID:        rec.FineCode,
						Embedding: normalized,
					},
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	go func() {
		for _, rec := range records {
			jobs <- rec
		}
		close(jobs)
	}()

	var vectors []jobVector
	var failures int
	for res := range results {
		if res.err != nil {
			failures++
			log.Println(res.err)
			continue
		}
		vectors = append(vectors, res.vector)
		if len(vectors)%100 == 0 {
			log.Printf("Embeddings progress: %d/%d", len(vectors), len(records))
		}
	}

	if len(vectors) == 0 {
		return nil, errors.New("no embeddings produced")
	}
	if failures > 0 {
		log.Printf("warning: %d records failed to embed", failures)
	}

	return vectors, nil
}

type chromaClient struct {
	httpClient   *http.Client
	collectionID string
}

func loadVectorsFromChroma(ctx context.Context) ([]jobVector, error) {
	client, err := newChromaClient(ctx)
	if err != nil {
		return nil, err
	}
	count, err := client.count(ctx)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	log.Printf("Fetching %d embeddings from ChromaDB...", count)
	var vectors []jobVector
	for offset := 0; offset < count; offset += chromaPageSize {
		page, err := client.getPage(ctx, offset, chromaPageSize)
		if err != nil {
			return nil, err
		}
		for i := range page.Metadatas {
			meta := page.Metadatas[i]
			jobID, _ := meta["细类职业"].(string)
			if jobID == "" {
				continue
			}
			embedding := page.Embeddings[i]
			if len(embedding) == 0 {
				continue
			}
			normalized, err := normalize(embedding)
			if err != nil {
				return nil, fmt.Errorf("normalize Chroma embedding failed for %s: %w", jobID, err)
			}
			vectors = append(vectors, jobVector{
				ID:        jobID,
				Embedding: normalized,
			})
		}
		log.Printf("Fetched %d/%d vectors from ChromaDB", len(vectors), count)
	}
	return deduplicateVectors(vectors), nil
}

func deduplicateVectors(vectors []jobVector) []jobVector {
	seen := make(map[string]struct{})
	var deduped []jobVector
	for _, v := range vectors {
		if _, ok := seen[v.ID]; ok {
			continue
		}
		seen[v.ID] = struct{}{}
		deduped = append(deduped, v)
	}
	return deduped
}

func newChromaClient(ctx context.Context) (*chromaClient, error) {
	client := &chromaClient{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
	if err := client.loadCollectionID(ctx); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *chromaClient) loadCollectionID(ctx context.Context) error {
	url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s", chromaDBURL, collectionName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to load collection %s: status %d", collectionName, resp.StatusCode)
	}
	var payload struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return err
	}
	if payload.ID == "" {
		return errors.New("collection id is empty")
	}
	c.collectionID = payload.ID
	return nil
}

func (c *chromaClient) count(ctx context.Context) (int, error) {
	url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s/count", chromaDBURL, c.collectionID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("count request failed: status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var count int
	if _, err := fmt.Sscanf(string(data), "%d", &count); err != nil {
		return 0, err
	}
	return count, nil
}

type chromaGetResponse struct {
	IDs        []string                 `json:"ids"`
	Embeddings [][]float32              `json:"embeddings"`
	Metadatas  []map[string]interface{} `json:"metadatas"`
	Documents  []string                 `json:"documents"`
	Include    []string                 `json:"include"`
}

func (c *chromaClient) getPage(ctx context.Context, offset, limit int) (*chromaGetResponse, error) {
	url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s/get", chromaDBURL, c.collectionID)
	body := map[string]interface{}{
		"offset":  offset,
		"limit":   limit,
		"include": []string{"metadatas", "embeddings"},
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("chroma get failed: status %d", resp.StatusCode)
	}
	var result chromaGetResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func findMissing(records []classificationRecord, vectors []jobVector) []classificationRecord {
	known := make(map[string]struct{}, len(vectors))
	for _, v := range vectors {
		known[v.ID] = struct{}{}
	}
	var missing []classificationRecord
	for _, rec := range records {
		if rec.FineCode == "" {
			continue
		}
		if _, ok := known[rec.FineCode]; !ok {
			missing = append(missing, rec)
		}
	}
	return missing
}

type jobVectorResult struct {
	vector jobVector
	err    error
}

func buildEmbeddingText(rec classificationRecord) string {
	var parts []string
	if rec.Overview != "" {
		parts = append(parts, fmt.Sprintf("岗位概述: %s", rec.Overview))
	}
	if len(rec.Responsibilities) > 0 {
		parts = append(parts, fmt.Sprintf("核心职责: %s", strings.Join(rec.Responsibilities, "；")))
	}
	if len(rec.Keywords) > 0 {
		parts = append(parts, fmt.Sprintf("关联关键词: %s", strings.Join(rec.Keywords, "、")))
	}
	if len(rec.TypicalTitles) > 0 {
		parts = append(parts, fmt.Sprintf("典型岗位: %s", strings.Join(rec.TypicalTitles, "、")))
	}
	if len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("大类: %s", rec.BigCategory))
		if rec.BigMeaning != "" {
			parts = append(parts, fmt.Sprintf("大类含义: %s", rec.BigMeaning))
		}
		parts = append(parts, fmt.Sprintf("中类: %s", rec.MiddleCategory))
		if rec.MiddleMeaning != "" {
			parts = append(parts, fmt.Sprintf("中类含义: %s", rec.MiddleMeaning))
		}
		parts = append(parts, fmt.Sprintf("小类: %s", rec.SmallCategory))
		if rec.SmallMeaning != "" {
			parts = append(parts, fmt.Sprintf("小类含义: %s", rec.SmallMeaning))
		}
	}
	parts = append(parts, fmt.Sprintf("职业: %s", rec.FineCode))
	if rec.FineMeaning != "" {
		parts = append(parts, fmt.Sprintf("职业含义: %s", rec.FineMeaning))
	}
	if rec.Tasks != "" {
		parts = append(parts, fmt.Sprintf("主要任务: %s", rec.Tasks))
	}
	if rec.IncludedJobs != "" {
		parts = append(parts, fmt.Sprintf("包含工种: %s", rec.IncludedJobs))
	}
	return strings.Join(parts, "\n")
}

func normalize(vec []float32) ([]float32, error) {
	var sum float64
	for _, v := range vec {
		sum += float64(v) * float64(v)
	}
	if sum == 0 {
		return nil, errors.New("vector norm is zero")
	}
	norm := float32(1 / math.Sqrt(sum))
	normalized := make([]float32, len(vec))
	for i, v := range vec {
		normalized[i] = v * norm
	}
	return normalized, nil
}

func buildGraph(vectors []jobVector, topN int) map[string][]neighbor {
	graph := make(map[string][]neighbor, len(vectors))
	for i, v := range vectors {
		var items []neighbor
		for j, other := range vectors {
			if i == j {
				continue
			}
			score := dot(v.Embedding, other.Embedding)
			items = append(items, neighbor{ID: other.ID, Score: score})
		}
		sort.Slice(items, func(a, b int) bool {
			return items[a].Score > items[b].Score
		})
		if len(items) > topN {
			items = items[:topN]
		}
		graph[v.ID] = items
	}
	return graph
}

type neighbor struct {
	ID    string
	Score float64
}

func dot(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}
	var sum float64
	for i := range a {
		sum += float64(a[i]) * float64(b[i])
	}
	return sum
}

func persistGraph(dbPath string, graph map[string][]neighbor) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	createSQL := `
	CREATE TABLE IF NOT EXISTS job_neighbors (
		job_id TEXT NOT NULL,
		neighbor_id TEXT NOT NULL,
		score REAL NOT NULL,
		PRIMARY KEY (job_id, neighbor_id)
	);
	`

	if _, err := db.Exec(createSQL); err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM job_neighbors"); err != nil {
		tx.Rollback()
		return err
	}

	stmt, err := tx.Prepare("INSERT INTO job_neighbors(job_id, neighbor_id, score) VALUES (?, ?, ?)")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for jobID, neighbors := range graph {
		for _, n := range neighbors {
			if _, err := stmt.Exec(jobID, n.ID, n.Score); err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func countEdges(graph map[string][]neighbor) int {
	var total int
	for _, neighbors := range graph {
		total += len(neighbors)
	}
	return total
}
