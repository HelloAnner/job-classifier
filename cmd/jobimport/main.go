package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	ollamaAPIURL       = "http://localhost:11434/api/embeddings"
	embeddingModel     = "bge-zh"
	chromaDBURL        = "http://localhost:8000"
	collectionName     = "job_classification"
	embeddingDimension = 1024
	batchSize          = 50
)

type JobClassification struct {
	中类       string `json:"中类"`
	中类含义     string `json:"中类含义"`
	大类       string `json:"大类"`
	大类含义     string `json:"大类含义"`
	小类       string `json:"小类"`
	小类含义     string `json:"小类含义"`
	源行号      string `json:"源行号"`
	细类主要工作任务 string `json:"细类主要工作任务"`
	细类包含工种   string `json:"细类包含工种"`
	细类含义     string `json:"细类含义"`
	细类编码     string `json:"细类（职业）"`
}

func (j JobClassification) Get职业名称() string {
	return j.细类编码
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

type ChromaRepository struct {
	httpClient   *http.Client
	collectionID string
}

var errCollectionNotFound = errors.New("collection not found")

type collectionInfo struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	ConfigurationJSON struct {
		HNSW struct {
			Space string `json:"space"`
		} `json:"hnsw"`
	} `json:"configuration_json"`
}

type chromaAddRequest struct {
	Embeddings [][]float32              `json:"embeddings"`
	Documents  []string                 `json:"documents"`
	Metadatas  []map[string]interface{} `json:"metadatas"`
	IDs        []string                 `json:"ids"`
}

func NewChromaRepository(ctx context.Context) (*ChromaRepository, error) {
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	repo := &ChromaRepository{
		httpClient: httpClient,
	}

	if err := repo.ensureCollection(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure collection exists: %w", err)
	}

	log.Printf("成功连接 ChromaDB 集合 %s (ID: %s)", collectionName, repo.collectionID)
	return repo, nil
}

func (r *ChromaRepository) ensureCollection(ctx context.Context) error {
	info, err := r.fetchCollection(ctx)
	if err != nil && !errors.Is(err, errCollectionNotFound) {
		return err
	}

	if info != nil {
		r.collectionID = info.ID
		space := strings.ToLower(info.ConfigurationJSON.HNSW.Space)
		if space == "" || space == "cosine" {
			return nil
		}
		log.Printf("检测到集合 %s 使用 %s 距离度量，准备重建为 cosine", collectionName, space)
		if err := r.deleteCollection(ctx); err != nil {
			return err
		}
	}

	newID, err := r.createCollection(ctx)
	if err != nil {
		return err
	}
	r.collectionID = newID
	log.Printf("已创建 ChromaDB 集合 %s (ID: %s)，使用 cosine 距离", collectionName, newID)
	return nil
}

func (r *ChromaRepository) fetchCollection(ctx context.Context) (*collectionInfo, error) {
	collectionURL := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s", chromaDBURL, collectionName)
	req, err := http.NewRequestWithContext(ctx, "GET", collectionURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build collection lookup request: %w", err)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query collection: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, errCollectionNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status when checking collection: %d %s", resp.StatusCode, string(body))
	}

	var info collectionInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("failed to decode collection info: %w", err)
	}
	return &info, nil
}

func (r *ChromaRepository) deleteCollection(ctx context.Context) error {
	deleteURL := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s", chromaDBURL, collectionName)
	req, err := http.NewRequestWithContext(ctx, "DELETE", deleteURL, nil)
	if err != nil {
		return fmt.Errorf("failed to build delete request: %w", err)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete collection: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete collection, status=%d body=%s", resp.StatusCode, string(body))
	}

	log.Printf("已删除旧的集合 %s，准备重新创建", collectionName)
	r.collectionID = ""
	return nil
}

func (r *ChromaRepository) createCollection(ctx context.Context) (string, error) {
	payload := map[string]interface{}{
		"name": collectionName,
		"metadata": map[string]interface{}{
			"hnsw:space": "cosine",
		},
		"dimension": embeddingDimension,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal create collection payload: %w", err)
	}

	createURL := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections", chromaDBURL)
	req, err := http.NewRequestWithContext(ctx, "POST", createURL, bytes.NewBuffer(body))
	if err != nil {
		return "", fmt.Errorf("failed to build collection create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to create collection: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to create collection, status=%d body=%s", resp.StatusCode, string(respBody))
	}

	var info collectionInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return "", fmt.Errorf("failed to decode create response: %w", err)
	}
	if info.ID == "" {
		return "", errors.New("collection id is empty")
	}
	return info.ID, nil
}

func (r *ChromaRepository) AddBatch(ctx context.Context, embeddings [][]float32, documents []string, metadatas []map[string]interface{}, ids []string) error {
	if len(embeddings) == 0 {
		return nil
	}

	log.Printf("DEBUG: Adding batch with %d items, first ID: %s", len(ids), ids[0])

	if r.collectionID == "" {
		return fmt.Errorf("collection id is empty, please ensure collection is created first")
	}
	addURL := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s/add", chromaDBURL, r.collectionID)

	reqBody := map[string]interface{}{
		"embeddings": embeddings,
		"documents":  documents,
		"metadatas":  metadatas,
		"ids":        ids,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", addURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to add batch to chroma: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("DEBUG: Request IDs sample: %v", ids[:min(3, len(ids))])
		return fmt.Errorf("chroma API returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type DataProcessor struct {
	embeddingService *EmbeddingService
	chromaRepo       *ChromaRepository
}

func NewDataProcessor(embeddingService *EmbeddingService, chromaRepo *ChromaRepository) *DataProcessor {
	return &DataProcessor{
		embeddingService: embeddingService,
		chromaRepo:       chromaRepo,
	}
}

func (p *DataProcessor) getEmbeddingText(job JobClassification) string {
	var parts []string
	parts = append(parts, fmt.Sprintf("大类: %s", job.大类))
	if job.大类含义 != "" {
		parts = append(parts, fmt.Sprintf("大类含义: %s", job.大类含义))
	}
	parts = append(parts, fmt.Sprintf("中类: %s", job.中类))
	if job.中类含义 != "" {
		parts = append(parts, fmt.Sprintf("中类含义: %s", job.中类含义))
	}
	parts = append(parts, fmt.Sprintf("小类: %s", job.小类))
	if job.小类含义 != "" {
		parts = append(parts, fmt.Sprintf("小类含义: %s", job.小类含义))
	}
	parts = append(parts, fmt.Sprintf("职业: %s", job.细类编码))
	if job.细类含义 != "" {
		parts = append(parts, fmt.Sprintf("职业含义: %s", job.细类含义))
	}
	if job.细类主要工作任务 != "" {
		parts = append(parts, fmt.Sprintf("工作任务: %s", job.细类主要工作任务))
	}
	return strings.Join(parts, "\n")
}

func generateUUID() string {
	return uuid.New().String()
}

func cleanString(s string) string {
	return strings.ReplaceAll(s, "**", "")
}

func cleanJobClassification(job *JobClassification) {
	job.大类 = cleanString(job.大类)
	job.大类含义 = cleanString(job.大类含义)
	job.中类 = cleanString(job.中类)
	job.中类含义 = cleanString(job.中类含义)
	job.小类 = cleanString(job.小类)
	job.小类含义 = cleanString(job.小类含义)
	job.源行号 = cleanString(job.源行号)
	job.细类主要工作任务 = cleanString(job.细类主要工作任务)
	job.细类包含工种 = cleanString(job.细类包含工种)
	job.细类含义 = cleanString(job.细类含义)
	job.细类编码 = cleanString(job.细类编码)
}

func (p *DataProcessor) ProcessFile(ctx context.Context, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var rawJobs []map[string]interface{}
	decoder := json.NewDecoder(file)

	if err := decoder.Decode(&rawJobs); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	var jobs []JobClassification
	for _, rawJob := range rawJobs {
		job := JobClassification{}

		if v, ok := rawJob["大类"].(string); ok {
			job.大类 = cleanString(v)
		}
		if v, ok := rawJob["大类含义"].(string); ok {
			job.大类含义 = cleanString(v)
		}
		if v, ok := rawJob["中类"].(string); ok {
			job.中类 = cleanString(v)
		}
		if v, ok := rawJob["中类含义"].(string); ok {
			job.中类含义 = cleanString(v)
		}
		if v, ok := rawJob["小类"].(string); ok {
			job.小类 = cleanString(v)
		}
		if v, ok := rawJob["小类含义"].(string); ok {
			job.小类含义 = cleanString(v)
		}
		if v, ok := rawJob["源行号"].(string); ok {
			job.源行号 = cleanString(v)
		}
		if v, ok := rawJob["细类主要工作任务"].(string); ok {
			job.细类主要工作任务 = cleanString(v)
		}
		if v, ok := rawJob["细类包含工种"].(string); ok {
			job.细类包含工种 = cleanString(v)
		}
		if v, ok := rawJob["细类含义"].(string); ok {
			job.细类含义 = cleanString(v)
		}
		if v, ok := rawJob["细类（职业）"].(string); ok {
			job.细类编码 = cleanString(v)
		}

		jobs = append(jobs, job)
	}

	totalJobs := len(jobs)
	log.Printf("Total records to process: %d", totalJobs)

	var batchEmbeddings [][]float32
	var batchDocuments []string
	var batchMetadatas []map[string]interface{}
	var batchIDs []string

	for i, job := range jobs {
		text := p.getEmbeddingText(job)

		if job.细类编码 == "" {
			log.Printf("Warning: Skipping record %d with empty 细类编码", i)
			continue
		}

		embedding, err := p.embeddingService.GetEmbedding(text)
		if err != nil {
			log.Printf("Warning: Failed to get embedding for record %d (%s): %v", i, job.细类编码, err)
			continue
		}

		embedding, err = normalizeEmbedding(embedding)
		if err != nil {
			log.Printf("Warning: Failed to normalize embedding for record %d (%s): %v", i, job.细类编码, err)
			continue
		}

		metadata := map[string]interface{}{
			"大类":     job.大类,
			"大类含义":   job.大类含义,
			"中类":     job.中类,
			"中类含义":   job.中类含义,
			"小类":     job.小类,
			"小类含义":   job.小类含义,
			"细类职业":   job.细类编码,
			"细类含义":   job.细类含义,
			"细类包含工种": job.细类包含工种,
			"源行号":    job.源行号,
		}

		id := generateUUID()
		batchEmbeddings = append(batchEmbeddings, embedding)
		batchDocuments = append(batchDocuments, text)
		batchMetadatas = append(batchMetadatas, metadata)
		batchIDs = append(batchIDs, id)

		if i < 3 {
			log.Printf("DEBUG: Job %d: id=%s, 细类编码='%s'", i, id, job.细类编码)
		}

		if len(batchEmbeddings) >= batchSize {
			if err := p.chromaRepo.AddBatch(ctx, batchEmbeddings, batchDocuments, batchMetadatas, batchIDs); err != nil {
				log.Printf("Error adding batch: %v", err)
				return err
			}
			log.Printf("Progress: %d/%d records processed", i+1, totalJobs)

			batchEmbeddings = nil
			batchDocuments = nil
			batchMetadatas = nil
			batchIDs = nil
		}
	}

	if len(batchEmbeddings) > 0 {
		if err := p.chromaRepo.AddBatch(ctx, batchEmbeddings, batchDocuments, batchMetadatas, batchIDs); err != nil {
			log.Printf("Error adding final batch: %v", err)
			return err
		}
	}

	log.Printf("Completed: Successfully processed %d records", totalJobs)
	return nil
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

func main() {
	ctx := context.Background()

	log.Println("Starting job classification data import...")

	embeddingService := NewEmbeddingService()

	chromaRepo, err := NewChromaRepository(ctx)
	if err != nil {
		log.Fatalf("Failed to initialize ChromaDB repository: %v", err)
	}

	processor := NewDataProcessor(embeddingService, chromaRepo)

	filename := "data/classification.json"
	if err := processor.ProcessFile(ctx, filename); err != nil {
		log.Fatalf("Failed to process file: %v", err)
	}

	log.Println("Import completed successfully!")
}
