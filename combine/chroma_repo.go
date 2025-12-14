package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Chroma repo（集合 ID 动态获取，与 cmd/query 一致）
type ChromaRepository struct {
	httpClient   *http.Client
	collectionID string
}

func NewChromaRepository(ctx context.Context) (*ChromaRepository, error) {
	client := chromaClientPool.Get().(*http.Client)
	r := &ChromaRepository{httpClient: client}
	if err := r.loadCollectionID(ctx); err != nil {
		chromaClientPool.Put(client) // 出错时放回池中
		return nil, err
	}
	return r, nil
}

// Close 方法用于释放资源
func (r *ChromaRepository) Close() {
	if r.httpClient != nil {
		chromaClientPool.Put(r.httpClient)
		r.httpClient = nil
	}
}
func (r *ChromaRepository) loadCollectionID(ctx context.Context) error {
	url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s", chromaDBURL, collectionName)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("查询集合 %s 失败: %w", collectionName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("未在 ChromaDB 中找到集合 %s，请先执行 job-import", collectionName)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("获取集合 %s 失败，状态码 %d: %s", collectionName, resp.StatusCode, string(b))
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

func (r *ChromaRepository) Query(ctx context.Context, embedding []float32, topK int) (*ChromaQueryResponse, error) {
	url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s/query", chromaDBURL, r.collectionID)
	reqBody := ChromaQueryRequest{QueryEmbeddings: [][]float32{embedding}, NResults: topK, Include: []string{"metadatas", "distances"}}
	data, _ := json.Marshal(reqBody)
	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query chroma: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("chroma API returned status %d: %s", resp.StatusCode, string(b))
	}
	var out ChromaQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	return &out, nil
}

// 批量 query：一次提交多个 query_embeddings，返回与输入等长的结果矩阵。
func (r *ChromaRepository) QueryBatch(ctx context.Context, embeddings [][]float32, topK int) (*ChromaQueryResponse, error) {
	if len(embeddings) == 0 {
		return &ChromaQueryResponse{}, nil
	}
	url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s/query", chromaDBURL, r.collectionID)
	reqBody := ChromaQueryRequest{QueryEmbeddings: embeddings, NResults: topK, Include: []string{"metadatas", "distances"}}
	data, _ := json.Marshal(reqBody)
	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query chroma (batch): %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("chroma API returned status %d: %s", resp.StatusCode, string(b))
	}
	var out ChromaQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	return &out, nil
}
