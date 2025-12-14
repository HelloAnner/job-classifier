package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// Embedding
type EmbeddingService struct{ client *http.Client }
type EmbeddingRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}
type EmbeddingResponse struct {
	Embedding []float32 `json:"embedding"`
}

// /api/embed 批量 embedding 的请求/响应
type EmbedBatchRequest struct {
	Model     string      `json:"model"`
	Input     []string    `json:"input"`
	KeepAlive interface{} `json:"keep_alive,omitempty"`
}
type EmbedBatchResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
}

func NewEmbeddingService() *EmbeddingService {
	return NewEmbeddingServiceWithTimeout(300 * time.Second)
}

func NewEmbeddingServiceWithTimeout(timeout time.Duration) *EmbeddingService {
	client := httpClientPool.Get().(*http.Client)
	// 如果提供了不同的超时时间，创建新的client
	if timeout != 300*time.Second {
		httpClientPool.Put(client) // 放回池中
		client = &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	}
	return &EmbeddingService{client: client}
}

// Close 方法用于释放资源
func (s *EmbeddingService) Close() {
	if s.client != nil {
		// 检查是否是标准超时的client，是则放回池中
		if s.client.Timeout == 300*time.Second {
			httpClientPool.Put(s.client)
		}
		s.client = nil
	}
}
func (s *EmbeddingService) GetEmbedding(text string) ([]float32, error) {
	// 兼容单条：复用批量接口
	out, err := s.GetEmbeddings([]string{text})
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("ollama batch embedding returned empty result")
	}
	return out[0], nil
}

// 当前使用的 embedding 模型（优先 CLI/env 配置）
var embModelInUse = defaultEmbModel

func currentEmbModel() string { return embModelInUse }

func (s *EmbeddingService) GetEmbeddings(texts []string) ([][]float32, error) {
	reqBody := EmbedBatchRequest{Model: currentEmbModel(), Input: texts, KeepAlive: -1}
	data, _ := json.Marshal(reqBody)
	resp, err := s.client.Post(ollamaEmbedBatchURL, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return nil, fmt.Errorf("failed to call ollama batch API: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ollama batch API returned status %d: %s", resp.StatusCode, string(b))
	}
	var out EmbedBatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("failed to decode batch response: %w", err)
	}
	if len(out.Embeddings) != len(texts) {
		return nil, fmt.Errorf("ollama batch embeddings size mismatch: got=%d expect=%d", len(out.Embeddings), len(texts))
	}
	return out.Embeddings, nil
}

// 在批量 embedding 失败时（典型为超时），递归拆分批次，保证尽量返回成功结果，避免长时间卡住。
func getEmbeddingsWithRetry(svc *EmbeddingService, texts []string) ([][]float32, error) {
	embs, err := svc.GetEmbeddings(texts)
	if err == nil {
		return embs, nil
	}
	if len(texts) <= 1 {
		return nil, err
	}
	msg := err.Error()
	if strings.Contains(msg, "timeout") || strings.Contains(msg, "context deadline exceeded") || strings.Contains(msg, "Client.Timeout") {
		mid := len(texts) / 2
		if mid < 1 {
			mid = 1
		}
		log.Printf("[EMB-RETRY-SPLIT] size=%d -> %d+%d due to: %v", len(texts), mid, len(texts)-mid, err)
		left, errL := getEmbeddingsWithRetry(svc, texts[:mid])
		right, errR := getEmbeddingsWithRetry(svc, texts[mid:])
		if errL == nil && errR == nil {
			return append(left, right...), nil
		}
		// 若任一侧仍失败，返回原始错误
	}
	return nil, err
}
