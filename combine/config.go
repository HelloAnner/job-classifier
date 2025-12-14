package main

import (
	"net/http"
	"sync"
	"time"
)

// ===== 与 cmd/query 保持一致的常量/配置 =====
const (
	ollamaAPIURL = "http://localhost:11434/api/embeddings"
	// /api/embed 支持 input[] 批量 embedding（Ollama 新版）
	ollamaEmbedBatchURL = "http://localhost:11434/api/embed"
	defaultEmbModel     = "qllama/bge-small-zh-v1.5:latest"
	chromaDBURL         = "http://localhost:8000"
	collectionName      = "job_classification"
	graphNeighborLimit  = 5
	similarityThreshold = 0.55
)

// ===== HTTP Client 连接池配置 =====
var (
	// HTTP Client 连接池，避免频繁创建和销毁
	httpClientPool = sync.Pool{
		New: func() interface{} {
			return &http.Client{
				Timeout: 300 * time.Second,
				Transport: &http.Transport{
					MaxIdleConns:        100,
					MaxIdleConnsPerHost: 100,
					IdleConnTimeout:     90 * time.Second,
					DisableCompression:  false,
					DisableKeepAlives:   false,
				},
			}
		},
	}

	// Chroma专用的HTTP Client（较短超时）
	chromaClientPool = sync.Pool{
		New: func() interface{} {
			return &http.Client{
				Timeout: 30 * time.Second,
				Transport: &http.Transport{
					MaxIdleConns:        50,
					MaxIdleConnsPerHost: 50,
					IdleConnTimeout:     60 * time.Second,
				},
			}
		},
	}

	// Embedding 缓存（全局单例）
	embeddingCache = NewEmbeddingCache(10000) // 缓存10000个embedding
)
