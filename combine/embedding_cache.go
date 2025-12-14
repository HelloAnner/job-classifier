package main

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	"strings"
	"sync"
	"time"
)

// ===== Embedding 缓存实现 =====

// EmbeddingCacheItem 缓存项
type EmbeddingCacheItem struct {
	embedding []float32
	timestamp time.Time
}

// EmbeddingCache LRU缓存实现
type EmbeddingCache struct {
	mu      sync.RWMutex
	cache   map[string]*EmbeddingCacheItem
	order   []string // LRU顺序
	maxSize int
}

// NewEmbeddingCache 创建新的embedding缓存
func NewEmbeddingCache(maxSize int) *EmbeddingCache {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &EmbeddingCache{
		cache:   make(map[string]*EmbeddingCacheItem),
		order:   make([]string, 0, maxSize),
		maxSize: maxSize,
	}
}

// Get 从缓存中获取embedding
func (c *EmbeddingCache) Get(key string) ([]float32, bool) {
	c.mu.RLock()
	item, exists := c.cache[key]
	c.mu.RUnlock()

	if !exists {
		return nil, false
	}

	// 更新访问时间（移到最近使用）
	c.mu.Lock()
	// 从order中移除
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
	// 添加到末尾
	c.order = append(c.order, key)
	item.timestamp = time.Now()
	c.mu.Unlock()

	// 返回副本，避免外部修改
	result := make([]float32, len(item.embedding))
	copy(result, item.embedding)
	return result, true
}

// Set 设置缓存项
func (c *EmbeddingCache) Set(key string, embedding []float32) {
	if len(embedding) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果已存在，更新
	if _, exists := c.cache[key]; exists {
		// 更新embedding
		c.cache[key].embedding = make([]float32, len(embedding))
		copy(c.cache[key].embedding, embedding)
		c.cache[key].timestamp = time.Now()

		// 移到最近使用
		for i, k := range c.order {
			if k == key {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
		c.order = append(c.order, key)
		return
	}

	// 如果缓存已满，移除最久未使用的
	if len(c.order) >= c.maxSize {
		oldestKey := c.order[0]
		delete(c.cache, oldestKey)
		c.order = c.order[1:]
	}

	// 添加新项
	c.cache[key] = &EmbeddingCacheItem{
		embedding: make([]float32, len(embedding)),
		timestamp: time.Now(),
	}
	copy(c.cache[key].embedding, embedding)
	c.order = append(c.order, key)
}

// Size 返回缓存大小
func (c *EmbeddingCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

// Clear 清空缓存
func (c *EmbeddingCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]*EmbeddingCacheItem)
	c.order = make([]string, 0, c.maxSize)
}

// generateCacheKey 生成缓存键
func generateCacheKey(text string) string {
	hash := sha256.Sum256([]byte(text))
	return hex.EncodeToString(hash[:16]) // 使用前16字节作为键
}

// GetEmbeddingsWithCache 带缓存的embedding获取
func GetEmbeddingsWithCache(svc *EmbeddingService, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	// 检查缓存
	cachedResults := make([][]float32, len(texts))
	uncachedTexts := make([]string, 0, len(texts))
	uncachedIndices := make([]int, 0, len(texts))

	for i, text := range texts {
		key := generateCacheKey(text)
		if emb, found := embeddingCache.Get(key); found {
			cachedResults[i] = emb
		} else {
			uncachedTexts = append(uncachedTexts, text)
			uncachedIndices = append(uncachedIndices, i)
		}
	}

	// 所有结果都在缓存中
	if len(uncachedTexts) == 0 {
		return cachedResults, nil
	}

	// 获取未缓存的embedding
	uncachedEmbs, err := svc.GetEmbeddings(uncachedTexts)
	if err != nil {
		return nil, err
	}

	// 合并结果并更新缓存
	for j, idx := range uncachedIndices {
		if j < len(uncachedEmbs) {
			emb := uncachedEmbs[j]
			cachedResults[idx] = emb

			// 更新缓存
			key := generateCacheKey(uncachedTexts[j])
			embeddingCache.Set(key, emb)
		}
	}

	return cachedResults, nil
}

// getEmbeddingsWithRetryAndCache 带缓存的embedding获取，支持重试
func getEmbeddingsWithRetryAndCache(svc *EmbeddingService, texts []string) ([][]float32, error) {
	// 首先尝试使用缓存
	embs, err := GetEmbeddingsWithCache(svc, texts)
	if err == nil {
		return embs, nil
	}

	// 如果失败，使用原始的重试逻辑
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
		left, errL := getEmbeddingsWithRetryAndCache(svc, texts[:mid])
		right, errR := getEmbeddingsWithRetryAndCache(svc, texts[mid:])
		if errL == nil && errR == nil {
			return append(left, right...), nil
		}
		// 若任一侧仍失败，返回原始错误
	}
	return nil, err
}
