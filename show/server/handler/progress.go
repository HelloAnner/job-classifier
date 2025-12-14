package handler

import (
	"encoding/json"
	"net/http"

	"show/redis"
)

// KeysResponse key 列表响应
type KeysResponse struct {
	Keys []string `json:"keys"`
}

// ProgressHandler 进度数据处理器
type ProgressHandler struct {
	redisClient *redis.Client
	keys        []string
}

// NewProgressHandler 创建进度处理器
func NewProgressHandler(client *redis.Client, keys []string) *ProgressHandler {
	return &ProgressHandler{
		redisClient: client,
		keys:        keys,
	}
}

// GetKeys 获取 key 列表
func (h *ProgressHandler) GetKeys(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(KeysResponse{
		Keys: h.keys,
	})
}

// GetProgress 获取指定 key 的进度数据
func (h *ProgressHandler) GetProgress(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	var result redis.ScanResult
	if err := h.redisClient.GetProgress(key, &result); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
