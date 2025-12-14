package handler

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"show/redis"
)

// SSEHandler SSE 事件推送处理器
type SSEHandler struct {
	redisClient *redis.Client
	keys        []string
	interval    time.Duration
	clients     map[chan string]bool
	mu          sync.RWMutex
	startOnce   sync.Once
	stopChan    chan struct{}
}

// NewSSEHandler 创建 SSE 处理器
func NewSSEHandler(client *redis.Client, keys []string, interval time.Duration) *SSEHandler {
	return &SSEHandler{
		redisClient: client,
		keys:        keys,
		interval:    interval,
		clients:     make(map[chan string]bool),
		stopChan:    make(chan struct{}),
	}
}

// ServeHTTP 处理 SSE 请求
func (h *SSEHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 设置 SSE 响应头
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("X-Accel-Buffering", "no") // 禁用 nginx 缓冲

	// 启动全局推送协程（只启动一次）
	h.startOnce.Do(func() {
		go h.globalPushLoop()
	})

	// 创建客户端通道
	clientChan := make(chan string, 100)
	h.addClient(clientChan)
	defer h.removeClient(clientChan)

	// 立即发送初始数据
	h.sendInitialData(clientChan)

	// 刷新响应
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	// 保持连接，监听客户端断开
	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case data := <-clientChan:
			fmt.Fprintf(w, "data: %s\n\n", data)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case <-heartbeat.C:
			// 发送心跳保持连接
			fmt.Fprintf(w, ": heartbeat\n\n")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case <-r.Context().Done():
			return
		}
	}
}

// sendInitialData 发送初始数据
func (h *SSEHandler) sendInitialData(clientChan chan string) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("SSE panic in sendInitialData: %v", r)
		}
	}()

	// 获取所有 key 的数据
	allData := make(map[string]*redis.ScanResult)

	for _, key := range h.keys {
		var result redis.ScanResult
		if err := h.redisClient.GetProgress(key, &result); err == nil {
			allData[key] = &result
		}
	}

	// 序列化为 JSON
	data, err := json.Marshal(allData)
	if err != nil {
		return
	}

	// 发送到客户端
	select {
	case clientChan <- string(data):
	default:
	}
}

// addClient 添加客户端
func (h *SSEHandler) addClient(clientChan chan string) {
	h.mu.Lock()
	h.clients[clientChan] = true
	h.mu.Unlock()
}

// removeClient 移除客户端
func (h *SSEHandler) removeClient(clientChan chan string) {
	h.mu.Lock()
	delete(h.clients, clientChan)
	close(clientChan)
	h.mu.Unlock()
}

// globalPushLoop 全局推送循环
func (h *SSEHandler) globalPushLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("SSE panic in globalPushLoop: %v", r)
			// 重启推送循环
			time.Sleep(1 * time.Second)
			h.startOnce = sync.Once{} // 重置 Once
			h.startOnce.Do(func() {
				go h.globalPushLoop()
			})
		}
	}()

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.pushDataToAll()
		case <-h.stopChan:
			return
		}
	}
}

// pushDataToAll 推送数据到所有客户端
func (h *SSEHandler) pushDataToAll() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("SSE panic in pushDataToAll: %v", r)
		}
	}()

	// 获取所有 key 的数据
	allData := make(map[string]*redis.ScanResult)

	for _, key := range h.keys {
		var result redis.ScanResult
		if err := h.redisClient.GetProgress(key, &result); err == nil {
			allData[key] = &result
		}
	}

	// 序列化为 JSON
	data, err := json.Marshal(allData)
	if err != nil {
		return
	}

	// 推送给所有客户端
	h.mu.RLock()
	for clientChan := range h.clients {
		select {
		case clientChan <- string(data):
		default:
			// 通道满，跳过这次推送
		}
	}
	h.mu.RUnlock()
}
