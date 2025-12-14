package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"show/embed"
	"show/handler"
	"show/redis"

	"github.com/joho/godotenv"
	"github.com/rs/cors"
)

func main() {
	// 加载配置 - 简化逻辑
	envFiles := []string{"./.env", "../.env"}
	var envLoaded bool
	for _, file := range envFiles {
		if err := godotenv.Load(file); err == nil {
			envLoaded = true
			log.Printf("Loaded config from: %s", file)
			break
		}
	}
	if !envLoaded {
		log.Printf("Warning: .env file not found, using default values")
	}

	port := getEnv("PORT", "8080")
	keysStr := getEnv("REDIS_KEYS", "bo,51job")
	updateInterval := getEnv("UPDATE_INTERVAL", "3")

	// 解析 key 列表
	keys := strings.Split(keysStr, ",")
	for i := range keys {
		keys[i] = strings.TrimSpace(keys[i])
	}

	// 解析更新间隔
	intervalSec, _ := strconv.Atoi(updateInterval)
	if intervalSec <= 0 {
		intervalSec = 3
	}

	log.Printf("Starting server on port %s", port)
	log.Printf("Monitoring keys: %v", keys)
	log.Printf("Update interval: %ds", intervalSec)

	// 初始化 Redis 客户端
	redisClient, err := redis.NewClient(&redis.Config{
		Host:     getEnv("REDIS_HOST", "localhost"),
		Port:     getEnv("REDIS_PORT", "6379"),
		Password: getEnv("REDIS_PASSWORD", ""),
	})
	if err != nil {
		log.Fatalf("Failed to connect Redis: %v", err)
	}
	defer redisClient.Close()

	// 初始化处理器
	progressHandler := handler.NewProgressHandler(redisClient, keys)
	sseHandler := handler.NewSSEHandler(redisClient, keys, time.Duration(intervalSec)*time.Second)

	// 注册路由
	mux := http.NewServeMux()

	// 使用嵌入的文件系统
	fileServer := http.FileServer(http.FS(embed.FS))
	mux.Handle("/", fileServer)

	// API 路由
	mux.HandleFunc("/api/keys", progressHandler.GetKeys)
	mux.HandleFunc("/api/progress", progressHandler.GetProgress)
	mux.Handle("/api/stream", sseHandler)

	// 添加 CORS 支持
	c := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"*"},
	})

	// 启动服务器
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      c.Handler(mux),
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 300 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
