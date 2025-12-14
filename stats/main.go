package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"stats/config"
	"stats/redis"
	"stats/scanner"
)

func main() {
	// 加载配置
	cfg, err := config.Load(".env")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Source dir: %s", cfg.SourceDir)
	log.Printf("Output dir: %s", cfg.OutputDir)
	log.Printf("Scan interval: %ds", cfg.ScanInterval)

	// 连接 Redis
	redisClient, err := redis.NewClient(cfg)
	if err != nil {
		log.Fatalf("Failed to connect redis: %v", err)
	}
	defer redisClient.Close()
	log.Println("Redis connected")

	// 扫描原始目录
	log.Println("Scanning source directory...")
	sourceFiles, err := scanner.ScanSource(cfg.SourceDir)
	if err != nil {
		log.Fatalf("Failed to scan source: %v", err)
	}
	log.Printf("Found %d source files", len(sourceFiles))

	// 打印原始文件信息
	for name, info := range sourceFiles {
		log.Printf("  - %s: %d lines", name, info.Lines)
	}

	// 创建输出扫描器
	outputScanner := scanner.NewOutputScanner(cfg.OutputDir)

	// 启动定时扫描
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ticker := time.NewTicker(time.Duration(cfg.ScanInterval) * time.Second)
	defer ticker.Stop()

	// 监听退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 立即执行一次扫描
	doScan(ctx, outputScanner, sourceFiles, redisClient)

	log.Println("Monitoring started. Press Ctrl+C to exit.")

	for {
		select {
		case <-ticker.C:
			doScan(ctx, outputScanner, sourceFiles, redisClient)
		case <-sigCh:
			log.Println("Shutting down...")
			return
		}
	}
}

func doScan(ctx context.Context, s *scanner.OutputScanner, sourceFiles map[string]*scanner.SourceInfo, rc *redis.Client) {
	result, err := s.Scan(sourceFiles)
	if err != nil {
		log.Printf("Scan error: %v", err)
		return
	}

	// 上传到 Redis
	if err := rc.Upload(ctx, result); err != nil {
		log.Printf("Upload error: %v", err)
		return
	}

	// 打印进度摘要
	fmt.Printf("\r[%s] Progress: %.2f%% (%d/%d files completed, %d/%d lines processed)",
		result.ScanTime.Format("15:04:05"),
		result.OverallPct,
		result.CompletedFiles,
		result.TotalFiles,
		result.ProcessedLines,
		result.TotalLines,
	)
}
