package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config 应用配置
type Config struct {
	SourceDir     string // 原始数据目录
	OutputDir     string // 输出目录
	RedisHost     string
	RedisPort     string
	RedisPassword string
	RedisKey      string // Redis 存储键名
	ScanInterval  int    // 扫描间隔(秒)
}

// Load 加载配置
func Load(envPath string) (*Config, error) {
	if err := godotenv.Load(envPath); err != nil {
		return nil, err
	}

	interval, _ := strconv.Atoi(os.Getenv("SCAN_INTERVAL"))
	if interval <= 0 {
		interval = 3
	}

	redisKey := os.Getenv("REDIS_KEY")
	if redisKey == "" {
		redisKey = "bo"
	}

	return &Config{
		SourceDir:     os.Getenv("SOURCE_DIR"),
		OutputDir:     os.Getenv("OUTPUT_DIR"),
		RedisHost:     os.Getenv("REDIS_HOST"),
		RedisPort:     os.Getenv("REDIS_PORT"),
		RedisPassword: os.Getenv("REDIS_PASSWORD"),
		RedisKey:      redisKey,
		ScanInterval:  interval,
	}, nil
}
