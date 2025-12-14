package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"stats/config"
	"stats/model"
)

// Client Redis 客户端封装
type Client struct {
	rdb       *redis.Client
	progressKey string
}

// NewClient 创建 Redis 客户端
func NewClient(cfg *config.Config) (*Client, error) {
	addr := fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort)
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: cfg.RedisPassword,
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return &Client{
		rdb:         rdb,
		progressKey: cfg.RedisKey,
	}, nil
}

// Upload 上传扫描结果到 Redis
func (c *Client) Upload(ctx context.Context, result *model.ScanResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}

	return c.rdb.Set(ctx, c.progressKey, string(data), 0).Err()
}

// Close 关闭连接
func (c *Client) Close() error {
	return c.rdb.Close()
}
