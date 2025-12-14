package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// Config Redis 配置
type Config struct {
	Host     string
	Port     string
	Password string
}

// Client Redis 客户端
type Client struct {
	rdb *redis.Client
}

// NewClient 创建 Redis 客户端
func NewClient(cfg *Config) (*Client, error) {
	addr := fmt.Sprintf("%s:%s", cfg.Host, cfg.Port)
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: cfg.Password,
		DB:       0,
	})

	return &Client{rdb: rdb}, nil
}

// GetProgress 获取指定 key 的进度数据
func (c *Client) GetProgress(key string, result interface{}) error {
	val, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(val), result)
}

// Close 关闭连接
func (c *Client) Close() error {
	return c.rdb.Close()
}
