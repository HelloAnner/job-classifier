package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"stats/config"
	"stats/model"
)

// Client Redis 客户端封装
type Client struct {
	rdb         *redis.Client
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

// Upload 上传扫描结果到 Redis（并发安全的按文件合并）
//
// 兼容性：保持与旧版相同的调用方式，但内部改为：
// 1) 读取 Redis 现有快照
// 2) 仅用本机扫描到的文件条目覆盖/新增对应文件
// 3) 基于合并后的全量文件集合重新计算 totals 与 overall
// 4) 使用 WATCH + TxPipelined 保证并发下不丢失更新
func (c *Client) Upload(ctx context.Context, partial *model.ScanResult) error {
	// 最多重试 5 次以应对并发覆盖
	const maxRetry = 5
	var lastErr error
	for i := 0; i < maxRetry; i++ {
		err := c.rdb.Watch(ctx, func(tx *redis.Tx) error {
			// 1) 读取旧值
			oldJSON, err := tx.Get(ctx, c.progressKey).Result()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}

			var merged *model.ScanResult
			if errors.Is(err, redis.Nil) || oldJSON == "" {
				// 无旧值，直接规范化本次结果（按文件回填 totals）
				merged = snapshotFromPartial(partial)
			} else {
				var old model.ScanResult
				if uErr := json.Unmarshal([]byte(oldJSON), &old); uErr != nil {
					// 旧值不可解析，回退为仅用本地值
					merged = snapshotFromPartial(partial)
				} else {
					merged = mergeResults(&old, partial)
				}
			}

			buf, mErr := json.Marshal(merged)
			if mErr != nil {
				return mErr
			}

			// 2) 原子写入
			_, pErr := tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.Set(ctx, c.progressKey, string(buf), 0)
				return nil
			})
			return pErr
		}, c.progressKey)

		if err == nil {
			return nil
		}
		if errors.Is(err, redis.TxFailedErr) {
			// 被并发抢占，稍等重试
			lastErr = err
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return err
	}
	if lastErr != nil {
		return fmt.Errorf("upload failed after retries: %w", lastErr)
	}
	return fmt.Errorf("upload failed after retries")
}

// Close 关闭连接
func (c *Client) Close() error {
	return c.rdb.Close()
}

// snapshotFromPartial 仅基于本地扫描结果生成一个规范化快照（补齐 totals/overall）。
func snapshotFromPartial(partial *model.ScanResult) *model.ScanResult {
	// 拷贝避免外部修改
	var out model.ScanResult
	b, _ := json.Marshal(partial)
	_ = json.Unmarshal(b, &out)
	recomputeTotals(&out)
	out.ScanTime = time.Now()
	return &out
}

// mergeResults 基于旧快照与本地增量结果按文件合并，返回新快照。
func mergeResults(old *model.ScanResult, partial *model.ScanResult) *model.ScanResult {
	// 旧快照文件索引
	idx := make(map[string]int, len(old.Files))
	for i, f := range old.Files {
		idx[f.FileName] = i
	}

    // 合并/覆盖文件（仅覆盖本机观察到输出的文件，避免把别的机器进度用 0 覆盖）
    for _, nf := range partial.Files {
        if nf == nil {
            continue
        }
        if pos, ok := idx[nf.FileName]; ok {
            // 已存在：仅当本机观察到输出才考虑覆盖，且避免回退
            if !nf.HasOutput {
                continue
            }
            op := old.Files[pos].ResultLines + old.Files[pos].IgnoreLines
            np := nf.ResultLines + nf.IgnoreLines
            if np >= op {
                old.Files[pos] = cloneFile(nf)
            }
        } else {
            // 新文件：直接加入（即使当前无输出，也让 totals 预先包含来源行数）
            old.Files = append(old.Files, cloneFile(nf))
            idx[nf.FileName] = len(old.Files) - 1
        }
    }

	// 重新计算 totals/overall
	recomputeTotals(old)
	old.ScanTime = time.Now()
	return old
}

// cloneFile 浅拷贝 FileProgress
func cloneFile(in *model.FileProgress) *model.FileProgress {
	if in == nil {
		return nil
	}
	out := *in
	// 进度百分比可能需要根据行数回算一次，避免遥远历史的异常值
	normalizeFile(&out)
	return &out
}

// normalizeFile 基于行数回算单文件进度百分比（与 scanner.OutputScanner 逻辑保持一致）。
func normalizeFile(fp *model.FileProgress) {
	processed := fp.ResultLines + fp.IgnoreLines
	if fp.SourceLines > 0 {
		if processed == fp.SourceLines {
			fp.ProcessedPct = 100
		} else {
			pct := float64(processed) / float64(fp.SourceLines) * 100
			if pct > 99.9 {
				pct = 99.9
			}
			if pct < 0 {
				pct = 0
			}
			fp.ProcessedPct = pct
		}
	} else {
		fp.ProcessedPct = 0
	}
}

// recomputeTotals 以 Files 的并集重算聚合字段。
func recomputeTotals(sr *model.ScanResult) {
	sr.TotalFiles = 0
	sr.CompletedFiles = 0
	sr.TotalLines = 0
	sr.ProcessedLines = 0

	for _, f := range sr.Files {
		if f == nil {
			continue
		}
		normalizeFile(f)
		processed := f.ResultLines + f.IgnoreLines
		sr.TotalFiles++
		sr.TotalLines += f.SourceLines
		sr.ProcessedLines += processed
		if f.ProcessedPct >= 100 {
			sr.CompletedFiles++
		}
	}

	if sr.TotalLines > 0 {
		sr.OverallPct = float64(sr.ProcessedLines) / float64(sr.TotalLines) * 100
	} else {
		sr.OverallPct = 0
	}
}
