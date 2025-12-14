package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

// ===== 导入阶段 =====
type importStats struct {
	Total    int64
	Imported int64
	Filtered int64
}

type csvRow struct {
	MID      string
	Intro    string
	Filtered bool // true: intro 长度 < 6，需要忽略匹配
}

// 流式读取 CSV：一边写 ignore.csv，一边按 batch 写入 DB，避免大文件占用大量内存
func streamCSVToDB(ctx context.Context, csvPath string, ignorePath string, db *sql.DB, source string, batchSize int, trace bool) (stats importStats, err error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return stats, err
	}
	defer f.Close()
	r := csv.NewReader(bufio.NewReader(f))
	r.ReuseRecord = true
	r.LazyQuotes = true
	r.FieldsPerRecord = -1

	// 打开 ignore.csv
	igf, err := os.Create(ignorePath)
	if err != nil {
		return stats, err
	}
	defer func() { _ = igf.Close() }()
	igw := csv.NewWriter(igf)
	if err := igw.Write([]string{"mid", "job_intro"}); err != nil {
		return stats, err
	}

	// 先读表头
	header, err := r.Read()
	if err == io.EOF {
		igw.Flush()
		return stats, igw.Error()
	}
	if err != nil {
		return stats, err
	}
	if len(header) < 2 {
		// 不规范表头也继续读取数据
	}

	// 累计一个小批次写 DB，避免持久占用内存
	batch := make([]csvRow, 0, batchSize)
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := importRowsToDB(ctx, db, batch, source, batchSize, trace); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return stats, err
		}
		// 对于不规范行：字段不足，既不入库也不写入 ignore，避免造成空行
		if len(rec) < 2 {
			if trace {
				log.Printf("[SKIP] malformed row: fields<2")
			}
			continue
		}
		mid := strings.TrimSpace(rec[0])
		intro := strings.TrimSpace(rec[1])
		if mid == "" {
			if trace {
				log.Printf("[SKIP] empty mid row, intro_len=%d", introRuneLen(intro))
			}
			continue
		}
		stats.Total++
		if introRuneLen(intro) < 6 {
			stats.Filtered++
			_ = igw.Write([]string{mid, intro})
			if trace {
				// 行号无法直接取得，这里打印 mid 与长度
				log.Printf("[FILTER] mid=%s intro_len=%d(<6)", mid, introRuneLen(intro))
			}
			// 仍然写入 DB，但打上过滤标记，保证最终计数对齐（result+ignore==db）
			if mid != "" {
				batch = append(batch, csvRow{MID: mid, Intro: intro, Filtered: true})
			}
			if len(batch) >= batchSize {
				if err := flushBatch(); err != nil {
					return stats, err
				}
			}
			continue
		}
		stats.Imported++
		batch = append(batch, csvRow{MID: mid, Intro: intro, Filtered: false})
		if len(batch) >= batchSize {
			if err := flushBatch(); err != nil {
				return stats, err
			}
		}
	}
	if err := flushBatch(); err != nil {
		return stats, err
	}
	igw.Flush()
	if err := igw.Error(); err != nil {
		return stats, err
	}
	return stats, nil
}

func introRuneLen(s string) int { return len([]rune(strings.TrimSpace(s))) }

func ensureDB(dbPath string) (*sql.DB, error) {
	// dsn 参考 cmd/importer 的参数（WAL / NORMAL / 忙等待）
	dsn := fmt.Sprintf("file:%s?_cache_size=200000&_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=60000&_foreign_keys=off", dbPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	schema := []string{
		`CREATE TABLE IF NOT EXISTS jobs (
            mid TEXT PRIMARY KEY,
            job_intro TEXT,
            source TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT '待处理',
            category TEXT,
            updated_at TEXT NOT NULL,
            structured_json TEXT,
            structured_summary TEXT,
            structured_responsibilities TEXT,
            structured_skills TEXT,
            structured_industry TEXT,
            structured_locations TEXT,
            structured_category_hints TEXT,
            structured_updated_at TEXT
        );`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source);`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_category ON jobs(category);`,
		// 优化StreamJobs查询的复合索引（SQLite支持条件索引）
		`CREATE INDEX IF NOT EXISTS idx_jobs_status_jobintro ON jobs(status, job_intro) WHERE job_intro IS NOT NULL AND job_intro != '';`,
		// 优化mid查询的索引
		`CREATE INDEX IF NOT EXISTS idx_jobs_mid ON jobs(mid);`,
		// 优化状态过滤查询（SQLite支持条件索引）
		`CREATE INDEX IF NOT EXISTS idx_jobs_status_not_done ON jobs(status) WHERE status NOT IN ('处理完成', '过滤');`,
		// 复合索引：status + mid，用于排序和过滤
		`CREATE INDEX IF NOT EXISTS idx_jobs_status_mid ON jobs(status, mid);`,
	}
	for _, stmt := range schema {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}

func importRowsToDB(ctx context.Context, db *sql.DB, rows []csvRow, source string, batchSize int, trace bool) error {
	if len(rows) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	// 单 writer 事务批量写，避免并发写锁
	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
		if err != nil {
			return err
		}
		// 两类 UPSERT：正常数据（保留旧 status）、过滤数据（强制标记为 '过滤'）
		stmtNorm, err := tx.PrepareContext(ctx, `INSERT INTO jobs(mid, job_intro, source, status, category, updated_at)
            VALUES (?, ?, ?, COALESCE((SELECT NULLIF(status, '过滤') FROM jobs WHERE mid = ?), '待处理'), COALESCE((SELECT category FROM jobs WHERE mid = ?), ''), ?)
            ON CONFLICT(mid) DO UPDATE SET
                job_intro=excluded.job_intro,
                source=excluded.source,
                updated_at=excluded.updated_at,
                status=CASE WHEN jobs.status='过滤' THEN '待处理' ELSE jobs.status END`)
		if err != nil {
			tx.Rollback()
			return err
		}
		stmtFilt, err := tx.PrepareContext(ctx, `INSERT INTO jobs(mid, job_intro, source, status, category, updated_at)
            VALUES (?, ?, ?, '过滤', COALESCE((SELECT category FROM jobs WHERE mid = ?), ''), ?)
            ON CONFLICT(mid) DO UPDATE SET
                job_intro=excluded.job_intro,
                source=excluded.source,
                status='过滤',
                updated_at=excluded.updated_at`)
		if err != nil {
			stmtNorm.Close()
			tx.Rollback()
			return err
		}

		for idx, r := range rows[i:end] {
			mid := strings.TrimSpace(r.MID)
			if mid == "" {
				continue
			}
			if r.Filtered {
				if _, err := stmtFilt.ExecContext(ctx, mid, r.Intro, source, mid, now); err != nil {
					stmtFilt.Close()
					stmtNorm.Close()
					tx.Rollback()
					return err
				}
			} else {
				if _, err := stmtNorm.ExecContext(ctx, mid, r.Intro, source, mid, mid, now); err != nil {
					stmtFilt.Close()
					stmtNorm.Close()
					tx.Rollback()
					return err
				}
			}
			if trace {
				tag := "norm"
				if r.Filtered {
					tag = "filt"
				}
				log.Printf("[DB-INSERT] mid=%s batch=%d item=%d kind=%s", mid, i/batchSize+1, idx+1, tag)
			}
		}
		stmtFilt.Close()
		stmtNorm.Close()
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
