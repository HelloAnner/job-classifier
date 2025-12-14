package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// ===== 与 cmd/query 等价的查询组件（尽量原样移植，仅对输出路径做最小变更） =====
type JobRecord struct {
	MID        string
	JobIntro   string
	Structured sql.NullString
	Source     string
	Status     string
	Category   string
}

type DatabaseService struct{ db *sql.DB }

func NewDatabaseService(dbPath string) (*DatabaseService, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	return &DatabaseService{db: db}, nil
}
func (s *DatabaseService) Close() error { return s.db.Close() }

// 与 cmd/query 的行规范化逻辑一致
type jobRow struct {
	rowID           int64
	mid, intro, src string
}

func (s *DatabaseService) NormalizeJobIDs(ctx context.Context) (int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT rowid, mid, job_intro, source FROM jobs`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	used := make(map[string]struct{})
	var invalid []jobRow
	for rows.Next() {
		var r jobRow
		if err := rows.Scan(&r.rowID, &r.mid, &r.intro, &r.src); err != nil {
			return 0, err
		}
		clean := strings.TrimSpace(r.mid)
		if isValidJobID(clean) {
			used[clean] = struct{}{}
			continue
		}
		r.mid = clean
		invalid = append(invalid, r)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(invalid) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	stmt, err := tx.PrepareContext(ctx, `UPDATE jobs SET mid = ? WHERE rowid = ?`)
	if err != nil {
		tx.Rollback()
		return 0, err
	}
	defer stmt.Close()
	for _, row := range invalid {
		newID := generateJobID(row, used)
		if _, err := stmt.ExecContext(ctx, newID, row.rowID); err != nil {
			tx.Rollback()
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(invalid), nil
}

func isValidJobID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, ch := range id {
		if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F') {
			continue
		}
		return false
	}
	return true
}
func generateJobID(row jobRow, used map[string]struct{}) string {
	base := fmt.Sprintf("%d|%s|%s|%s", row.rowID, row.mid, row.intro, row.src)
	for salt := 0; ; salt++ {
		data := base
		if salt > 0 {
			data = fmt.Sprintf("%s|%d", base, salt)
		}
		sum := sha256.Sum256([]byte(data))
		candidate := hex.EncodeToString(sum[:16])
		if _, exists := used[candidate]; exists {
			continue
		}
		used[candidate] = struct{}{}
		return candidate
	}
}

func (s *DatabaseService) CountJobs(ctx context.Context) (int, error) {
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM jobs WHERE job_intro IS NOT NULL AND job_intro != ''`)
	var n int
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *DatabaseService) CountAllJobs(ctx context.Context) (int, error) {
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM jobs`)
	var n int
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *DatabaseService) CountFilteredJobs(ctx context.Context) (int, error) {
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM jobs WHERE status='过滤'`)
	var n int
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *DatabaseService) StreamJobs(ctx context.Context) (*sql.Rows, error) {
	// 断点续跑：以 result.csv 为准补漏。
	// 这里返回所有非过滤(status!='过滤')的岗位，允许补齐“DB 已处理但 result.csv 丢失/缺失”的情况；
	// 是否跳过已写入 result.csv 的记录由 processedSet 控制。
	return s.db.QueryContext(ctx, `SELECT mid, job_intro, structured_json, source, status, category
        FROM jobs
        WHERE status != '过滤'
          AND job_intro IS NOT NULL AND job_intro != ''
        ORDER BY mid`)
}

func (s *DatabaseService) CountProcessed(ctx context.Context) (int, error) {
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM jobs WHERE job_intro IS NOT NULL AND job_intro != '' AND status='处理完成'`)
	var n int
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func (s *DatabaseService) MarkProcessed(ctx context.Context, mid string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status='处理完成' WHERE mid=?`, mid)
	return err
}

// MarkProcessedBatch 批量更新状态，显著减少事务开销
func (s *DatabaseService) MarkProcessedBatch(ctx context.Context, mids []string) error {
	if len(mids) == 0 {
		return nil
	}

	// 使用事务批量更新
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// 准备批量更新语句
	stmt, err := tx.PrepareContext(ctx, `UPDATE jobs SET status='处理完成' WHERE mid=?`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// 批量执行
	for _, mid := range mids {
		if _, err := stmt.ExecContext(ctx, mid); err != nil {
			return fmt.Errorf("failed to update mid %s: %w", mid, err)
		}
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// MarkProcessedBatchWithChunk 分块批量更新，避免SQL语句过长
func (s *DatabaseService) MarkProcessedBatchWithChunk(ctx context.Context, mids []string, chunkSize int) error {
	if len(mids) == 0 {
		return nil
	}

	if chunkSize <= 0 {
		chunkSize = 100 // 默认分块大小
	}

	// 分块处理
	for i := 0; i < len(mids); i += chunkSize {
		end := i + chunkSize
		if end > len(mids) {
			end = len(mids)
		}

		chunk := mids[i:end]
		if err := s.MarkProcessedBatch(ctx, chunk); err != nil {
			return fmt.Errorf("failed to process chunk %d-%d: %w", i, end, err)
		}
	}

	return nil
}
