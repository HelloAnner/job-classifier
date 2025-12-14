package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
)

// regenerateIgnoreCSV 从数据库重新生成 ignore.csv（status='过滤'）
func regenerateIgnoreCSV(ctx context.Context, t fileTask) error {
	dbSvc, err := NewDatabaseService(t.dbPath)
	if err != nil {
		return fmt.Errorf("open database failed: %w", err)
	}
	defer dbSvc.Close()

	// 查询状态为'过滤'的记录
	rows, err := dbSvc.db.QueryContext(ctx, `SELECT mid, job_intro FROM jobs WHERE status='过滤'`)
	if err != nil {
		return fmt.Errorf("query filtered jobs failed: %w", err)
	}
	defer rows.Close()

	// 创建ignore.csv
	igf, err := os.Create(t.ignorePath)
	if err != nil {
		return fmt.Errorf("create ignore.csv failed: %w", err)
	}
	defer igf.Close()

	igw := csv.NewWriter(igf)
	if err := igw.Write([]string{"mid", "job_intro"}); err != nil {
		return err
	}

	count := 0
	for rows.Next() {
		var mid, intro string
		if err := rows.Scan(&mid, &intro); err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}
		if err := igw.Write([]string{mid, intro}); err != nil {
			return fmt.Errorf("write row failed: %w", err)
		}
		count++
	}

	igw.Flush()
	if err := igw.Error(); err != nil {
		return fmt.Errorf("flush csv failed: %w", err)
	}

	log.Printf("[IGNORE-REGEN] %s: regenerated ignore.csv with %d records", t.baseName, count)
	return nil
}
