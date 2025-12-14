package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"os"
	"strings"
)

// 统计原始 CSV 的有效数据行数（与导入逻辑一致：mid 非空且至少 2 列）。
// 返回：total=有效数据行数；filtered=岗位描述长度<6 的行数（会进入 ignore.csv）。
func countOriginStats(csvPath string) (total int64, filtered int64, err error) {
	f, err := os.Open(csvPath)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	r.ReuseRecord = true
	r.LazyQuotes = true
	r.FieldsPerRecord = -1

	// header
	if _, err := r.Read(); err != nil {
		if err == io.EOF {
			return 0, 0, nil
		}
		return 0, 0, err
	}

	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, 0, err
		}
		if len(rec) < 2 {
			continue
		}
		mid := strings.TrimSpace(rec[0])
		intro := strings.TrimSpace(rec[1])
		if mid == "" {
			continue
		}
		total++
		if introRuneLen(intro) < 6 {
			filtered++
		}
	}
	return total, filtered, nil
}
