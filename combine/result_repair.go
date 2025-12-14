package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func loadProcessedSet(path string) (map[string]struct{}, int64, error) {
	set := make(map[string]struct{})
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return set, 0, nil
		}
		return nil, 0, err
	}
	defer f.Close()
	r := csv.NewReader(bufio.NewReader(f))
	r.ReuseRecord = true
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		if err == io.EOF {
			return set, 0, nil
		}
		return set, 0, err
	}
	// 定位 job_id 列
	jobIdx := 0
	for i, h := range header {
		if strings.TrimSpace(h) == "job_id" {
			jobIdx = i
			break
		}
	}
	var n int64
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return set, n, err
		}
		if jobIdx < len(rec) {
			id := strings.TrimSpace(rec[jobIdx])
			if id != "" {
				set[id] = struct{}{}
			}
		}
		n++
	}
	return set, n, nil
}

// rewriteResultCSVDedupAndDrop 会将 result.csv 去重并删除 dropMIDs 中的行（按 job_id/mid）。
// 规则：
// - 保留第一条出现的 mid
// - 删除 mid 为空 / 行字段不足 的记录
// - 删除 dropMIDs 中的 mid（通常为 DB 不存在/已过滤的 mid）
func rewriteResultCSVDedupAndDrop(resultPath string, dropMIDs map[string]struct{}) (map[string]struct{}, int64, error) {
	f, err := os.Open(resultPath)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	r.ReuseRecord = true
	r.LazyQuotes = true
	r.FieldsPerRecord = -1

	header, err := r.Read()
	if err != nil {
		if err == io.EOF {
			return make(map[string]struct{}), 0, nil
		}
		return nil, 0, err
	}

	jobIdx := 0
	for i, h := range header {
		if strings.TrimSpace(h) == "job_id" {
			jobIdx = i
			break
		}
	}

	tmp := filepath.Join(filepath.Dir(resultPath), ".result.csv.tmp")
	outF, err := os.Create(tmp)
	if err != nil {
		return nil, 0, err
	}
	outW := csv.NewWriter(outF)

	if err := outW.Write(header); err != nil {
		outF.Close()
		_ = os.Remove(tmp)
		return nil, 0, err
	}

	seen := make(map[string]struct{}, 1024)
	var kept int64
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			outF.Close()
			_ = os.Remove(tmp)
			return nil, kept, err
		}
		if jobIdx >= len(rec) {
			continue
		}
		mid := strings.TrimSpace(rec[jobIdx])
		if mid == "" {
			continue
		}
		if dropMIDs != nil {
			if _, ok := dropMIDs[mid]; ok {
				continue
			}
		}
		if _, ok := seen[mid]; ok {
			continue
		}
		seen[mid] = struct{}{}
		if err := outW.Write(rec); err != nil {
			outF.Close()
			_ = os.Remove(tmp)
			return nil, kept, err
		}
		kept++
	}

	outW.Flush()
	if err := outW.Error(); err != nil {
		outF.Close()
		_ = os.Remove(tmp)
		return nil, kept, err
	}
	if err := outF.Close(); err != nil {
		_ = os.Remove(tmp)
		return nil, kept, err
	}

	if err := os.Rename(tmp, resultPath); err != nil {
		_ = os.Remove(tmp)
		return nil, kept, fmt.Errorf("rename result.csv failed: %w", err)
	}

	return seen, kept, nil
}
