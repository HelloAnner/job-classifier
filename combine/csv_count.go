package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"os"
)

func countProcessedRows(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()
	r := csv.NewReader(bufio.NewReader(f))
	r.ReuseRecord = true
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	// 读表头
	if _, err := r.Read(); err != nil {
		if err == io.EOF {
			return 0, nil
		}
		return 0, err
	}
	var n int64
	for {
		if _, err := r.Read(); err != nil {
			if err == io.EOF {
				break
			}
			return n, err
		}
		n++
	}
	return n, nil
}

// 统计 CSV 的数据行数（含表头时减1）；不存在则返回0
func countCSVRows(path string) (int64, error) { return countProcessedRows(path) }
