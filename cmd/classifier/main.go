package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/xuri/excelize/v2"
)

// 配置项，便于命令行传参。
type options struct {
	inputPath  string
	outputPath string
	sheetName  string
	headerRow  int
}

func main() {
	opts := parseFlags()

	records, err := extractRecords(opts)
	if err != nil {
		log.Fatalf("解析 Excel 失败: %v", err)
	}

	if err := writeJSON(opts.outputPath, records); err != nil {
		log.Fatalf("写入 JSON 失败: %v", err)
	}
	log.Printf("完成，共导出 %d 条记录 -> %s", len(records), opts.outputPath)
}

func parseFlags() options {
	var opts options
	flag.StringVar(&opts.inputPath, "input", "中国职业分类大典_去掉一些大类_去掉99(1).xlsx", "待解析的 Excel 文件路径")
	flag.StringVar(&opts.outputPath, "output", "data/classification.json", "输出 JSON 文件路径，支持 - 表示标准输出")
	flag.StringVar(&opts.sheetName, "sheet", "", "指定工作表名称，不填默认取第一个")
	flag.IntVar(&opts.headerRow, "header", 1, "表头所在行号（从 1 开始）")
	flag.Parse()
	return opts
}

func extractRecords(opts options) ([]map[string]string, error) {
	if opts.inputPath == "" {
		return nil, errors.New("input 不能为空")
	}
	wb, err := excelize.OpenFile(opts.inputPath)
	if err != nil {
		return nil, err
	}
	defer wb.Close()

	sheet := opts.sheetName
	if sheet == "" {
		sheets := wb.GetSheetList()
		if len(sheets) == 0 {
			return nil, errors.New("Excel 中没有任何工作表")
		}
		sheet = sheets[0]
	}

	rows, err := wb.GetRows(sheet)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("工作表 %s 为空", sheet)
	}
	headerIdx := opts.headerRow - 1
	if headerIdx < 0 || headerIdx >= len(rows) {
		return nil, fmt.Errorf("header 行 %d 越界，总行数 %d", opts.headerRow, len(rows))
	}

	headers := normalizeHeaders(rows[headerIdx])
	carry := make([]string, len(headers))
	var records []map[string]string

	for rowIdx := headerIdx + 1; rowIdx < len(rows); rowIdx++ {
		row := rows[rowIdx]
		if isRowEmpty(row) {
			continue
		}
		if len(row) < len(headers) {
			row = append(row, make([]string, len(headers)-len(row))...)
		}
		entry := make(map[string]string, len(headers)+1)
		for col := range headers {
			val := ""
			if col < len(row) {
				val = row[col]
			}
			if val == "" {
				val = carry[col]
			} else {
				carry[col] = val
			}
			entry[headers[col]] = val
		}
		entry["源行号"] = strconv.Itoa(rowIdx + 1)
		records = append(records, entry)
	}
	return records, nil
}

func normalizeHeaders(row []string) []string {
	headers := make([]string, len(row))
	seen := make(map[string]int)
	for i, cell := range row {
		name := strings.TrimSpace(cell)
		if name == "" {
			name = fmt.Sprintf("未命名列%d", i+1)
		}
		if count, ok := seen[name]; ok {
			count++
			seen[name] = count
			name = fmt.Sprintf("%s_%d", name, count)
		} else {
			seen[name] = 0
		}
		headers[i] = name
	}
	return headers
}

func isRowEmpty(row []string) bool {
	for _, cell := range row {
		if strings.TrimSpace(cell) != "" {
			return false
		}
	}
	return true
}

func writeJSON(path string, records []map[string]string) error {
	var f *os.File
	var err error
	if path == "-" {
		f = os.Stdout
	} else {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		f, err = os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(records)
}
