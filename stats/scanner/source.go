package scanner

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// SourceInfo 原始文件信息
type SourceInfo struct {
	FileName string // 文件名(不含扩展名)
	Lines    int    // 行数
}

// ScanSource 扫描原始目录，返回所有 CSV 文件信息
func ScanSource(sourceDir string) (map[string]*SourceInfo, error) {
	result := make(map[string]*SourceInfo)

	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".csv") {
			continue
		}

		filePath := filepath.Join(sourceDir, name)
		lines, err := countLines(filePath)
		if err != nil {
			continue
		}

		// 去掉 .csv 后缀作为 key
		baseName := strings.TrimSuffix(name, filepath.Ext(name))
		result[baseName] = &SourceInfo{
			FileName: baseName,
			Lines:    lines,
		}
	}

	return result, nil
}

// countLines 计算文件行数
func countLines(filePath string) (int, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// 增大缓冲区以处理长行
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	count := 0
	for scanner.Scan() {
		count++
	}
	return count, scanner.Err()
}
