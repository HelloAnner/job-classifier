package scanner

import (
	"os"
	"path/filepath"
	"strings"
)

// SourceInfo 原始文件信息
type SourceInfo struct {
	FileName string // 文件名(不含扩展名)
	Lines    int    // 数据行数（CSV records，排除 header）
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
		lines, err := countCSVDataRows(filePath, CSVCountSpec{HasHeader: true, MinCols: 2, KeyCol: 0})
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
