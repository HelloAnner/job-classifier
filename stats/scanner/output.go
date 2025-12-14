package scanner

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"stats/model"
)

// OutputScanner 输出目录扫描器
type OutputScanner struct {
	outputDir string
	cache     map[string]*model.FileCache // key: filePath
	mu        sync.RWMutex
}

// NewOutputScanner 创建扫描器
func NewOutputScanner(outputDir string) *OutputScanner {
	return &OutputScanner{
		outputDir: outputDir,
		cache:     make(map[string]*model.FileCache),
	}
}

// Scan 扫描输出目录，返回各文件的 result 和 ignore 行数
func (s *OutputScanner) Scan(sourceFiles map[string]*SourceInfo) (*model.ScanResult, error) {
	result := &model.ScanResult{
		ScanTime: time.Now(),
		Files:    make([]*model.FileProgress, 0, len(sourceFiles)),
	}

	entries, err := os.ReadDir(s.outputDir)
	if err != nil {
		return nil, err
	}

	// 构建输出目录映射
	outputDirs := make(map[string]string)
	for _, entry := range entries {
		if entry.IsDir() {
			outputDirs[entry.Name()] = filepath.Join(s.outputDir, entry.Name())
		}
	}

	for baseName, srcInfo := range sourceFiles {
		fp := &model.FileProgress{
			FileName:    baseName,
			SourceLines: srcInfo.Lines,
			UpdatedAt:   time.Now(),
		}

		// 查找对应的输出目录
		if outputPath, ok := outputDirs[baseName]; ok {
			fp.ResultLines = s.getLineCount(filepath.Join(outputPath, "result.csv"))
			fp.IgnoreLines = s.getLineCount(filepath.Join(outputPath, "ignore.csv"))
		}

		// 计算进度百分比(减去 header 行)
		processed := fp.ResultLines + fp.IgnoreLines
		if processed > 0 {
			processed-- // result.csv 有 header
		}
		if fp.IgnoreLines > 0 {
			processed-- // ignore.csv 有 header
		}
		
		sourceData := srcInfo.Lines - 1 // 原始文件减去 header
		if sourceData > 0 {
			fp.ProcessedPct = float64(processed) / float64(sourceData) * 100
			if fp.ProcessedPct > 100 {
				fp.ProcessedPct = 100
			}
		}

		result.Files = append(result.Files, fp)
		result.TotalLines += sourceData
		result.ProcessedLines += processed
		result.TotalFiles++

		if fp.ProcessedPct >= 100 {
			result.CompletedFiles++
		}
	}

	if result.TotalLines > 0 {
		result.OverallPct = float64(result.ProcessedLines) / float64(result.TotalLines) * 100
	}

	return result, nil
}

// getLineCount 获取文件行数，使用缓存优化
func (s *OutputScanner) getLineCount(filePath string) int {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0
	}

	s.mu.RLock()
	cached, exists := s.cache[filePath]
	s.mu.RUnlock()

	// 如果修改时间未变，直接返回缓存
	if exists && cached.ModTime.Equal(info.ModTime()) {
		return cached.Lines
	}

	// 重新计算行数
	lines, err := countLines(filePath)
	if err != nil {
		return 0
	}

	s.mu.Lock()
	s.cache[filePath] = &model.FileCache{
		ModTime: info.ModTime(),
		Lines:   lines,
	}
	s.mu.Unlock()

	return lines
}
