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
            // 优先使用 count.txt 作为“已完成且对账通过”的权威来源（combine 会严格校验后再写入）
            countPath := filepath.Join(outputPath, "count.txt")
            if ci, err := ReadCountInfo(countPath); err == nil && ci.Valid() {
                fp.SourceLines = ci.Total
                fp.ResultLines = ci.Processed
                fp.IgnoreLines = ci.Filtered
                fp.ProcessedPct = 100
                fp.HasOutput = true
            } else {
                resultCount := s.getDataRowCount(filepath.Join(outputPath, "result.csv"), true)
                ignoreCount := s.getDataRowCount(filepath.Join(outputPath, "ignore.csv"), true)
                fp.ResultLines = resultCount
                fp.IgnoreLines = ignoreCount
                if resultCount > 0 || ignoreCount > 0 {
                    fp.HasOutput = true
                }
            }
        }

		// 计算进度百分比（本模块已按 CSV record 计数，天然排除 header；并且处理完成必须满足 source == result + ignore）
		processed := fp.ResultLines + fp.IgnoreLines
		if fp.SourceLines > 0 && fp.ProcessedPct < 100 {
			if processed == fp.SourceLines {
				fp.ProcessedPct = 100
			} else {
				fp.ProcessedPct = float64(processed) / float64(fp.SourceLines) * 100
				// 避免“超 100% 但实际不对账”被前端误判为已完成
				if fp.ProcessedPct > 99.9 {
					fp.ProcessedPct = 99.9
				}
				if fp.ProcessedPct < 0 {
					fp.ProcessedPct = 0
				}
			}
		}

		result.Files = append(result.Files, fp)
		result.TotalLines += fp.SourceLines
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

// getDataRowCount 获取 CSV 数据行数（CSV records，排除 header），使用缓存优化
func (s *OutputScanner) getDataRowCount(filePath string, hasHeader bool) int {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0
	}

	s.mu.RLock()
	cached, exists := s.cache[filePath]
	s.mu.RUnlock()

	// 如果修改时间未变，直接返回缓存
	if exists && cached.ModTime.Equal(info.ModTime()) && cached.Size == info.Size() {
		return cached.Lines
	}

	lines := 0
	if filepath.Base(filePath) == "result.csv" {
		// result.csv 每条记录是单行（combine 会清洗掉字段内换行），可用增量统计换行数，保证 3 秒刷新。
		if exists && info.Size() >= cached.Size {
			delta, err := countNewlinesFromOffset(filePath, cached.Size)
			if err == nil {
				lines = cached.Lines + delta
			}
		}
		if lines == 0 {
			nl, err := countNewlinesFromOffset(filePath, 0)
			if err != nil {
				return 0
			}
			if hasHeader && nl > 0 {
				lines = nl - 1
			} else {
				lines = nl
			}
		}
	} else {
		// 重新计算数据行数（对 ignore：可能存在字段内换行，只能用 CSV 解析按 record 统计）
		n, err := countCSVDataRows(filePath, CSVCountSpec{HasHeader: hasHeader, MinCols: 1, KeyCol: 0})
		if err != nil {
			return 0
		}
		lines = n
	}

	s.mu.Lock()
	s.cache[filePath] = &model.FileCache{
		ModTime: info.ModTime(),
		Lines:   lines,
		Size:    info.Size(),
	}
	s.mu.Unlock()

	return lines
}
