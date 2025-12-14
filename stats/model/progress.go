package model

import "time"

// FileProgress 单个文件的处理进度
type FileProgress struct {
	FileName     string    `json:"file_name"`      // 原始 CSV 文件名
	SourceLines  int       `json:"source_lines"`   // 原始文件行数
	ResultLines  int       `json:"result_lines"`   // result.csv 行数
	IgnoreLines  int       `json:"ignore_lines"`   // ignore.csv 行数
	ProcessedPct float64   `json:"processed_pct"`  // 处理进度百分比
	UpdatedAt    time.Time `json:"updated_at"`     // 最后更新时间
}

// ScanResult 扫描结果汇总
type ScanResult struct {
	TotalFiles      int             `json:"total_files"`      // 总文件数
	CompletedFiles  int             `json:"completed_files"`  // 已完成文件数
	TotalLines      int             `json:"total_lines"`      // 总行数
	ProcessedLines  int             `json:"processed_lines"`  // 已处理行数
	OverallPct      float64         `json:"overall_pct"`      // 整体进度
	Files           []*FileProgress `json:"files"`            // 各文件进度
	ScanTime        time.Time       `json:"scan_time"`        // 扫描时间
}

// FileCache 文件缓存，用于避免重复扫描
type FileCache struct {
	ModTime time.Time
	Lines   int
}
