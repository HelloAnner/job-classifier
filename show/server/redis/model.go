package redis

import "time"

// FileProgress 单个文件的处理进度
type FileProgress struct {
	FileName     string    `json:"file_name"`
	SourceLines  int       `json:"source_lines"`
	ResultLines  int       `json:"result_lines"`
	IgnoreLines  int       `json:"ignore_lines"`
	ProcessedPct float64   `json:"processed_pct"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// ScanResult 扫描结果汇总
type ScanResult struct {
	TotalFiles      int             `json:"total_files"`
	CompletedFiles  int             `json:"completed_files"`
	TotalLines      int             `json:"total_lines"`
	ProcessedLines  int             `json:"processed_lines"`
	OverallPct      float64         `json:"overall_pct"`
	Files           []*FileProgress `json:"files"`
	ScanTime        time.Time       `json:"scan_time"`
}
