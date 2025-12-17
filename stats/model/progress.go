package model

//
// 进度与汇总模型定义
//
// 说明：
// - 本文件仅定义数据结构，不包含业务逻辑。
// - 为支持多机并发上报 Redis 的“按文件粒度合并”，新增了 FileProgress.HasOutput 字段，
//   仅用于本地合并策略判断，JSON 序列化时忽略，不影响现有前端/消费方兼容性。
//
// 作者: Anner
// 创建时间: 2025/12/17

import "time"

// FileProgress 单个文件的处理进度
type FileProgress struct {
    FileName     string    `json:"file_name"`     // 原始 CSV 文件名
    SourceLines  int       `json:"source_lines"`  // 原始文件行数
    ResultLines  int       `json:"result_lines"`  // result.csv 行数
    IgnoreLines  int       `json:"ignore_lines"`  // ignore.csv 行数
    ProcessedPct float64   `json:"processed_pct"` // 处理进度百分比
    UpdatedAt    time.Time `json:"updated_at"`    // 最后更新时间
    HasOutput    bool      `json:"-"`             // 本机是否观察到输出（本地合并用；不写入 Redis JSON）
    Authoritative bool     `json:"-"`             // 是否来自权威来源（count.txt）；合并时优先
}

// ScanResult 扫描结果汇总
type ScanResult struct {
	TotalFiles     int             `json:"total_files"`     // 总文件数
	CompletedFiles int             `json:"completed_files"` // 已完成文件数
	TotalLines     int             `json:"total_lines"`     // 总行数
	ProcessedLines int             `json:"processed_lines"` // 已处理行数
	OverallPct     float64         `json:"overall_pct"`     // 整体进度
	Files          []*FileProgress `json:"files"`           // 各文件进度
	ScanTime       time.Time       `json:"scan_time"`       // 扫描时间
}

// FileCache 文件缓存，用于避免重复扫描
type FileCache struct {
	ModTime time.Time
	Lines   int
	Size    int64
}
