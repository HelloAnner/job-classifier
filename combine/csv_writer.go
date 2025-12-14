package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// CSV 写入（对单文件结果加锁，避免并发问题）
type CSVWriter struct {
	mu   sync.Mutex
	file *os.File
	w    *csv.Writer
}

func NewCSVWriterTo(path string, appendMode bool) (*CSVWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	var f *os.File
	info, err := os.Stat(path)
	if appendMode && err == nil && !info.IsDir() {
		f, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		w := csv.NewWriter(f)
		return &CSVWriter{file: f, w: w}, nil
	}
	// 新建并写表头
	f, err = os.Create(path)
	if err != nil {
		return nil, err
	}
	w := csv.NewWriter(f)
	headers := []string{"job_id", "job_intro", "similarity_score", "大类", "大类含义", "中类", "中类含义", "小类", "小类含义", "细类职业", "细类含义", "细类主要工作任务"}
	if err := w.Write(headers); err != nil {
		f.Close()
		return nil, err
	}
	return &CSVWriter{file: f, w: w}, nil
}

// 读取可选的人工/上次会话基线（用于精确续跑进度的对齐）。
// 若存在 output/<base>/.resume_base 文件，取其整数值作为建议基线；仅用于显示，不影响处理逻辑。
func readResumeBase(resultPath string) int {
	dir := filepath.Dir(resultPath)
	resumeFile := filepath.Join(dir, ".resume_base")
	data, err := os.ReadFile(resumeFile)
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(data))
	if s == "" {
		return 0
	}
	var n int
	_, err = fmt.Sscanf(s, "%d", &n)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// 原子写入断点进度到 output/<base>/.resume_base
func writeResumeBase(resultPath string, val int) {
	dir := filepath.Dir(resultPath)
	tmp := filepath.Join(dir, ".resume_base.tmp")
	dst := filepath.Join(dir, ".resume_base")
	_ = os.WriteFile(tmp, []byte(fmt.Sprintf("%d\n", val)), 0o644)
	_ = os.Rename(tmp, dst)
}
func (c *CSVWriter) WriteRow(job JobRecord, similarity float64, meta map[string]interface{}) error {
	row := []string{
		cleanCell(job.MID),
		truncate(job.JobIntro, 500),
		fmt.Sprintf("%.4f", similarity),
		safeString(meta, "大类"),
		safeString(meta, "大类含义"),
		safeString(meta, "中类"),
		safeString(meta, "中类含义"),
		safeString(meta, "小类"),
		safeString(meta, "小类含义"),
		safeString(meta, "细类职业"),
		safeString(meta, "细类含义"),
		truncate(safeString(meta, "细类主要工作任务"), 500),
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.w.Write(row)
}
func (c *CSVWriter) Flush() error { c.mu.Lock(); defer c.mu.Unlock(); c.w.Flush(); return c.w.Error() }
func (c *CSVWriter) Close() error { _ = c.Flush(); return c.file.Close() }
