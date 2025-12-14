package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type fileTask struct {
	csvPath    string
	baseName   string
	source     string
	outDir     string
	resultPath string
	ignorePath string
	countPath  string // output/<base>/count.txt
	dbPath     string
}

func isGlobPattern(s string) bool {
	return strings.ContainsAny(s, "*?[")
}

func discoverCSV(inputPath string) ([]string, error) {
	// 1) 直接存在：目录或文件
	if fi, err := os.Stat(inputPath); err == nil {
		if fi.IsDir() {
			var files []string
			entries, err := os.ReadDir(inputPath)
			if err != nil {
				return nil, err
			}
			for _, e := range entries {
				if !e.IsDir() && strings.HasSuffix(strings.ToLower(e.Name()), ".csv") {
					files = append(files, filepath.Join(inputPath, e.Name()))
				}
			}
			sort.Strings(files)
			return files, nil
		}
		if strings.HasSuffix(strings.ToLower(inputPath), ".csv") {
			return []string{inputPath}, nil
		}
		return nil, fmt.Errorf("input is neither a directory nor a CSV file: %s", inputPath)
	}

	// 2) 不存在：按通配符（glob）处理，例如 data/51job/51job_2024_*.csv 或 data/51job/*2024*.csv
	if isGlobPattern(inputPath) {
		matched, err := filepath.Glob(inputPath)
		if err != nil {
			return nil, fmt.Errorf("invalid glob pattern: %w", err)
		}
		if len(matched) == 0 {
			return nil, fmt.Errorf("glob matched no files: %s", inputPath)
		}
		var files []string
		for _, p := range matched {
			if fi, err := os.Stat(p); err == nil && !fi.IsDir() && strings.HasSuffix(strings.ToLower(p), ".csv") {
				files = append(files, p)
			}
		}
		if len(files) == 0 {
			return nil, fmt.Errorf("glob produced no CSV files: %s", inputPath)
		}
		sort.Strings(files)
		log.Printf("[MATCH] glob=%s files=%d", inputPath, len(files))
		return files, nil
	}

	return nil, fmt.Errorf("input path not found and not a glob: %s", inputPath)
}

func buildTasks(csvFiles []string, opts options) ([]fileTask, error) {
	var tasks []fileTask
	for _, p := range csvFiles {
		base := strings.TrimSuffix(filepath.Base(p), filepath.Ext(p))
		outDir := filepath.Join("output", base)
		// 续跑：默认不清空；若 --clean 指定，则清空重建
		if fi, err := os.Stat(outDir); err == nil && fi.IsDir() {
			if opts.clean {
				log.Printf("[CLEAN] remove existing output folder: %s", outDir)
				if err := os.RemoveAll(outDir); err != nil {
					return nil, fmt.Errorf("failed to clean output folder %s: %w", outDir, err)
				}
			}
		}
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			return nil, err
		}
		src := filepath.Base(filepath.Dir(p)) // 上一级目录名作为 source，例如 51job
		resultPath := filepath.Join(outDir, "result.csv")
		ignorePath := filepath.Join(outDir, "ignore.csv")
		cleanupLegacyCountDir(base)
		countPath := filepath.Join(outDir, "count.txt")
		dbPath := filepath.Join(outDir, "origin.db")
		tasks = append(tasks, fileTask{csvPath: p, baseName: base, source: src, outDir: outDir, resultPath: resultPath, ignorePath: ignorePath, countPath: countPath, dbPath: dbPath})
	}
	return tasks, nil
}

// cleanupLegacyCountDir 删除 data/<base> 下仅残留 count.txt 的历史目录（count.txt 已迁移到 output）。
func cleanupLegacyCountDir(base string) {
	legacyDir := filepath.Join("data", base)
	entries, err := os.ReadDir(legacyDir)
	if err != nil {
		return
	}
	keep := false
	for _, e := range entries {
		name := e.Name()
		if name == "count.txt" || name == ".DS_Store" {
			continue
		}
		keep = true
		break
	}
	if keep {
		return
	}
	_ = os.RemoveAll(legacyDir)
}

func cleanupAllCountFilesInOutput() {
	root := "output"
	if _, err := os.Stat(root); err != nil {
		return
	}
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if strings.EqualFold(d.Name(), "count.txt") {
			_ = os.Remove(path)
		}
		return nil
	})
}
