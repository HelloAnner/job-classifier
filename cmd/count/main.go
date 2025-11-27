package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type FileCount struct {
	FilePath string
	Platform string
	Count    int
}

type PlatformStats struct {
	TotalFiles int
	TotalRows  int
}

func main() {
	startTime := time.Now()

	// Define platforms to process
	platforms := []string{"51job", "58tong", "BO", "zhilian"}

	// Channel for collecting results
	results := make(chan FileCount, 100)

	// WaitGroup to wait for all goroutines
	var wg sync.WaitGroup

	// Process each platform concurrently
	for _, platform := range platforms {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			processPlatform(p, results)
		}(platform)
	}

	// Close results channel when all goroutines are done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect all results
	var allCounts []FileCount
	platformStats := make(map[string]PlatformStats)

	for result := range results {
		allCounts = append(allCounts, result)

		// Update platform statistics
		stats := platformStats[result.Platform]
		stats.TotalFiles++
		stats.TotalRows += result.Count
		platformStats[result.Platform] = stats
	}

	// Sort results by platform and filename
	sort.Slice(allCounts, func(i, j int) bool {
		if allCounts[i].Platform != allCounts[j].Platform {
			return allCounts[i].Platform < allCounts[j].Platform
		}
		return allCounts[i].FilePath < allCounts[j].FilePath
	})

	// Generate markdown report
	generateMarkdownReport(allCounts, platformStats)

	elapsed := time.Since(startTime)
	fmt.Printf("统计完成! 处理了 %d 个文件，耗时 %v\n", len(allCounts), elapsed)

	// Print summary
	fmt.Println("\n平台统计汇总:")
	for _, platform := range platforms {
		if stats, exists := platformStats[platform]; exists {
			fmt.Printf("  %s: %d 个文件, %d 行数据\n", platform, stats.TotalFiles, stats.TotalRows)
		}
	}
}

func processPlatform(platform string, results chan<- FileCount) {
	dataDir := filepath.Join("../../data", platform)

	// Check if directory exists
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		log.Printf("警告: 目录 %s 不存在", dataDir)
		return
	}

	// Find all CSV files in the directory
	pattern := filepath.Join(dataDir, "*.csv")
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Printf("错误: 无法读取 %s 目录: %v", dataDir, err)
		return
	}

	if len(files) == 0 {
		log.Printf("警告: 在 %s 目录中未找到CSV文件", dataDir)
		return
	}

	// Process each file
	for _, file := range files {
		count, err := countCSVRows(file)
		if err != nil {
			log.Printf("错误: 处理文件 %s 失败: %v", file, err)
			continue
		}

		results <- FileCount{
			FilePath: file,
			Platform: platform,
			Count:    count,
		}
	}
}

func countCSVRows(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Skip header row
	_, err = reader.Read()
	if err != nil {
		if err == io.EOF {
			return 0, nil // Empty file
		}
		return 0, fmt.Errorf("读取表头失败: %w", err)
	}

	// Count data rows
	rowCount := 0
	for {
		_, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return rowCount, fmt.Errorf("读取数据行失败: %w", err)
		}
		rowCount++
	}

	return rowCount, nil
}

func generateMarkdownReport(counts []FileCount, platformStats map[string]PlatformStats) {
	file, err := os.Create("count.md")
	if err != nil {
		log.Fatalf("创建count.md文件失败: %v", err)
	}
	defer file.Close()

	// Write header
	file.WriteString("# 招聘平台数据统计\n\n")
	file.WriteString("| 文件名称 | 平台 | 数量 |\n")
	file.WriteString("|----------|------|------|\n")

	// Write individual file counts
	for _, count := range counts {
		line := fmt.Sprintf("| %s | %s | %d |\n",
			count.FilePath,
			count.Platform,
			count.Count)
		file.WriteString(line)
	}

	// Write summary section
	file.WriteString("\n## 平台汇总统计\n\n")
	file.WriteString("| 平台 | 文件数量 | 总数据行数 |\n")
	file.WriteString("|------|----------|------------|\n")

	// Sort platforms for consistent output
	var platforms []string
	for platform := range platformStats {
		platforms = append(platforms, platform)
	}
	sort.Strings(platforms)

	for _, platform := range platforms {
		stats := platformStats[platform]
		line := fmt.Sprintf("| %s | %d | %d |\n",
			platform,
			stats.TotalFiles,
			stats.TotalRows)
		file.WriteString(line)
	}

	// Calculate and write total
	totalFiles := 0
	totalRows := 0
	for _, stats := range platformStats {
		totalFiles += stats.TotalFiles
		totalRows += stats.TotalRows
	}

	file.WriteString("\n## 总体统计\n\n")
	file.WriteString(fmt.Sprintf("- **总文件数**: %d\n", totalFiles))
	file.WriteString(fmt.Sprintf("- **总数据行数**: %d\n", totalRows))
	file.WriteString(fmt.Sprintf("- **统计时间**: %s\n", time.Now().Format("2006-01-02 15:04:05")))
}