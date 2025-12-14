package main

// 整合程序：
// - 扫描 --input 指定的目录（或单个 CSV 文件）
// - 每个 CSV -> output/<csv_basename> 目录
//   - 导入到 SQLite: output/<csv_basename>/origin.db （表结构兼容 cmd/importer/query）
//   - 过滤岗位描述长度 < 6 的记录到 output/<csv_basename>/ignore.csv
//   - 运行与 cmd/query 相同的查询逻辑（向量 + 知识图谱 + ChromaDB）
//   - 输出分类结果到 output/<csv_basename>/result.csv
// - 在 output/<csv_basename>/count.txt 写入统计：原始、处理、过滤三项，保证 DB/CSV/原始三方一致
// - 导入与查询可并发（文件粒度并发；文件内查询保持与 cmd/query 相同的 worker 数，写 CSV 加锁）

import (
	"context"
	"log"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	opts := parseFlags()
	embModelInUse = opts.embModel
	ctx := context.Background()
	if err := ensureJobGraphDB(ctx, opts); err != nil {
		log.Fatalf("ensure job graph db failed: %v", err)
	}
	files, err := discoverCSV(opts.inputPath)
	if err != nil {
		log.Fatalf("discover csv failed: %v", err)
	}
	if len(files) == 0 {
		log.Println("no csv files found, exit")
		return
	}
	tasks, err := buildTasks(files, opts)
	if err != nil {
		log.Fatalf("build tasks failed: %v", err)
	}
	// 单文件顺序处理：逐个 CSV 依次执行，避免多文件同时占用资源
	log.Printf("发现 %d 个 CSV，将按顺序逐个处理（单文件模式）", len(tasks))

	for _, t := range tasks {
		if err := processSingleCSV(ctx, t, opts); err != nil {
			log.Fatalf("[ERROR] %s: %v", t.baseName, err)
		}
	}
	log.Printf("全部完成，共处理 %d 个 CSV。", len(tasks))
}
