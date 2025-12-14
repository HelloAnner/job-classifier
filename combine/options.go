package main

import (
	"flag"
	"os"
	"strings"
	"time"
)

// ===== CLI 选项 =====
type options struct {
	inputPath    string        // 目录或单个 CSV
	embModel     string        // embedding 模型
	batchSize    int           // 导入时事务批量
	queryWorkers int           // 查询阶段的并发（默认与 cmd/query 的 10 一致）
	embBatch     int           // embedding 批量大小（/api/embed input[]）
	embTimeout   time.Duration // embedding HTTP 超时（批量时可能较长）
	chromaTopK   int           // Chroma 查询 topK（越小越快，需验证一致率）
	trace        bool          // 逐条记录阶段日志
	limitJobs    int           // 仅处理前 N 条（用于联调验证准确性）
	clean        bool          // 强制清空已存在的输出目录后重跑（默认增量续跑）
	minSim       float64       // 最低相似度阈值（仅用于日志提示；匹配始终取最优1条）
}

func parseFlags() options {
	var opts options
	envModel := os.Getenv("EMB_MODEL")
	if strings.TrimSpace(envModel) == "" {
		envModel = defaultEmbModel
	}
	flag.StringVar(&opts.inputPath, "input", "data/51job", "输入目录或单个CSV（例如 data/51job 或 data/51job/51job_2021.csv）")
	flag.StringVar(&opts.embModel, "emb-model", envModel, "embedding 模型名称（默认取环境 EMB_MODEL，空则使用内置默认）")
	flag.IntVar(&opts.batchSize, "batch", 2000, "导入 SQLite 的提交批量")
	flag.IntVar(&opts.queryWorkers, "query-workers", 10, "每个CSV在查询阶段的并发数")
	flag.IntVar(&opts.embBatch, "emb-batch", 256, "embedding 批量大小（使用 /api/embed input[]；越大吞吐越高但单次延迟越大）")
	flag.DurationVar(&opts.embTimeout, "emb-timeout", 300*time.Second, "embedding 请求超时（批量模式下建议>=300s）")
	flag.IntVar(&opts.chromaTopK, "chroma-topk", 5, "Chroma 查询返回 topK（默认5；仅取 top1+图纠偏）")
	flag.BoolVar(&opts.trace, "trace", false, "开启逐条记录阶段日志（用于准确性排查）")
	flag.IntVar(&opts.limitJobs, "limit-jobs", 0, "仅处理前 N 条岗位（0 表示全部）")
	flag.BoolVar(&opts.clean, "clean", false, "存在输出目录时先清空再重跑（默认增量续跑）")
	flag.Float64Var(&opts.minSim, "min-sim", 0.0, "最低相似度阈值（仅日志提示，不拒绝落表；默认0.0）")
	flag.Parse()
	if opts.batchSize < 1 {
		opts.batchSize = 1
	}
	if opts.queryWorkers < 1 {
		opts.queryWorkers = 1
	}
	if opts.embBatch < 1 {
		opts.embBatch = 1
	}
	if opts.chromaTopK < 1 {
		opts.chromaTopK = 1
	}
	if strings.TrimSpace(opts.embModel) == "" {
		opts.embModel = defaultEmbModel
	}
	return opts
}
