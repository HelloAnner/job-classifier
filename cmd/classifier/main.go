package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	defaultBaseURL   = "https://ark.cn-beijing.volces.com/api/v3"
	defaultModelName = "deepseek-v3-250324"
)

type options struct {
	inputPath   string
	outputPath  string
	workers     int
	force       bool
	limit       int
	temperature float64
	maxTokens   int
	timeout     time.Duration
	maxRetries  int
	baseURL     string
	apiKey      string
	model       string
}

func main() {
	opts := parseFlags()

	records, err := loadRecords(opts.inputPath)
	if err != nil {
		log.Fatalf("读取 JSON 失败: %v", err)
	}

	client, err := newLLMClient(opts)
	if err != nil {
		log.Fatalf("初始化 LLM 客户端失败: %v", err)
	}

	workList := buildWorkList(records, opts)
	if len(workList) == 0 {
		log.Printf("没有需要处理的记录，force=%v", opts.force)
		if err := writeRecords(opts.outputPath, records); err != nil {
			log.Fatalf("写入 JSON 失败: %v", err)
		}
		log.Println("输出文件未发生变化。")
		return
	}

	log.Printf("总记录数: %d, 待增强: %d, 并发: %d", len(records), len(workList), opts.workers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results := processRecords(ctx, client, workList, opts)

	var failed []error
	total := len(workList)
	processed := 0
	success := 0
	for res := range results {
		processed++
		if res.err != nil {
			failed = append(failed, res.err)
			log.Printf("失败: %v", res.err)
			continue
		}
		applyLLMResult(records[res.index], res.payload)
		success++
		if processed%50 == 0 || processed == total {
			log.Printf("进度: %d/%d (成功 %d)", processed, total, success)
		}
	}

	if len(failed) > 0 {
		for _, ferr := range failed {
			log.Printf("ERROR: %v", ferr)
		}
		log.Fatalf("共有 %d 条记录增强失败，请处理后重试", len(failed))
	}

	if err := writeRecords(opts.outputPath, records); err != nil {
		log.Fatalf("写入 JSON 失败: %v", err)
	}

	log.Printf("完成，已增强 %d 条记录 -> %s", len(workList), pathOrStdout(opts.outputPath))
}

func parseFlags() options {
	var opts options

	defaultWorkers := runtime.NumCPU()
	if defaultWorkers < 2 {
		defaultWorkers = 2
	}

	flag.StringVar(&opts.inputPath, "input", "classification.json", "输入 JSON 文件路径")
	flag.StringVar(&opts.outputPath, "output", "classification.json", "输出 JSON 文件路径，- 表示标准输出")
	flag.IntVar(&opts.workers, "workers", defaultWorkers/2, "并发请求数量")
	flag.BoolVar(&opts.force, "force", false, "无论是否已有 LLM 描述都重新生成")
	flag.IntVar(&opts.limit, "limit", 0, "仅处理前 N 条待增强记录，0 表示全部")
	flag.Float64Var(&opts.temperature, "temperature", 0.4, "模型温度，范围 0~1")
	flag.IntVar(&opts.maxTokens, "max-tokens", 800, "每次补全的最大 tokens")
	flag.DurationVar(&opts.timeout, "timeout", 60*time.Second, "单次请求超时时间")
	flag.IntVar(&opts.maxRetries, "retries", 3, "调用失败后的最大重试次数")

	flag.StringVar(&opts.baseURL, "base-url", envOrDefault("LLM_BASE_URL", defaultBaseURL), "LLM 服务基础地址")
	flag.StringVar(&opts.apiKey, "api-key", os.Getenv("LLM_API_KEY"), "LLM API Key")
	flag.StringVar(&opts.model, "model", envOrDefault("LLM_MODEL_NAME", defaultModelName), "LLM 模型名")

	flag.Parse()

	if opts.workers <= 0 {
		opts.workers = 1
	}
	if opts.apiKey == "" {
		log.Fatal("LLM_API_KEY 不能为空，可通过环境变量或 -api-key 指定")
	}

	return opts
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func loadRecords(path string) ([]map[string]interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var records []map[string]interface{}
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}
	return records, nil
}

type workItem struct {
	index int
	rec   classificationRecord
}

type classificationRecord struct {
	Index          int
	BigCategory    string
	BigMeaning     string
	MiddleCategory string
	MiddleMeaning  string
	SmallCategory  string
	SmallMeaning   string
	FineCode       string
	FineMeaning    string
	Tasks          string
	IncludedJobs   string
	SourceRow      string
}

func (r classificationRecord) label() string {
	if r.FineCode != "" {
		return r.FineCode
	}
	if r.SmallCategory != "" {
		return r.SmallCategory
	}
	if r.MiddleCategory != "" {
		return r.MiddleCategory
	}
	if r.BigCategory != "" {
		return r.BigCategory
	}
	return fmt.Sprintf("记录#%d", r.Index+1)
}

func buildWorkList(records []map[string]interface{}, opts options) []workItem {
	var list []workItem
	for idx, raw := range records {
		if !opts.force && hasLLMOverview(raw) {
			continue
		}
		rec := mapToClassification(idx, raw)
		list = append(list, workItem{index: idx, rec: rec})
	}

	if opts.limit > 0 && len(list) > opts.limit {
		list = list[:opts.limit]
	}

	return list
}

func hasLLMOverview(raw map[string]interface{}) bool {
	v, ok := raw["LLM岗位概述"]
	if !ok {
		return false
	}
	return strings.TrimSpace(fmt.Sprint(v)) != ""
}

func mapToClassification(idx int, raw map[string]interface{}) classificationRecord {
	return classificationRecord{
		Index:          idx,
		BigCategory:    asString(raw, "大类"),
		BigMeaning:     asString(raw, "大类含义"),
		MiddleCategory: asString(raw, "中类"),
		MiddleMeaning:  asString(raw, "中类含义"),
		SmallCategory:  asString(raw, "小类"),
		SmallMeaning:   asString(raw, "小类含义"),
		FineCode:       asString(raw, "细类（职业）"),
		FineMeaning:    asString(raw, "细类含义"),
		Tasks:          asString(raw, "细类主要工作任务"),
		IncludedJobs:   asString(raw, "细类包含工种"),
		SourceRow:      asString(raw, "源行号"),
	}
}

func asString(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case fmt.Stringer:
		return strings.TrimSpace(t.String())
	default:
		return strings.TrimSpace(fmt.Sprint(t))
	}
}

func pathOrStdout(path string) string {
	if path == "-" {
		return "stdout"
	}
	return path
}

type resultItem struct {
	index   int
	payload llmPayload
	err     error
}

func processRecords(ctx context.Context, client *llmClient, workList []workItem, opts options) <-chan resultItem {
	jobs := make(chan workItem)
	results := make(chan resultItem)

	var wg sync.WaitGroup
	workerCount := opts.workers
	if workerCount < 1 {
		workerCount = 1
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for job := range jobs {
				payload, err := client.Enrich(ctx, job.rec)
				if err != nil {
					results <- resultItem{index: job.index, err: fmt.Errorf("%s: %w", job.rec.label(), err)}
					continue
				}
				results <- resultItem{index: job.index, payload: payload}
			}
		}(i + 1)
	}

	go func() {
		for _, job := range workList {
			jobs <- job
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}

type llmPayload struct {
	Overview         string   `json:"LLM岗位概述"`
	Responsibilities []string `json:"LLM典型职责"`
	Keywords         []string `json:"LLM关联关键词"`
	Titles           []string `json:"LLM典型岗位"`
}

func (p llmPayload) validate() error {
	if strings.TrimSpace(p.Overview) == "" {
		return errors.New("LLM岗位概述 为空")
	}
	if len(p.Responsibilities) == 0 {
		return errors.New("LLM典型职责 为空")
	}
	if len(p.Keywords) == 0 {
		return errors.New("LLM关联关键词 为空")
	}
	if len(p.Titles) == 0 {
		return errors.New("LLM典型岗位 为空")
	}
	return nil
}

func applyLLMResult(raw map[string]interface{}, payload llmPayload) {
	raw["LLM岗位概述"] = payload.Overview
	raw["LLM典型职责"] = payload.Responsibilities
	raw["LLM关联关键词"] = payload.Keywords
	raw["LLM典型岗位"] = payload.Titles
}

type llmClient struct {
	baseURL     string
	apiKey      string
	model       string
	httpClient  *http.Client
	temperature float64
	maxTokens   int
	maxRetries  int
}

func newLLMClient(opts options) (*llmClient, error) {
	baseURL := strings.TrimRight(opts.baseURL, "/")
	if baseURL == "" {
		baseURL = defaultBaseURL
	}

	return &llmClient{
		baseURL: baseURL,
		apiKey:  opts.apiKey,
		model:   opts.model,
		httpClient: &http.Client{
			Timeout: opts.timeout,
		},
		temperature: opts.temperature,
		maxTokens:   opts.maxTokens,
		maxRetries:  opts.maxRetries,
	}, nil
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model          string        `json:"model"`
	Messages       []chatMessage `json:"messages"`
	Temperature    float64       `json:"temperature"`
	MaxTokens      int           `json:"max_tokens"`
	ResponseFormat responseFmt   `json:"response_format"`
}

type responseFmt struct {
	Type string `json:"type"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
}

func (c *llmClient) Enrich(ctx context.Context, rec classificationRecord) (llmPayload, error) {
	prompt := buildPrompt(rec)

	reqBody := chatRequest{
		Model: c.model,
		Messages: []chatMessage{
			{
				Role:    "system",
				Content: "你是一名资深岗位分类专家，需要将结构化职业分类信息扩写成更丰富的自然语言描述，以便和企业的真实招聘 JD 做语义匹配。请严格输出 JSON 数据。",
			},
			{
				Role:    "user",
				Content: prompt,
			},
		},
		Temperature:    c.temperature,
		MaxTokens:      c.maxTokens,
		ResponseFormat: responseFmt{Type: "json_object"},
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return llmPayload{}, err
	}

	endpoint := c.baseURL + "/chat/completions"

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return llmPayload{}, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+c.apiKey)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("status %d: %s", resp.StatusCode, truncate(string(respBody), 200))
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		var cr chatResponse
		if err := json.Unmarshal(respBody, &cr); err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}
		if len(cr.Choices) == 0 {
			lastErr = errors.New("LLM 返回空 choices")
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		content := strings.TrimSpace(cr.Choices[0].Message.Content)
		payload, err := decodePayload(content)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}
		if err := payload.validate(); err != nil {
			lastErr = err
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}
		return payload, nil
	}

	return llmPayload{}, fmt.Errorf("调用 LLM 失败: %w", lastErr)
}

func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit] + "..."
}

func decodePayload(content string) (llmPayload, error) {
	trimmed := strings.TrimSpace(content)

	if strings.HasPrefix(trimmed, "```") {
		trimmed = strings.TrimPrefix(trimmed, "```")
		trimmed = strings.TrimSpace(trimmed)
		if strings.HasPrefix(strings.ToLower(trimmed), "json") {
			trimmed = strings.TrimSpace(trimmed[4:])
		}
		if idx := strings.LastIndex(trimmed, "```"); idx >= 0 {
			trimmed = trimmed[:idx]
		}
		trimmed = strings.TrimSpace(trimmed)
	}

	var payload llmPayload
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return llmPayload{}, fmt.Errorf("解析 LLM 返回 JSON 失败: %w", err)
	}
	return payload, nil
}

func buildPrompt(rec classificationRecord) string {
	var sb strings.Builder
	sb.WriteString("请参考以下职业分类信息，为该岗位生成更丰富的自然语言描述：\n")
	appendLine(&sb, "大类", rec.BigCategory)
	appendLine(&sb, "大类含义", rec.BigMeaning)
	appendLine(&sb, "中类", rec.MiddleCategory)
	appendLine(&sb, "中类含义", rec.MiddleMeaning)
	appendLine(&sb, "小类", rec.SmallCategory)
	appendLine(&sb, "小类含义", rec.SmallMeaning)
	appendLine(&sb, "细类（职业）", rec.FineCode)
	appendLine(&sb, "细类含义", rec.FineMeaning)
	appendLine(&sb, "细类主要工作任务", rec.Tasks)
	appendLine(&sb, "细类包含工种", rec.IncludedJobs)

	sb.WriteString("\n输出必须是 JSON，字段要求如下：\n")
	sb.WriteString("1. LLM岗位概述：120~180 个中文字符，说明该岗位在真实企业招聘中的典型定位、业务场景、价值贡献、常见任职企业或行业。\n")
	sb.WriteString("2. LLM典型职责：3~5 条，每条 15~30 个中文字符，描述核心工作职责，使用动宾结构。\n")
	sb.WriteString("3. LLM关联关键词：6~10 个词或词组，突出技能、工具、业务领域或工作对象。\n")
	sb.WriteString("4. LLM典型岗位：3~5 个常见岗位名称，用于与招聘 JD 匹配。\n")
	sb.WriteString("5. 所有字段必须用中文，禁止出现 JSON 以外的任何说明。\n")

	schema := map[string]string{
		"LLM岗位概述":  "string",
		"LLM典型职责":  "string[]",
		"LLM关联关键词": "string[]",
		"LLM典型岗位":  "string[]",
	}
	schemaBytes, _ := json.Marshal(schema)
	sb.WriteString("JSON 字段示例（类型说明，不要直接复用内容）：\n")
	sb.Write(schemaBytes)

	return sb.String()
}

func appendLine(sb *strings.Builder, label, value string) {
	if strings.TrimSpace(value) == "" {
		return
	}
	sb.WriteString(label)
	sb.WriteString(": ")
	sb.WriteString(value)
	sb.WriteString("\n")
}

func writeRecords(path string, records []map[string]interface{}) error {
	var writer io.Writer
	var file *os.File
	var err error

	if path == "-" {
		writer = os.Stdout
	} else {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		tmp := path + ".tmp"
		file, err = os.Create(tmp)
		if err != nil {
			return err
		}
		defer func() {
			file.Close()
			if err == nil {
				os.Rename(tmp, path)
			} else {
				os.Remove(tmp)
			}
		}()
		writer = file
	}

	enc := json.NewEncoder(writer)
	enc.SetIndent("", "  ")
	if err = enc.Encode(records); err != nil {
		return err
	}

	return nil
}
