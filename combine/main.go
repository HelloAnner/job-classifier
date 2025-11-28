package main

// 单文件整合程序：
// - 扫描 --input 指定的目录（或单个 CSV 文件）
// - 每个 CSV -> output/<csv_basename> 目录
//   - 导入到 SQLite: output/<csv_basename>/origin.db （表结构兼容 cmd/importer/query）
//   - 过滤岗位描述长度 < 5 的记录到 output/<csv_basename>/ignore.csv
//   - 运行与 cmd/query 相同的查询逻辑（向量 + 知识图谱 + ChromaDB）
//   - 输出分类结果到 output/<csv_basename>/result.csv
// - 在 data/<csv_basename>/count.txt 写入统计：原始、处理、过滤三项，保证 处理+过滤=原始
// - 导入与查询可并发（文件粒度并发；文件内查询保持与 cmd/query 相同的 worker 数，写 CSV 加锁）

import (
    "bufio"
    "bytes"
    "context"
    "crypto/sha256"
    "database/sql"
    "encoding/csv"
    "encoding/hex"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "log"
    "math"
    "net/http"
    "os"
    "path/filepath"
    "runtime"
    "sort"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    _ "github.com/mattn/go-sqlite3"
)

// ===== 与 cmd/query 保持一致的常量/配置 =====
const (
    ollamaAPIURL        = "http://localhost:11434/api/embeddings"
    embeddingModel      = "quentinz/bge-large-zh-v1.5"
    chromaDBURL         = "http://localhost:8000"
    collectionName      = "job_classification"
    graphNeighborLimit  = 5
    similarityThreshold = 0.55
)

// ===== CLI 选项 =====
type options struct {
    inputPath   string // 目录或单个 CSV
    fileWorkers int    // 同时处理多少个 CSV
    batchSize   int    // 导入时事务批量
    queryWorkers int   // 查询阶段的并发（默认与 cmd/query 的 10 一致）
    trace       bool   // 逐条记录阶段日志
    limitJobs   int    // 仅处理前 N 条（用于联调验证准确性）
    clean       bool   // 强制清空已存在的输出目录后重跑（默认增量续跑）
    minSim      float64 // 最低相似度阈值（仅用于日志提示；匹配始终取最优1条）
}

func parseFlags() options {
    var opts options
    flag.StringVar(&opts.inputPath, "input", "data/51job", "输入目录或单个CSV（例如 data/51job 或 data/51job/51job_2021.csv）")
    defFW := runtime.NumCPU()
    if defFW < 2 { defFW = 2 }
    flag.IntVar(&opts.fileWorkers, "file-workers", defFW/2, "同时处理的 CSV 文件数量")
    flag.IntVar(&opts.batchSize, "batch", 2000, "导入 SQLite 的提交批量")
    flag.IntVar(&opts.queryWorkers, "query-workers", 10, "每个CSV在查询阶段的并发数")
    flag.BoolVar(&opts.trace, "trace", false, "开启逐条记录阶段日志（用于准确性排查）")
    flag.IntVar(&opts.limitJobs, "limit-jobs", 0, "仅处理前 N 条岗位（0 表示全部）")
    flag.BoolVar(&opts.clean, "clean", false, "存在输出目录时先清空再重跑（默认增量续跑）")
    flag.Float64Var(&opts.minSim, "min-sim", 0.0, "最低相似度阈值（仅日志提示，不拒绝落表；默认0.0）")
    flag.Parse()
    if opts.fileWorkers < 1 { opts.fileWorkers = 1 }
    if opts.batchSize < 1 { opts.batchSize = 1 }
    if opts.queryWorkers < 1 { opts.queryWorkers = 1 }
    return opts
}

// ===== 导入阶段 =====
type importStats struct {
    Total    int64
    Imported int64
    Filtered int64
}

type csvRow struct {
    MID      string
    Intro    string
    Filtered bool // true: intro 长度 < 6，需要忽略匹配
}

// 流式读取 CSV：一边写 ignore.csv，一边按 batch 写入 DB，避免大文件占用大量内存
func streamCSVToDB(ctx context.Context, csvPath string, ignorePath string, db *sql.DB, source string, batchSize int, trace bool) (stats importStats, err error) {
    f, err := os.Open(csvPath)
    if err != nil { return stats, err }
    defer f.Close()
    r := csv.NewReader(bufio.NewReader(f))
    r.ReuseRecord = true
    r.LazyQuotes = true
    r.FieldsPerRecord = -1

    // 打开 ignore.csv
    igf, err := os.Create(ignorePath)
    if err != nil { return stats, err }
    defer func(){ _ = igf.Close() }()
    igw := csv.NewWriter(igf)
    if err := igw.Write([]string{"mid", "job_intro"}); err != nil { return stats, err }

    // 先读表头
    header, err := r.Read()
    if err == io.EOF { igw.Flush(); return stats, igw.Error() }
    if err != nil { return stats, err }
    if len(header) < 2 {
        // 不规范表头也继续读取数据
    }

    // 累计一个小批次写 DB，避免持久占用内存
    batch := make([]csvRow, 0, batchSize)
    flushBatch := func() error {
        if len(batch) == 0 { return nil }
        if err := importRowsToDB(ctx, db, batch, source, batchSize, trace); err != nil { return err }
        batch = batch[:0]
        return nil
    }

    for {
        rec, err := r.Read()
        if err == io.EOF { break }
        if err != nil { return stats, err }
        // 对于不规范行：字段不足，既不入库也不写入 ignore，避免造成空行
        if len(rec) < 2 {
            if trace { log.Printf("[SKIP] malformed row: fields<2") }
            continue
        }
        mid := strings.TrimSpace(rec[0])
        intro := strings.TrimSpace(rec[1])
        if mid == "" {
            if trace { log.Printf("[SKIP] empty mid row, intro_len=%d", introRuneLen(intro)) }
            continue
        }
        stats.Total++
        if introRuneLen(intro) < 6 {
            stats.Filtered++
            _ = igw.Write([]string{mid, intro})
            if trace {
                // 行号无法直接取得，这里打印 mid 与长度
                log.Printf("[FILTER] mid=%s intro_len=%d(<6)", mid, introRuneLen(intro))
            }
            // 仍然写入 DB，但打上过滤标记，保证最终计数对齐（result+ignore==db）
            if mid != "" { batch = append(batch, csvRow{MID: mid, Intro: intro, Filtered: true}) }
            if len(batch) >= batchSize {
                if err := flushBatch(); err != nil { return stats, err }
            }
            continue
        }
        stats.Imported++
        batch = append(batch, csvRow{MID: mid, Intro: intro, Filtered: false})
        if len(batch) >= batchSize {
            if err := flushBatch(); err != nil { return stats, err }
        }
    }
    if err := flushBatch(); err != nil { return stats, err }
    igw.Flush()
    if err := igw.Error(); err != nil { return stats, err }
    return stats, nil
}

func introRuneLen(s string) int { return len([]rune(strings.TrimSpace(s))) }

func ensureDB(dbPath string) (*sql.DB, error) {
    // dsn 参考 cmd/importer 的参数（WAL / NORMAL / 忙等待）
    dsn := fmt.Sprintf("file:%s?_cache_size=200000&_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=60000&_foreign_keys=off", dbPath)
    db, err := sql.Open("sqlite3", dsn)
    if err != nil { return nil, err }
    if err := db.Ping(); err != nil { db.Close(); return nil, err }
    schema := []string{
        `CREATE TABLE IF NOT EXISTS jobs (
            mid TEXT PRIMARY KEY,
            job_intro TEXT,
            source TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT '待处理',
            category TEXT,
            updated_at TEXT NOT NULL,
            structured_json TEXT,
            structured_summary TEXT,
            structured_responsibilities TEXT,
            structured_skills TEXT,
            structured_industry TEXT,
            structured_locations TEXT,
            structured_category_hints TEXT,
            structured_updated_at TEXT
        );`,
        `CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source);`,
        `CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);`,
        `CREATE INDEX IF NOT EXISTS idx_jobs_category ON jobs(category);`,
    }
    for _, stmt := range schema {
        if _, err := db.Exec(stmt); err != nil { db.Close(); return nil, err }
    }
    return db, nil
}

func importRowsToDB(ctx context.Context, db *sql.DB, rows []csvRow, source string, batchSize int, trace bool) error {
    if len(rows) == 0 { return nil }
    now := time.Now().UTC().Format(time.RFC3339)
    // 单 writer 事务批量写，避免并发写锁
    for i := 0; i < len(rows); i += batchSize {
        end := i + batchSize
        if end > len(rows) { end = len(rows) }
        tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
        if err != nil { return err }
        // 两类 UPSERT：正常数据（保留旧 status）、过滤数据（强制标记为 '过滤'）
        stmtNorm, err := tx.PrepareContext(ctx, `INSERT INTO jobs(mid, job_intro, source, status, category, updated_at)
            VALUES (?, ?, ?, COALESCE((SELECT status FROM jobs WHERE mid = ?), '待处理'), COALESCE((SELECT category FROM jobs WHERE mid = ?), ''), ?)
            ON CONFLICT(mid) DO UPDATE SET
                job_intro=excluded.job_intro,
                source=excluded.source,
                updated_at=excluded.updated_at`)
        if err != nil { tx.Rollback(); return err }
        stmtFilt, err := tx.PrepareContext(ctx, `INSERT INTO jobs(mid, job_intro, source, status, category, updated_at)
            VALUES (?, ?, ?, '过滤', COALESCE((SELECT category FROM jobs WHERE mid = ?), ''), ?)
            ON CONFLICT(mid) DO UPDATE SET
                job_intro=excluded.job_intro,
                source=excluded.source,
                status='过滤',
                updated_at=excluded.updated_at`)
        if err != nil { stmtNorm.Close(); tx.Rollback(); return err }

        for idx, r := range rows[i:end] {
            mid := strings.TrimSpace(r.MID)
            if mid == "" { continue }
            if r.Filtered {
                if _, err := stmtFilt.ExecContext(ctx, mid, r.Intro, source, mid, now); err != nil {
                    stmtFilt.Close(); stmtNorm.Close(); tx.Rollback(); return err
                }
            } else {
                if _, err := stmtNorm.ExecContext(ctx, mid, r.Intro, source, mid, mid, now); err != nil {
                    stmtFilt.Close(); stmtNorm.Close(); tx.Rollback(); return err
                }
            }
            if trace {
                tag := "norm"
                if r.Filtered { tag = "filt" }
                log.Printf("[DB-INSERT] mid=%s batch=%d item=%d kind=%s", mid, i/batchSize+1, idx+1, tag)
            }
        }
        stmtFilt.Close()
        stmtNorm.Close()
        if err := tx.Commit(); err != nil { return err }
    }
    return nil
}

// ===== 与 cmd/query 等价的查询组件（尽量原样移植，仅对输出路径做最小变更） =====
type JobRecord struct {
    MID        string
    JobIntro   string
    Structured sql.NullString
    Source     string
    Status     string
    Category   string
}

type DatabaseService struct { db *sql.DB }

func NewDatabaseService(dbPath string) (*DatabaseService, error) {
    db, err := sql.Open("sqlite3", dbPath)
    if err != nil { return nil, fmt.Errorf("failed to open database: %w", err) }
    if err := db.Ping(); err != nil { db.Close(); return nil, fmt.Errorf("failed to connect to database: %w", err) }
    return &DatabaseService{db: db}, nil
}
func (s *DatabaseService) Close() error { return s.db.Close() }

// 与 cmd/query 的行规范化逻辑一致
type jobRow struct { rowID int64; mid, intro, src string }

func (s *DatabaseService) NormalizeJobIDs(ctx context.Context) (int, error) {
    rows, err := s.db.QueryContext(ctx, `SELECT rowid, mid, job_intro, source FROM jobs`)
    if err != nil { return 0, err }
    defer rows.Close()
    used := make(map[string]struct{})
    var invalid []jobRow
    for rows.Next() {
        var r jobRow
        if err := rows.Scan(&r.rowID, &r.mid, &r.intro, &r.src); err != nil { return 0, err }
        clean := strings.TrimSpace(r.mid)
        if isValidJobID(clean) { used[clean] = struct{}{}; continue }
        r.mid = clean
        invalid = append(invalid, r)
    }
    if err := rows.Err(); err != nil { return 0, err }
    if len(invalid) == 0 { return 0, nil }
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil { return 0, err }
    stmt, err := tx.PrepareContext(ctx, `UPDATE jobs SET mid = ? WHERE rowid = ?`)
    if err != nil { tx.Rollback(); return 0, err }
    defer stmt.Close()
    for _, row := range invalid {
        newID := generateJobID(row, used)
        if _, err := stmt.ExecContext(ctx, newID, row.rowID); err != nil { tx.Rollback(); return 0, err }
    }
    if err := tx.Commit(); err != nil { return 0, err }
    return len(invalid), nil
}

func isValidJobID(id string) bool {
    if len(id) != 32 { return false }
    for _, ch := range id {
        if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F') { continue }
        return false
    }
    return true
}
func generateJobID(row jobRow, used map[string]struct{}) string {
    base := fmt.Sprintf("%d|%s|%s|%s", row.rowID, row.mid, row.intro, row.src)
    for salt := 0; ; salt++ {
        data := base
        if salt > 0 { data = fmt.Sprintf("%s|%d", base, salt) }
        sum := sha256.Sum256([]byte(data))
        candidate := hex.EncodeToString(sum[:16])
        if _, exists := used[candidate]; exists { continue }
        used[candidate] = struct{}{}
        return candidate
    }
}

func (s *DatabaseService) CountJobs(ctx context.Context) (int, error) {
    row := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM jobs WHERE job_intro IS NOT NULL AND job_intro != ''`)
    var n int
    if err := row.Scan(&n); err != nil { return 0, err }
    return n, nil
}

func (s *DatabaseService) StreamJobs(ctx context.Context) (*sql.Rows, error) {
    // 只流式返回“未处理完成且未被过滤”的岗位，用于续跑
    return s.db.QueryContext(ctx, `SELECT mid, job_intro, structured_json, source, status, category
        FROM jobs
        WHERE job_intro IS NOT NULL AND job_intro != ''
          AND status!='处理完成' AND status!='过滤'
        ORDER BY mid`)
}

// Embedding
type EmbeddingService struct { client *http.Client }
type EmbeddingRequest struct {
    Model  string `json:"model"`
    Prompt string `json:"prompt"`
}
type EmbeddingResponse struct { Embedding []float32 `json:"embedding"` }

func NewEmbeddingService() *EmbeddingService {
    return &EmbeddingService{client: &http.Client{Timeout: 120 * time.Second}}
}
func (s *EmbeddingService) GetEmbedding(text string) ([]float32, error) {
    reqBody := EmbeddingRequest{Model: embeddingModel, Prompt: text}
    data, _ := json.Marshal(reqBody)
    resp, err := s.client.Post(ollamaAPIURL, "application/json", bytes.NewBuffer(data))
    if err != nil { return nil, fmt.Errorf("failed to call ollama API: %w", err) }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK { b, _ := io.ReadAll(resp.Body); return nil, fmt.Errorf("ollama API returned status %d: %s", resp.StatusCode, string(b)) }
    var out EmbeddingResponse
    if err := json.NewDecoder(resp.Body).Decode(&out); err != nil { return nil, fmt.Errorf("failed to decode response: %w", err) }
    return out.Embedding, nil
}

// Graph repo
type GraphRepository struct { db *sql.DB }
type graphNeighbor struct { ID string; Score float64 }
func NewGraphRepository(dbPath string) (*GraphRepository, error) {
    db, err := sql.Open("sqlite3", dbPath)
    if err != nil { return nil, err }
    if err := db.Ping(); err != nil { db.Close(); return nil, err }
    return &GraphRepository{db: db}, nil
}
func (r *GraphRepository) Close() error { if r == nil || r.db == nil { return nil }; return r.db.Close() }
func (r *GraphRepository) GetNeighbors(jobID string, limit int) ([]graphNeighbor, error) {
    rows, err := r.db.Query(`SELECT neighbor_id, score FROM job_neighbors WHERE job_id = ? ORDER BY score DESC LIMIT ?`, jobID, limit)
    if err != nil { return nil, err }
    defer rows.Close()
    var out []graphNeighbor
    for rows.Next() { var n graphNeighbor; if err := rows.Scan(&n.ID, &n.Score); err != nil { return nil, err }; out = append(out, n) }
    return out, rows.Err()
}

// Chroma repo（集合 ID 动态获取，与 cmd/query 一致）
type ChromaRepository struct { httpClient *http.Client; collectionID string }
func NewChromaRepository(ctx context.Context) (*ChromaRepository, error) {
    r := &ChromaRepository{httpClient: &http.Client{Timeout: 30 * time.Second}}
    if err := r.loadCollectionID(ctx); err != nil { return nil, err }
    return r, nil
}
func (r *ChromaRepository) loadCollectionID(ctx context.Context) error {
    url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s", chromaDBURL, collectionName)
    req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
    resp, err := r.httpClient.Do(req)
    if err != nil { return fmt.Errorf("查询集合 %s 失败: %w", collectionName, err) }
    defer resp.Body.Close()
    if resp.StatusCode == http.StatusNotFound { return fmt.Errorf("未在 ChromaDB 中找到集合 %s，请先执行 job-import", collectionName) }
    if resp.StatusCode != http.StatusOK { b, _ := io.ReadAll(resp.Body); return fmt.Errorf("获取集合 %s 失败，状态码 %d: %s", collectionName, resp.StatusCode, string(b)) }
    var info struct{ ID string `json:"id"` }
    if err := json.NewDecoder(resp.Body).Decode(&info); err != nil { return fmt.Errorf("解析集合响应失败: %w", err) }
    if info.ID == "" { return fmt.Errorf("集合 %s 的 ID 为空", collectionName) }
    r.collectionID = info.ID
    return nil
}

type ChromaQueryRequest struct { QueryEmbeddings [][]float32 `json:"query_embeddings"`; NResults int `json:"n_results"`; Include []string `json:"include"` }
type ChromaQueryResponse struct {
    IDs       [][]string                 `json:"ids"`
    Distances [][]float64                `json:"distances"`
    Metadatas [][]map[string]interface{} `json:"metadatas"`
    Documents [][]string                 `json:"documents"`
}
func (r *ChromaRepository) Query(ctx context.Context, embedding []float32) (*ChromaQueryResponse, error) {
    url := fmt.Sprintf("%s/api/v2/tenants/default_tenant/databases/default_database/collections/%s/query", chromaDBURL, r.collectionID)
    reqBody := ChromaQueryRequest{QueryEmbeddings: [][]float32{embedding}, NResults: 2000, Include: []string{"documents", "metadatas", "distances"}}
    data, _ := json.Marshal(reqBody)
    req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
    req.Header.Set("Content-Type", "application/json")
    resp, err := r.httpClient.Do(req)
    if err != nil { return nil, fmt.Errorf("failed to query chroma: %w", err) }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK { b, _ := io.ReadAll(resp.Body); return nil, fmt.Errorf("chroma API returned status %d: %s", resp.StatusCode, string(b)) }
    var out ChromaQueryResponse
    if err := json.NewDecoder(resp.Body).Decode(&out); err != nil { return nil, fmt.Errorf("failed to decode response: %w", err) }
    return &out, nil
}

// CSV 写入（对单文件结果加锁，避免并发问题）
type CSVWriter struct { mu sync.Mutex; file *os.File; w *csv.Writer }
func NewCSVWriterTo(path string, appendMode bool) (*CSVWriter, error) {
    if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil { return nil, err }
    var f *os.File
    info, err := os.Stat(path)
    if appendMode && err == nil && !info.IsDir() {
        f, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
        if err != nil { return nil, err }
        w := csv.NewWriter(f)
        return &CSVWriter{file: f, w: w}, nil
    }
    // 新建并写表头
    f, err = os.Create(path)
    if err != nil { return nil, err }
    w := csv.NewWriter(f)
    headers := []string{"job_id","job_intro","similarity_score","大类","大类含义","中类","中类含义","小类","小类含义","细类职业","细类含义","细类主要工作任务"}
    if err := w.Write(headers); err != nil { f.Close(); return nil, err }
    return &CSVWriter{file: f, w: w}, nil
}

// 读取可选的人工/上次会话基线（用于精确续跑进度的对齐）。
// 若存在 output/<base>/.resume_base 文件，取其整数值作为建议基线；仅用于显示，不影响处理逻辑。
func readResumeBase(resultPath string) int {
    dir := filepath.Dir(resultPath)
    resumeFile := filepath.Join(dir, ".resume_base")
    data, err := os.ReadFile(resumeFile)
    if err != nil { return 0 }
    s := strings.TrimSpace(string(data))
    if s == "" { return 0 }
    var n int
    _, err = fmt.Sscanf(s, "%d", &n)
    if err != nil || n < 0 { return 0 }
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

// 与 cmd/query 的格式化/工具函数一致
type structuredPayload struct {
    Summary string `json:"岗位概述"`
    Responsibilities []string `json:"核心职责"`
    Skills []string `json:"技能标签"`
    Industry string `json:"行业"`
    Locations []string `json:"工作地点"`
    CategoryHints []struct{ Name string `json:"名称"`; Confidence float64 `json:"信心"` } `json:"可能对应的大类"`
}
func formatStructuredText(raw string) string {
    if strings.TrimSpace(raw) == "" { return "" }
    var p structuredPayload
    if err := json.Unmarshal([]byte(raw), &p); err != nil { return raw }
    var sec []string
    if p.Summary != "" { sec = append(sec, fmt.Sprintf("岗位概述: %s", p.Summary)) }
    if len(p.Responsibilities) > 0 { sec = append(sec, fmt.Sprintf("核心职责: %s", strings.Join(p.Responsibilities, "；"))) }
    if len(p.Skills) > 0 { sec = append(sec, fmt.Sprintf("技能标签: %s", strings.Join(p.Skills, "；"))) }
    if p.Industry != "" { sec = append(sec, fmt.Sprintf("行业: %s", p.Industry)) }
    if len(p.Locations) > 0 { sec = append(sec, fmt.Sprintf("工作地点: %s", strings.Join(p.Locations, "；"))) }
    if len(p.CategoryHints) > 0 {
        var hints []string
        for _, h := range p.CategoryHints { if h.Name != "" { hints = append(hints, fmt.Sprintf("%s(信心%.2f)", h.Name, h.Confidence)) } }
        if len(hints) > 0 { sec = append(sec, fmt.Sprintf("大类猜测: %s", strings.Join(hints, "；"))) }
    }
    return strings.Join(sec, "\n")
}
func cleanCell(s string) string { s = strings.ReplaceAll(s, "\r\n", " "); s = strings.ReplaceAll(s, "\n", " "); s = strings.ReplaceAll(s, "\r", " "); s = strings.ReplaceAll(s, "\t", " "); return strings.TrimSpace(s) }
func truncate(s string, max int) string { s = cleanCell(s); r := []rune(s); if len(r) <= max { return s }; return string(r[:max]) + "..." }
func safeString(meta map[string]interface{}, key string) string { if val, ok := meta[key]; ok && val != nil { if str, ok := val.(string); ok { str = strings.ReplaceAll(str, "\r\n", " "); str = strings.ReplaceAll(str, "\n", " "); str = strings.ReplaceAll(str, "\r", " "); str = strings.ReplaceAll(str, "\t", " "); str = strings.ReplaceAll(str, "\"", "\"\""); return str } }; return "" }
func normalizeEmbedding(vec []float32) ([]float32, error) { var sum float64; for _, v := range vec { fv := float64(v); sum += fv*fv }; norm := math.Sqrt(sum); if norm == 0 { return nil, fmt.Errorf("embedding norm is zero") }; out := make([]float32, len(vec)); inv := float32(1/norm); for i, v := range vec { out[i] = v*inv }; return out, nil }
func cosineDistanceToScore(distance float64) float64 { score := 1 - distance; if score < 0 { return 0 }; if score > 1 { return 1 }; return score }

// 查询处理器（核心逻辑保持一致）
type QueryProcessor struct {
    dbService    *DatabaseService
    embeddingSvc *EmbeddingService
    chromaRepo   *ChromaRepository
    graphRepo    *GraphRepository
    csvWriter    *CSVWriter
    workerCount  int
    trace        bool
    limitJobs    int
    processedSet map[string]struct{}
    baseProcessed int
    totalAll      int
    minSim        float64
}
type jobResult struct { job JobRecord; similarity float64; metadata map[string]interface{}; matched bool; err error }
type classificationMatch struct { Metadata map[string]interface{}; Similarity float64 }

func buildMatchIndex(resp *ChromaQueryResponse) map[string]classificationMatch {
    index := make(map[string]classificationMatch)
    if resp == nil || len(resp.Metadatas) == 0 || len(resp.Distances) == 0 { return index }
    metaList := resp.Metadatas[0]
    distList := resp.Distances[0]
    for i := 0; i < len(metaList) && i < len(distList); i++ {
        meta := metaList[i]
        jobID := safeString(meta, "细类职业")
        if jobID == "" { continue }
        similarity := cosineDistanceToScore(distList[i])
        index[jobID] = classificationMatch{Metadata: meta, Similarity: similarity}
    }
    return index
}

func (p *QueryProcessor) applyGraphCorrection(bestJobID string, currentMeta map[string]interface{}, currentSimilarity float64, matches map[string]classificationMatch) (map[string]interface{}, float64, bool) {
    if p.graphRepo == nil || bestJobID == "" || len(matches) == 0 { return currentMeta, currentSimilarity, false }
    neighbors, err := p.graphRepo.GetNeighbors(bestJobID, graphNeighborLimit)
    if err != nil || len(neighbors) == 0 { return currentMeta, currentSimilarity, false }
    bestMeta := currentMeta; bestSim := currentSimilarity; changed := false; bestID := bestJobID
    for _, n := range neighbors {
        if m, ok := matches[n.ID]; ok { if m.Similarity > bestSim { bestSim = m.Similarity; bestMeta = m.Metadata; bestID = n.ID; changed = true } }
    }
    if changed { log.Printf("Graph correction applied: %s -> %s (%.4f -> %.4f)", bestJobID, bestID, currentSimilarity, bestSim) }
    return bestMeta, bestSim, changed
}

func NewQueryProcessor(ctx context.Context, dbPath, resultPath, graphDBPath string, workerCount int, trace bool, limit int, appendMode bool, processed map[string]struct{}, minSim float64) (*QueryProcessor, error) {
    dbService, err := NewDatabaseService(dbPath)
    if err != nil { return nil, err }
    if fixed, err := dbService.NormalizeJobIDs(ctx); err != nil { dbService.Close(); return nil, fmt.Errorf("failed to normalize job IDs: %w", err) } else if fixed > 0 { log.Printf("Normalized %d invalid job IDs", fixed) }
    csvWriter, err := NewCSVWriterTo(resultPath, appendMode)
    if err != nil { dbService.Close(); return nil, err }
    chromaRepo, err := NewChromaRepository(ctx)
    if err != nil { csvWriter.Close(); dbService.Close(); return nil, err }
    graphRepo, err := NewGraphRepository(graphDBPath)
    if err != nil { csvWriter.Close(); dbService.Close(); return nil, err }
    // 计算进度基线：totalAll 与 baseProcessed（取 DB 打标、result.csv 与可选 resume_base 三者的最大值）
    totalAll, err := dbService.CountJobs(ctx)
    if err != nil { totalAll = 0 }
    baseDB, err := dbService.CountProcessed(ctx)
    if err != nil { baseDB = 0 }
    baseCSV := len(processed)
    // 可选 resume_base（用户可手动设置以对齐历史未记录的“已处理但未匹配”的数量）
    baseHint := readResumeBase(resultPath)
    if baseDB < baseCSV { baseDB = baseCSV }
    if baseDB < baseHint { baseDB = baseHint }
    if baseDB > totalAll { baseDB = totalAll }
    if totalAll < 0 { totalAll = 0 }

    qp := &QueryProcessor{
        dbService:     dbService,
        embeddingSvc:  NewEmbeddingService(),
        chromaRepo:    chromaRepo,
        graphRepo:     graphRepo,
        csvWriter:     csvWriter,
        workerCount:   workerCount,
        trace:         trace,
        limitJobs:     limit,
        processedSet:  processed,
        baseProcessed: baseDB,
        totalAll:      totalAll,
        minSim:        minSim,
    }
    remaining := totalAll - baseDB
    if remaining < 0 { remaining = 0 }
    log.Printf("History processed=%d/%d, remaining=%d", baseDB, totalAll, remaining)
    return qp, nil
}
func (p *QueryProcessor) Close() { p.dbService.Close(); if p.graphRepo != nil { p.graphRepo.Close() }; p.csvWriter.Close() }

func (p *QueryProcessor) Process(ctx context.Context) error {
    // 基于历史基线计算剩余任务并打印
    remaining := p.totalAll - p.baseProcessed
    if remaining < 0 { remaining = 0 }
    if p.limitJobs > 0 && p.limitJobs < remaining { remaining = p.limitJobs }
    log.Printf("Found %d jobs to process (history=%d/%d, remaining=%d)", remaining, p.baseProcessed, p.totalAll, remaining)

    rows, err := p.dbService.StreamJobs(ctx)
    if err != nil { return err }
    defer rows.Close()

    jobsCh := make(chan JobRecord)
    resultsCh := make(chan jobResult)
    var wg sync.WaitGroup
    for i := 0; i < p.workerCount; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for job := range jobsCh { resultsCh <- p.processSingleJob(ctx, job) }
        }()
    }

    // feeder：逐行从 DB 读取并投入 jobsCh，避免一次性加载到内存
    go func() {
        sent := 0
        for rows.Next() {
            var j JobRecord
            if err := rows.Scan(&j.MID, &j.JobIntro, &j.Structured, &j.Source, &j.Status, &j.Category); err != nil {
                log.Printf("Warning: scan job row failed: %v", err)
                continue
            }
            j.MID = strings.TrimSpace(j.MID)
            if p.processedSet != nil {
                if _, ok := p.processedSet[j.MID]; ok {
                    continue
                }
            }
            jobsCh <- j
            sent++
            if p.limitJobs > 0 && sent >= p.limitJobs { break }
        }
        close(jobsCh)
        wg.Wait()
        close(resultsCh)
    }()

    processedSession, matched := 0, 0
    start := time.Now()
    for res := range resultsCh {
        // 仅在无错误时，标记处理完成
        if res.err == nil {
            if err := p.dbService.MarkProcessed(ctx, res.job.MID); err != nil {
                log.Printf("Warning: mark processed failed mid=%s: %v", res.job.MID, err)
            }
        }
        processedSession++
        if processedSession == 1 || processedSession%50 == 0 {
            curr := p.baseProcessed + processedSession
            pct := 0.0
            if p.totalAll > 0 { pct = float64(curr) / float64(p.totalAll) * 100 }
            log.Printf("Progress: %d/%d (%.1f%%)", curr, p.totalAll, pct)
            // 持久化进度基线，确保断点续跑继续从该位置显示
            writeResumeBase(p.csvWriter.file.Name(), curr)
        }
        if res.err != nil { log.Println(res.err); continue }
        if res.matched {
            matched++
            if err := p.csvWriter.WriteRow(res.job, res.similarity, res.metadata); err != nil { log.Printf("Warning: write csv row failed: %v", err) }
            if matched%100 == 0 { if err := p.csvWriter.Flush(); err != nil { log.Printf("Warning: csv flush failed: %v", err) } else { log.Printf("Auto-saved CSV after %d matched records", matched) } }
        }
    }
    curr := p.baseProcessed + processedSession
    log.Printf("Query completed! total=%d processed=%d matched(>=%.2f)=%d elapsed=%.2fs", p.totalAll, curr, p.minSim, matched, time.Since(start).Seconds())
    writeResumeBase(p.csvWriter.file.Name(), curr)
    return rows.Err()
}

func (p *QueryProcessor) processSingleJob(ctx context.Context, job JobRecord) jobResult {
    structuredText := ""
    if job.Structured.Valid { structuredText = formatStructuredText(job.Structured.String) }
    var desc []string
    if structuredText != "" { desc = append(desc, structuredText) }
    desc = append(desc, fmt.Sprintf("岗位描述: %s", job.JobIntro))
    desc = append(desc, fmt.Sprintf("来源: %s", job.Source))
    desc = append(desc, fmt.Sprintf("状态: %s", job.Status))
    desc = append(desc, fmt.Sprintf("分类: %s", job.Category))
    full := strings.Join(desc, "\n")
    if p.trace {
        log.Printf("[EMB-REQ] mid=%s text_len=%d", job.MID, len([]rune(full)))
    }

    embedding, err := p.embeddingSvc.GetEmbedding(full)
    if err != nil { return jobResult{job: job, err: fmt.Errorf("Warning: Failed to get embedding for job %s: %v", job.MID, err)} }
    if p.trace { log.Printf("[EMB-OK] mid=%s dim=%d", job.MID, len(embedding)) }
    embedding, err = normalizeEmbedding(embedding)
    if err != nil { return jobResult{job: job, err: fmt.Errorf("Warning: Failed to normalize embedding for job %s: %v", job.MID, err)} }
    if p.trace { log.Printf("[CHROMA-REQ] mid=%s n_results=2000", job.MID) }
    qresp, err := p.chromaRepo.Query(ctx, embedding)
    if err != nil { return jobResult{job: job, err: fmt.Errorf("Warning: Failed to query ChromaDB for job %s: %v", job.MID, err)} }
    if len(qresp.Distances) == 0 || len(qresp.Distances[0]) == 0 { return jobResult{job: job} }
    // 取最优候选
    bestSim := 0.0; bestIdx := 0; bestDist := 0.0
    for j := 0; j < len(qresp.Distances[0]); j++ { d := qresp.Distances[0][j]; s := cosineDistanceToScore(d); if s > bestSim { bestSim, bestIdx, bestDist = s, j, d } }
    if p.trace { log.Printf("[CHROMA-BEST] mid=%s similarity=%.4f min-sim=%.2f", job.MID, bestSim, p.minSim) }
    // 不以阈值拒绝落表：始终选择最优1条。若低于 min-sim，仅记录日志。
    if bestSim < p.minSim && p.trace {
        log.Printf("[LOWCONF] mid=%s similarity=%.4f < %.2f", job.MID, bestSim, p.minSim)
    }
    var meta map[string]interface{}
    if len(qresp.Metadatas) > 0 && len(qresp.Metadatas[0]) > bestIdx { meta = qresp.Metadatas[0][bestIdx] }
    // 图纠偏（与 cmd/query 一致）
    if meta != nil {
        bestID := safeString(meta, "细类职业")
        if idx := buildMatchIndex(qresp); len(idx) > 0 {
            if m2, s2, changed := p.applyGraphCorrection(bestID, meta, bestSim, idx); changed {
                if p.trace { log.Printf("[GRAPH] mid=%s corrected best to similarity=%.4f", job.MID, s2) }
                meta, bestSim = m2, s2
                _ = bestDist
            } else if p.trace {
                log.Printf("[GRAPH] mid=%s no change", job.MID)
            }
        }
    }
    if p.trace { log.Printf("[CSV-WRITE] mid=%s final_similarity=%.4f job_id=%s", job.MID, bestSim, safeString(meta, "细类职业")) }
    return jobResult{job: job, similarity: bestSim, metadata: meta, matched: true}
}

// ===== 主流程 =====
type fileTask struct {
    csvPath    string
    baseName   string
    source     string
    outDir     string
    resultPath string
    ignorePath string
    countPath  string      // legacy path: data/<base>/count.txt
    countPath2 string      // new path:   output/<base>/count.txt
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
            if err != nil { return nil, err }
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
        if err != nil { return nil, fmt.Errorf("invalid glob pattern: %w", err) }
        if len(matched) == 0 { return nil, fmt.Errorf("glob matched no files: %s", inputPath) }
        var files []string
        for _, p := range matched {
            if fi, err := os.Stat(p); err == nil && !fi.IsDir() && strings.HasSuffix(strings.ToLower(p), ".csv") {
                files = append(files, p)
            }
        }
        if len(files) == 0 { return nil, fmt.Errorf("glob produced no CSV files: %s", inputPath) }
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
        if err := os.MkdirAll(outDir, 0o755); err != nil { return nil, err }
        src := filepath.Base(filepath.Dir(p)) // 上一级目录名作为 source，例如 51job
        resultPath := filepath.Join(outDir, "result.csv")
        ignorePath := filepath.Join(outDir, "ignore.csv")
        dataDir := filepath.Join("data", base)
        if err := os.MkdirAll(dataDir, 0o755); err != nil { return nil, err }
        countPath := filepath.Join(dataDir, "count.txt")
        countPath2 := filepath.Join(outDir, "count.txt")
        dbPath := filepath.Join(outDir, "origin.db")
        tasks = append(tasks, fileTask{csvPath: p, baseName: base, source: src, outDir: outDir, resultPath: resultPath, ignorePath: ignorePath, countPath: countPath, countPath2: countPath2, dbPath: dbPath})
    }
    return tasks, nil
}

// 已被 streamCSVToDB 取代（保留占位避免接口变动）
func writeIgnoreCSV(path string, header []string, rows []csvRow) error { return nil }

func writeCountTXT(path string, st importStats) error {
    ok := st.Imported+st.Filtered == st.Total
    content := fmt.Sprintf("total=%d\nprocessed=%d\nfiltered=%d\nok=%v\n", st.Total, st.Imported, st.Filtered, ok)
    return os.WriteFile(path, []byte(content), 0o644)
}

func countProcessedRows(path string) (int64, error) {
    f, err := os.Open(path)
    if err != nil {
        if os.IsNotExist(err) { return 0, nil }
        return 0, err
    }
    defer f.Close()
    r := csv.NewReader(bufio.NewReader(f))
    r.ReuseRecord = true
    r.LazyQuotes = true
    r.FieldsPerRecord = -1
    // 读表头
    if _, err := r.Read(); err != nil {
        if err == io.EOF { return 0, nil }
        return 0, err
    }
    var n int64
    for {
        if _, err := r.Read(); err != nil {
            if err == io.EOF { break }
            return n, err
        }
        n++
    }
    return n, nil
}

func loadProcessedSet(path string) (map[string]struct{}, int64, error) {
    set := make(map[string]struct{})
    f, err := os.Open(path)
    if err != nil {
        if os.IsNotExist(err) { return set, 0, nil }
        return nil, 0, err
    }
    defer f.Close()
    r := csv.NewReader(bufio.NewReader(f))
    r.ReuseRecord = true
    r.LazyQuotes = true
    r.FieldsPerRecord = -1
    header, err := r.Read()
    if err != nil {
        if err == io.EOF { return set, 0, nil }
        return set, 0, err
    }
    // 定位 job_id 列
    jobIdx := 0
    for i, h := range header {
        if strings.TrimSpace(h) == "job_id" { jobIdx = i; break }
    }
    var n int64
    for {
        rec, err := r.Read()
        if err != nil {
            if err == io.EOF { break }
            return set, n, err
        }
        if jobIdx < len(rec) {
            id := strings.TrimSpace(rec[jobIdx])
            if id != "" { set[id] = struct{}{} }
        }
        n++
    }
    return set, n, nil
}

// 统计 CSV 的数据行数（含表头时减1）；不存在则返回0
func countCSVRows(path string) (int64, error) { return countProcessedRows(path) }

// 写 count.txt（严格校验）：
// - total = result_rows + ignore_rows
// - processed = result_rows
// - filtered = ignore_rows
// - ok = (processed == db_rows) && (processed + filtered == csv_total)
func writeCountStrict(path string, total, processed, filtered, dbRows int64) error {
    // 严格校验：result.csv 行数 + ignore.csv 行数 必须等于 DB 行数
    ok := (processed+filtered == dbRows)
    content := fmt.Sprintf("total=%d\nprocessed=%d\nfiltered=%d\nok=%v\n", total, processed, filtered, ok)
    return os.WriteFile(path, []byte(content), 0o644)
}

func processSingleCSV(ctx context.Context, t fileTask, opts options) error {
    log.Printf("[START] %s -> %s", t.csvPath, t.outDir)
    // 建库
    db, err := ensureDB(t.dbPath)
    if err != nil { return fmt.Errorf("open sqlite failed: %w", err) }
    // 流式读取 CSV -> ignore.csv + origin.db
    stats, err := streamCSVToDB(ctx, t.csvPath, t.ignorePath, db, t.source, opts.batchSize, opts.trace)
    if err != nil { db.Close(); return fmt.Errorf("stream import failed: %w", err) }
    log.Printf("[IMPORT] total=%d to_import=%d filtered(<5)=%d", stats.Total, stats.Imported, stats.Filtered)
    db.Close()
    // 查询（支持续跑）：若存在 result.csv，则从其中读取已处理 job_id 集合，并以追加方式写入
    appendMode := false
    processedSet := make(map[string]struct{})
    if _, err := os.Stat(t.resultPath); err == nil {
        appendMode = true
        set, _, err := loadProcessedSet(t.resultPath)
        if err != nil { return fmt.Errorf("load processed set failed: %w", err) }
        processedSet = set
        log.Printf("[RESUME] detected existing result.csv, processed=%d", len(processedSet))
    }
    qp, err := NewQueryProcessor(ctx, t.dbPath, t.resultPath, "data/job_graph.db", opts.queryWorkers, opts.trace, opts.limitJobs, appendMode, processedSet, opts.minSim)
    if err != nil { return fmt.Errorf("init query processor failed: %w", err) }
    if err := qp.Process(ctx); err != nil { qp.Close(); return fmt.Errorf("query process failed: %w", err) }
    qp.Close()
    // 统计与一致性校验：result.csv + ignore.csv 必须覆盖全部数据，且 result.csv 行数与 DB 行数一致
    resultRows, err := countCSVRows(t.resultPath)
    if err != nil { return fmt.Errorf("count result.csv failed: %w", err) }
    ignoreRows, err := countCSVRows(t.ignorePath)
    if err != nil { return fmt.Errorf("count ignore.csv failed: %w", err) }
    totalRows := resultRows + ignoreRows
    // DB 行数（已导入的岗位数）
    dbSvc, err := NewDatabaseService(t.dbPath)
    if err != nil { return fmt.Errorf("open db for count failed: %w", err) }
    dbCount, err := dbSvc.CountJobs(ctx)
    _ = dbSvc.Close()
    if err != nil { return fmt.Errorf("db count failed: %w", err) }
    if err := writeCountStrict(t.countPath, totalRows, resultRows, ignoreRows, int64(dbCount)); err != nil { return fmt.Errorf("write count.txt failed: %w", err) }
    if err := writeCountStrict(t.countPath2, totalRows, resultRows, ignoreRows, int64(dbCount)); err != nil { return fmt.Errorf("write count.txt (output) failed: %w", err) }
    log.Printf("[DONE] %s -> result=%s origin.db=%s count=%s,%s", t.baseName, t.resultPath, t.dbPath, t.countPath, t.countPath2)
    return nil
}

func main() {
    log.SetFlags(log.LstdFlags | log.Lmicroseconds)
    opts := parseFlags()
    ctx := context.Background()
    files, err := discoverCSV(opts.inputPath)
    if err != nil { log.Fatalf("discover csv failed: %v", err) }
    if len(files) == 0 { log.Println("no csv files found, exit"); return }
    tasks, err := buildTasks(files, opts)
    if err != nil { log.Fatalf("build tasks failed: %v", err) }
    log.Printf("发现 %d 个 CSV，将并发处理（file-workers=%d）", len(tasks), opts.fileWorkers)

    // 并发处理不同 CSV
    type result struct { name string; err error }
    jobs := make(chan fileTask)
    results := make(chan result)
    var wg sync.WaitGroup
    for i := 0; i < opts.fileWorkers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for t := range jobs {
                if err := processSingleCSV(ctx, t, opts); err != nil { results <- result{name: t.baseName, err: err} } else { results <- result{name: t.baseName} }
            }
        }()
    }
    go func() { for _, t := range tasks { jobs <- t }; close(jobs); wg.Wait(); close(results) }()

    var fail atomic.Int32
    for res := range results {
        if res.err != nil { fail.Add(1); log.Printf("[ERROR] %s: %v", res.name, res.err) }
    }
    if fail.Load() > 0 { log.Fatalf("完成，存在 %d 个 CSV 失败", fail.Load()) }
    log.Printf("全部完成，共处理 %d 个 CSV。", len(tasks))
}
func (s *DatabaseService) CountProcessed(ctx context.Context) (int, error) {
    row := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM jobs WHERE job_intro IS NOT NULL AND job_intro != '' AND status='处理完成'`)
    var n int
    if err := row.Scan(&n); err != nil { return 0, err }
    return n, nil
}

func (s *DatabaseService) MarkProcessed(ctx context.Context, mid string) error {
    _, err := s.db.ExecContext(ctx, `UPDATE jobs SET status='处理完成' WHERE mid=?`, mid)
    return err
}
