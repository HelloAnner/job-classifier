package main

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultStatus = "待处理"
)

// Record 表示一条入库记录。
type Record struct {
	Mid      string
	Intro    string
	Source   string
	Status   string
	Category string
	Updated  string
}

// fileTask 将文件路径与所属来源目录、修改时间绑定，避免重复计算。
type fileTask struct {
	path    string
	source  string
	modTime time.Time
}

// quotaManager 负责控制每个来源目录的最大写入条数。
type quotaManager struct {
	limit  int
	counts map[string]int
	mu     sync.Mutex
}

func newQuotaManager(limit int) *quotaManager {
	return &quotaManager{limit: limit, counts: make(map[string]int)}
}

func (q *quotaManager) Allow(source string) bool {
	if q == nil {
		return true
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.limit <= 0 {
		q.counts[source]++
		return true
	}
	if q.counts[source] >= q.limit {
		return false
	}
	q.counts[source]++
	return true
}

func initDB(dbPath string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?_cache_size=200000&_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=60000&_foreign_keys=off", dbPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	pragmas := []string{
		"PRAGMA mmap_size=134217728;",
		"PRAGMA temp_store=MEMORY;",
	}
	for _, p := range pragmas {
		if _, err = db.Exec(p); err != nil {
			return nil, err
		}
	}

	ddl := `CREATE TABLE IF NOT EXISTS jobs (
                mid TEXT PRIMARY KEY,
                job_intro TEXT,
                source TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT '待处理',
                category TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source);
            CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
            CREATE INDEX IF NOT EXISTS idx_jobs_category ON jobs(category);`
	if _, err = db.Exec(ddl); err != nil {
		return nil, err
	}
	return db, nil
}

func main() {
	var (
		rootDir string
		dbPath  string
		workers int
		batch   int
		limit   int
	)
	flag.StringVar(&rootDir, "root", "data", "数据根目录，默认指向 data 下的各平台子目录")
	flag.StringVar(&dbPath, "db", "data/jobs.db", "SQLite 文件路径")
	flag.IntVar(&workers, "workers", runtime.NumCPU()*2, "CSV 解析并发数")
	flag.IntVar(&batch, "batch", 5000, "提交批量大小")
	flag.IntVar(&limit, "limit", 1000, "每个来源目录的最大写入条数（默认 1000，0 表示不限制）")
	flag.Parse()

	start := time.Now()
	absRoot, err := filepath.Abs(rootDir)
	if err != nil {
		log.Fatalf("解析 root 目录失败: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		log.Fatalf("创建 data 目录失败: %v", err)
	}

	db, err := initDB(dbPath)
	if err != nil {
		log.Fatalf("初始化数据库失败: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rowsCh := make(chan Record, 10000)
	files := make(chan fileTask, workers)
	var wg sync.WaitGroup

	stats := make(map[string]int64)
	var statsMu sync.Mutex
	var inserted atomic.Int64
	quota := newQuotaManager(limit)
	writerErrCh := make(chan error, 1)

	go func() {
		var (
			tx   *sql.Tx
			stmt *sql.Stmt
		)

		begin := func() error {
			var err error
			tx, err = db.Begin()
			if err != nil {
				return err
			}
			stmt, err = tx.Prepare(`INSERT OR REPLACE INTO jobs(mid, job_intro, source, status, category, updated_at) VALUES(?,?,?,?,?,?)`)
			if err != nil {
				tx.Rollback()
				tx = nil
			}
			return err
		}

		commit := func() error {
			if stmt != nil {
				if err := stmt.Close(); err != nil {
					return err
				}
				stmt = nil
			}
			if tx != nil {
				if err := tx.Commit(); err != nil {
					return err
				}
				tx = nil
			}
			return nil
		}

		reset := func() error {
			if err := commit(); err != nil {
				return err
			}
			return begin()
		}

		if err := begin(); err != nil {
			writerErrCh <- err
			cancel()
			return
		}
		defer func() {
			if tx != nil {
				tx.Rollback()
			}
		}()

		count := 0
		for r := range rowsCh {
			if _, err := stmt.Exec(r.Mid, r.Intro, r.Source, r.Status, r.Category, r.Updated); err != nil {
				writerErrCh <- err
				cancel()
				return
			}
			count++
			if batch > 0 && count%batch == 0 {
				if err := reset(); err != nil {
					writerErrCh <- err
					cancel()
					return
				}
			}
			inserted.Add(1)
			statsMu.Lock()
			stats[r.Source]++
			statsMu.Unlock()
		}

		if err := commit(); err != nil {
			writerErrCh <- err
			cancel()
			return
		}
		writerErrCh <- nil
	}()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range files {
				if ctx.Err() != nil {
					continue
				}
				if err := processFile(ctx, task, rowsCh, quota); err != nil {
					log.Printf("处理文件 %s 出错: %v", task.path, err)
				}
			}
		}()
	}

	walkErr := filepath.WalkDir(absRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return context.Canceled
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".csv") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		relSource := detectSource(absRoot, path)
		select {
		case <-ctx.Done():
			return context.Canceled
		case files <- fileTask{path: path, source: relSource, modTime: info.ModTime()}:
		}
		return nil
	})
	close(files)
	if walkErr != nil && !errors.Is(walkErr, context.Canceled) {
		log.Fatalf("遍历目录失败: %v", walkErr)
	}

	wg.Wait()
	close(rowsCh)

	if err := <-writerErrCh; err != nil {
		log.Fatalf("写入失败: %v", err)
	}

	duration := time.Since(start)
	total := inserted.Load()
	statsMu.Lock()
	log.Printf("导入完成，共写入 %d 行，用时 %s", total, duration)
	for source, c := range stats {
		log.Printf("%s: %d", source, c)
	}
	statsMu.Unlock()
}

func detectSource(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.Base(filepath.Dir(path))
	}
	rel = filepath.ToSlash(rel)
	parts := strings.Split(rel, "/")
	if len(parts) == 0 || parts[0] == "." {
		return filepath.Base(filepath.Dir(path))
	}
	return parts[0]
}

func processFile(ctx context.Context, task fileTask, rowsCh chan<- Record, quota *quotaManager) error {
	f, err := os.Open(task.path)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024*10)
	isHeader := true
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		line := scanner.Text()
		if isHeader {
			isHeader = false
			if strings.HasPrefix(strings.ToLower(line), "mid") {
				continue
			}
		}
		idx := strings.IndexByte(line, ',')
		if idx <= 0 {
			continue
		}
		if quota != nil && !quota.Allow(task.source) {
			break
		}
		mid := line[:idx]
		intro := ""
		if len(line) > idx+1 {
			intro = line[idx+1:]
		}
		record := Record{
			Mid:      mid,
			Intro:    intro,
			Source:   task.source,
			Status:   defaultStatus,
			Category: "",
			Updated:  task.modTime.UTC().Format(time.RFC3339),
		}
		select {
		case <-ctx.Done():
			return nil
		case rowsCh <- record:
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("扫描文件失败 %s: %w", task.path, err)
	}
	return nil
}
