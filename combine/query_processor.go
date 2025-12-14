package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// 查询处理器（核心逻辑保持一致）
type QueryProcessor struct {
	dbService     *DatabaseService
	embeddingSvc  *EmbeddingService
	chromaRepo    *ChromaRepository
	graphRepo     *GraphRepository
	csvWriter     *CSVWriter
	workerCount   int
	embBatchSize  int
	chromaTopK    int
	trace         bool
	limitJobs     int
	processedSet  map[string]struct{}
	baseProcessed int
	totalAll      int
	minSim        float64

	// 批量更新相关字段
	pendingMids []string       // 待批量更新的MID列表
	pendingMu   sync.Mutex     // 保护pendingMids的互斥锁
	batchSize   int            // 批量更新的大小
	flushTicker *time.Ticker   // 定期刷新定时器
	flushWG     sync.WaitGroup // 等待刷新goroutine完成

	// worker专用的context
	cancelFunc context.CancelFunc // 用于取消worker
	workerCtx  context.Context    // worker的context
}

type jobResult struct {
	job        JobRecord
	similarity float64
	metadata   map[string]interface{}
	matched    bool
	err        error
}

type classificationMatch struct {
	Metadata   map[string]interface{}
	Similarity float64
}

func buildMatchIndex(resp *ChromaQueryResponse) map[string]classificationMatch {
	index := make(map[string]classificationMatch)
	if resp == nil || len(resp.Metadatas) == 0 || len(resp.Distances) == 0 {
		return index
	}
	metaList := resp.Metadatas[0]
	distList := resp.Distances[0]
	for i := 0; i < len(metaList) && i < len(distList); i++ {
		meta := metaList[i]
		jobID := safeString(meta, "细类职业")
		if jobID == "" {
			continue
		}
		similarity := cosineDistanceToScore(distList[i])
		index[jobID] = classificationMatch{Metadata: meta, Similarity: similarity}
	}
	return index
}

func buildMatchIndexForList(metaList []map[string]interface{}, distList []float64) map[string]classificationMatch {
	index := make(map[string]classificationMatch)
	for i := 0; i < len(metaList) && i < len(distList); i++ {
		meta := metaList[i]
		jobID := safeString(meta, "细类职业")
		if jobID == "" {
			continue
		}
		similarity := cosineDistanceToScore(distList[i])
		index[jobID] = classificationMatch{Metadata: meta, Similarity: similarity}
	}
	return index
}

func (p *QueryProcessor) applyGraphCorrection(bestJobID string, currentMeta map[string]interface{}, currentSimilarity float64, matches map[string]classificationMatch) (map[string]interface{}, float64, bool) {
	if p.graphRepo == nil || bestJobID == "" || len(matches) == 0 {
		return currentMeta, currentSimilarity, false
	}
	neighbors, err := p.graphRepo.GetNeighbors(bestJobID, graphNeighborLimit)
	if err != nil || len(neighbors) == 0 {
		return currentMeta, currentSimilarity, false
	}
	bestMeta := currentMeta
	bestSim := currentSimilarity
	changed := false
	bestID := bestJobID
	for _, n := range neighbors {
		if m, ok := matches[n.ID]; ok {
			if m.Similarity > bestSim {
				bestSim = m.Similarity
				bestMeta = m.Metadata
				bestID = n.ID
				changed = true
			}
		}
	}
	if changed {
		log.Printf("Graph correction applied: %s -> %s (%.4f -> %.4f)", bestJobID, bestID, currentSimilarity, bestSim)
	}
	return bestMeta, bestSim, changed
}

func NewQueryProcessor(ctx context.Context, dbPath, resultPath, graphDBPath string, workerCount int, embBatchSize int, chromaTopK int, embTimeout time.Duration, trace bool, limit int, appendMode bool, processed map[string]struct{}, minSim float64) (*QueryProcessor, error) {
	dbService, err := NewDatabaseService(dbPath)
	if err != nil {
		return nil, err
	}
	if fixed, err := dbService.NormalizeJobIDs(ctx); err != nil {
		dbService.Close()
		return nil, fmt.Errorf("failed to normalize job IDs: %w", err)
	} else if fixed > 0 {
		log.Printf("Normalized %d invalid job IDs", fixed)
	}
	csvWriter, err := NewCSVWriterTo(resultPath, appendMode)
	if err != nil {
		dbService.Close()
		return nil, err
	}
	chromaRepo, err := NewChromaRepository(ctx)
	if err != nil {
		csvWriter.Close()
		dbService.Close()
		return nil, err
	}
	graphRepo, err := NewGraphRepository(graphDBPath)
	if err != nil {
		csvWriter.Close()
		dbService.Close()
		return nil, err
	}
	// 计算进度基线：totalAll 与 baseProcessed（取 DB 打标、result.csv 与可选 resume_base 三者的最大值）
	totalAll, err := dbService.CountJobs(ctx)
	if err != nil {
		totalAll = 0
	}
	baseDB, err := dbService.CountProcessed(ctx)
	if err != nil {
		baseDB = 0
	}
	baseCSV := len(processed)
	// 可选 resume_base（用户可手动设置以对齐历史未记录的“已处理但未匹配”的数量）
	baseHint := readResumeBase(resultPath)
	if baseDB < baseCSV {
		baseDB = baseCSV
	}
	if baseDB < baseHint {
		baseDB = baseHint
	}
	if baseDB > totalAll {
		baseDB = totalAll
	}
	if totalAll < 0 {
		totalAll = 0
	}

	// 创建可取消的context
	workerCtx, cancelFunc := context.WithCancel(context.Background())

	qp := &QueryProcessor{
		dbService:     dbService,
		embeddingSvc:  NewEmbeddingServiceWithTimeout(embTimeout),
		chromaRepo:    chromaRepo,
		graphRepo:     graphRepo,
		csvWriter:     csvWriter,
		workerCount:   workerCount,
		embBatchSize:  embBatchSize,
		chromaTopK:    chromaTopK,
		trace:         trace,
		limitJobs:     limit,
		processedSet:  processed,
		baseProcessed: baseDB,
		totalAll:      totalAll,
		minSim:        minSim,

		// 批量更新初始化
		pendingMids: make([]string, 0, 100),
		batchSize:   100,                             // 默认批量大小
		flushTicker: time.NewTicker(5 * time.Second), // 5秒刷新一次

		// worker专用的context
		cancelFunc: cancelFunc,
		workerCtx:  workerCtx,
	}
	remaining := totalAll - baseDB
	if remaining < 0 {
		remaining = 0
	}
	log.Printf("History processed=%d/%d, remaining=%d", baseDB, totalAll, remaining)
	return qp, nil
}

// Chroma 批量查询时若 topK 很大，批次过大会触发 SQLite 参数上限(999)。
// 这里按 topK 计算一个安全的最大批次；若仍报错则降级为单条查询，保证任务能继续推进。
func (p *QueryProcessor) queryChromaWithSplit(ctx context.Context, jobs []JobRecord, embeddings [][]float32, resultsCh chan<- jobResult) {
	maxBatch := 1
	if p.chromaTopK > 0 {
		maxBatch = 900 / p.chromaTopK
	}
	if maxBatch < 1 {
		maxBatch = 1
	}
	for start := 0; start < len(jobs); start += maxBatch {
		end := start + maxBatch
		if end > len(jobs) {
			end = len(jobs)
		}
		subJobs := jobs[start:end]
		subEmb := embeddings[start:end]
		if p.trace {
			log.Printf("[CHROMA-BATCH-REQ] size=%d n_results=%d (split)", len(subJobs), p.chromaTopK)
		}
		resp, err := p.chromaRepo.QueryBatch(ctx, subEmb, p.chromaTopK)
		if err != nil && strings.Contains(err.Error(), "too many SQL variables") {
			for i, job := range subJobs {
				if p.trace {
					log.Printf("[CHROMA-RETRY-SINGLE] mid=%s", job.MID)
				}
				p.queryChromaSingle(ctx, job, subEmb[i], resultsCh)
			}
			continue
		}
		if err != nil {
			for _, job := range subJobs {
				resultsCh <- jobResult{job: job, err: fmt.Errorf("Warning: Failed to query ChromaDB (batch) for job %s: %v", job.MID, err)}
			}
			continue
		}
		for i, job := range subJobs {
			if i >= len(resp.Distances) || i >= len(resp.Metadatas) {
				resultsCh <- jobResult{job: job, err: fmt.Errorf("Warning: Chroma batch response size mismatch: distances=%d metadatas=%d expect=%d", len(resp.Distances), len(resp.Metadatas), len(subJobs))}
				continue
			}
			resultsCh <- p.processSingleJobWithLists(ctx, job, resp.Metadatas[i], resp.Distances[i])
		}
	}
}

func (p *QueryProcessor) queryChromaSingle(ctx context.Context, job JobRecord, embedding []float32, resultsCh chan<- jobResult) {
	if p.trace {
		log.Printf("[CHROMA-REQ] mid=%s n_results=%d (single)", job.MID, p.chromaTopK)
	}
	qresp, err := p.chromaRepo.Query(ctx, embedding, p.chromaTopK)
	if err != nil {
		resultsCh <- jobResult{job: job, err: fmt.Errorf("Warning: Failed to query ChromaDB for job %s: %v", job.MID, err)}
		return
	}
	if len(qresp.Metadatas) == 0 || len(qresp.Distances) == 0 {
		resultsCh <- jobResult{job: job, err: fmt.Errorf("Warning: Chroma response empty for job %s", job.MID)}
		return
	}
	resultsCh <- p.processSingleJobWithLists(ctx, job, qresp.Metadatas[0], qresp.Distances[0])
}

func (p *QueryProcessor) Close() {
	// 取消所有worker
	if p.cancelFunc != nil {
		p.cancelFunc()
	}

	// 刷新所有待处理的MID
	p.flushPendingMids(context.Background())

	// 等待所有刷新goroutine完成
	p.flushWG.Wait()

	p.dbService.Close()
	if p.graphRepo != nil {
		p.graphRepo.Close()
	}
	if p.embeddingSvc != nil {
		p.embeddingSvc.Close()
	}
	if p.chromaRepo != nil {
		p.chromaRepo.Close()
	}
	if p.flushTicker != nil {
		p.flushTicker.Stop()
	}
	p.csvWriter.Close()
}

// addPendingMid 添加待更新的MID到缓冲区
func (p *QueryProcessor) addPendingMid(mid string) {
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()

	p.pendingMids = append(p.pendingMids, mid)

	// 如果达到批量大小，立即刷新
	if len(p.pendingMids) >= p.batchSize {
		p.flushWG.Add(1)
		go func() {
			defer p.flushWG.Done()
			// 使用background context，因为这个刷新操作应该完成
			p.flushPendingMids(context.Background())
		}()
	}
}

// flushPendingMids 刷新缓冲区中的MID到数据库
func (p *QueryProcessor) flushPendingMids(ctx context.Context) error {
	p.pendingMu.Lock()
	if len(p.pendingMids) == 0 {
		p.pendingMu.Unlock()
		return nil
	}

	// 复制数据并清空缓冲区
	mids := make([]string, len(p.pendingMids))
	copy(mids, p.pendingMids)
	p.pendingMids = p.pendingMids[:0]
	p.pendingMu.Unlock()

	// 批量更新到数据库
	if err := p.dbService.MarkProcessedBatchWithChunk(ctx, mids, p.batchSize); err != nil {
		// 出错时重新添加回缓冲区（除了最后一部分）
		p.pendingMu.Lock()
		p.pendingMids = append(mids, p.pendingMids...)
		p.pendingMu.Unlock()
		return fmt.Errorf("failed to flush pending MIDs: %w", err)
	}

	if p.trace {
		log.Printf("[BATCH-UPDATE] flushed %d MIDs", len(mids))
	}

	return nil
}

// startFlushWorker 启动定期刷新worker
func (p *QueryProcessor) startFlushWorker() {
	p.flushWG.Add(1)
	go func() {
		defer p.flushWG.Done()
		for {
			select {
			case <-p.flushTicker.C:
				if err := p.flushPendingMids(p.workerCtx); err != nil {
					log.Printf("Warning: failed to flush pending MIDs: %v", err)
				}
			case <-p.workerCtx.Done():
				return
			}
		}
	}()
}

func (p *QueryProcessor) Process(ctx context.Context) error {
	// 启动批量更新刷新worker
	p.startFlushWorker()
	defer p.flushPendingMids(ctx) // 处理完成后刷新剩余数据

	// 基于历史基线计算剩余任务并打印
	remaining := p.totalAll - p.baseProcessed
	if remaining < 0 {
		remaining = 0
	}
	if p.limitJobs > 0 && p.limitJobs < remaining {
		remaining = p.limitJobs
	}
	log.Printf("Found %d jobs to process (history=%d/%d, remaining=%d)", remaining, p.baseProcessed, p.totalAll, remaining)

	rows, err := p.dbService.StreamJobs(ctx)
	if err != nil {
		return err
	}
	defer rows.Close()

	jobsCh := make(chan JobRecord)
	resultsCh := make(chan jobResult)
	var wg sync.WaitGroup
	for i := 0; i < p.workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batchSize := p.embBatchSize
			if batchSize < 1 {
				batchSize = 1
			}
			for {
				// 组批：尽量拿满 batchSize；channel 关闭后处理尾批
				batchJobs := make([]JobRecord, 0, batchSize)
				batchTexts := make([]string, 0, batchSize)
				for len(batchJobs) < batchSize {
					job, ok := <-jobsCh
					if !ok {
						break
					}
					batchJobs = append(batchJobs, job)
					batchTexts = append(batchTexts, buildFullText(job))
				}
				if len(batchJobs) == 0 {
					return
				}
				if p.trace {
					log.Printf("[EMB-BATCH-REQ] size=%d", len(batchJobs))
				}
				embeddings, err := getEmbeddingsWithRetryAndCache(p.embeddingSvc, batchTexts)
				if err != nil {
					for _, job := range batchJobs {
						resultsCh <- jobResult{job: job, err: fmt.Errorf("Warning: Failed to get batch embedding for job %s: %v", job.MID, err)}
					}
					continue
				}

				// normalize 后做 Chroma 批量 query
				normJobs := make([]JobRecord, 0, len(batchJobs))
				normEmbeddings := make([][]float32, 0, len(batchJobs))
				for bi, job := range batchJobs {
					emb := embeddings[bi]
					norm, err := normalizeEmbedding(emb)
					if err != nil {
						resultsCh <- jobResult{job: job, err: fmt.Errorf("Warning: Failed to normalize embedding for job %s: %v", job.MID, err)}
						continue
					}
					normJobs = append(normJobs, job)
					normEmbeddings = append(normEmbeddings, norm)
				}
				if len(normJobs) == 0 {
					continue
				}
				p.queryChromaWithSplit(ctx, normJobs, normEmbeddings, resultsCh)
			}
		}()
	}

	// feeder：逐行从 DB 读取并投入 jobsCh，避免一次性加载到内存
	go func() {
		defer func() {
			close(jobsCh)
			wg.Wait()
			close(resultsCh)
		}()

		sent := 0
		for rows.Next() {
			// 检查context是否已取消
			select {
			case <-ctx.Done():
				return
			default:
			}

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

			select {
			case jobsCh <- j:
				sent++
				if p.limitJobs > 0 && sent >= p.limitJobs {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	processedSession, processedOkSession, matched := 0, 0, 0
	start := time.Now()
	// 周期性统计输出（每 5s），基于完成通道汇总真实 TPS 与平均值
	statTicker := time.NewTicker(5 * time.Second)
	defer statTicker.Stop()
	doneStat := make(chan struct{})
	completionCh := make(chan struct{}, 10000)
	go func() {
		defer statTicker.Stop()
		prevTime := start
		var totalOk int64
		for {
			select {
			case <-statTicker.C:
				// 取出过去 1s 内完成的数量
				deltaOk := 0
				for {
					select {
					case <-completionCh:
						deltaOk++
					default:
						goto drained
					}
				}
			drained:
				now := time.Now()
				deltaSec := now.Sub(prevTime).Seconds()
				tps := 0.0
				if deltaSec > 0 {
					tps = float64(deltaOk) / deltaSec
				}
				totalOk += int64(deltaOk)
				totalSec := now.Sub(start).Seconds()
				tpsAvg := 0.0
				if totalSec > 0 {
					tpsAvg = float64(totalOk) / totalSec
				}
				curr := p.baseProcessed + int(totalOk)
				pct := 0.0
				if p.totalAll > 0 {
					pct = float64(curr) / float64(p.totalAll) * 100
				}
				log.Printf("[STAT] progress=%.2f%% tps=%.2f/s avg=%.2f/s result=%s", pct, tps, tpsAvg, p.csvWriter.file.Name())
				prevTime = now
			case <-doneStat:
				return
			}
		}
	}()

	// 异步写 CSV（单线程顺序写），无界缓冲通道，处理与写盘完全解耦
	writeCh := make(chan jobResult, 0)
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		pending := 0
		for {
			select {
			case res, ok := <-writeCh:
				if !ok {
					_ = p.csvWriter.Flush()
					return
				}
				if err := p.csvWriter.WriteRow(res.job, res.similarity, res.metadata); err != nil {
					log.Printf("Warning: write csv row failed: %v", err)
				}
				pending++
				if pending%2000 == 0 {
					if err := p.csvWriter.Flush(); err != nil {
						log.Printf("Warning: csv flush failed: %v", err)
					}
				}
			case <-ticker.C:
				if err := p.csvWriter.Flush(); err != nil {
					log.Printf("Warning: csv flush failed: %v", err)
				}
			}
		}
	}()

	for res := range resultsCh {
		// 仅在无错误时，标记处理完成
		if res.err == nil {
			// 使用批量更新机制
			p.addPendingMid(res.job.MID)
			processedOkSession++
			select {
			case completionCh <- struct{}{}:
			default:
			}
		}
		processedSession++
		// 周期性写入断点（无进度日志输出，降低日志噪音）
		if processedOkSession == 1 || processedOkSession%50 == 0 {
			curr := p.baseProcessed + processedOkSession
			writeResumeBase(p.csvWriter.file.Name(), curr)
		}
		if res.err != nil {
			log.Println(res.err)
			continue
		}
		if res.matched {
			matched++
		}
		// 即使未匹配到分类也写入一行，保证 result.csv 行数可与原始/DB 对齐
		writeCh <- res
	}
	close(writeCh)
	writerWG.Wait()
	close(doneStat)
	curr := p.baseProcessed + processedOkSession
	log.Printf("Query completed! total=%d processed=%d matched(>=%.2f)=%d elapsed=%.2fs", p.totalAll, curr, p.minSim, matched, time.Since(start).Seconds())
	writeResumeBase(p.csvWriter.file.Name(), curr)
	return rows.Err()
}

func (p *QueryProcessor) processSingleJob(ctx context.Context, job JobRecord) jobResult {
	full := buildFullText(job)
	if p.trace {
		log.Printf("[EMB-REQ] mid=%s text_len=%d", job.MID, len([]rune(full)))
	}
	embedding, err := p.embeddingSvc.GetEmbedding(full)
	if err != nil {
		return jobResult{job: job, err: fmt.Errorf("Warning: Failed to get embedding for job %s: %v", job.MID, err)}
	}
	return p.processSingleJobWithEmbedding(ctx, job, embedding)
}

func (p *QueryProcessor) processSingleJobWithEmbedding(ctx context.Context, job JobRecord, embedding []float32) jobResult {
	if p.trace {
		log.Printf("[EMB-OK] mid=%s dim=%d", job.MID, len(embedding))
	}
	embedding, err := normalizeEmbedding(embedding)
	if err != nil {
		return jobResult{job: job, err: fmt.Errorf("Warning: Failed to normalize embedding for job %s: %v", job.MID, err)}
	}
	if p.trace {
		log.Printf("[CHROMA-REQ] mid=%s n_results=%d", job.MID, p.chromaTopK)
	}
	qresp, err := p.chromaRepo.Query(ctx, embedding, p.chromaTopK)
	if err != nil {
		return jobResult{job: job, err: fmt.Errorf("Warning: Failed to query ChromaDB for job %s: %v", job.MID, err)}
	}
	if len(qresp.Distances) == 0 || len(qresp.Distances[0]) == 0 {
		return jobResult{job: job}
	}
	metaList := qresp.Metadatas[0]
	distList := qresp.Distances[0]
	return p.processSingleJobWithLists(ctx, job, metaList, distList)
}

// 基于 Chroma 返回的候选列表完成 top1 选择 + 图纠偏（批量/单条共用）。
func (p *QueryProcessor) processSingleJobWithLists(ctx context.Context, job JobRecord, metaList []map[string]interface{}, distList []float64) jobResult {
	if len(distList) == 0 {
		return jobResult{job: job}
	}
	// 取最优候选
	bestSim := 0.0
	bestIdx := 0
	bestDist := 0.0
	for j := 0; j < len(distList); j++ {
		d := distList[j]
		s := cosineDistanceToScore(d)
		if s > bestSim {
			bestSim, bestIdx, bestDist = s, j, d
		}
	}
	if p.trace {
		log.Printf("[CHROMA-BEST] mid=%s similarity=%.4f min-sim=%.2f", job.MID, bestSim, p.minSim)
	}
	// 不以阈值拒绝落表：始终选择最优1条。若低于 min-sim，仅记录日志。
	if bestSim < p.minSim && p.trace {
		log.Printf("[LOWCONF] mid=%s similarity=%.4f < %.2f", job.MID, bestSim, p.minSim)
	}

	var meta map[string]interface{}
	if len(metaList) > bestIdx {
		meta = metaList[bestIdx]
	}

	// 图纠偏（与 cmd/query 一致）
	if meta != nil {
		bestID := safeString(meta, "细类职业")
		if idx := buildMatchIndexForList(metaList, distList); len(idx) > 0 {
			if m2, s2, changed := p.applyGraphCorrection(bestID, meta, bestSim, idx); changed {
				if p.trace {
					log.Printf("[GRAPH] mid=%s corrected best to similarity=%.4f", job.MID, s2)
				}
				meta, bestSim = m2, s2
				_ = bestDist
			} else if p.trace {
				log.Printf("[GRAPH] mid=%s no change", job.MID)
			}
		}
	}

	if p.trace {
		log.Printf("[CSV-WRITE] mid=%s final_similarity=%.4f job_id=%s", job.MID, bestSim, safeString(meta, "细类职业"))
	}
	return jobResult{job: job, similarity: bestSim, metadata: meta, matched: true}
}
