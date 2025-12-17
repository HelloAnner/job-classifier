package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

func processSingleCSV(ctx context.Context, t fileTask, opts options) error {
	log.Printf("[START] %s -> %s", t.csvPath, t.outDir)

    // 启动检查：若 count.txt 存在且自洽，则“默认跳过”。
    // 但在跳过之前，做一次轻量级历史数据体检：
    // - 仅检查 result.csv 是否存在重复 job_id；如存在则原地去重（保留首条）。
    // 这样可以修复“后续追加导致的重复行，但 count.txt 仍然有效”的场景。
    if ci, err := readCountInfo(t.countPath); err == nil {
        if ci.Valid() {
            if _, err := os.Stat(t.dbPath); err == nil {
                // 轻量去重：仅在发现重复时才改写 result.csv；不触发完整重对账，成本可控。
                if fixed, fixErr := preflightFixResultDuplicates(t); fixErr != nil {
                    log.Printf("[WARN] %s: preflight duplicate check failed: %v", t.baseName, fixErr)
                } else if fixed {
                    // 去重后尽量刷新 count.txt，使 processed 与文件实际行数一致；
                    // 若统计不匹配则仅告警，不中断流程。
                    if rows, err1 := countCSVRows(t.resultPath); err1 == nil {
                        if ig, err2 := countCSVRows(t.ignorePath); err2 == nil {
                            if dbAll, _, err3 := countDBAllAndFiltered(context.Background(), t.dbPath); err3 == nil {
                                if err := writeCountStrict(t.countPath, ci.Total, rows, ig, dbAll); err != nil {
                                    log.Printf("[WARN] %s: update count.txt after dedup failed: %v (rows=%d ignore=%d db=%d total=%d)", t.baseName, err, rows, ig, dbAll, ci.Total)
                                }
                            }
                        }
                    }
                }
                log.Printf("[SKIP] %s: valid count.txt found (total=%d processed=%d filtered=%d db=%d)", t.baseName, ci.Total, ci.Processed, ci.Filtered, ci.DB)
                return nil
            }
            _ = os.Remove(t.countPath)
            log.Printf("[COUNT-RESET] %s: count.txt exists but origin.db missing, removed and will continue", t.baseName)
        } else {
            _ = os.Remove(t.countPath)
            log.Printf("[COUNT-RESET] %s: count.txt invalid, removed and will continue", t.baseName)
        }
    }

	// 尝试多轮修复：去重/补漏/回填，直到三方数据一致再写 count.txt
	for attempt := 1; attempt <= 3; attempt++ {
		originTotal, originFiltered, err := ensureDBConsistent(ctx, t, opts)
		if err != nil {
			return err
		}

		dbAll, dbFiltered, err := countDBAllAndFiltered(ctx, t.dbPath)
		if err != nil {
			return err
		}
		if dbAll != originTotal {
			return fmt.Errorf("%s: db rows mismatch after import: db=%d origin=%d", t.baseName, dbAll, originTotal)
		}
		if dbFiltered != originFiltered {
			log.Printf("[DB-FILTER-MISMATCH] %s: db_filtered=%d origin_filtered=%d, will re-import to fix status", t.baseName, dbFiltered, originFiltered)
			if err := forceUpsertImport(ctx, t, opts); err != nil {
				return err
			}
			dbAll, dbFiltered, err = countDBAllAndFiltered(ctx, t.dbPath)
			if err != nil {
				return err
			}
			if dbAll != originTotal || dbFiltered != originFiltered {
				return fmt.Errorf("%s: db filtered mismatch after retry: db_total=%d db_filtered=%d origin_total=%d origin_filtered=%d", t.baseName, dbAll, dbFiltered, originTotal, originFiltered)
			}
		}

		// ignore.csv：数量不一致就重建（避免重复/缺失）
		ignoreRows, err := countCSVRows(t.ignorePath)
		if err != nil {
			return fmt.Errorf("count ignore.csv failed: %w", err)
		}
		if ignoreRows != originFiltered {
			log.Printf("[IGNORE-FIX] %s: ignore_rows=%d != origin_filtered=%d, will regenerate ignore.csv from DB", t.baseName, ignoreRows, originFiltered)
			if err := regenerateIgnoreCSV(ctx, t); err != nil {
				return fmt.Errorf("regenerate ignore.csv failed: %w", err)
			}
			ignoreRows, err = countCSVRows(t.ignorePath)
			if err != nil {
				return fmt.Errorf("count ignore.csv failed: %w", err)
			}
		}

		expectedResult := originTotal - originFiltered

		processedSet, resultRows, err := loadProcessedSet(t.resultPath)
		if err != nil {
			log.Printf("[RESULT-INVALID] %s: read result.csv failed, will remove and rebuild: %v", t.baseName, err)
			_ = os.Remove(t.resultPath)
			processedSet = make(map[string]struct{})
			resultRows = 0
		}

		uniqueResult := int64(len(processedSet))
		needRewrite := resultRows != uniqueResult

		validCount, missingCount, missingSample, extrasCount, err := diffResultAgainstDB(ctx, t.dbPath, processedSet)
		if err != nil {
			return err
		}
		if extrasCount > 0 {
			needRewrite = true
		}
		if needRewrite {
			log.Printf("[RESULT-FIX] %s: rows=%d unique=%d valid=%d missing=%d extras=%d, will rewrite result.csv", t.baseName, resultRows, uniqueResult, validCount, missingCount, extrasCount)
			if len(missingSample) > 0 {
				log.Printf("[MISSING-SAMPLE] %s: %s", t.baseName, strings.Join(missingSample, ","))
			}

			drop, err := buildResultDropSet(ctx, t.dbPath, processedSet)
			if err != nil {
				return err
			}
			newSet, kept, err := rewriteResultCSVDedupAndDrop(t.resultPath, drop)
			if err != nil {
				return fmt.Errorf("rewrite result.csv failed: %w", err)
			}
			processedSet = newSet
			resultRows = kept
			uniqueResult = int64(len(processedSet))
		}

		// 结果缺失：按 mid 补齐（QueryProcessor 自带按 processedSet 跳过机制）
		needQuery := uniqueResult < expectedResult
		if needQuery {
			appendMode := false
			if _, err := os.Stat(t.resultPath); err == nil {
				appendMode = true
			}
			log.Printf("[RESUME] %s: result_unique=%d expected=%d, will backfill missing by querying", t.baseName, uniqueResult, expectedResult)
			qp, err := NewQueryProcessor(ctx, t.dbPath, t.resultPath, "db/job_graph.db", opts.queryWorkers, opts.embBatch, opts.chromaTopK, opts.embTimeout, opts.trace, opts.limitJobs, appendMode, processedSet, opts.minSim)
			if err != nil {
				return fmt.Errorf("init query processor failed: %w", err)
			}
			if err := qp.Process(ctx); err != nil {
				qp.Close()
				return fmt.Errorf("query process failed: %w", err)
			}
			qp.Close()
		}

		// 最终对账（只看三方一致）
		resultRows, err = countCSVRows(t.resultPath)
		if err != nil {
			return fmt.Errorf("count result.csv failed: %w", err)
		}
		ignoreRows, err = countCSVRows(t.ignorePath)
		if err != nil {
			return fmt.Errorf("count ignore.csv failed: %w", err)
		}
		dbAll, _, err = countDBAllAndFiltered(ctx, t.dbPath)
		if err != nil {
			return err
		}

		if err := writeCountStrict(t.countPath, originTotal, resultRows, ignoreRows, dbAll); err != nil {
			log.Printf("[REPAIR-RETRY] %s attempt=%d: %v (origin=%d result=%d ignore=%d db=%d)", t.baseName, attempt, err, originTotal, resultRows, ignoreRows, dbAll)
			_ = os.Remove(t.countPath)

			// 下一轮：再次去重 + 补漏
			time.Sleep(200 * time.Millisecond)
			continue
		}

		log.Printf("[DONE] %s -> result=%s ignore=%s origin.db=%s count=%s", t.baseName, t.resultPath, t.ignorePath, t.dbPath, t.countPath)
		return nil
	}

	return fmt.Errorf("%s: failed to reconcile after retries", t.baseName)
}

func ensureDBConsistent(ctx context.Context, t fileTask, opts options) (originTotal, originFiltered int64, err error) {
	// DB 不存在：直接导入
	if _, err := os.Stat(t.dbPath); err != nil {
		db, err := ensureDB(t.dbPath)
		if err != nil {
			return 0, 0, fmt.Errorf("open sqlite failed: %w", err)
		}
		stats, err := streamCSVToDB(ctx, t.csvPath, t.ignorePath, db, t.source, opts.batchSize, opts.trace)
		db.Close()
		if err != nil {
			return 0, 0, fmt.Errorf("stream import failed: %w", err)
		}
		log.Printf("[IMPORT] %s: total=%d to_import=%d filtered(<6)=%d", t.baseName, stats.Total, stats.Imported, stats.Filtered)
		return stats.Total, stats.Filtered, nil
	}

	// DB 存在：先基于原始 CSV 统计，再校验 DB 行数
	originTotal, originFiltered, err = countOriginStats(t.csvPath)
	if err != nil {
		return 0, 0, fmt.Errorf("count origin csv failed: %w", err)
	}

	dbSvc, err := NewDatabaseService(t.dbPath)
	if err != nil {
		return 0, 0, fmt.Errorf("open origin.db failed: %w", err)
	}
	dbCount, err := dbSvc.CountAllJobs(ctx)
	_ = dbSvc.Close()
	if err != nil {
		return 0, 0, fmt.Errorf("count db rows failed: %w", err)
	}

	if int64(dbCount) < originTotal {
		log.Printf("[DB-FIX] %s: db_rows=%d < origin_total=%d, will upsert import missing rows", t.baseName, dbCount, originTotal)
		return originTotal, originFiltered, forceUpsertImport(ctx, t, opts)
	}
	if int64(dbCount) > originTotal {
		log.Printf("[DB-REBUILD] %s: db_rows=%d > origin_total=%d, will rebuild output folder", t.baseName, dbCount, originTotal)
		if err := rebuildOutputAndImport(ctx, t, opts); err != nil {
			return 0, 0, err
		}
		// rebuild 后 stats 由 import 重新计算
		ot, of, err := countOriginStats(t.csvPath)
		if err != nil {
			return 0, 0, err
		}
		return ot, of, nil
	}

	return originTotal, originFiltered, nil
}

func rebuildOutputAndImport(ctx context.Context, t fileTask, opts options) error {
	_ = os.RemoveAll(t.outDir)
	if err := os.MkdirAll(t.outDir, 0o755); err != nil {
		return fmt.Errorf("recreate output folder failed: %w", err)
	}
	db, err := ensureDB(t.dbPath)
	if err != nil {
		return fmt.Errorf("open sqlite failed: %w", err)
	}
	stats, err := streamCSVToDB(ctx, t.csvPath, t.ignorePath, db, t.source, opts.batchSize, opts.trace)
	db.Close()
	if err != nil {
		return fmt.Errorf("stream import failed: %w", err)
	}
	log.Printf("[IMPORT] %s: total=%d to_import=%d filtered(<6)=%d", t.baseName, stats.Total, stats.Imported, stats.Filtered)
	return nil
}

func forceUpsertImport(ctx context.Context, t fileTask, opts options) error {
	db, err := ensureDB(t.dbPath)
	if err != nil {
		return fmt.Errorf("open sqlite failed: %w", err)
	}
	if _, err := streamCSVToDB(ctx, t.csvPath, t.ignorePath, db, t.source, opts.batchSize, opts.trace); err != nil {
		db.Close()
		return fmt.Errorf("stream upsert import failed: %w", err)
	}
	db.Close()
	return nil
}

func countDBAllAndFiltered(ctx context.Context, dbPath string) (all int64, filtered int64, err error) {
	dbSvc, err := NewDatabaseService(dbPath)
	if err != nil {
		return 0, 0, fmt.Errorf("open origin.db failed: %w", err)
	}
	defer dbSvc.Close()

	allN, err := dbSvc.CountAllJobs(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("count db rows failed: %w", err)
	}
	filtN, err := dbSvc.CountFilteredJobs(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("count db filtered failed: %w", err)
	}
	return int64(allN), int64(filtN), nil
}

// diffResultAgainstDB 计算 result.csv(mid集合) 与 DB(非过滤) 的差异。
// 返回值：
// - validCount: resultSet 中存在于 DB 非过滤集合的数量
// - missingCount: DB 非过滤集合中缺失的数量
// - missingSample: 缺失 mid 的样例（最多 10 个）
// - extrasCount: resultSet 中不在 DB 非过滤集合的数量（含 DB 不存在 或 DB 已过滤）
func diffResultAgainstDB(ctx context.Context, dbPath string, resultSet map[string]struct{}) (validCount int64, missingCount int64, missingSample []string, extrasCount int64, err error) {
	dbSvc, err := NewDatabaseService(dbPath)
	if err != nil {
		return 0, 0, nil, 0, fmt.Errorf("open origin.db failed: %w", err)
	}
	defer dbSvc.Close()

	rows, err := dbSvc.db.QueryContext(ctx, `SELECT mid FROM jobs
        WHERE status != '过滤'
          AND job_intro IS NOT NULL AND job_intro != ''
        ORDER BY mid`)
	if err != nil {
		return 0, 0, nil, 0, fmt.Errorf("query db mids failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var mid string
		if err := rows.Scan(&mid); err != nil {
			return 0, 0, nil, 0, fmt.Errorf("scan db mid failed: %w", err)
		}
		mid = strings.TrimSpace(mid)
		if mid == "" {
			continue
		}
		if _, ok := resultSet[mid]; ok {
			validCount++
		} else {
			missingCount++
			if len(missingSample) < 10 {
				missingSample = append(missingSample, mid)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, nil, 0, err
	}

	unique := int64(len(resultSet))
	extrasCount = unique - validCount
	return validCount, missingCount, missingSample, extrasCount, nil
}

// buildResultDropSet 计算需要从 result.csv 删除的 mid 集合（DB 不存在或已过滤）。
// 入参 resultSet 会被复用构造成 dropSet；调用者不应再使用 resultSet。
func buildResultDropSet(ctx context.Context, dbPath string, resultSet map[string]struct{}) (map[string]struct{}, error) {
	dbSvc, err := NewDatabaseService(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open origin.db failed: %w", err)
	}
	defer dbSvc.Close()

	rows, err := dbSvc.db.QueryContext(ctx, `SELECT mid FROM jobs
        WHERE status != '过滤'
          AND job_intro IS NOT NULL AND job_intro != ''
        ORDER BY mid`)
	if err != nil {
		return nil, fmt.Errorf("query db mids failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var mid string
		if err := rows.Scan(&mid); err != nil {
			return nil, fmt.Errorf("scan db mid failed: %w", err)
		}
		mid = strings.TrimSpace(mid)
		if mid == "" {
			continue
		}
		delete(resultSet, mid)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return resultSet, nil
}
