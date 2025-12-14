package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type CountInfo struct {
	Total     int64
	Processed int64
	Filtered  int64
	DB        int64
	OK        bool
}

func (c CountInfo) Valid() bool {
	if !c.OK {
		return false
	}
	if c.Total < 0 || c.Processed < 0 || c.Filtered < 0 || c.DB < 0 {
		return false
	}
	if c.Processed+c.Filtered != c.Total {
		return false
	}
	return c.DB == c.Total
}

func readCountInfo(path string) (CountInfo, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return CountInfo{}, err
	}

	var out CountInfo
	var hasTotal, hasProcessed, hasFiltered, hasDB, hasOK bool

	lines := strings.Split(string(b), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return CountInfo{}, fmt.Errorf("invalid count.txt line: %q", line)
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		switch key {
		case "total":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return CountInfo{}, fmt.Errorf("parse total failed: %w", err)
			}
			out.Total = n
			hasTotal = true
		case "processed":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return CountInfo{}, fmt.Errorf("parse processed failed: %w", err)
			}
			out.Processed = n
			hasProcessed = true
		case "filtered":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return CountInfo{}, fmt.Errorf("parse filtered failed: %w", err)
			}
			out.Filtered = n
			hasFiltered = true
		case "db":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return CountInfo{}, fmt.Errorf("parse db failed: %w", err)
			}
			out.DB = n
			hasDB = true
		case "ok":
			switch strings.ToLower(val) {
			case "true":
				out.OK = true
			case "false":
				out.OK = false
			default:
				return CountInfo{}, fmt.Errorf("parse ok failed: %q", val)
			}
			hasOK = true
		}
	}

	if !hasTotal || !hasProcessed || !hasFiltered || !hasDB || !hasOK {
		return CountInfo{}, fmt.Errorf("count.txt missing keys (total=%v processed=%v filtered=%v db=%v ok=%v)", hasTotal, hasProcessed, hasFiltered, hasDB, hasOK)
	}
	return out, nil
}

// 写 count.txt（严格校验）：
// - total = 原始 CSV（有效行）行数
// - processed = result_rows
// - filtered = ignore_rows
// - ok = (processed + filtered == total) && (db_rows == total)
func writeCountStrict(path string, originTotal, processed, filtered, dbRows int64) error {
	ok := (processed+filtered == originTotal) && (dbRows == originTotal)
	if !ok {
		return fmt.Errorf("count mismatch: total=%d processed=%d filtered=%d db=%d", originTotal, processed, filtered, dbRows)
	}
	content := fmt.Sprintf("total=%d\nprocessed=%d\nfiltered=%d\ndb=%d\nok=%v\n", originTotal, processed, filtered, dbRows, ok)
	return os.WriteFile(path, []byte(content), 0o644)
}
