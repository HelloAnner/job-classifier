package scanner

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type CountInfo struct {
	Total     int
	Processed int
	Filtered  int
	OK        bool
}

func (c CountInfo) Valid() bool {
	if !c.OK {
		return false
	}
	if c.Total < 0 || c.Processed < 0 || c.Filtered < 0 {
		return false
	}
	return c.Processed+c.Filtered == c.Total
}

func ReadCountInfo(path string) (CountInfo, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return CountInfo{}, err
	}

	var out CountInfo
	var hasTotal, hasProcessed, hasFiltered, hasOK bool

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
			n, err := strconv.Atoi(val)
			if err != nil {
				return CountInfo{}, fmt.Errorf("parse total failed: %w", err)
			}
			out.Total = n
			hasTotal = true
		case "processed":
			n, err := strconv.Atoi(val)
			if err != nil {
				return CountInfo{}, fmt.Errorf("parse processed failed: %w", err)
			}
			out.Processed = n
			hasProcessed = true
		case "filtered":
			n, err := strconv.Atoi(val)
			if err != nil {
				return CountInfo{}, fmt.Errorf("parse filtered failed: %w", err)
			}
			out.Filtered = n
			hasFiltered = true
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

	if !hasTotal || !hasProcessed || !hasFiltered || !hasOK {
		return CountInfo{}, fmt.Errorf("count.txt missing keys (total=%v processed=%v filtered=%v ok=%v)", hasTotal, hasProcessed, hasFiltered, hasOK)
	}
	return out, nil
}
