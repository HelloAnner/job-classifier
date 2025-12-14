package scanner

import (
	"bufio"
	"encoding/csv"
	"io"
	"os"
	"strings"
)

type CSVCountSpec struct {
	HasHeader bool
	MinCols   int
	KeyCol    int
}

func countCSVDataRows(filePath string, spec CSVCountSpec) (int, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	r.ReuseRecord = true
	r.LazyQuotes = true
	r.FieldsPerRecord = -1

	if spec.HasHeader {
		if _, err := r.Read(); err != nil {
			if err == io.EOF {
				return 0, nil
			}
			return 0, err
		}
	}

	count := 0
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return count, err
		}
		if spec.MinCols > 0 && len(rec) < spec.MinCols {
			continue
		}
		if spec.KeyCol >= 0 && spec.KeyCol < len(rec) {
			if strings.TrimSpace(rec[spec.KeyCol]) == "" {
				continue
			}
		}
		count++
	}
	return count, nil
}
