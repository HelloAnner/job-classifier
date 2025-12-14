package scanner

import (
	"bytes"
	"io"
	"os"
)

func countNewlinesFromOffset(filePath string, offset int64) (int, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return 0, err
		}
	}

	buf := make([]byte, 1024*1024)
	count := 0
	for {
		n, err := f.Read(buf)
		if n > 0 {
			count += bytes.Count(buf[:n], []byte{'\n'})
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return count, err
		}
	}
	return count, nil
}
