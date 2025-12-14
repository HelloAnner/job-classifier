package main

import (
	"fmt"
	"math"
	"strings"
)

func cleanCell(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	return strings.TrimSpace(s)
}
func truncate(s string, max int) string {
	s = cleanCell(s)
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "..."
}
func safeString(meta map[string]interface{}, key string) string {
	if val, ok := meta[key]; ok && val != nil {
		if str, ok := val.(string); ok {
			str = strings.ReplaceAll(str, "\r\n", " ")
			str = strings.ReplaceAll(str, "\n", " ")
			str = strings.ReplaceAll(str, "\r", " ")
			str = strings.ReplaceAll(str, "\t", " ")
			str = strings.ReplaceAll(str, "\"", "\"\"")
			return str
		}
	}
	return ""
}
func normalizeEmbedding(vec []float32) ([]float32, error) {
	var sum float64
	for _, v := range vec {
		fv := float64(v)
		sum += fv * fv
	}
	norm := math.Sqrt(sum)
	if norm == 0 {
		return nil, fmt.Errorf("embedding norm is zero")
	}
	out := make([]float32, len(vec))
	inv := float32(1 / norm)
	for i, v := range vec {
		out[i] = v * inv
	}
	return out, nil
}
func cosineDistanceToScore(distance float64) float64 {
	score := 1 - distance
	if score < 0 {
		return 0
	}
	if score > 1 {
		return 1
	}
	return score
}
