package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// 与 cmd/query 的格式化/工具函数一致
type structuredPayload struct {
	Summary          string   `json:"岗位概述"`
	Responsibilities []string `json:"核心职责"`
	Skills           []string `json:"技能标签"`
	Industry         string   `json:"行业"`
	Locations        []string `json:"工作地点"`
	CategoryHints    []struct {
		Name       string  `json:"名称"`
		Confidence float64 `json:"信心"`
	} `json:"可能对应的大类"`
}

func formatStructuredText(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	var p structuredPayload
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return raw
	}
	var sec []string
	if p.Summary != "" {
		sec = append(sec, fmt.Sprintf("岗位概述: %s", p.Summary))
	}
	if len(p.Responsibilities) > 0 {
		sec = append(sec, fmt.Sprintf("核心职责: %s", strings.Join(p.Responsibilities, "；")))
	}
	if len(p.Skills) > 0 {
		sec = append(sec, fmt.Sprintf("技能标签: %s", strings.Join(p.Skills, "；")))
	}
	if p.Industry != "" {
		sec = append(sec, fmt.Sprintf("行业: %s", p.Industry))
	}
	if len(p.Locations) > 0 {
		sec = append(sec, fmt.Sprintf("工作地点: %s", strings.Join(p.Locations, "；")))
	}
	if len(p.CategoryHints) > 0 {
		var hints []string
		for _, h := range p.CategoryHints {
			if h.Name != "" {
				hints = append(hints, fmt.Sprintf("%s(信心%.2f)", h.Name, h.Confidence))
			}
		}
		if len(hints) > 0 {
			sec = append(sec, fmt.Sprintf("大类猜测: %s", strings.Join(hints, "；")))
		}
	}
	return strings.Join(sec, "\n")
}

// 构造与历史逻辑一致的 embedding 输入文本。
func buildFullText(job JobRecord) string {
	structuredText := ""
	if job.Structured.Valid {
		structuredText = formatStructuredText(job.Structured.String)
	}
	var desc []string
	if structuredText != "" {
		desc = append(desc, structuredText)
	}
	desc = append(desc, fmt.Sprintf("岗位描述: %s", job.JobIntro))
	desc = append(desc, fmt.Sprintf("来源: %s", job.Source))
	desc = append(desc, fmt.Sprintf("状态: %s", job.Status))
	desc = append(desc, fmt.Sprintf("分类: %s", job.Category))
	return strings.Join(desc, "\n")
}
