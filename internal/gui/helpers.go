package gui

// helpers.go —— 显示辅助

import (
	"fmt"
	"log"
	"time"
)

// logf 全局日志（经 LogBuffer 双写）
var logWriter logger = stdLogger{}

type logger interface{ Write(p []byte) (int, error) }

type stdLogger struct{}

func (stdLogger) Write(p []byte) (int, error) { return len(p), nil }

func logf(format string, args ...any) {
	log.Printf(format, args...)
}

// fmtDuration 运行时长（自动单位）
func fmtDuration(d time.Duration) string {
	d = d.Round(time.Second)
	days := int64(d) / int64(24*time.Hour)
	d -= time.Duration(days) * 24 * time.Hour
	h := int64(d) / int64(time.Hour)
	d -= time.Duration(h) * time.Hour
	m := int64(d) / int64(time.Minute)
	d -= time.Duration(m) * time.Minute
	s := int64(d) / int64(time.Second)
	if days > 0 {
		return fmt.Sprintf("%d天%02d:%02d:%02d", days, h, m, s)
	}
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func fmtInt(v int64) string {
	if v < 0 {
		return "-"
	}
	s := fmt.Sprintf("%d", v)
	// 千分位
	n := len(s)
	if n <= 3 {
		return s
	}
	out := make([]byte, 0, n+n/3)
	for i, c := range []byte(s) {
		out = append(out, c)
		rem := n - i - 1
		if rem > 0 && rem%3 == 0 {
			out = append(out, ',')
		}
	}
	return string(out)
}

func fmtBytes(v int64) string {
	const kb, mb, gb = 1<<10, 1<<20, 1<<30
	switch {
	case v >= gb:
		return fmt.Sprintf("%.2f GB", float64(v)/gb)
	case v >= mb:
		return fmt.Sprintf("%.2f MB", float64(v)/mb)
	case v >= kb:
		return fmt.Sprintf("%.2f KB", float64(v)/kb)
	default:
		return fmt.Sprintf("%d B", v)
	}
}
