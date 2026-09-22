package gui

// logbuf.go —— 日志双写缓冲：写文件 + 内存环形缓冲（窗口显示）

import (
	"io"
	"os"
	"strings"
	"sync"
)

const logBufMax = 300

// LogBuffer 日志缓冲
type LogBuffer struct {
	mu   sync.Mutex
	buf  []string
	file *os.File
	dirty bool
}

// NewLogBuffer 创建（追加写日志文件）
func NewLogBuffer(path string) *LogBuffer {
	lb := &LogBuffer{}
	if f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
		lb.file = f
	}
	return lb
}

// Write 实现 io.Writer（供 log.SetOutput 使用）
func (lb *LogBuffer) Write(p []byte) (int, error) {
	s := strings.TrimRight(string(p), "\n")
	lb.mu.Lock()
	lb.buf = append(lb.buf, s)
	if len(lb.buf) > logBufMax {
		lb.buf = lb.buf[len(lb.buf)-logBufMax:]
	}
	lb.dirty = true
	var ferr error
	if lb.file != nil {
		_, ferr = lb.file.Write(append([]byte(s), '\n'))
	}
	lb.mu.Unlock()
	return len(p), ferr
}

// Snapshot 返回最近日志文本
func (lb *LogBuffer) Snapshot() string {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return strings.Join(lb.buf, "\r\n")
}

// Close 关闭日志文件
func (lb *LogBuffer) Close() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.file != nil {
		lb.file.Close()
		lb.file = nil
	}
}

// TeeWriter 多路写
type TeeWriter []io.Writer

func (t TeeWriter) Write(p []byte) (int, error) {
	for _, w := range t {
		w.Write(p)
	}
	return len(p), nil
}
