package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"winmq/internal/config"
	"winmq/internal/engine"
)

// Server HTTP 服务
type Server struct {
	Eng    *engine.Engine
	Cfg    *config.Config
	Paused atomic.Bool
	mux    *http.ServeMux

	// 网络流量统计（原子计数，开销可忽略）
	ReqTotal  atomic.Int64 // 请求总数
	BytesIn   atomic.Int64 // 累计请求字节
	BytesOut  atomic.Int64 // 累计响应字节

	rateMu        sync.Mutex
	lastReqSample int64
}

// NewServer 创建并注册路由
func NewServer(eng *engine.Engine, cfg *config.Config) *Server {
	s := &Server{Eng: eng, Cfg: cfg}
	mux := http.NewServeMux()

	// ---- 队列与消息 API（X-Api-Key 认证）----
	mux.HandleFunc("POST /api/queues", s.auth(s.handleCreateQueue))
	mux.HandleFunc("GET /api/queues", s.auth(s.handleListQueues))
	mux.HandleFunc("GET /api/queues/{name}", s.auth(s.handleGetQueue))
	mux.HandleFunc("DELETE /api/queues/{name}", s.auth(s.handleDeleteQueue))
	mux.HandleFunc("POST /api/queues/{name}/messages", s.auth(s.handleSend))
	mux.HandleFunc("POST /api/queues/{name}/messages/batch", s.auth(s.handleSendBatch))
	// 接收即删除：取出的消息立即被移除，不再需要 ack / 可见性超时接口
	mux.HandleFunc("GET /api/queues/{name}/messages", s.auth(s.handleReceive))
	mux.HandleFunc("GET /api/queues/{name}/messages/peek", s.auth(s.handlePeek))
	mux.HandleFunc("POST /api/queues/{name}/peek-batch", s.auth(s.handlePeekBatch))

	// ---- 管理 API（token 登录）----
	mux.HandleFunc("POST /admin/login", s.handleAdminLogin)
	mux.HandleFunc("GET /admin/api/overview", s.admin(s.handleOverview))
	mux.HandleFunc("POST /admin/api/queues", s.admin(s.handleAdminCreateQueue))
	mux.HandleFunc("DELETE /admin/api/queues/{name}", s.admin(s.handleAdminDeleteQueue))
	mux.HandleFunc("GET /admin/api/queues/{name}/messages", s.admin(s.handleAdminPeek))
	mux.HandleFunc("POST /admin/api/queues/{name}/test-messages", s.admin(s.handleTestMessages))
	mux.HandleFunc("POST /admin/api/queues/{name}/compact", s.admin(s.handleCompactQueue))
	mux.HandleFunc("POST /admin/api/bench", s.admin(s.handleBench))
	mux.HandleFunc("GET /admin/api/config", s.admin(s.handleAdminConfig))
	mux.HandleFunc("GET /admin/api/keys", s.admin(s.handleListKeys))
	mux.HandleFunc("POST /admin/api/keys", s.admin(s.handleCreateKey))
	mux.HandleFunc("DELETE /admin/api/keys/{key}", s.admin(s.handleDeleteKey))
	mux.HandleFunc("GET /admin/api/daily", s.admin(s.handleDaily))

	// ---- 静态管理页（本地资源，无外部 CDN）----
	mux.HandleFunc("/", s.handleStatic)

	s.mux = mux
	return s
}

// Handler 返回带全局中间件的根 handler
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[http] panic: %v", rec)
				writeErr(w, 500, "internal error")
			}
		}()
		if s.Paused.Load() && strings.HasPrefix(r.URL.Path, "/api/") {
			writeErr(w, http.StatusServiceUnavailable, "service paused")
			return
		}
		// 轻量流量统计（每次请求 3 次原子加，无锁）
		s.ReqTotal.Add(1)
		if r.ContentLength > 0 {
			s.BytesIn.Add(r.ContentLength)
		}
		if s.Eng != nil {
			s.Eng.RecordRequest(r.ContentLength, 0) // 发送字节由 countWriter 统计
		}
		s.mux.ServeHTTP(&countWriter{ResponseWriter: w, s: s}, r)
	})
}

// ReqRate 请求速率（次/秒；UI 每秒采样一次取增量）
func (s *Server) ReqRate() int64 {
	s.rateMu.Lock()
	defer s.rateMu.Unlock()
	cur := s.ReqTotal.Load()
	rate := cur - s.lastReqSample
	if rate < 0 {
		rate = 0
	}
	s.lastReqSample = cur
	return rate
}

// countWriter 包装 ResponseWriter 统计响应字节
type countWriter struct {
	http.ResponseWriter
	s      *Server
	wrote  int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.ResponseWriter.Write(p)
	c.wrote += int64(n)
	c.s.BytesOut.Add(int64(n))
	if c.s.Eng != nil {
		c.s.Eng.RecordBytesOut(int64(n))
	}
	return n, err
}

func (c *countWriter) WriteHeader(code int) {
	c.ResponseWriter.WriteHeader(code)
}

// ---------- API Key 鉴权 ----------

// 消息 API 认证：请求头 X-Api-Key: <key>（后台 key 管理中创建，可设过期时间）
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Api-Key")
		if key == "" {
			key = r.URL.Query().Get("key") // 兼容 query 方式
		}
		if key == "" {
			writeErr(w, 401, "missing X-Api-Key header")
			return
		}
		if !s.Eng.ValidateKey(key) {
			writeErr(w, 401, "api key 无效或已过期")
			return
		}
		next(w, r)
	}
}

// ---------- 管理 token ----------

func (s *Server) adminToken() string {
	return hmacSHA256Hex(s.Cfg.AccessKeySecret, "winmq-admin-session")
}

func (s *Server) admin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tk := r.Header.Get("X-Admin-Token")
		if subtle.ConstantTimeCompare([]byte(tk), []byte(s.adminToken())) != 1 {
			writeErr(w, 401, "admin token invalid, please login")
			return
		}
		next(w, r)
	}
}

func (s *Server) handleAdminLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Secret string `json:"secret"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "bad json")
		return
	}
	if subtle.ConstantTimeCompare([]byte(req.Secret), []byte(s.Cfg.AccessKeySecret)) != 1 {
		writeErr(w, 401, "secret 错误，请查看 config.json 中的 accessKeySecret")
		return
	}
	writeJSON(w, 200, map[string]string{"token": s.adminToken()})
}

// ---------- 通用工具 ----------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func readBody(r *http.Request) []byte {
	buf, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	return buf
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func hmacSHA256Hex(key, str string) string {
	m := hmac.New(sha256.New, []byte(key))
	m.Write([]byte(str))
	return hex.EncodeToString(m.Sum(nil))
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func pathQueueName(r *http.Request) string {
	return r.PathValue("name")
}
