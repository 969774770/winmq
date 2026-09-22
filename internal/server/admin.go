package server

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"winmq/internal/engine"
)

// handleOverview 管理页总览数据
func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	st := s.Eng.Stats()
	tenq, tdeq := s.Eng.TotalCounters()
	writeJSON(w, 200, map[string]any{
		"queues":         s.Eng.QueueNames(),
		"queueCount":     st.QueueCount,
		"activeMessages": s.Eng.TotalActiveMessages(), // 待消费
		"enqTotal":       tenq,                         // 累计入队（队列汇总，持久化）
		"deqTotal":       tdeq,                         // 累计消费
		"enqRate":        st.EnqRate,
		"deqRate":        st.DeqRate,
		"paused":         s.Paused.Load(),
		// 网络流量（进程累计，供核对与展示）
		"reqTotal": s.ReqTotal.Load(),
		"bytesIn":  s.BytesIn.Load(),
		"bytesOut": s.BytesOut.Load(),
		// 存储磁盘占用
		"diskUsage": s.Eng.DiskUsage(),
	})
}

// handleAdminConfig 管理页显示连接信息
func (s *Server) handleAdminConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{
		"httpAddr":    s.Cfg.HTTPAddr,
		"accessKeyId": s.Cfg.AccessKeyID,
		"dataDir":     s.Cfg.DataDir,
	})
}

// ---------- API Key 管理 ----------

// handleListKeys 列出全部 key
func (s *Server) handleListKeys(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UnixMilli()
	keys := s.Eng.ListKeys()
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		status := "有效"
		if k.ExpiresAt != 0 && now >= k.ExpiresAt {
			status = "已过期"
		}
		out = append(out, map[string]any{
			"key":       k.Key,
			"name":      k.Name,
			"createdAt": k.CreatedAt,
			"expiresAt": k.ExpiresAt, // 0=永不过期
			"status":    status,
		})
	}
	writeJSON(w, 200, map[string]any{"keys": out})
}

// handleCreateKey 创建 key
func (s *Server) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name          string `json:"name"`          // 备注
		ExpiresInDays int    `json:"expiresInDays"` // 有效天数，0=永久
		ExpiresAt     int64  `json:"expiresAt"`     // 或直接指定过期毫秒时间戳
	}
	if err := json.Unmarshal(readBody(r), &req); err != nil {
		writeErr(w, 400, "bad json")
		return
	}
	var expiresAt int64
	if req.ExpiresAt > 0 {
		expiresAt = req.ExpiresAt
	} else if req.ExpiresInDays > 0 {
		expiresAt = time.Now().UnixMilli() + int64(req.ExpiresInDays)*24*3600*1000
	}
	k, err := s.Eng.CreateKey(req.Name, expiresAt)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 201, map[string]any{
		"key":       k.Key,
		"name":      k.Name,
		"createdAt": k.CreatedAt,
		"expiresAt": k.ExpiresAt,
	})
}

// handleDeleteKey 删除 key
func (s *Server) handleDeleteKey(w http.ResponseWriter, r *http.Request) {
	key := pathQueueName(r)
	if err := s.Eng.DeleteKey(key); err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"deleted": true})
}

// ---------- 每日统计 ----------

// handleDaily 每日统计：?days=7 返回最近 N 天 + 今日实时
func (s *Server) handleDaily(w http.ResponseWriter, r *http.Request) {
	days := parseIntDefault(r.URL.Query().Get("days"), 7)
	if days < 1 || days > 90 {
		days = 7
	}
	writeJSON(w, 200, map[string]any{
		"today": s.Eng.Today(),
		"days":  s.Eng.RecentDays(days),
	})
}

// handleCompactQueue 触发队列磁盘空间回收（后台执行，立即返回）
func (s *Server) handleCompactQueue(w http.ResponseWriter, r *http.Request) {
	name := pathQueueName(r)
	if err := s.Eng.CompactQueue(name); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{
		"queueName": name,
		"started":   true,
		"message":   "磁盘回收已在后台开始，完成后自动释放空间",
	})
}

// handleAdminPeek 管理页分页查看队列消息
// GET /admin/api/queues/{name}/messages?offset=0&limit=20
func (s *Server) handleAdminPeek(w http.ResponseWriter, r *http.Request) {
	name := pathQueueName(r)
	q := s.Eng.GetQueue(name)
	if q == nil {
		writeErr(w, 404, "queue not found")
		return
	}
	offset := parseIntDefault(r.URL.Query().Get("offset"), 0)
	if offset < 0 {
		offset = 0
	}
	n := parseIntDefault(r.URL.Query().Get("limit"), 20)
	if n < 1 || n > 200 {
		n = 20
	}
	msgs, total := q.PeekPage(offset, n)
	writeJSON(w, 200, map[string]any{
		"queue":    s.queueView(name),
		"messages": msgs,
		"total":    total,
		"offset":   offset,
		"limit":    n,
	})
}

func parseIntDefault(s string, def int) int {
	v := 0
	if s == "" {
		return def
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return def
		}
		v = v*10 + int(c-'0')
	}
	if v == 0 {
		return def
	}
	return v
}

// handleAdminCreateQueue 管理页创建队列
func (s *Server) handleAdminCreateQueue(w http.ResponseWriter, r *http.Request) {
	var req struct {
		QueueName string `json:"queueName"`
	}
	if err := json.Unmarshal(readBody(r), &req); err != nil {
		writeErr(w, 400, "bad json")
		return
	}
	if err := s.Eng.CreateQueue(req.QueueName); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	writeJSON(w, 201, s.queueView(req.QueueName))
}

// handleAdminDeleteQueue 管理页删除队列
func (s *Server) handleAdminDeleteQueue(w http.ResponseWriter, r *http.Request) {
	name := pathQueueName(r)
	if err := s.Eng.DeleteQueue(name); err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"queueName": name, "deleted": true})
}

// handleTestMessages 发送测试消息
func (s *Server) handleTestMessages(w http.ResponseWriter, r *http.Request) {
	q := s.Eng.GetQueue(pathQueueName(r))
	if q == nil {
		writeErr(w, 404, "queue not found")
		return
	}
	var req struct {
		Count int    `json:"count"`
		Body  string `json:"body"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "bad json")
		return
	}
	if req.Count < 1 || req.Count > 1000 {
		req.Count = 1
	}
	if req.Body == "" {
		req.Body = "测试消息"
	}
	for i := 0; i < req.Count; i++ {
		if _, err := q.Send(req.Body); err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		s.Eng.EnqTotal.Add(1)
	}
	writeJSON(w, 200, map[string]any{"sent": req.Count})
}

// ---------- 内置压测 ----------

type benchResult struct {
	SendCount  int64   `json:"sendCount"`
	SendMs     int64   `json:"sendMs"`
	SendRate   float64 `json:"sendRate"` // 条/秒
	RecvCount  int64   `json:"recvCount"`
	RecvMs     int64   `json:"recvMs"`
	RecvRate   float64 `json:"recvRate"` // 条/秒（接收即删除）
	SendFailed int64   `json:"sendFailed"`
	RecvFailed int64   `json:"recvFailed"`
}

// handleBench 对指定队列做引擎级压测（不走 HTTP），验证 1w/s 指标
func (s *Server) handleBench(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Queue       string `json:"queue"`
		Count       int    `json:"count"`
		Concurrency int    `json:"concurrency"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "bad json")
		return
	}
	if req.Count < 1 || req.Count > 5000000 {
		req.Count = 10000
	}
	if req.Concurrency < 1 || req.Concurrency > 128 {
		req.Concurrency = 16
	}
	q := s.Eng.GetQueue(req.Queue)
	if q == nil {
		// 自动创建压测队列
		if err := s.Eng.CreateQueue(req.Queue); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		q = s.Eng.GetQueue(req.Queue)
	}

	res := runEngineBench(q, req.Count, req.Concurrency)
	writeJSON(w, 200, res)
}

// runEngineBench 引擎级压测：并发发送 count 条（每条 fsync 落盘），
// 再并发消费 count 条（接收即删除）清空，统计两端速率
func runEngineBench(q *engine.Queue, count, concurrency int) *benchResult {
	res := &benchResult{}
	var sendFail atomic.Int64

	// ---- 发送阶段 ----
	var swg sync.WaitGroup
	per := count / concurrency
	rem := count % concurrency
	start := time.Now()
	for i := 0; i < concurrency; i++ {
		n := per
		if i < rem {
			n++
		}
		swg.Add(1)
		go func(n int) {
			defer swg.Done()
			for j := 0; j < n; j++ {
				if _, err := q.Send("bench-msg-0123456789abcdef"); err != nil {
					sendFail.Add(1)
				}
			}
		}(n)
	}
	swg.Wait()
	res.SendMs = time.Since(start).Milliseconds()
	res.SendCount = int64(count) - sendFail.Load()
	res.SendFailed = sendFail.Load()
	if res.SendMs > 0 {
		res.SendRate = float64(res.SendCount) / float64(res.SendMs) * 1000
	}

	// ---- 消费阶段（接收即删除）----
	var rwg sync.WaitGroup
	var got atomic.Int64
	recvStart := time.Now()
	for i := 0; i < concurrency; i++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			empty := 0
			for {
				if got.Load() >= int64(count) {
					return
				}
				if q.Receive(0) == nil {
					if got.Load() >= int64(count) {
						return
					}
					empty++
					if empty > 500 { // 连续空转保护
						return
					}
					time.Sleep(time.Millisecond)
					continue
				}
				empty = 0
				got.Add(1)
			}
		}()
	}
	rwg.Wait()
	res.RecvMs = time.Since(recvStart).Milliseconds()
	res.RecvCount = got.Load()
	res.RecvFailed = int64(count) - res.RecvCount
	if res.RecvMs > 0 {
		res.RecvRate = float64(res.RecvCount) / float64(res.RecvMs) * 1000
	}
	return res
}
