package server

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"

	"winmq/internal/engine"
)

// ---------- 队列管理 API ----------

// POST /api/queues  {"queueName":"q1"}
func (s *Server) handleCreateQueue(w http.ResponseWriter, r *http.Request) {
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
	writeJSON(w, 201, map[string]any{"queueName": req.QueueName, "created": true})
}

func (s *Server) handleListQueues(w http.ResponseWriter, r *http.Request) {
	names := s.Eng.QueueNames()
	out := make([]map[string]any, 0, len(names))
	for _, n := range names {
		out = append(out, s.queueView(n))
	}
	writeJSON(w, 200, map[string]any{"queues": out})
}

func (s *Server) handleGetQueue(w http.ResponseWriter, r *http.Request) {
	name := pathQueueName(r)
	if s.Eng.GetQueue(name) == nil {
		writeErr(w, 404, "queue not found")
		return
	}
	writeJSON(w, 200, s.queueView(name))
}

func (s *Server) handleDeleteQueue(w http.ResponseWriter, r *http.Request) {
	name := pathQueueName(r)
	if err := s.Eng.DeleteQueue(name); err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"queueName": name, "deleted": true})
}

// queueView 队列视图（队列只有名字，无属性）
func (s *Server) queueView(name string) map[string]any {
	q := s.Eng.GetQueue(name)
	if q == nil {
		return nil
	}
	st := q.Stats()
	enq, deq := q.Counters()
	return map[string]any{
		"queueName":      name,
		"activeMessages": st.Active, // 待消费
		"enqTotal":       enq,       // 累计入队
		"deqTotal":       deq,       // 累计消费
		"compacting":     s.Eng.Compacting(name),
	}
}

// ---------- 消息 API ----------

// POST /api/queues/{name}/messages  {"body":"..."}
func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	q := s.Eng.GetQueue(pathQueueName(r))
	if q == nil {
		writeErr(w, 404, "queue not found")
		return
	}
	var req struct {
		Body string `json:"body"`
	}
	if err := json.Unmarshal(readBody(r), &req); err != nil {
		writeErr(w, 400, "bad json")
		return
	}
	if req.Body == "" {
		writeErr(w, 400, "body is required")
		return
	}
	m, err := q.Send(req.Body)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	s.Eng.EnqTotal.Add(1)
	writeJSON(w, 201, sendView(m))
}

// POST /api/queues/{name}/messages/batch  {"bodies":["..",".."]}
func (s *Server) handleSendBatch(w http.ResponseWriter, r *http.Request) {
	q := s.Eng.GetQueue(pathQueueName(r))
	if q == nil {
		writeErr(w, 404, "queue not found")
		return
	}
	var req struct {
		Bodies []string `json:"bodies"`
	}
	if err := json.Unmarshal(readBody(r), &req); err != nil {
		writeErr(w, 400, "bad json")
		return
	}
	if len(req.Bodies) == 0 || len(req.Bodies) > 16 {
		writeErr(w, 400, "bodies 长度需在 1-16")
		return
	}
	out := make([]map[string]any, 0, len(req.Bodies))
	for _, body := range req.Bodies {
		if body == "" {
			writeErr(w, 400, "body is required")
			return
		}
		m, err := q.Send(body)
		if err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		s.Eng.EnqTotal.Add(1)
		out = append(out, sendView(m))
	}
	writeJSON(w, 201, map[string]any{"messages": out})
}

func sendView(m *engine.Message) map[string]any {
	sum := md5.Sum([]byte(m.Body))
	return map[string]any{
		"messageId": m.ID,
		"bodyMd5":   hex.EncodeToString(sum[:]),
	}
}

// GET /api/queues/{name}/messages?num=1&wait=5
// 接收即删除：返回的消息已被移除，不会再次投递
func (s *Server) handleReceive(w http.ResponseWriter, r *http.Request) {
	q := s.Eng.GetQueue(pathQueueName(r))
	if q == nil {
		writeErr(w, 404, "queue not found")
		return
	}
	query := r.URL.Query()
	wait, _ := strconv.ParseInt(query.Get("wait"), 10, 64)
	num, _ := strconv.ParseInt(query.Get("num"), 10, 64)
	if num < 1 {
		num = 1
	}
	if num > 16 {
		num = 16
	}
	if wait < 0 || wait > 30 {
		wait = 0
	}

	msgs := make([]map[string]any, 0, num)
	for int64(len(msgs)) < num {
		var waitHere int64
		if len(msgs) == 0 {
			waitHere = wait // 只有第一条允许长轮询
		}
		m := q.Receive(waitHere)
		if m == nil {
			break
		}
		s.Eng.DeqTotal.Add(1)
		msgs = append(msgs, map[string]any{
			"messageId": m.ID,
			"body":      m.Body,
		})
	}
	writeJSON(w, 200, map[string]any{"messages": msgs})
}

// GET /api/queues/{name}/messages/peek
func (s *Server) handlePeek(w http.ResponseWriter, r *http.Request) {
	q := s.Eng.GetQueue(pathQueueName(r))
	if q == nil {
		writeErr(w, 404, "queue not found")
		return
	}
	m := q.Peek()
	if m == nil {
		writeErr(w, 404, "queue is empty")
		return
	}
	writeJSON(w, 200, map[string]any{"messageId": m.ID, "body": m.Body})
}

// POST /api/queues/{name}/peek-batch {"offset":0,"limit":10}
func (s *Server) handlePeekBatch(w http.ResponseWriter, r *http.Request) {
	q := s.Eng.GetQueue(pathQueueName(r))
	if q == nil {
		writeErr(w, 404, "queue not found")
		return
	}
	var req struct {
		Offset int `json:"offset"`
		Limit  int `json:"limit"`
	}
	json.Unmarshal(readBody(r), &req)
	if req.Limit < 1 || req.Limit > 200 {
		req.Limit = 10
	}
	if req.Offset < 0 {
		req.Offset = 0
	}
	msgs, total := q.PeekPage(req.Offset, req.Limit)
	writeJSON(w, 200, map[string]any{
		"messages": msgs,
		"total":    total,
		"offset":   req.Offset,
		"limit":    req.Limit,
	})
}
