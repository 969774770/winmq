package engine

// daily.go —— 每日访问统计：按天累计入队/出队/确认/过期/请求数/流量
// 当日数据每秒落盘（非 fsync，允许掉电丢最后 1 秒统计），跨天自动切换

import (
	"encoding/json"
	"log"
	"time"

	"github.com/cockroachdb/pebble"
)

const keyPrefixDay = "d:" // d:{2006-01-02} → JSON

// DayStats 单日统计
type DayStats struct {
	Date     string `json:"date"`
	Enq      int64  `json:"enq"`      // 入队
	Deq      int64  `json:"deq"`      // 消费（接收即删除）
	Req      int64  `json:"req"`      // 请求数
	BytesIn  int64  `json:"bytesIn"`  // 接收字节
	BytesOut int64  `json:"bytesOut"` // 发送字节
}

// loadToday 重启恢复当日已落盘基准
func (e *Engine) loadToday() {
	e.todayDate = time.Now().Format("2006-01-02")
	if data, err := e.st.GetRaw([]byte(keyPrefixDay + e.todayDate)); err == nil {
		var ds DayStats
		if json.Unmarshal(data, &ds) == nil {
			e.dayBase = ds
		}
	}
}

// RecordRequest 请求打点（HTTP 层每次请求调用，原子无锁）
func (e *Engine) RecordRequest(bytesIn, bytesOut int64) {
	e.todayReq.Add(1)
	if bytesIn > 0 {
		e.todayBytesIn.Add(bytesIn)
	}
	e.todayBytesOut.Add(bytesOut)
}

// RecordBytesOut 响应字节打点（countWriter 每次写入调用）
func (e *Engine) RecordBytesOut(n int64) {
	e.todayBytesOut.Add(n)
}

// tickDaily 每秒调用：累加消息类增量、跨天切换、当日统计落盘
func (e *Engine) tickDaily(enqDelta, deqDelta int64) {
	today := time.Now().Format("2006-01-02")
	if today != e.todayDate {
		// 跨天：重置当日计数
		e.todayDate = today
		e.dayBase = DayStats{}
		e.todayEnq.Store(0)
		e.todayDeq.Store(0)
		e.todayReq.Store(0)
		e.todayBytesIn.Store(0)
		e.todayBytesOut.Store(0)
		log.Printf("进入新的一天 %s，每日统计已重置", today)
	}
	e.todayEnq.Add(enqDelta)
	e.todayDeq.Add(deqDelta)

	// 当日累计 = 已落盘基准 + 内存增量
	ds := DayStats{
		Date:     e.todayDate,
		Enq:      e.dayBase.Enq + e.todayEnq.Load(),
		Deq:      e.dayBase.Deq + e.todayDeq.Load(),
		Req:      e.dayBase.Req + e.todayReq.Load(),
		BytesIn:  e.dayBase.BytesIn + e.todayBytesIn.Load(),
		BytesOut: e.dayBase.BytesOut + e.todayBytesOut.Load(),
	}
	if data, err := json.Marshal(&ds); err == nil {
		e.st.PutDay(e.todayDate, data)
	}
}

// Today 今日实时统计（含未落盘部分）
func (e *Engine) Today() DayStats {
	return DayStats{
		Date:     e.todayDate,
		Enq:      e.dayBase.Enq + e.todayEnq.Load(),
		Deq:      e.dayBase.Deq + e.todayDeq.Load(),
		Req:      e.dayBase.Req + e.todayReq.Load(),
		BytesIn:  e.dayBase.BytesIn + e.todayBytesIn.Load(),
		BytesOut: e.dayBase.BytesOut + e.todayBytesOut.Load(),
	}
}

// RecentDays 最近 n 天统计（日期倒序，不含今日实时部分——今日单独取）
func (e *Engine) RecentDays(n int) []DayStats {
	all := e.st.ListDays()
	if len(all) > n {
		all = all[len(all)-n:]
	}
	// 倒序（最新在前）
	out := make([]DayStats, 0, len(all))
	for i := len(all) - 1; i >= 0; i-- {
		out = append(out, all[i])
	}
	return out
}

// ---------- Store 层 ----------

func (s *Store) PutDay(date string, value []byte) error {
	return s.db.Set([]byte(keyPrefixDay+date), value, nil)
}

func (s *Store) ListDays() []DayStats {
	var out []DayStats
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(keyPrefixDay),
		UpperBound: []byte(keyPrefixDay + "\xff"),
	})
	if err != nil {
		return nil
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		var ds DayStats
		if json.Unmarshal(iter.Value(), &ds) == nil {
			out = append(out, ds)
		}
	}
	return out
}
