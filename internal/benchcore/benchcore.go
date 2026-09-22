package benchcore

// benchcore 压测核心：供 GUI 工具和 CLI 复用。
// 认证方式：请求头 X-Api-Key: <key>（后台 key 管理创建）

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Options 压测参数
type Options struct {
	BaseURL     string // 如 http://192.168.80.118:6166
	APIKey      string // API Key（后台 key 管理创建）
	Queue       string
	Count       int    // 目标条数（接收模式=上限，收空即停）
	Concurrency int    // 并发数
	BodySize    int    // 消息体大小（字节）
	Mode        string // both=收发全链路 / send=仅发送 / recv=接收（接收即删除）
}

type Stats struct {
	Sent     atomic.Int64
	SendFail atomic.Int64
	Recv     atomic.Int64 // 接收成功数
	RecvFail atomic.Int64
	Stopped  atomic.Bool
	Phase    atomic.Value // string: 准备/发送中/接收中/完成
}

type Summary struct {
	SendCount int64
	SendFail  int64
	SendMs    int64
	SendRate  float64

	RecvCount int64 // 接收成功数（接收即删除）
	RecvFail  int64
	RecvMs    int64
	RecvRate  float64

	Stopped bool
}

// Result 输出结果文本
func (s Summary) Result(o Options) string {
	lines := ""
	if o.Mode == "both" || o.Mode == "send" {
		lines += fmt.Sprintf("发送: 成功 %d  失败 %d  耗时 %dms  速率 %.0f 条/秒\n",
			s.SendCount, s.SendFail, s.SendMs, s.SendRate)
	}
	if o.Mode != "send" {
		lines += fmt.Sprintf("接收(即删除): 成功 %d  失败 %d  耗时 %dms  速率 %.0f 条/秒\n",
			s.RecvCount, s.RecvFail, s.RecvMs, s.RecvRate)
	}
	if s.Stopped {
		lines += "\n(已被手动停止)"
	}
	if !s.Stopped && o.Mode == "both" {
		pass := s.SendRate >= 10000 && s.RecvRate >= 10000
		if pass {
			lines += "\n结论: PASS —— 两侧均达到 10000 条/秒及格线"
		} else {
			lines += "\n结论: FAIL —— 未达到 10000 条/秒及格线"
		}
	}
	return lines
}

type client struct {
	hc      *http.Client
	opt     Options
	letters []byte // 随机字母池（消息体随机部分来源）
}

func (c *client) sign() http.Header {
	h := http.Header{}
	h.Set("X-Api-Key", c.opt.APIKey)
	h.Set("Content-Type", "application/json")
	return h
}

func (c *client) do(method, path string, body []byte) (int, []byte, error) {
	req, err := http.NewRequest(method, c.opt.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header = c.sign()
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, data, err
}

// claimN 原子预占 n 个名额（保证总数不超过 limit）；返回实际预占数，0 表示名额已满。
// 用于接收阶段：请求前先占额度，避免"批量接收 × 并发"导致接收数超出目标条数。
func claimN(c *atomic.Int64, limit, want int64) int64 {
	for {
		cur := c.Load()
		if cur >= limit {
			return 0
		}
		n := want
		if cur+n > limit {
			n = limit - cur
		}
		if c.CompareAndSwap(cur, cur+n) {
			return n
		}
	}
}

// makeBody 生成消息体：当前时间 + 全随机字母
func (c *client) makeBody(size int) string {
	if size < 1 {
		size = 1
	}
	ts := time.Now().Format("2006-01-02T15:04:05.000")
	n := size - len(ts) - 1
	if n < 8 {
		n = 8
	}
	if n > len(c.letters) {
		n = len(c.letters)
	}
	off := rand.Intn(len(c.letters) - n)
	return ts + " " + string(c.letters[off:off+n])
}

// newLetterPool 预生成随机字母池
func newLetterPool(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.Intn(26)) + 'a'
	}
	return b
}

// Ping 连通性检查
func Ping(o Options) error {
	c := &client{hc: &http.Client{Timeout: 5 * time.Second}, opt: o}
	code, _, err := c.do("GET", "/api/queues", nil)
	if err != nil {
		return err
	}
	if code == 401 {
		return fmt.Errorf("认证失败：API Key 无效或已过期")
	}
	if code != 200 {
		return fmt.Errorf("服务器返回 HTTP %d", code)
	}
	return nil
}

// Run 执行压测（阻塞），进度实时写入 st
func Run(o Options, st *Stats) Summary {
	if st == nil {
		st = &Stats{}
	}
	if o.Concurrency < 1 {
		o.Concurrency = 1
	}
	if o.BodySize < 1 {
		o.BodySize = 32
	}

	c := &client{
		hc: &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: o.Concurrency * 2,
				MaxConnsPerHost:     o.Concurrency * 2,
			},
		},
		opt:     o,
		letters: newLetterPool(65536),
	}

	st.Phase.Store("准备")
	// 确保队列存在（幂等）
	createBody, _ := json.Marshal(map[string]string{"queueName": o.Queue})
	c.do("POST", "/api/queues", createBody)

	sum := Summary{}

	// ---- 发送阶段 ----
	if o.Mode == "both" || o.Mode == "send" {
		st.Phase.Store("发送中")
		start := time.Now()
		var wg sync.WaitGroup
		per := o.Count / o.Concurrency
		rem := o.Count % o.Concurrency
		for i := 0; i < o.Concurrency; i++ {
			n := per
			if i < rem {
				n++
			}
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				for j := 0; j < n; j++ {
					if st.Stopped.Load() {
						return
					}
					body := c.makeBody(o.BodySize)
					sb, _ := json.Marshal(map[string]any{"body": body})
					code, _, err := c.do("POST", "/api/queues/"+o.Queue+"/messages", sb)
					if err != nil || code != 201 {
						st.SendFail.Add(1)
					} else {
						st.Sent.Add(1)
					}
				}
			}(n)
		}
		wg.Wait()
		sum.SendMs = time.Since(start).Milliseconds()
		sum.SendCount = st.Sent.Load()
		sum.SendFail = st.SendFail.Load()
		if sum.SendMs > 0 {
			sum.SendRate = float64(sum.SendCount) / float64(sum.SendMs) * 1000
		}
	}

	// ---- 接收阶段（接收即删除：1 请求 = 1 条消息，测真实单条处理性能）----
	if !st.Stopped.Load() && o.Mode != "send" {
		st.Phase.Store("接收中")
		start := time.Now()
		var wg sync.WaitGroup
		var claimed atomic.Int64 // 已预占的接收名额（保证接收总数不超过目标）
		limit := int64(o.Count)

		for i := 0; i < o.Concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				consecFail := 0  // 连续失败保护（防服务异常时死循环）
				emptyStreak := 0 // 连续收到空次数
				for {
					if st.Stopped.Load() {
						return
					}
					// 预占 1 个名额，避免并发下超出目标条数
					if limit > 0 && claimN(&claimed, limit, 1) == 0 {
						return // 名额已满，目标条数已达
					}

					code, data, err := c.do("GET", "/api/queues/"+o.Queue+"/messages?num=1&wait=0", nil)
					if err != nil || code != 200 {
						st.RecvFail.Add(1)
						if limit > 0 {
							claimed.Add(-1) // 归还名额
						}
						consecFail++
						if consecFail > 200 {
							return
						}
						continue
					}
					consecFail = 0

					var out struct {
						Messages []struct{} `json:"messages"`
					}
					got := 0
					if json.Unmarshal(data, &out) == nil {
						got = len(out.Messages)
					}
					if got == 0 {
						if limit > 0 {
							claimed.Add(-1) // 归还名额
						}
						// 暂时取不到消息：重试几次仍空则退出
						emptyStreak++
						if emptyStreak >= 3 {
							return
						}
						time.Sleep(10 * time.Millisecond)
						continue
					}
					emptyStreak = 0
					st.Recv.Add(1) // 接收即删除，无需再调删除接口
				}
			}()
		}
		wg.Wait()
		sum.RecvMs = time.Since(start).Milliseconds()
		sum.RecvCount = st.Recv.Load()
		sum.RecvFail = st.RecvFail.Load()
		if sum.RecvMs > 0 {
			sum.RecvRate = float64(sum.RecvCount) / float64(sum.RecvMs) * 1000
		}
	}

	st.Phase.Store("完成")
	sum.Stopped = st.Stopped.Load()
	return sum
}

// ---------- 管理 API 辅助（诊断工具用） ----------

// HMACHex 计算 HMAC-SHA256-Hex
func HMACHex(key, str string) string {
	m := hmac.New(sha256.New, []byte(key))
	m.Write([]byte(str))
	return hex.EncodeToString(m.Sum(nil))
}

// AdminBench 通过管理 API 触发服务端压测
func AdminBench(base, adminToken, queue string, count, conc int) {
	body, _ := json.Marshal(map[string]any{"queue": queue, "count": count, "concurrency": conc})
	req, _ := http.NewRequest("POST", base+"/admin/api/bench", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-Token", adminToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("bench 请求失败:", err)
		return
	}
	defer resp.Body.Close()
	d, _ := io.ReadAll(resp.Body)
	fmt.Println("bench:", string(d))
}

// AdminOverview 打印服务端总览（含累计计数）
func AdminOverview(base, adminToken string) {
	req, _ := http.NewRequest("GET", base+"/admin/api/overview", nil)
	req.Header.Set("X-Admin-Token", adminToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("overview 请求失败:", err)
		return
	}
	defer resp.Body.Close()
	d, _ := io.ReadAll(resp.Body)
	fmt.Println("overview:", string(d))
}
