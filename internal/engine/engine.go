package engine

// engine.go —— 引擎：管理所有队列 + 后台任务 + 统计。
// 数据层为 Pebble（Go 版 RocksDB）：消息全量落盘，内存只保留待消费序号。
//
// 重构版语义（队列只有名字，没有任何属性）：
//   - 无延迟可见、无可见性超时、无 TTL 过期 → 后台没有任何定时器
//   - 接收即删除：没有 ack / 收据句柄
//   - 磁盘回收只在"队列已消费空"时自动触发，绝不在消费过程中跑全范围压实

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Engine 消息队列引擎：管理所有队列 + 后台任务 + 统计
type Engine struct {
	dataDir string
	st      *Store

	mu       sync.RWMutex
	queues   map[string]*Queue
	keyCache *keyCache

	stop chan struct{}

	// 每日统计（当日计数原子累计；跨天由后台任务切换）
	todayDate     string
	dayBase       DayStats // 当日已落盘基准
	todayEnq      atomic.Int64
	todayDeq      atomic.Int64
	todayReq      atomic.Int64
	todayBytesIn  atomic.Int64
	todayBytesOut atomic.Int64

	// 累计计数（原子）
	EnqTotal atomic.Int64
	DeqTotal atomic.Int64
	// 最近 1 秒速率
	EnqRate int64
	DeqRate int64

	lastEnq, lastDeq int64

	// 启动耗时（界面展示）
	BootDuration time.Duration
	recounting   atomic.Bool // 存量统计后台校准中

	// 磁盘回收：记录各队列上次回收时的消费量，避免频繁压实
	compactMu     sync.Mutex
	lastCompactAt map[string]int64
}

// Recounting 存量统计是否正在后台校准
func (e *Engine) Recounting() bool { return e.recounting.Load() }

// queueNameRe 队列名：允许 Unicode 字母/数字（含中文）与下划线、中划线，1-64 个字符。
// 字符集限制是接口层策略（URL 路径 / 控制台展示友好），与存储无关——
// 队列名进 KV 前一律经 QueueID() 转义成定长 ID。
var queueNameRe = regexp.MustCompile(`^[\p{L}\p{N}_-]{1,64}$`)

// queueIDLen 队列 ID 长度（16 位十六进制 = 8 字节）
const queueIDLen = 16

// QueueID 把队列名转义成定长的安全 ID：MD5(name) 前 8 字节 → 16 位大写十六进制。
//
// 这样做的收益：
//   - 键里只有 [0-9A-F]，任意字符（中文、冒号、空格…）都不会破坏键或键范围
//   - 键前缀定长，范围扫描 [m:{id}, m:{id}\xff) 严格只覆盖本队列，不会串队列
//   - 长队列名（中文名通常十几字节）反而比原名直接入键更省空间
//
// 8 字节（64 位）在不同队列名之间的碰撞概率可忽略；CreateQueue 仍会做一次
// 显式碰撞检测，万一撞上会直接报错而不会静默串数据。
func QueueID(name string) string {
	sum := md5.Sum([]byte(name))
	return strings.ToUpper(hex.EncodeToString(sum[:8]))
}

// Open 打开引擎：打开 KV 存储，恢复所有队列（O(1) 每队列，不遍历消息）
func Open(dataDir string) (*Engine, error) {
	t0 := time.Now()
	dbDir := filepath.Join(dataDir, "db")
	st, err := OpenStore(dbDir)
	if err != nil {
		return nil, err
	}
	e := &Engine{
		dataDir:       dataDir,
		st:            st,
		queues:        make(map[string]*Queue),
		keyCache:      newKeyCache(),
		stop:          make(chan struct{}),
		lastCompactAt: make(map[string]int64),
	}
	// 恢复所有队列：只读注册表与计数，消息序号由预取协程按需灌入（与堆积量无关）
	needRecount := false
	for _, ent := range st.ListQueues() {
		q := newQueue(ent.ID, ent.Name, st)
		if sdata, err := st.GetStats(ent.ID); err == nil {
			var cs queueStatsRec
			if json.Unmarshal(sdata, &cs) == nil {
				q.SetCounters(cs.Enq, cs.Deq)
				if cs.Ver >= 1 {
					q.SetAlive(cs.Alive) // 新格式：现存计数可信，无需校准
				}
			}
		}
		if err := q.loadQueue(); err != nil {
			log.Printf("[engine] 恢复队列 %s 失败: %v", ent.Name, err)
			continue
		}
		if !q.AliveKnown() {
			needRecount = true // 旧数据升级：后台异步校准现存数
		}
		e.queues[ent.Name] = q
	}
	if n := st.CountLegacyQueues(); n > 0 {
		log.Printf("[engine] 检测到 %d 个重构前旧格式队列（按队列名直接入键），新版无法读取；请清空数据目录后重新导入", n)
	}
	// 恢复 API Key 与当日统计
	e.loadKeys()
	e.ensureDefaultKey()
	e.loadToday()
	e.BootDuration = time.Since(t0)
	go e.backgroundLoop()
	if needRecount {
		go e.recountLoop()
	}
	log.Printf("[engine] 启动完成，耗时 %v（%d 个队列，消息序号按需预取）",
		e.BootDuration.Round(time.Millisecond), len(e.queues))
	return e, nil
}

// CreateQueue 创建队列（幂等；队列只有名字，无属性）
func (e *Engine) CreateQueue(name string) error {
	if !queueNameRe.MatchString(name) {
		return errors.New("队列名只能包含字母、数字、中文、下划线、中划线，长度 1-64 个字符")
	}
	id := QueueID(name)

	e.mu.Lock()
	if _, ok := e.queues[name]; ok {
		e.mu.Unlock()
		return nil // 已存在视为成功
	}
	// 防 ID 碰撞：不同队列名不允许映射到同一个存储 ID
	for _, o := range e.queues {
		if o.id == id {
			e.mu.Unlock()
			return fmt.Errorf("队列名 %s 与已有队列 %s 的存储 ID 冲突，请更换队列名", name, o.name)
		}
	}
	e.queues[name] = newQueue(id, name, e.st)
	e.mu.Unlock()

	return e.st.PutQueue(id, name)
}

// DeleteQueue 删除队列及其全部数据
func (e *Engine) DeleteQueue(name string) error {
	e.mu.Lock()
	q, ok := e.queues[name]
	if !ok {
		e.mu.Unlock()
		return errors.New("队列不存在")
	}
	delete(e.queues, name)
	e.mu.Unlock()

	q.stop() // 先拒绝写入并退出预取协程
	if err := e.st.DeleteQueueData(q.id, q.name); err != nil {
		log.Printf("[engine] 删除队列 %s 数据失败: %v", name, err)
	}
	if err := e.st.DeleteQueueMeta(q.id); err != nil {
		log.Printf("[engine] 删除队列 %s 注册信息失败: %v", name, err)
	}
	e.st.DeleteStats(q.id)
	return nil
}

// queueStatsRec 累计计数持久化结构
type queueStatsRec struct {
	Ver   int   `json:"ver"` // 格式版本：>=1 表示 Alive 字段可信
	Enq   int64 `json:"enq"`
	Deq   int64 `json:"deq"`
	Alive int64 `json:"alive"` // 当前现存消息数
}

// saveStats 把有变更的队列计数落盘（每秒一次，非 fsync）
func (e *Engine) saveStats() {
	e.mu.RLock()
	qs := make([]*Queue, 0, len(e.queues))
	for _, q := range e.queues {
		qs = append(qs, q)
	}
	e.mu.RUnlock()
	for _, q := range qs {
		if !q.statsDirty.CompareAndSwap(true, false) {
			continue
		}
		enq, deq := q.Counters()
		data, _ := json.Marshal(&queueStatsRec{
			Ver: 1, Enq: enq, Deq: deq, Alive: q.alive.Load(),
		})
		if err := e.st.PutStats(q.id, data); err != nil {
			log.Printf("[engine] 保存队列 %s 计数失败: %v", q.Name(), err)
		}
	}
}

// recountLoop 后台校准现存计数：对未校准的队列做一次全量计数（不解析 value，很快）。
// 仅用于旧数据升级（无持久化计数）场景，不阻塞启动。
func (e *Engine) recountLoop() {
	e.recounting.Store(true)
	defer e.recounting.Store(false)
	for _, name := range e.QueueNames() {
		select {
		case <-e.stop:
			return
		default:
		}
		q := e.GetQueue(name)
		if q == nil || q.AliveKnown() {
			continue
		}
		t0 := time.Now()
		n := e.st.CountQueueMessages(name, func() bool {
			select {
			case <-e.stop:
				return false
			default:
				return true
			}
		})
		q.SetAlive(n)
		q.statsDirty.Store(true)
		log.Printf("[engine] 队列 %s 现存计数校准完成：%d 条（耗时 %v）", name, n, time.Since(t0).Round(time.Millisecond))
	}
}

// TotalCounters 所有队列累计计数汇总
func (e *Engine) TotalCounters() (enq, deq int64) {
	e.mu.RLock()
	qs := make([]*Queue, 0, len(e.queues))
	for _, q := range e.queues {
		qs = append(qs, q)
	}
	e.mu.RUnlock()
	for _, q := range qs {
		a, b := q.Counters()
		enq += a
		deq += b
	}
	return
}

// CompactQueue 触发队列磁盘空间回收（后台执行）
func (e *Engine) CompactQueue(name string) error {
	q := e.GetQueue(name)
	if q == nil {
		return errors.New("队列不存在")
	}
	return e.st.CompactQueueAsync(q.id, q.name)
}

// Compacting 队列是否正在回收磁盘
func (e *Engine) Compacting(name string) bool {
	q := e.GetQueue(name)
	if q == nil {
		return false
	}
	return e.st.Compacting(q.id)
}

// DiskUsage 存储磁盘占用
func (e *Engine) DiskUsage() int64 { return e.st.DiskUsage() }

// autoCompactMinDeq 单队列累计消费达到该数量、且队列已消费空时，后台回收磁盘空间。
// 关键：全范围压实很重（上亿条约需数分钟），只在队列为空时跑，
// 否则会与消费/写入抢磁盘 IO，导致消费速率"忽快忽 0"。
const autoCompactMinDeq = 200000

// maybeAutoCompact 队列消费空且累计消费量达标时，后台回收磁盘空间
func (e *Engine) maybeAutoCompact() {
	for _, name := range e.QueueNames() {
		q := e.GetQueue(name)
		if q == nil || e.Compacting(name) {
			continue
		}
		if q.alive.Load() != 0 {
			continue // 队列非空：不压实，避免拖垮消费
		}
		_, deq := q.Counters()

		e.compactMu.Lock()
		last := e.lastCompactAt[name]
		if deq-last < autoCompactMinDeq {
			e.compactMu.Unlock()
			continue
		}
		e.lastCompactAt[name] = deq
		e.compactMu.Unlock()

		if err := e.CompactQueue(name); err != nil {
			log.Printf("[engine] 队列 %s 自动回收磁盘失败: %v", name, err)
		} else {
			log.Printf("[engine] 队列 %s 已消费空（累计 %d 条），触发后台磁盘回收", name, deq)
		}
	}
}

// GetQueue 取队列（可能为 nil）
func (e *Engine) GetQueue(name string) *Queue {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.queues[name]
}

// QueueNames 队列名列表
func (e *Engine) QueueNames() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	names := make([]string, 0, len(e.queues))
	for n := range e.queues {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// QueueCount 队列数量
func (e *Engine) QueueCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.queues)
}

// TotalActiveMessages 所有队列待消费消息总数（无 IO）
func (e *Engine) TotalActiveMessages() int64 {
	e.mu.RLock()
	qs := make([]*Queue, 0, len(e.queues))
	for _, q := range e.queues {
		qs = append(qs, q)
	}
	e.mu.RUnlock()
	var active int64
	for _, q := range qs {
		active += q.Stats().Active
	}
	return active
}

// Close 停止后台任务并关闭存储
func (e *Engine) Close() {
	close(e.stop)
	e.mu.Lock()
	qs := make([]*Queue, 0, len(e.queues))
	for _, q := range e.queues {
		qs = append(qs, q)
	}
	e.mu.Unlock()
	for _, q := range qs {
		q.stop()
	}
	e.saveStats()  // 最后一次计数落盘
	e.tickDaily(0, 0) // 最后一次每日统计落盘
	e.st.WaitIdle()
	e.st.Close()
}

// EngineStats 引擎统计
type EngineStats struct {
	EnqTotal   int64 `json:"enqTotal"`
	DeqTotal   int64 `json:"deqTotal"`
	EnqRate    int64 `json:"enqRate"` // 条/秒
	DeqRate    int64 `json:"deqRate"`
	QueueCount int   `json:"queueCount"`
}

func (e *Engine) Stats() EngineStats {
	return EngineStats{
		EnqTotal:   e.EnqTotal.Load(),
		DeqTotal:   e.DeqTotal.Load(),
		EnqRate:    e.EnqRate,
		DeqRate:    e.DeqRate,
		QueueCount: e.QueueCount(),
	}
}

// backgroundLoop 后台任务：速率统计 / 每日统计落盘 / 空队列磁盘回收
func (e *Engine) backgroundLoop() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	n := 0
	for {
		select {
		case <-e.stop:
			return
		case <-tick.C:
			n++
			// 每 30 秒检查一次是否可以回收磁盘（仅空队列）
			if n%30 == 0 {
				e.maybeAutoCompact()
			}
			curEnq, curDeq := e.EnqTotal.Load(), e.DeqTotal.Load()
			e.EnqRate = curEnq - e.lastEnq
			e.DeqRate = curDeq - e.lastDeq
			e.lastEnq, e.lastDeq = curEnq, curDeq

			// 累计计数落盘 + 每日统计
			e.saveStats()
			e.tickDaily(e.EnqRate, e.DeqRate)
		}
	}
}

// TrimName 去空格
func TrimName(s string) string { return strings.TrimSpace(s) }
