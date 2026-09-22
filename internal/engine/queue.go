package engine

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var ErrQueueClosed = errors.New("queue is closed")

// 消费路径的关键参数
const (
	readyMax      = 1000000 // 内存中"待消费序号"上限（100 万 × 8B ≈ 8MB）
	scanBatchSize = 8192    // 顺序预取单批条数
	delBacklogMax = 200000  // 待落盘删除操作上限，超过则同步等待（反压，保证内存有界）
)

// Queue 队列：消息体全量存于 Pebble，内存只维护"待消费序号 FIFO"。
//
// 重构要点（目标是消费稳定高速、与堆积量无关）：
//   - 无可见性超时、无延迟、无 TTL、无队列属性 → 引擎里没有任何定时器堆
//   - 接收即删除：取出一条立刻写删除，不需要 ack / 收据句柄
//   - 顺序预取：后台协程按 seq 顺序批量把待消费序号灌入 ready（只读 key，不解 value），
//     单条摊销成本极低；ready 有上限，内存不随堆积量增长
//   - 删除走组提交且不阻塞接收，靠待落盘积压数做反压
type Queue struct {
	id   string // 队列名转义出的定长 ID（只它参与 KV 键拼接）
	name string // 队列名原文（对外展示）
	st   *Store

	sendMu  sync.Mutex // 串行"分配 seq + 入写批"，保证落盘顺序与 seq 顺序一致
	headSeq uint64     // 下一个待分配 seq（原子）
	sentSeq uint64     // 已确认落盘的最大连续 seq（原子，扫描水位依据）

	mu         sync.Mutex
	ready      []uint64 // 待消费序号 FIFO
	readyOff   int
	scannedSeq uint64 // 已灌入 ready 的水位（持 mu）
	stopped    bool

	notify chan struct{} // 有新消息可用（唤醒消费者）
	wake   chan struct{} // 唤醒预取协程
	stopCh chan struct{}
	once   sync.Once

	enqTotal   atomic.Int64 // 累计入队
	deqTotal   atomic.Int64 // 累计消费（接收即删除）
	alive      atomic.Int64 // KV 中现存消息数
	aliveKnown atomic.Bool
	statsDirty atomic.Bool
}

func newQueue(id, name string, st *Store) *Queue {
	q := &Queue{
		id:     id,
		name:   name,
		st:     st,
		notify: make(chan struct{}, 1),
		wake:   make(chan struct{}, 1),
		stopCh: make(chan struct{}),
	}
	go q.scanLoop()
	return q
}

func (q *Queue) Name() string { return q.name }

// Counters 返回累计计数
func (q *Queue) Counters() (enq, deq int64) {
	return q.enqTotal.Load(), q.deqTotal.Load()
}

// SetCounters 恢复累计计数（重启加载）
func (q *Queue) SetCounters(enq, deq int64) {
	q.enqTotal.Store(enq)
	q.deqTotal.Store(deq)
}

// SetAlive 设置现存消息数（持久化恢复或后台校准）
func (q *Queue) SetAlive(n int64) {
	q.alive.Store(n)
	q.aliveKnown.Store(true)
}

// AliveKnown 计数是否已校准
func (q *Queue) AliveKnown() bool { return q.aliveKnown.Load() }

// QueueStats 队列统计
type QueueStats struct {
	Active int64 // 待消费消息数
}

// Stats 待消费消息数（来自持久化计数，无 IO）
func (q *Queue) Stats() QueueStats {
	n := q.alive.Load()
	if n < 0 {
		n = 0
	}
	return QueueStats{Active: n}
}

// ---------- 唤醒 ----------

func (q *Queue) kick() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *Queue) kickScan() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// ---------- 入队 ----------

// Send 发送消息：落盘（组提交 + fsync）成功后返回
func (q *Queue) Send(body string) (*Message, error) {
	q.sendMu.Lock()
	if q.stopped {
		q.sendMu.Unlock()
		return nil, ErrQueueClosed
	}
	seq := atomic.AddUint64(&q.headSeq, 1)
	key := msgKey(q.id, seq)
	ch := q.st.EnqueuePut(key, []byte(body))
	q.sendMu.Unlock()

	if err := <-ch; err != nil {
		return nil, err
	}

	// 落盘完成顺序与 seq 顺序一致（sendMu 串行入批），故 sentSeq 只增即是连续水位
	q.advanceSent(seq)
	q.enqTotal.Add(1)
	q.alive.Add(1)
	q.statsDirty.Store(true)
	q.kickScan() // 唤醒预取协程把新消息灌入 ready
	q.kick()
	return &Message{ID: itoh16(seq), Seq: seq, Body: body}, nil
}

func (q *Queue) advanceSent(seq uint64) {
	for {
		cur := atomic.LoadUint64(&q.sentSeq)
		if seq <= cur {
			return
		}
		if atomic.CompareAndSwapUint64(&q.sentSeq, cur, seq) {
			return
		}
	}
}

// ---------- 顺序预取 ----------

// scanLoop 后台预取协程：按 seq 顺序批量把待消费序号灌入 ready。
// 与堆积量无关：ready 满就停，消费腾出空间再继续。
func (q *Queue) scanLoop() {
	for {
		select {
		case <-q.stopCh:
			return
		default:
		}

		q.mu.Lock()
		full := len(q.ready)-q.readyOff >= readyMax
		sc := q.scannedSeq
		q.mu.Unlock()

		if full {
			if !q.waitWake() {
				return
			}
			continue
		}
		if n := q.scanBatch(); n > 0 {
			continue
		}
		// 没扫到数据：若水位仍落后于已落盘水位，稍等后重试（正常情况下数据已落盘必可见）
		if atomic.LoadUint64(&q.sentSeq) > sc {
			time.Sleep(time.Millisecond)
			continue
		}
		if !q.waitWake() {
			return
		}
	}
}

func (q *Queue) waitWake() bool {
	select {
	case <-q.wake:
		return true
	case <-q.stopCh:
		return false
	}
}

// scanBatch 从水位后继续扫一批，返回实际灌入条数
func (q *Queue) scanBatch() int {
	q.mu.Lock()
	from := q.scannedSeq + 1
	q.mu.Unlock()

	// 迭代器创建于 sentSeq 读取之后，故快照必然覆盖所有已落盘消息
	if from > atomic.LoadUint64(&q.sentSeq) {
		return 0
	}

	batch := make([]uint64, 0, scanBatchSize)
	var maxSeq uint64
	q.st.IterateQueueFrom(q.id, from, func(seq uint64, _ []byte) bool {
		batch = append(batch, seq)
		maxSeq = seq
		return len(batch) < scanBatchSize
	})
	if len(batch) == 0 {
		return 0
	}

	q.mu.Lock()
	q.ready = append(q.ready, batch...)
	if maxSeq > q.scannedSeq {
		q.scannedSeq = maxSeq
	}
	q.mu.Unlock()
	q.kick()
	return len(batch)
}

// pop 弹出下一个待消费序号；顺带压缩已消费头部
func (q *Queue) pop() (uint64, bool) {
	q.mu.Lock()
	if q.readyOff < len(q.ready) {
		seq := q.ready[q.readyOff]
		q.readyOff++
		if q.readyOff >= 4096 && q.readyOff*2 >= len(q.ready) {
			q.ready = append(q.ready[:0], q.ready[q.readyOff:]...)
			q.readyOff = 0
		}
		q.mu.Unlock()
		q.kickScan() // ready 腾出空间，唤醒预取协程
		return seq, true
	}
	q.mu.Unlock()
	return 0, false
}

// ---------- 消费（接收即删除） ----------

// Receive 接收一条消息并立即删除；无消息时最多等待 waitSec 秒（长轮询），未收到返回 nil
func (q *Queue) Receive(waitSec int64) *Message {
	deadline := time.Now().Add(time.Duration(waitSec) * time.Second)
	for {
		if seq, ok := q.pop(); ok {
			if m := q.take(seq); m != nil {
				return m
			}
			continue // 序号对应数据已不存在，继续取下一条
		}

		// ready 空：若 KV 里还有未预取的堆积，短暂等预取协程而不是直接返回空
		q.mu.Lock()
		sc := q.scannedSeq
		q.mu.Unlock()
		if atomic.LoadUint64(&q.sentSeq) > sc {
			select {
			case <-q.notify:
			case <-time.After(time.Millisecond):
			}
			continue
		}

		remain := time.Until(deadline)
		if remain <= 0 {
			return nil
		}
		// notify 为单信号，多消费者下可能唤醒不足，故加 3ms 兜底轮询
		wait := remain
		if wait > 3*time.Millisecond {
			wait = 3 * time.Millisecond
		}
		select {
		case <-q.notify:
		case <-time.After(wait):
		}
	}
}

// take 读取并删除一条消息
func (q *Queue) take(seq uint64) *Message {
	key := msgKey(q.id, seq)
	val, err := q.st.GetRaw(key)
	if err != nil {
		return nil
	}
	// 删除走组提交：不等落盘即返回消息（至少一次语义）；积压过多时同步等待以限制内存
	if q.st.PendingOps() > delBacklogMax {
		q.st.WaitIdle()
	}
	q.st.EnqueueDelAsync(key)
	q.deqTotal.Add(1)
	q.alive.Add(-1)
	q.statsDirty.Store(true)
	return &Message{ID: itoh16(seq), Seq: seq, Body: string(val)}
}

// ---------- 查看 ----------

// Peek 查看队首待消费消息（不删除）
func (q *Queue) Peek() *Message {
	q.mu.Lock()
	if q.readyOff < len(q.ready) {
		seq := q.ready[q.readyOff]
		q.mu.Unlock()
		return q.read(seq)
	}
	from := q.scannedSeq + 1
	q.mu.Unlock()

	var found uint64
	q.st.IterateQueueFrom(q.id, from, func(seq uint64, _ []byte) bool {
		found = seq
		return false
	})
	if found == 0 {
		return nil
	}
	return q.read(found)
}

func (q *Queue) read(seq uint64) *Message {
	val, err := q.st.GetRaw(msgKey(q.name, seq))
	if err != nil {
		return nil
	}
	return &Message{ID: itoh16(seq), Seq: seq, Body: string(val)}
}

// PeekPage 分页查看现存消息（不删除；跳过 offset 条后取 limit 条）
func (q *Queue) PeekPage(offset, limit int) ([]map[string]any, int64) {
	if offset < 0 {
		offset = 0
	}
	if limit < 1 {
		limit = 20
	}
	skipped := 0
	out := make([]map[string]any, 0, limit)

	q.st.IterateQueueFrom(q.id, 1, func(seq uint64, value []byte) bool {
		if skipped < offset {
			skipped++
			return true
		}
		if len(out) >= limit {
			return false
		}
		out = append(out, map[string]any{
			"messageId": itoh16(seq),
			"body":      string(value),
		})
		return len(out) < limit
	})
	return out, q.alive.Load()
}

// ---------- 生命周期 ----------

// loadQueue 恢复：只取首尾序号（O(1)），不遍历数据；索引由预取协程按需灌入
func (q *Queue) loadQueue() error {
	first, last, ok := q.st.QueueSeqRange(q.id)
	if !ok {
		q.SetAlive(0)
		return nil
	}
	q.mu.Lock()
	q.scannedSeq = first - 1
	q.mu.Unlock()
	atomic.StoreUint64(&q.headSeq, last)
	atomic.StoreUint64(&q.sentSeq, last) // 已有数据视为已落盘
	q.kickScan()
	return nil
}

// stop 停止队列（拒绝新写入并退出预取协程）
func (q *Queue) stop() {
	q.mu.Lock()
	q.stopped = true
	q.mu.Unlock()
	q.once.Do(func() { close(q.stopCh) })
}
