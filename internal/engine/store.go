package engine

// store.go —— Pebble 存储层（Go 版 RocksDB，成熟嵌入式 LSM KV 引擎）。
// 消息数据全量落盘，内存只保留"待消费序号"。
//
// 写入采用全局 group commit：并发提交合并为一个 Batch，一次 db.Apply(Sync=true)，
// 每个提交方在自己的通道上等到落盘完成（等效"每条 fsync"语义，吞吐大幅提高）。
//
// 消费侧删除用 EnqueueDelAsync（不等落盘，组提交批量写墓碑），靠 PendingOps 反压，
// 使"接收即删除"的吞吐不再被磁盘 fsync 逐个拖住。

import (
	"encoding/binary"
	"errors"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

// KV 键布局（所有键都是定长、ASCII 安全的前缀 + 内容）：
//
//	m:{id}{seq:016X} → 消息体
//	q:{id}           → 队列名（UTF-8 原文）
//	s:{id}           → 队列累计计数 JSON
//	d:{2006-01-02}   → 每日统计（daily.go）
//	k:{key}          → API Key（key.go）
//
// 其中 {id} 是队列名经 QueueID() 转义出来的定长 16 位十六进制 ID。
// 队列名本身不参与键拼接，因此任意字符（中文、符号）都不会破坏键与键范围。
const (
	keyPrefixMsg   = "m:" // m:{id}{seq:016x} → 消息体
	keyPrefixQueue = "q:" // q:{id}           → 队列名
	keyPrefixStats = "s:" // s:{id}           → 队列累计计数 JSON
)

// syncWrite 每批落盘
var syncWrite = pebble.Sync

// Store 全局共享的单实例 KV 存储
type Store struct {
	db *pebble.DB

	mu         sync.Mutex
	pend       *pendBatch   // 当前待写批
	waiters    []chan error // 等待本批落盘的提交方
	pendingOps atomic.Int64 // 待落盘操作数（用于消费侧反压）
	wake       chan struct{}
	closed     bool
	done       chan struct{}

	compacting sync.Map // 正在回收磁盘的队列，防止重复压实
}

// kvPair 待写批的单个键值
type kvPair struct {
	Key   []byte
	Value []byte
}

// pendBatch 待写批
type pendBatch struct {
	sets []kvPair
	dels [][]byte
}

func (b *pendBatch) empty() bool { return len(b.sets) == 0 && len(b.dels) == 0 }

func (b *pendBatch) apply(lb *pebble.Batch) {
	for _, kv := range b.sets {
		lb.Set(kv.Key, kv.Value, nil)
	}
	for _, k := range b.dels {
		lb.Delete(k, nil)
	}
}

// OpenStore 打开存储
func OpenStore(dataDir string) (*Store, error) {
	cache := pebble.NewCache(64 << 20) // 64MB 读缓存
	defer cache.Unref()

	opts := &pebble.Options{
		Cache:                       cache,
		MemTableSize:                32 << 20, // 32MB 内存表（堆积写入友好）
		MemTableStopWritesThreshold: 4,
		MaxConcurrentCompactions:    func() int { return 2 },
		L0CompactionThreshold:       8, // 延迟压实，优先保写入吞吐
		L0StopWritesThreshold:       24,
		LBaseMaxBytes:               128 << 20,
	}
	db, err := pebble.Open(dataDir, opts)
	if err != nil {
		return nil, err
	}
	s := &Store{
		db:   db,
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
	s.pend = &pendBatch{}
	go s.writerLoop()
	return s, nil
}

func (s *Store) enqueue(b *pendBatch) <-chan error {
	ch := make(chan error, 1)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ch <- errors.New("store closed")
		return ch
	}
	s.pend.sets = append(s.pend.sets, b.sets...)
	s.pend.dels = append(s.pend.dels, b.dels...)
	s.pendingOps.Add(int64(len(b.sets) + len(b.dels)))
	s.waiters = append(s.waiters, ch)
	s.mu.Unlock()
	s.kick()
	return ch
}

// EnqueuePut 把 Put 操作加入待写批，落盘完成后通道返回
func (s *Store) EnqueuePut(key, value []byte) <-chan error {
	return s.enqueue(&pendBatch{sets: []kvPair{{Key: key, Value: value}}})
}

// EnqueueDelAsync 把删除操作加入待写批，不等待落盘（消费侧删除用，组提交批量写墓碑）
func (s *Store) EnqueueDelAsync(key []byte) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.pend.dels = append(s.pend.dels, key)
	s.pendingOps.Add(1)
	s.mu.Unlock()
	s.kick()
}

// PendingOps 待落盘操作数（消费侧据此做反压，保证内存有界）
func (s *Store) PendingOps() int64 { return s.pendingOps.Load() }

// WaitIdle 等待当前所有已提交操作落盘（仅作为同步屏障，不产生写操作）
func (s *Store) WaitIdle() error {
	ch := make(chan error, 1)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("store closed")
	}
	s.waiters = append(s.waiters, ch)
	s.mu.Unlock()
	s.kick()
	return <-ch
}

func (s *Store) kick() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Store) writerLoop() {
	defer close(s.done)
	for {
		s.mu.Lock()
		pend := s.pend
		waiters := s.waiters
		s.pend = &pendBatch{}
		s.waiters = nil
		closed := s.closed
		s.mu.Unlock()

		var err error
		if !pend.empty() {
			lb := s.db.NewBatch()
			pend.apply(lb)
			err = lb.Commit(syncWrite)
			if err != nil {
				err = errors.New("kv write: " + err.Error())
			}
			s.pendingOps.Add(-int64(len(pend.sets) + len(pend.dels)))
		}
		for _, ch := range waiters {
			ch <- err
		}

		if closed {
			s.mu.Lock()
			remain := !s.pend.empty() || len(s.waiters) > 0
			s.mu.Unlock()
			if !remain {
				s.db.Close()
				return
			}
			continue
		}

		s.mu.Lock()
		idle := s.pend.empty() && len(s.waiters) == 0
		s.mu.Unlock()
		if idle {
			<-s.wake
		}
	}
}

// Close 关闭存储（先落盘所有待写批）
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	s.kick()
	select {
	case <-s.done:
		return nil
	case <-time.After(10 * time.Second):
		return errors.New("store close timeout")
	}
}

// GetRaw 读取原始值
func (s *Store) GetRaw(key []byte) ([]byte, error) {
	val, closer, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(val))
	copy(out, val)
	return out, closer.Close()
}

// ---------- 队列注册表（id → 队列名） ----------

// QueueEntry 队列注册表项
type QueueEntry struct {
	ID   string // 队列名转义出的定长 ID
	Name string // 队列名原文
}

// PutQueue 登记队列（id → 名称，同步落盘）
func (s *Store) PutQueue(id, name string) error {
	return s.db.Set([]byte(keyPrefixQueue+id), []byte(name), syncWrite)
}

// DeleteQueueMeta 删除队列登记
func (s *Store) DeleteQueueMeta(id string) error {
	return s.db.Delete([]byte(keyPrefixQueue+id), syncWrite)
}

// ListQueues 列出全部队列登记
func (s *Store) ListQueues() []QueueEntry {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(keyPrefixQueue),
		UpperBound: []byte(keyPrefixQueue + "\xff"),
	})
	if err != nil {
		return nil
	}
	defer iter.Close()
	var out []QueueEntry
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		if len(k) <= len(keyPrefixQueue) {
			continue
		}
		out = append(out, QueueEntry{
			ID:   string(k[len(keyPrefixQueue):]),
			Name: string(iter.Value()),
		})
	}
	return out
}

// CountLegacyQueues 统计旧格式（重构前按队列名直接入键）的队列登记数，仅用于升级提示
func (s *Store) CountLegacyQueues() int {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("qt:"),
		UpperBound: []byte("qt:\xff"),
	})
	if err != nil {
		return 0
	}
	defer iter.Close()
	n := 0
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	return n
}

// PutStats 写队列累计计数（每秒定期落盘，非 fsync，允许掉电丢最后 1 秒）
func (s *Store) PutStats(id string, value []byte) error {
	return s.db.Set([]byte(keyPrefixStats+id), value, nil)
}

// GetStats 读队列累计计数
func (s *Store) GetStats(id string) ([]byte, error) {
	return s.GetRaw([]byte(keyPrefixStats + id))
}

// DeleteStats 删除队列累计计数
func (s *Store) DeleteStats(id string) error {
	return s.db.Delete([]byte(keyPrefixStats+id), nil)
}

// IterateQueueFrom 从指定 seq 起顺序迭代队列消息（顺序预取/分页查看用；fn 返回 false 停止）
func (s *Store) IterateQueueFrom(id string, fromSeq uint64, fn func(seq uint64, value []byte) bool) {
	if fromSeq < 1 {
		fromSeq = 1
	}
	startKey := msgKey(id, fromSeq)
	_, endKey := queueKeyRange(id)

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		return
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		// 键长固定：前缀定长 ID + 16 位 hex 序号，长度不符说明不是本队列的消息
		seq, ok := parseSeq(iter.Key())
		if !ok {
			continue
		}
		// value 仅在本次迭代内有效，调用方需在回调内完成解析
		if !fn(seq, iter.Value()) {
			return
		}
	}
}

// QueueSeqRange 返回队列的最小、最大消息序号（O(1)，不遍历全部数据）
func (s *Store) QueueSeqRange(id string) (first, last uint64, ok bool) {
	start, end := queueKeyRange(id)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return 0, 0, false
	}
	defer iter.Close()
	if !iter.First() {
		return 0, 0, false
	}
	f, ok := parseSeq(iter.Key())
	if !ok {
		return 0, 0, false
	}
	if !iter.Last() {
		return f, f, true
	}
	l, ok := parseSeq(iter.Key())
	if !ok {
		return f, f, true
	}
	return f, l, true
}

// CountQueueMessages 统计队列现存消息数（全量扫描，仅用于后台校准）
func (s *Store) CountQueueMessages(id string, fn func() bool) int64 {
	var n int64
	start, end := queueKeyRange(id)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return 0
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		n++
		if n%100000 == 0 && fn != nil && !fn() {
			break
		}
	}
	return n
}

// queueKeyRange 返回队列消息键的范围 [start, end)。
// start = "m:{id}"，end = start+0xff：因为 ID 定长且所有消息键都以它开头，
// 该范围严格只覆盖本队列，绝不会串到别的队列。
func queueKeyRange(id string) (start, end []byte) {
	start = msgPrefix(id)
	end = make([]byte, len(start)+1)
	copy(end, start)
	end[len(start)] = 0xff
	return start, end
}

// DeleteQueueData 物理删除队列全部消息。
// 用 DeleteRange 写入一个范围墓碑（而非上亿个单键墓碑），并立即触发压实回收磁盘。
func (s *Store) DeleteQueueData(id, name string) error {
	start, end := queueKeyRange(id)

	b := s.db.NewBatch()
	if err := b.DeleteRange(start, end, nil); err != nil {
		return err
	}
	if err := b.Commit(syncWrite); err != nil {
		return err
	}

	s.compactRange(id, name, start, end)
	return nil
}

// CompactQueueAsync 后台压实队列数据范围，物理回收已删除消息占用的磁盘空间。
// 只在队列已清空（或用户主动点击回收）时使用：全范围压实很重，绝不能在消费过程中自动跑。
func (s *Store) CompactQueueAsync(id, name string) error {
	if _, loaded := s.compacting.LoadOrStore(id, struct{}{}); loaded {
		return errors.New("该队列正在回收磁盘中，请稍候")
	}
	start, end := queueKeyRange(id)
	go func() {
		defer s.compacting.Delete(id)
		t0 := time.Now()
		if err := s.db.Compact(start, end, true); err != nil {
			log.Printf("[store] 队列 %s 磁盘回收失败: %v", name, err)
			return
		}
		log.Printf("[store] 队列 %s 磁盘回收完成（耗时 %v）", name, time.Since(t0).Round(time.Second))
	}()
	return nil
}

func (s *Store) compactRange(id, name string, start, end []byte) {
	if _, loaded := s.compacting.LoadOrStore(id, struct{}{}); loaded {
		return
	}
	go func() {
		defer s.compacting.Delete(id)
		t0 := time.Now()
		if err := s.db.Compact(start, end, true); err != nil {
			log.Printf("[store] 队列 %s 磁盘回收失败: %v", name, err)
			return
		}
		log.Printf("[store] 队列 %s 磁盘回收完成（耗时 %v）", name, time.Since(t0).Round(time.Second))
	}()
}

// Compacting 队列是否正在回收磁盘
func (s *Store) Compacting(id string) bool {
	_, ok := s.compacting.Load(id)
	return ok
}

// DiskUsage 返回存储磁盘占用（字节）
func (s *Store) DiskUsage() int64 {
	return int64(s.db.Metrics().DiskSpaceUsage())
}

// ---------- 键编码 ----------
//
// 消息键：m: + {id:16位hex} + {seq:16位hex}，总长恒定 34 字节。
// ID 定长保证前缀唯一，因此所有队列操作都能用精确的键范围，
// 不需要再依赖"队列名 + 分隔符 + 长度校验"这种脆弱方式。

// hexSeqLen 消息序号在键里的十六进制编码长度（16 位 = 8 字节）
const hexSeqLen = 16

// msgPrefix 消息键前缀（m: + 16 位队列 ID），共 18 字节
func msgPrefix(id string) []byte {
	b := make([]byte, 0, len(keyPrefixMsg)+queueIDLen)
	b = append(b, keyPrefixMsg...)
	return append(b, id...)
}

func msgKey(id string, seq uint64) []byte {
	b := make([]byte, 0, len(keyPrefixMsg)+queueIDLen+hexSeqLen)
	b = append(b, keyPrefixMsg...)
	b = append(b, id...)
	var num [8]byte
	binary.BigEndian.PutUint64(num[:], seq)
	return appendHex16(b, num[:])
}

// parseSeq 从消息键尾部解析序号（键长不符说明不是本队列的消息）
func parseSeq(k []byte) (uint64, bool) {
	if len(k) != len(keyPrefixMsg)+queueIDLen+hexSeqLen {
		return 0, false
	}
	v, err := strconv.ParseUint(string(k[len(keyPrefixMsg)+queueIDLen:]), 16, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func appendHex16(dst []byte, num []byte) []byte {
	const hexDigits = "0123456789ABCDEF"
	for _, v := range num {
		dst = append(dst, hexDigits[v>>4], hexDigits[v&0x0F])
	}
	return dst
}
