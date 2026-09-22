package main

// 造数工具：直接向 Pebble 写入海量消息，用于验证启动/消费性能。
// 键布局与引擎一致（队列名先经 engine.QueueID 转义为定长 ID）：
//
//	q:{id}            → 队列名
//	m:{id}{seq:016X}  → 消息体
//	s:{id}            → 队列计数
import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"

	"winmq/internal/engine"
)

// msgKey m:{id}{seq:016X}
func msgKey(id string, seq uint64) []byte {
	const hexDigits = "0123456789ABCDEF"
	b := make([]byte, 0, len("m:")+len(id)+16)
	b = append(b, "m:"...)
	b = append(b, id...)
	var num [8]byte
	binary.BigEndian.PutUint64(num[:], seq)
	for _, v := range num {
		b = append(b, hexDigits[v>>4], hexDigits[v&0x0F])
	}
	return b
}

func main() {
	queue := flag.String("queue", "big", "队列名")
	count := flag.Int("count", 1000000, "消息条数")
	dataDir := flag.String("data", `D:\AI\MNS\data`, "数据目录")
	bodySize := flag.Int("body", 32, "消息体字节数")
	noStats := flag.Bool("nostats", false, "不写 s 计数（触发后台校准路径）")
	flag.Parse()

	dbDir := filepath.Join(*dataDir, "db")
	os.MkdirAll(dbDir, 0755)

	cache := pebble.NewCache(256 << 20)
	defer cache.Unref()
	db, err := pebble.Open(dbDir, &pebble.Options{Cache: cache, MemTableSize: 64 << 20})
	if err != nil {
		fmt.Println("打开失败（服务是否仍在运行？）:", err)
		return
	}
	defer db.Close()

	id := engine.QueueID(*queue)
	fmt.Printf("队列 %s 的存储 ID: %s\n", *queue, id)

	// 队列注册（id → 名称）
	if err := db.Set([]byte("q:"+id), []byte(*queue), pebble.Sync); err != nil {
		fmt.Println("写队列注册失败:", err)
		return
	}

	body := make([]byte, *bodySize)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	t0 := time.Now()
	const batchSize = 20000
	for start := 0; start < *count; start += batchSize {
		batch := db.NewBatch()
		end := start + batchSize
		if end > *count {
			end = *count
		}
		for i := start; i < end; i++ {
			seq := uint64(i + 1)
			batch.Set(msgKey(id, seq), body, nil)
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			fmt.Println("写入失败:", err)
			return
		}
		if (start/batchSize)%50 == 0 {
			fmt.Printf("\r已写入 %d / %d 条（%.0f 条/秒）", end, *count,
				float64(end)/time.Since(t0).Seconds())
		}
	}
	db.Flush()

	if !*noStats {
		stats := map[string]any{"ver": 1, "enq": *count, "deq": 0, "alive": *count}
		sd, _ := json.Marshal(stats)
		if err := db.Set([]byte("s:"+id), sd, pebble.Sync); err != nil {
			fmt.Println("写计数失败:", err)
			return
		}
	}

	fmt.Printf("\n造数完成：队列 %s，%d 条，耗时 %v\n", *queue, *count, time.Since(t0).Round(time.Second))
}
