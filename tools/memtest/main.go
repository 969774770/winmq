package main

// memtest —— 引擎级入队/消费压测与内存观察工具（不走 HTTP，直接调用引擎）。
// 走的是与 HTTP 接口完全相同的引擎路径（Queue.Send / Queue.Receive → Store 组提交 → Pebble）。
//
// 用例：
//	入队 2000 万条并观察内存：go run ./tools/memtest -mode put -count 20000000
//	消费已有堆积并观察速率稳定性：go run ./tools/memtest -mode get -count 100000000 -keep
//
// -dur 可限制最长运行秒数（0=不限）。
import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"winmq/internal/engine"
)

func main() {
	mode := flag.String("mode", "put", "put=入队 / get=消费（接收即删除）")
	count := flag.Int64("count", 20000000, "目标条数")
	conc := flag.Int("concurrency", 128, "并发数")
	bodySize := flag.Int("body", 32, "消息体字节数（仅 put）")
	dur := flag.Int("dur", 0, "最长运行秒数（0=不限）")
	queue := flag.String("queue", "mem", "队列名")
	dataDir := flag.String("data", filepath.Join(os.TempDir(), "winmq-memtest"), "数据目录")
	fresh := flag.Bool("fresh", true, "启动前清空数据目录（get 模式请用 -fresh=false）")
	keep := flag.Bool("keep", false, "结束后保留数据目录")
	flag.Parse()

	if *fresh {
		os.RemoveAll(*dataDir)
	}
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		fmt.Println("创建数据目录失败:", err)
		return
	}
	if !*keep {
		defer os.RemoveAll(*dataDir)
	}

	t0 := time.Now()
	eng, err := engine.Open(*dataDir)
	if err != nil {
		fmt.Println("打开引擎失败:", err)
		return
	}
	defer eng.Close()
	fmt.Printf("引擎已打开（%v）\n", time.Since(t0).Round(time.Millisecond))

	if err := eng.CreateQueue(*queue); err != nil {
		fmt.Println("创建队列失败:", err)
		return
	}
	q := eng.GetQueue(*queue)
	if q == nil {
		fmt.Println("队列不存在")
		return
	}

	deadline := time.Now().Add(time.Duration(*dur) * time.Second)
	if *mode == "get" {
		runGet(eng, q, *count, *conc, deadline)
		return
	}
	runPut(eng, q, *count, *conc, *bodySize, deadline)
}

// runPut 并发入队，每 2 秒打印一次内存指标
func runPut(eng *engine.Engine, q *engine.Queue, count int64, conc, bodySize int, deadline time.Time) {
	body := strings.Repeat("a", bodySize)
	var done atomic.Int64
	stop := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sample(stop, &done, "入队", func() (int64, int64) {
			return eng.EnqTotal.Load(), eng.DiskUsage()
		})
	}()

	t0 := time.Now()
	var swg sync.WaitGroup
	per := count / int64(conc)
	rem := count % int64(conc)
	for i := 0; i < conc; i++ {
		n := per
		if int64(i) < rem {
			n++
		}
		swg.Add(1)
		go func(n int64) {
			defer swg.Done()
			for j := int64(0); j < n; j++ {
				if !deadline.IsZero() && time.Now().After(deadline) {
					return
				}
				if _, err := q.Send(body); err != nil {
					fmt.Println("发送失败:", err)
					return
				}
				done.Add(1)
			}
		}(n)
	}
	swg.Wait()
	close(stop)
	wg.Wait()

	el := time.Since(t0)
	fmt.Printf("\n入队完成：%d 条，耗时 %v，平均 %.0f 条/秒  磁盘 %dMB\n",
		done.Load(), el.Round(time.Second), float64(done.Load())/el.Seconds(), eng.DiskUsage()>>20)
	reportMemory()
}

// runGet 并发消费（接收即删除），每 2 秒打印一次速率与内存（用于观察稳定性）
func runGet(eng *engine.Engine, q *engine.Queue, count int64, conc int, deadline time.Time) {
	var done atomic.Int64
	stop := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sample(stop, &done, "消费", func() (int64, int64) {
			return eng.DeqTotal.Load(), eng.DiskUsage()
		})
	}()

	t0 := time.Now()
	var rwg sync.WaitGroup
	for i := 0; i < conc; i++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for {
				if done.Load() >= count {
					return
				}
				if !deadline.IsZero() && time.Now().After(deadline) {
					return
				}
				if q.Receive(0) == nil {
					if done.Load() >= count {
						return
					}
					time.Sleep(time.Millisecond)
					continue
				}
				done.Add(1)
			}
		}()
	}
	rwg.Wait()
	close(stop)
	wg.Wait()

	el := time.Since(t0)
	fmt.Printf("\n消费完成：%d 条，耗时 %v，平均 %.0f 条/秒  磁盘 %dMB\n",
		done.Load(), el.Round(time.Second), float64(done.Load())/el.Seconds(), eng.DiskUsage()>>20)
	reportMemory()
}

// sample 每 2 秒打印一次进度、速率与内存
func sample(stop chan struct{}, done *atomic.Int64, label string,
	counter func() (int64, int64)) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	t0 := time.Now()
	var last int64
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			cur := done.Load()
			_, disk := counter()
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			fmt.Printf("[%5.0fs] %s %10d 条  %6.0f 条/秒  堆存活=%4dMB 堆向系统申请=%4dMB 进程合计=%4dMB 磁盘=%5dMB GC=%d\n",
				time.Since(t0).Seconds(), label, cur, float64(cur-last)/2,
				ms.HeapAlloc>>20, ms.HeapSys>>20, ms.Sys>>20, disk>>20, ms.NumGC)
			last = cur
		}
	}
}

func reportMemory() {
	runtime.GC()
	debug.FreeOSMemory()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Printf("GC+归还系统后：堆存活=%dMB 进程合计=%dMB\n", ms.HeapAlloc>>20, ms.Sys>>20)
}
