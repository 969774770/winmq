// benchgui —— winMQ 消息队列压测工具（原生 Windows GUI）
package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lxn/walk"
	. "github.com/lxn/walk/declarative"

	"winmq/internal/benchcore"
)

type BenchWindow struct {
	*walk.MainWindow

	edURL   *walk.LineEdit
	edKey   *walk.LineEdit
	edQueue *walk.LineEdit
	neCount  *walk.NumberEdit
	neConc   *walk.NumberEdit
	neBody   *walk.NumberEdit
	cmbMode  *walk.ComboBox
	btnStart *walk.PushButton
	btnStop  *walk.PushButton
	pb       *walk.ProgressBar
	te       *walk.TextEdit

	lbPhase  *walk.Label
	lbSent   *walk.Label
	lbRecv   *walk.Label
	lbRate   *walk.Label

	st       benchcore.Stats
	running  atomic.Bool
	startAt  time.Time
	logBuf   string
	logMu    sync.Mutex
}

const (
	defaultURL = "http://192.168.80.118:6166"
	defaultKey = ""
)

func main() {
	log.SetFlags(log.LstdFlags)
	mw := new(BenchWindow)

	err := mw.setup()
	if err != nil {
		log.Fatalf("创建窗口失败: %v", err)
	}

	// walk NumberEdit 的 declarative Value 属性初始化顺序有 bug，
	// 窗口创建完成后显式设置初始值
	mw.neCount.SetValue(10000)
	mw.neConc.SetValue(32)
	mw.neBody.SetValue(32)

	// 实时刷新协程：统计 → UI（Synchronize 保证主线程）
	go func() {
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for range t.C {
			if !mw.running.Load() {
				continue
			}
			mw.Synchronize(mw.refresh)
		}
	}()

	mw.Run()
}

func (mw *BenchWindow) setup() error {
	return MainWindow{
		AssignTo: &mw.MainWindow,
		Title:    "winMQ 消息队列压测工具",
		MinSize:  Size{Width: 660, Height: 640},
		Size:     Size{Width: 720, Height: 720},
		Layout:   VBox{},
		Children: []Widget{
			GroupBox{
				Title:  "连接配置",
				Layout: Grid{Columns: 2},
				Children: []Widget{
					Label{Text: "服务器地址:"},
					LineEdit{AssignTo: &mw.edURL, Text: defaultURL, CueBanner: "http://IP:端口"},
					Label{Text: "API Key:"},
					LineEdit{AssignTo: &mw.edKey, Text: defaultKey, CueBanner: "后台 Key 管理中创建"},
				},
			},
			GroupBox{
				Title:  "压测参数",
				Layout: Grid{Columns: 2},
				Children: []Widget{
					Label{Text: "队列名:"},
					LineEdit{AssignTo: &mw.edQueue, Text: "bench", CueBanner: "不存在会自动创建"},
					Label{Text: "消息条数:"},
					NumberEdit{AssignTo: &mw.neCount, Value: 10000, MinValue: 0, MaxValue: 2147483647, Increment: 1000},
					Label{Text: "并发数:"},
					NumberEdit{AssignTo: &mw.neConc, Value: 32, MinValue: 0, MaxValue: 512, Increment: 8},
					Label{Text: "消息体大小(字节):"},
					NumberEdit{AssignTo: &mw.neBody, Value: 32, MinValue: 0, MaxValue: 65536, Increment: 16},
					Label{Text: "压测模式:"},
					ComboBox{AssignTo: &mw.cmbMode, Value: "收发全链路", Model: []string{"收发全链路", "仅发送", "仅接收(即删除)"}},
				},
			},
			Composite{
				Layout: HBox{},
				Children: []Widget{
					PushButton{AssignTo: &mw.btnStart, Text: "开始压测", OnClicked: mw.onStart},
					PushButton{AssignTo: &mw.btnStop, Text: "停止", Enabled: false, OnClicked: mw.onStop},
				},
			},
			ProgressBar{AssignTo: &mw.pb, MinValue: 0, MaxValue: 100},
			GroupBox{
				Title:  "实时状态",
				Layout: Grid{Columns: 4},
				Children: []Widget{
					Label{Text: "阶段:"},
					Label{AssignTo: &mw.lbPhase, Text: "空闲"},
					Label{Text: "耗时:"},
					Label{AssignTo: &mw.lbRate, Text: "0s"},
					Label{Text: "已发送:"},
					Label{AssignTo: &mw.lbSent, Text: "0"},
					Label{Text: "已接收+删除:"},
					Label{AssignTo: &mw.lbRecv, Text: "0"},
				},
			},
			GroupBox{
				Title:  "结果输出",
				Layout: VBox{},
				Children: []Widget{
					TextEdit{AssignTo: &mw.te, ReadOnly: true, VScroll: true, MinSize: Size{Height: 180}},
				},
			},
		},
	}.Create()
}

func (mw *BenchWindow) appendLog(s string) {
	mw.logMu.Lock()
	defer mw.logMu.Unlock()
	mw.logBuf += s + "\r\n"
	if len(mw.logBuf) > 64*1024 {
		mw.logBuf = mw.logBuf[len(mw.logBuf)-64*1024:]
	}
	if mw.te != nil {
		mw.te.SetText(mw.logBuf)
	}
}

// readOptions 从界面读参数（主线程调用）
func (mw *BenchWindow) readOptions() benchcore.Options {
	mode := "both"
	switch mw.cmbMode.Text() {
	case "仅发送":
		mode = "send"
	case "仅接收(即删除)":
		mode = "recv"
	}
	url := mw.edURL.Text()
	for len(url) > 0 && url[len(url)-1] == '/' {
		url = url[:len(url)-1]
	}
	count := int(mw.neCount.Value())
	if count < 0 {
		count = 0 // 接收模式 0 = 不限条数（收空即停）
	}
	conc := int(mw.neConc.Value())
	if conc < 1 {
		conc = 1
	}
	body := int(mw.neBody.Value())
	if body < 1 {
		body = 1
	}
	return benchcore.Options{
		BaseURL:     url,
		APIKey:      mw.edKey.Text(),
		Queue:       mw.edQueue.Text(),
		Count:       count,
		Concurrency: conc,
		BodySize:    body,
		Mode:        mode,
	}
}

func (mw *BenchWindow) onStart() {
	if mw.running.Load() {
		return
	}
	opts := mw.readOptions()

	// 重置统计
	mw.st = benchcore.Stats{}
	mw.st.Phase.Store("准备")
	mw.startAt = time.Now()
	mw.running.Store(true)
	mw.btnStart.SetEnabled(false)
	mw.btnStop.SetEnabled(true)
	mw.pb.SetValue(0)

	// 先测试连通性
	go func(o benchcore.Options) {
		if err := benchcore.Ping(o); err != nil {
			mw.Synchronize(func() {
				mw.appendLog("[错误] " + err.Error())
				mw.finishUI("连接失败")
			})
			return
		}
		mw.appendLog(fmt.Sprintf("[开始] %s  队列=%s  条数=%s  并发=%d  消息体=%dB  模式=%s",
			o.BaseURL, o.Queue, modeCountText(o), o.Concurrency, o.BodySize, modeName(o.Mode)))

		sum := benchcore.Run(o, &mw.st)

		mw.Synchronize(func() {
			if o.Mode == "both" || o.Mode == "send" {
				mw.appendLog(fmt.Sprintf("发送: 成功 %d  失败 %d  速率 %.0f 条/秒", sum.SendCount, sum.SendFail, sum.SendRate))
			}
			if o.Mode != "send" {
				mw.appendLog(fmt.Sprintf("接收(即删除): 成功 %d  失败 %d  速率 %.0f 条/秒", sum.RecvCount, sum.RecvFail, sum.RecvRate))
			}
			mw.appendLog(sum.Result(o))
			if sum.Stopped {
				mw.finishUI("已停止")
			} else {
				mw.finishUI("完成")
			}
		})
	}(opts)
}

func (mw *BenchWindow) onStop() {
	mw.st.Stopped.Store(true)
}

// modeName 模式中文名
func modeName(mode string) string {
	switch mode {
	case "send":
		return "仅发送"
	case "recv":
		return "仅接收(即删除)"
	}
	return "收发全链路"
}

// modeCountText 条数描述（0=不限）
func modeCountText(o benchcore.Options) string {
	if o.Count <= 0 {
		return "不限(收空即停)"
	}
	return fmt.Sprintf("%d", o.Count)
}

func (mw *BenchWindow) finishUI(phase string) {
	mw.running.Store(false)
	mw.st.Phase.Store(phase)
	mw.btnStart.SetEnabled(true)
	mw.btnStop.SetEnabled(false)
	mw.lbPhase.SetText(phase)
	mw.refresh()
}

// refresh 定时刷新实时状态（主线程）
func (mw *BenchWindow) refresh() {
	phase := "空闲"
	if v, ok := mw.st.Phase.Load().(string); ok {
		phase = v
	}
	mw.lbPhase.SetText(phase)
	elapsed := time.Since(mw.startAt)
	mw.lbRate.SetText(elapsed.Round(100*time.Millisecond).String())
	mw.lbSent.SetText(fmt.Sprintf("%d (失败 %d)", mw.st.Sent.Load(), mw.st.SendFail.Load()))
	mw.lbRecv.SetText(fmt.Sprintf("%d (失败 %d)", mw.st.Recv.Load(), mw.st.RecvFail.Load()))

	target := int64(int(mw.neCount.Value()) * 2)
	if mw.cmbMode.Text() != "收发全链路" {
		target = int64(int(mw.neCount.Value()))
	}
	done := mw.st.Sent.Load() + mw.st.Recv.Load()
	if target > 0 {
		p := int(done * 100 / target)
		if p > 100 {
			p = 100
		}
		mw.pb.SetValue(p)
	}
}
