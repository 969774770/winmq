package gui

// gui.go —— 主控制窗口（walk 原生 GUI + 内嵌托盘）
// 信息区采用只读文本框展示：一行一条，每秒刷新，大类别用长横线分隔。

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lxn/walk"
	"github.com/lxn/win"
	. "github.com/lxn/walk/declarative"

	"winmq/internal/app"
	"winmq/internal/config"
	"winmq/internal/trayicon"
	"winmq/internal/winsvc"
)

// App 主窗口应用
type App struct {
	*walk.MainWindow

	cfg     *config.Config
	cfgPath string
	mgr     *app.ServiceManager
	logBuf  *LogBuffer

	te            *walk.TextEdit // 信息区（只读文本框）
	edPort        *walk.LineEdit
	cbAuto        *walk.CheckBox
	btnStart      *walk.PushButton
	btnStop       *walk.PushButton
	btnRestart    *walk.PushButton
	btnSvcInstall *walk.PushButton
	btnSvcUninst  *walk.PushButton
	ni            *walk.NotifyIcon

	svcRunning   bool   // Windows 服务正在运行（此时本窗口不启动本地实例）
	svcInstalled bool   // 服务已安装
	svcState     string // 服务状态文本
	svcTick      int
	autoInited   bool
	lastText     string
	logMu        sync.Mutex
}

// Run 启动主窗口（阻塞）；返回后进程退出
func Run(cfg *config.Config, cfgPath string, logBuf *LogBuffer) error {
	a := &App{
		cfg:     cfg,
		cfgPath: cfgPath,
		mgr:     app.NewServiceManager(cfg, cfgPath),
		logBuf:  logBuf,
	}
	return a.run()
}

func (a *App) run() error {
	// 若已安装的 Windows 服务正在运行，本窗口不再启动本地实例
	// （两者会争用同一份数据目录与同一个端口）
	a.refreshServiceState()
	if a.svcRunning {
		logf("检测到 winMQ 服务正在运行，本窗口仅用于查看状态")
	} else {
		// 自动启动服务
		go func() {
			if err := a.mgr.Start(); err != nil {
				logf("自动启动失败: %v", err)
			}
		}()
	}

	// 每秒刷新 UI
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for range t.C {
			a.MainWindow.Synchronize(a.refresh)
		}
	}()

	if err := a.setup(); err != nil {
		return err
	}
	// 关闭窗口 → 隐藏到托盘
	a.Closing().Attach(func(cancelled *bool, reason walk.CloseReason) {
		*cancelled = true
		a.Hide()
	})
	if err := a.setupTray(); err != nil {
		logf("托盘初始化失败: %v", err)
	}
	a.refresh()
	a.Run()
	return nil
}

func (a *App) setup() error {
	port := a.mgr.CurrentPort()
	return MainWindow{
		AssignTo: &a.MainWindow,
		Title:    "winMQ 消息队列服务",
		MinSize:  Size{Width: 360, Height: 430},
		Size:     Size{Width: 380, Height: 460},
		Layout:   VBox{Margins: Margins{Left: 6, Top: 6, Right: 6, Bottom: 6}, Spacing: 4},
		Children: []Widget{
			TextEdit{
				AssignTo: &a.te, ReadOnly: true, VScroll: true,
				MinSize: Size{Height: 280},
				Font:    Font{Family: "Consolas", PointSize: 9},
			},
			Composite{
				Layout: HBox{MarginsZero: true, Spacing: 6},
				Children: []Widget{
					Label{Text: "端口:"},
					LineEdit{AssignTo: &a.edPort, Text: port, CueBanner: "6166", MaxSize: Size{Width: 64}},
					CheckBox{AssignTo: &a.cbAuto, Text: "登录后自启", OnClicked: a.onAutoStart},
				},
			},
			Composite{
				Layout: HBox{MarginsZero: true, Spacing: 6},
				Children: []Widget{
					PushButton{AssignTo: &a.btnSvcInstall, Text: "安装服务", MinSize: Size{Width: 72}, OnClicked: func() { a.svcAction(true) }},
					PushButton{AssignTo: &a.btnSvcUninst, Text: "卸载服务", MinSize: Size{Width: 72}, OnClicked: func() { a.svcAction(false) }},
					Label{Text: "开机自启·无需登录"},
				},
			},
			Composite{
				Layout: HBox{MarginsZero: true, Spacing: 6},
				Children: []Widget{
					PushButton{AssignTo: &a.btnStart, Text: "启动", MinSize: Size{Width: 72}, OnClicked: func() { a.action("start") }},
					PushButton{AssignTo: &a.btnStop, Text: "停止", MinSize: Size{Width: 72}, OnClicked: func() { a.action("stop") }},
					PushButton{AssignTo: &a.btnRestart, Text: "重启", MinSize: Size{Width: 72}, OnClicked: func() { a.action("restart") }},
					PushButton{Text: "管理页", MinSize: Size{Width: 72}, OnClicked: a.openAdmin},
				},
			},
		},
	}.Create()
}

// setupTray 创建托盘图标
func (a *App) setupTray() error {
	// walk 无 bytes 图标 API，落临时文件加载
	icoPath := filepath.Join(a.cfg.AbsDataDir(), "tray.ico")
	if _, err := os.Stat(icoPath); err != nil {
		os.WriteFile(icoPath, trayicon.Bytes(), 0644)
	}
	ico, err := walk.NewIconFromFile(icoPath)
	if err != nil {
		return err
	}
	ni, err := walk.NewNotifyIcon(a.MainWindow)
	if err != nil {
		return err
	}
	a.ni = ni
	ni.SetIcon(ico)
	ni.SetToolTip("winMQ 消息队列服务")

	menu := ni.ContextMenu()
	addItem := func(text string, fn func()) {
		act := walk.NewAction()
		act.SetText(text)
		act.Triggered().Attach(fn)
		menu.Actions().Add(act)
	}
	addItem("显示主窗口", a.showWindow)
	addItem("打开管理页", a.openAdmin)
	addItem("退出", a.quit)

	ni.MouseUp().Attach(func(x, y int, _ walk.MouseButton) {
		a.showWindow()
	})
	ni.SetVisible(true)
	return nil
}

func (a *App) showWindow() {
	a.Show()
	win.SetForegroundWindow(a.Handle())
}

func (a *App) openAdmin() {
	exec.Command("rundll32", "url.dll,FileProtocolHandler", "http://127.0.0.1:"+a.mgr.CurrentPort()).Start()
}

// quit 真正退出
func (a *App) quit() {
	if a.ni != nil {
		a.ni.SetVisible(false)
		a.ni.Dispose()
	}
	a.mgr.Stop()
	a.logBuf.Close()
	os.Exit(0)
}

// onAutoStart 自启勾选
func (a *App) onAutoStart() {
	on := a.cbAuto.CheckState() == walk.CheckChecked
	if err := SetAutoStart(on); err != nil {
		logf("设置开机自启失败: %v", err)
		return
	}
	if on {
		logf("已开启登录后自启（仅当前用户登录后生效；如需无人登录自启，请安装为服务：winmq.exe -install）")
	} else {
		logf("已关闭登录后自启")
	}
}

// refreshServiceState 查询 Windows 服务状态（用于信息区展示与按钮联动）
func (a *App) refreshServiceState() {
	a.svcRunning, a.svcState = winsvc.RunningAndText()
	a.svcInstalled = a.svcState != "未安装"
}

// svcAction 一键安装 / 卸载 Windows 服务。
// 安装需要管理员权限，会以 UAC 提权方式重新启动本程序来执行；
// 且服务与本窗口不能同时占用同一份数据目录和端口，故安装前先停掉本窗口的实例。
func (a *App) svcAction(install bool) {
	port := a.mgr.CurrentPort()
	var msg string
	if install {
		msg = "把 winMQ 安装为 Windows 服务并立即启动：\n\n" +
			"· 开机自动启动，无需任何用户登录\n" +
			"· 进程异常退出后由系统自动重启\n" +
			"· 无界面运行，管理页 http://127.0.0.1:" + port + " 照常可用\n\n" +
			"服务与本窗口不能同时占用同一份数据目录和同一个端口，\n" +
			"因此会先停止本窗口当前的实例，随后请在 UAC 窗口中确认提权。\n\n是否继续？"
	} else {
		msg = "停止并卸载 winMQ 服务？\n\n" +
			"· 正在运行的服务实例会被中断（管理页将暂时无法访问）\n" +
			"· 之后可点「启动」在本窗口重新运行\n\n是否继续？"
	}
	if walk.MsgBox(a.MainWindow, "winMQ 系统服务", msg,
		walk.MsgBoxYesNo|walk.MsgBoxIconQuestion) != walk.DlgCmdYes {
		return
	}

	if install && a.mgr.Running() {
		// 先释放端口与数据目录，否则服务起不来
		if err := a.mgr.Stop(); err != nil {
			logf("停止本窗口实例失败: %v", err)
		}
	}

	arg := "-install"
	what := "安装服务"
	if !install {
		arg = "-uninstall"
		what = "卸载服务"
	}
	if err := winsvc.RunElevated(arg); err != nil {
		walk.MsgBox(a.MainWindow, "winMQ 系统服务", "提权失败："+err.Error(),
			walk.MsgBoxOK|walk.MsgBoxIconError)
		return
	}
	logf("已请求系统%s，请在 UAC 窗口中确认", what)
}

// action 启动/停止/重启
func (a *App) action(what string) {
	go func() {
		var err error
		switch what {
		case "start":
			err = a.mgr.Start()
		case "stop":
			err = a.mgr.Stop()
		case "restart":
			// 应用端口修改
			if p := strings.TrimSpace(a.edPort.Text()); p != "" && p != a.mgr.CurrentPort() {
				if err = a.mgr.SetPort(p); err != nil {
					break
				}
				logf("端口已修改为 %s", p)
			}
			logf("正在重启服务...")
			if err = a.mgr.Restart(); err == nil {
				logf("重启完成")
			}
		}
		if err != nil {
			logf("操作失败: %v", err)
		}
		a.MainWindow.Synchronize(a.refresh)
	}()
}

// buildStatusText 生成信息区文本（一行一条，横线分大类）
func (a *App) buildStatusText() string {
	const line = "──────────────────────────────"
	var b strings.Builder
	w := func(label, value string) {
		b.WriteString(" " + pad(label, 8) + ": " + value + "\r\n")
	}

	snap := a.mgr.State()
	switch {
	case snap.Starting:
		w("状态", "◌ 启动中…")
		w("地址", "http://127.0.0.1:"+snap.Port)
		w("启动时间", "-")
		w("运行时长", "-")
		w("启动耗时", "-")
	case snap.Running:
		w("状态", "● 运行中")
		w("地址", "http://127.0.0.1:"+snap.Port)
		w("启动时间", snap.StartAt.Format("2006-01-02 15:04:05"))
		w("运行时长", fmtDuration(time.Since(snap.StartAt)))
		w("启动耗时", fmt.Sprintf("%d 毫秒", snap.BootMs))
	default:
		w("状态", "○ 已停止")
		w("地址", "-")
		w("启动时间", "-")
		w("运行时长", "-")
		w("启动耗时", "-")
	}
	if a.svcState != "" {
		txt := a.svcState
		if a.svcRunning {
			txt += "（本窗口未启动本地实例）"
		}
		w("系统服务", txt)
	}

	b.WriteString(line + "\r\n [消息]\r\n")
	if eng := a.mgr.Engine(); eng != nil {
		st := eng.Stats()
		w("队列数", fmtInt(int64(st.QueueCount)))
		w("待消费", fmtInt(eng.TotalActiveMessages()))
		w("入队速率", fmtInt(st.EnqRate)+" 条/秒")
		w("消费速率", fmtInt(st.DeqRate)+" 条/秒")
		if eng.Recounting() {
			w("提示", "存量统计校准中…")
		}
	} else {
		for _, s := range []string{"队列数", "待消费", "入队速率", "消费速率"} {
			w(s, "-")
		}
	}

	b.WriteString(line + "\r\n [累计]\r\n")
	if eng := a.mgr.Engine(); eng != nil {
		st := eng.Stats()
		w("累计入队", fmtInt(st.EnqTotal))
		w("累计消费", fmtInt(st.DeqTotal))
	} else {
		for _, s := range []string{"累计入队", "累计消费"} {
			w(s, "-")
		}
	}

	b.WriteString(line + "\r\n [流量]\r\n")
	if srv := a.mgr.Server(); srv != nil {
		w("请求数", fmtInt(srv.ReqTotal.Load()))
		w("请求速率", fmtInt(srv.ReqRate())+" 次/秒")
		w("累计接收", fmtBytes(srv.BytesIn.Load()))
		w("累计发送", fmtBytes(srv.BytesOut.Load()))
	} else {
		for _, s := range []string{"请求数", "请求速率", "累计接收", "累计发送"} {
			w(s, "-")
		}
	}
	return b.String()
}

// refresh 每秒刷新（主线程）
func (a *App) refresh() {
	if a.te == nil {
		return
	}
	text := a.buildStatusText()
	a.logMu.Lock()
	changed := text != a.lastText
	if changed {
		a.lastText = text
	}
	a.logMu.Unlock()
	if changed {
		a.te.SetText(text)
	}

	// 按钮状态：运行中 → 启动禁用；已停止 → 停止/重启禁用；启动中 → 全部禁用
	// 由系统服务运行时，本窗口不提供启停（避免与服务的实例抢数据目录/端口）
	snap := a.mgr.State()
	a.btnStart.SetEnabled(!snap.Running && !snap.Starting && !a.svcRunning)
	a.btnStop.SetEnabled(snap.Running)
	a.btnRestart.SetEnabled(snap.Running)

	// 服务状态每 3 秒查询一次（用于展示与按钮联动）
	a.svcTick++
	if a.svcTick%3 == 0 {
		a.refreshServiceState()
	}
	a.btnSvcInstall.SetEnabled(!a.svcInstalled)
	a.btnSvcUninst.SetEnabled(a.svcInstalled)

	// 自启勾选状态（仅首次初始化，避免循环触发）
	if !a.autoInited {
		a.autoInited = true
		if IsAutoStart() {
			a.cbAuto.SetCheckState(walk.CheckChecked)
		}
	}
}

// pad 中文按 2 倍宽度补齐（等宽字体下对齐）
func pad(s string, w int) string {
	dw := 0
	for _, r := range s {
		if r > 0x2E7F {
			dw += 2
		} else {
			dw++
		}
	}
	for dw < w {
		s += " "
		dw++
	}
	return s
}
