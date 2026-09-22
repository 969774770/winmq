package main

// winMQ 消息队列服务入口。
//
// 运行方式：
//
//	winmq.exe                 图形界面（托盘 + 控制窗口，当前用户登录时使用）
//	winmq.exe -mode=headless  无界面运行（后台/自测）
//	winmq.exe -install        安装为 Windows 服务（开机自启、无需用户登录，需管理员）
//	winmq.exe -uninstall      停止并卸载服务（需管理员）
//	winmq.exe -start / -stop / -status   服务启停与状态
//	winmq.exe -mode=bench     HTTP 全链路压测（命令行）

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/windows"

	"winmq/internal/app"
	"winmq/internal/benchcore"
	"winmq/internal/config"
	"winmq/internal/gui"
	"winmq/internal/winsvc"
)

func main() {
	log.SetFlags(log.LstdFlags)

	mode := flag.String("mode", "serve", "运行模式: serve=图形界面, headless=无界面, bench=HTTP压测")
	queue := flag.String("queue", "bench", "压测队列名")
	count := flag.Int("count", 10000, "压测消息条数")
	conc := flag.Int("concurrency", 16, "压测并发数")

	install := flag.Bool("install", false, "安装为 Windows 服务（开机自启、无需登录，需管理员权限）")
	uninstall := flag.Bool("uninstall", false, "停止并卸载 Windows 服务（需管理员权限）")
	svcStart := flag.Bool("start", false, "启动已安装的 Windows 服务")
	svcStop := flag.Bool("stop", false, "停止已安装的 Windows 服务")
	svcStatus := flag.Bool("status", false, "查看 Windows 服务状态")
	flag.Parse()

	// 服务管理命令：本程序是 GUI 子系统（无控制台），先接管父控制台才能看到输出
	if *install || *uninstall || *svcStart || *svcStop || *svcStatus {
		attachConsole()
		if err := runServiceCmd(*install, *uninstall, *svcStart, *svcStop, *svcStatus); err != nil {
			fmt.Println("失败:", err)
			os.Exit(1)
		}
		return
	}

	// 配置文件：优先可执行文件目录，其次当前目录
	exe, _ := os.Executable()
	cfgPath := filepath.Join(filepath.Dir(exe), "config.json")
	if _, err := os.Stat(cfgPath); err != nil {
		cfgPath = "config.json"
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("读取配置失败: %v", err)
	}

	// 由服务控制管理器（SCM）拉起 → 以服务方式运行（无界面）
	if winsvc.IsService() {
		if err := runService(cfg, cfgPath); err != nil {
			log.Printf("以服务方式运行失败: %v", err)
			os.Exit(1)
		}
		return
	}

	switch *mode {
	case "bench":
		// 压测客户端模式：只做 HTTP 客户端，不打开引擎（避免与服务进程争目录锁）
		// 程序为无控制台窗口（GUI 子系统）构建，stdout 不可见，故报告同时写入 data\winmq.log
		lb := gui.NewLogBuffer(filepath.Join(cfg.AbsDataDir(), "winmq.log"))
		defer lb.Close()
		runBenchClient(cfg, gui.TeeWriter{lb, os.Stdout}, *queue, *count, *conc)
	case "headless":
		// 无界面运行：不开窗口与托盘，供后台手动运行或服务化前的自测
		if err := runHeadless(cfg, cfgPath); err != nil {
			log.Printf("无界面运行失败: %v", err)
			os.Exit(1)
		}
	default:
		// GUI 服务模式：日志双写（文件+内存缓冲），主窗口运行
		lb := gui.NewLogBuffer(filepath.Join(cfg.AbsDataDir(), "winmq.log"))
		log.SetOutput(gui.TeeWriter{lb, os.Stdout})
		if err := gui.Run(cfg, cfgPath, lb); err != nil {
			log.Fatalf("窗口启动失败: %v", err)
		}
	}
}

// runServiceCmd 执行服务管理命令
func runServiceCmd(install, uninstall, start, stop, status bool) error {
	switch {
	case install:
		if err := winsvc.Install(); err != nil {
			return err
		}
		fmt.Printf("服务 %s 已安装（自动启动，开机无需登录即运行）。\n现在启动它：winmq.exe -start\n", winsvc.Name)
	case uninstall:
		if err := winsvc.Uninstall(); err != nil {
			return err
		}
		fmt.Printf("服务 %s 已卸载\n", winsvc.Name)
	case start:
		if err := winsvc.StartService(); err != nil {
			return err
		}
		fmt.Printf("服务 %s 已启动\n", winsvc.Name)
	case stop:
		if err := winsvc.StopService(); err != nil {
			return err
		}
		fmt.Printf("服务 %s 已停止\n", winsvc.Name)
	case status:
		fmt.Printf("服务 %s: %s\n", winsvc.Name, winsvc.StateText())
	}
	return nil
}

// runService 以 Windows 服务方式运行（无界面，session 0）
func runService(cfg *config.Config, cfgPath string) error {
	if f := openLogFile(cfg); f != nil {
		defer f.Close()
		log.SetOutput(f)
	}
	sm := app.NewServiceManager(cfg, cfgPath)
	log.Printf("[service] 以服务方式运行（无界面，session 0）")
	return winsvc.Run(
		func() error { return sm.Start() },
		func() { sm.Stop() },
	)
}

// runHeadless 无界面前台运行（Ctrl+C 退出）
func runHeadless(cfg *config.Config, cfgPath string) error {
	attachConsole()
	if f := openLogFile(cfg); f != nil {
		defer f.Close()
		log.SetOutput(gui.TeeWriter{f, os.Stdout})
	}
	sm := app.NewServiceManager(cfg, cfgPath)
	if err := sm.Start(); err != nil {
		return err
	}
	log.Printf("无界面模式已启动（日志 %s），按 Ctrl+C 退出", filepath.Join(cfg.AbsDataDir(), "winmq.log"))

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
	return sm.Stop()
}

// openLogFile 打开 data\winmq.log（无界面模式的日志）
func openLogFile(cfg *config.Config) *os.File {
	dir := cfg.AbsDataDir()
	os.MkdirAll(dir, 0755)
	f, err := os.OpenFile(filepath.Join(dir, "winmq.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil
	}
	return f
}

// attachConsole 让 GUI 子系统程序被 cmd/PowerShell 调用时也能输出到父控制台；
// 无父控制台时静默失败（例如被服务控制管理器启动时）。
func attachConsole() {
	// stdout 已被调用方重定向到管道/文件时不要抢
	if _, err := os.Stdout.Stat(); err == nil {
		return
	}
	const attachParentProcess = ^uintptr(0) // (DWORD)-1
	r, _, _ := procAttachConsole.Call(attachParentProcess)
	if r == 0 {
		return
	}
	if f, err := os.OpenFile("CONOUT$", os.O_WRONLY, 0); err == nil {
		os.Stdout, os.Stderr = f, f
	}
}

var procAttachConsole = windows.NewLazySystemDLL("kernel32.dll").NewProc("AttachConsole")

// runBenchClient HTTP 全链路压测客户端（收发全链路，接收即删除）
// 更完整的压测请用 winmq-bench.exe（可调参数、可选仅发送/仅接收）
func runBenchClient(cfg *config.Config, w io.Writer, queue string, count, conc int) {
	base := "http://127.0.0.1" + cfg.HTTPAddr
	defaultKey := getDefaultKey(base, cfg.AccessKeySecret)
	if defaultKey == "" {
		fmt.Fprintln(w, "未找到 API Key，请在管理页创建")
		return
	}

	fmt.Fprintf(w, "=== HTTP 全链路压测 ===\n队列: %s  条数: %d  并发: %d\n\n", queue, count, conc)
	sum := benchcore.Run(benchcore.Options{
		BaseURL:     base,
		APIKey:      defaultKey,
		Queue:       queue,
		Count:       count,
		Concurrency: conc,
		BodySize:    32,
		Mode:        "both",
	}, nil)
	fmt.Fprint(w, sum.Result(benchcore.Options{Mode: "both"}))
}

// getDefaultKey 通过管理接口取第一个可用 key（诊断模式便捷用）
func getDefaultKey(base, secret string) string {
	hm := hmac.New(sha256.New, []byte(secret))
	hm.Write([]byte("winmq-admin-session"))
	token := hex.EncodeToString(hm.Sum(nil))

	req, _ := http.NewRequest("GET", base+"/admin/api/keys", nil)
	req.Header.Set("X-Admin-Token", token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var out struct {
		Keys []struct {
			Key string `json:"key"`
		} `json:"keys"`
	}
	data, _ := io.ReadAll(resp.Body)
	if json.Unmarshal(data, &out) != nil || len(out.Keys) == 0 {
		return ""
	}
	return out.Keys[0].Key
}
