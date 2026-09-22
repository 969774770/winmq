package winsvc

// winsvc.go —— Windows 服务接入。
//
// 服务方式运行时进程处于 session 0（无桌面），因此**不创建窗口与托盘**，
// 只跑引擎 + HTTP 服务：开机即启、无需任何用户登录，管理页照常用浏览器访问。
//
// 常用命令（需管理员权限）：
//
//	winmq.exe -install      安装为服务（自动启动 + 崩溃自动重启）
//	winmq.exe -uninstall    停止并卸载服务
//	winmq.exe -start        启动服务
//	winmq.exe -stop         停止服务
//	winmq.exe -status       查看服务状态

import (
	"fmt"
	"log"
	"os"
	"time"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

const (
	// Name 服务名（sc query winMQ / net start winMQ）
	Name = "winMQ"
	// DisplayName 服务管理器里显示的名称
	DisplayName = "winMQ 消息队列服务"
	// Description 服务描述
	Description = "winMQ 消息队列 HTTP 服务。开机自动启动，无需用户登录；以 SYSTEM 账户运行，无界面。"
)

// IsService 当前进程是否由服务控制管理器（SCM）启动
func IsService() bool {
	ok, err := svc.IsWindowsService()
	return err == nil && ok
}

// Run 以服务方式运行：由 SCM 调用，阻塞直到服务被停止
func Run(start func() error, stop func()) error {
	return svc.Run(Name, &handler{start: start, stop: stop})
}

type handler struct {
	start func() error
	stop  func()
}

// Execute 实现 svc.Handler：处理 SCM 的启动/停止/关机指令
func (h *handler) Execute(args []string, r <-chan svc.ChangeRequest, changes chan<- svc.Status) (bool, uint32) {
	const accepted = svc.AcceptStop | svc.AcceptShutdown
	changes <- svc.Status{State: svc.StartPending}

	if err := h.start(); err != nil {
		log.Printf("[service] 启动失败: %v", err)
		changes <- svc.Status{State: svc.Stopped}
		return false, 1
	}
	changes <- svc.Status{State: svc.Running, Accepts: accepted}

	for c := range r {
		switch c.Cmd {
		case svc.Interrogate:
			changes <- c.CurrentStatus
		case svc.Stop, svc.Shutdown:
			changes <- svc.Status{State: svc.StopPending}
			h.stop()
			return false, 0
		default:
			log.Printf("[service] 收到未知控制指令: %d", c.Cmd)
		}
	}
	return false, 0
}

// Install 安装（或更新）服务：注册为自动启动 + 失败后自动重启。需要管理员权限。
func Install() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("连接服务管理器失败（请以管理员身份运行）: %w", err)
	}
	defer m.Disconnect()

	s, err := m.OpenService(Name)
	if err != nil {
		// 尚未安装 → 新建
		s, err = m.CreateService(Name, exe, mgr.Config{
			StartType:   mgr.StartAutomatic,
			DisplayName: DisplayName,
			Description: Description,
		})
		if err != nil {
			return fmt.Errorf("安装服务失败（请以管理员身份运行）: %w", err)
		}
		log.Printf("[service] 已安装服务 %s：%s", Name, exe)
	} else {
		// 已安装 → 覆盖路径与启动类型（程序挪了目录也能自动纠正）
		cur, cerr := s.Config()
		if cerr != nil {
			s.Close()
			return fmt.Errorf("读取服务配置失败: %w", cerr)
		}
		cur.BinaryPathName = exe
		cur.StartType = mgr.StartAutomatic
		cur.DisplayName = DisplayName
		cur.Description = Description
		if err := s.UpdateConfig(cur); err != nil {
			s.Close()
			return fmt.Errorf("更新服务配置失败: %w", err)
		}
		log.Printf("[service] 服务 %s 已存在，已更新可执行路径：%s", Name, exe)
	}
	defer s.Close()

	// 异常退出后自动重启：前两次隔 5 秒，之后隔 30 秒；1 天后重置计数
	actions := []mgr.RecoveryAction{
		{Type: mgr.ServiceRestart, Delay: 5 * time.Second},
		{Type: mgr.ServiceRestart, Delay: 5 * time.Second},
		{Type: mgr.ServiceRestart, Delay: 30 * time.Second},
	}
	if err := s.SetRecoveryActions(actions, 86400); err != nil {
		log.Printf("[service] 设置失败自动重启策略失败: %v", err)
	}
	if err := s.SetRecoveryActionsOnNonCrashFailures(true); err != nil {
		log.Printf("[service] 设置非崩溃失败也重启失败: %v", err)
	}
	return nil
}

// Uninstall 停止并删除服务。需要管理员权限。
func Uninstall() error {
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("连接服务管理器失败（请以管理员身份运行）: %w", err)
	}
	defer m.Disconnect()

	s, err := m.OpenService(Name)
	if err != nil {
		return fmt.Errorf("服务 %s 未安装", Name)
	}
	defer s.Close()

	if _, err := s.Control(svc.Stop); err == nil {
		// 最多等 10 秒停止完成
		for i := 0; i < 20; i++ {
			st, qerr := s.Query()
			if qerr != nil || st.State == svc.Stopped {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	if err := s.Delete(); err != nil {
		return fmt.Errorf("删除服务失败: %w", err)
	}
	log.Printf("[service] 已卸载服务 %s", Name)
	return nil
}

// StartService 启动已安装的服务
func StartService() error {
	m, s, err := open()
	if err != nil {
		return err
	}
	defer m.Disconnect()
	defer s.Close()
	return s.Start()
}

// StopService 停止已安装的服务
func StopService() error {
	m, s, err := open()
	if err != nil {
		return err
	}
	defer m.Disconnect()
	defer s.Close()
	_, err = s.Control(svc.Stop)
	return err
}

// State 查询服务状态；installed=false 表示服务未安装
func State() (st svc.State, installed bool) {
	m, err := mgr.Connect()
	if err != nil {
		return 0, false
	}
	defer m.Disconnect()
	s, err := m.OpenService(Name)
	if err != nil {
		return 0, false
	}
	defer s.Close()
	status, err := s.Query()
	if err != nil {
		return 0, false
	}
	return status.State, true
}

// IsRunning 服务是否已安装且正在运行（供 GUI 判断，避免同时占用数据目录与端口）
func IsRunning() bool {
	st, ok := State()
	return ok && st == svc.Running
}

// StateText 状态中文描述（未安装时返回"未安装"）
func StateText() string {
	st, ok := State()
	if !ok {
		return "未安装"
	}
	switch st {
	case svc.Stopped:
		return "已停止"
	case svc.StartPending:
		return "启动中"
	case svc.StopPending:
		return "停止中"
	case svc.Running:
		return "运行中"
	case svc.Paused:
		return "已暂停"
	}
	return fmt.Sprintf("状态码 %d", st)
}

func open() (*mgr.Mgr, *mgr.Service, error) {
	m, err := mgr.Connect()
	if err != nil {
		return nil, nil, fmt.Errorf("连接服务管理器失败（请以管理员身份运行）: %w", err)
	}
	s, err := m.OpenService(Name)
	if err != nil {
		m.Disconnect()
		return nil, nil, fmt.Errorf("服务 %s 未安装", Name)
	}
	return m, s, nil
}
