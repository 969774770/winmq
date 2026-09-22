package app

// service.go —— 服务生命周期管理（进程内启停）。
// 不含任何 GUI 依赖，因此既供图形界面调用，也供 Windows 服务（无界面）模式调用。

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"winmq/internal/config"
	"winmq/internal/engine"
	"winmq/internal/server"
)

// ServiceManager 服务生命周期管理
type ServiceManager struct {
	mu      sync.Mutex
	cfg     *config.Config
	cfgPath string

	eng *engine.Engine
	srv *server.Server
	ln  net.Listener

	running  bool
	starting bool
	startAt  time.Time
	bootMs   int64 // 上次启动耗时（毫秒）

	snap atomic.Value // *Snapshot：供界面无锁读取，避免启动期间卡住 UI
}

// Snapshot 服务状态快照（界面读取用，无锁、不阻塞）
type Snapshot struct {
	Running  bool
	Starting bool
	StartAt  time.Time
	BootMs   int64
	Eng      *engine.Engine
	Srv      *server.Server
	Port     string
}

// publish 发布状态快照（调用方须持有 m.mu）
func (m *ServiceManager) publish() {
	port := m.cfg.HTTPAddr
	if len(port) > 0 && port[0] == ':' {
		port = port[1:]
	}
	m.snap.Store(&Snapshot{
		Running:  m.running,
		Starting: m.starting,
		StartAt:  m.startAt,
		BootMs:   m.bootMs,
		Eng:      m.eng,
		Srv:      m.srv,
		Port:     port,
	})
}

// State 读取当前状态快照（无锁，界面每次刷新都走这里）
func (m *ServiceManager) State() *Snapshot {
	if v := m.snap.Load(); v != nil {
		return v.(*Snapshot)
	}
	return &Snapshot{}
}

func NewServiceManager(cfg *config.Config, cfgPath string) *ServiceManager {
	m := &ServiceManager{cfg: cfg, cfgPath: cfgPath}
	m.publish()
	return m
}

// Start 启动消息服务（打开引擎 + HTTP 监听）
func (m *ServiceManager) Start() error {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return fmt.Errorf("服务已在运行")
	}
	m.starting = true
	m.publish() // 先告知界面"启动中"，避免看起来像卡死
	m.mu.Unlock()

	t0 := time.Now()

	eng, err := engine.Open(m.cfg.AbsDataDir())
	if err != nil {
		m.mu.Lock()
		m.starting = false
		m.publish()
		m.mu.Unlock()
		return fmt.Errorf("打开存储失败: %w", err)
	}

	srv := server.NewServer(eng, m.cfg)
	ln, err := net.Listen("tcp", m.cfg.HTTPAddr)
	if err != nil {
		eng.Close()
		m.mu.Lock()
		m.starting = false
		m.publish()
		m.mu.Unlock()
		return fmt.Errorf("监听 %s 失败: %w", m.cfg.HTTPAddr, err)
	}

	// 空闲连接 2 分钟自动关闭，避免长时间运行后堆积无用连接
	hs := &http.Server{Handler: srv.Handler(), IdleTimeout: 120 * time.Second}
	go hs.Serve(ln)

	m.mu.Lock()
	m.eng = eng
	m.srv = srv
	m.ln = ln
	m.running = true
	m.starting = false
	m.startAt = time.Now()
	m.bootMs = time.Since(t0).Milliseconds()
	m.publish()
	boot := m.bootMs
	m.mu.Unlock()

	log.Printf("服务已启动: http://127.0.0.1%s （启动耗时 %d ms，数据目录 %s）",
		m.cfg.HTTPAddr, boot, m.cfg.AbsDataDir())
	return nil
}

// BootMs 上次启动耗时（毫秒）
func (m *ServiceManager) BootMs() int64 { return m.State().BootMs }

// Stop 停止消息服务
func (m *ServiceManager) Stop() error {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return fmt.Errorf("服务未运行")
	}
	ln, eng := m.ln, m.eng
	m.running = false
	m.eng = nil
	m.srv = nil
	m.ln = nil
	m.publish()
	m.mu.Unlock()

	ln.Close()
	eng.Close()
	log.Printf("服务已停止")
	return nil
}

// Restart 重启消息服务（端口修改后调用）
func (m *ServiceManager) Restart() error {
	m.mu.Lock()
	wasRunning := m.running
	ln, eng := m.ln, m.eng
	if wasRunning {
		m.running = false
		m.eng = nil
		m.srv = nil
		m.ln = nil
		m.publish()
	}
	m.mu.Unlock()

	if !wasRunning {
		return nil
	}
	ln.Close()
	eng.Close()
	time.Sleep(300 * time.Millisecond) // 等端口释放
	return m.Start()
}

// Running 服务是否在运行（无锁读快照）
func (m *ServiceManager) Running() bool { return m.State().Running }

// Starting 是否正在启动
func (m *ServiceManager) Starting() bool { return m.State().Starting }

// StartAt 服务启动时间（无锁读快照）
func (m *ServiceManager) StartAt() time.Time { return m.State().StartAt }

// Engine 当前引擎实例（无锁读快照，未运行返回 nil）
func (m *ServiceManager) Engine() *engine.Engine { return m.State().Eng }

// Server 当前 HTTP 服务实例（无锁读快照，未运行返回 nil）
func (m *ServiceManager) Server() *server.Server { return m.State().Srv }

// SetPort 修改端口并保存配置（重启后生效）
func (m *ServiceManager) SetPort(port string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if port == "" {
		return fmt.Errorf("端口不能为空")
	}
	m.cfg.HTTPAddr = ":" + port
	m.publish()
	return config.Save(m.cfgPath, m.cfg)
}

// CurrentPort 当前配置端口（无锁读快照）
func (m *ServiceManager) CurrentPort() string { return m.State().Port }
