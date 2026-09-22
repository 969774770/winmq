# winMQ

Windows 上的轻量 HTTP 消息队列服务，对标阿里云 MNS 的基本功能，单机入队/消费万级每秒。

- **存储**：Pebble（Go 版 RocksDB）全量落盘，内存只保留"待消费序号"，占用与堆积量无关（1 亿条堆积下进程稳定在 220–280 MB）
- **语义**：接收即删除——取到的消息立即从队列移除，没有可见性超时、延迟、TTL 与队列属性，引擎内没有任何定时器
- **运行形态**：图形界面（托盘 + 控制窗口）或 Windows 服务（开机自启、无需用户登录）
- **附带**：内置管理控制台（原生前端，资源全部本地化、无外部 CDN）、压测工具、造数与观测工具

## 编译

需要 Go 1.24+。本机无需 CGO（Pebble 是纯 Go 实现）：

```powershell
$env:GOARCH='amd64'; $env:CGO_ENABLED='0'
go build -ldflags "-H=windowsgui" -o winmq.exe .            # 主程序（GUI 子系统、无控制台窗口）
go build -ldflags "-H=windowsgui" -o winmq-bench.exe ./cmd/benchgui   # 压测工具
```

> `-H=windowsgui` 是**必须**的，否则运行时会多一个黑色控制台窗口。
> 32 位编译不适用：引擎里使用了 64 位原子操作，Go 在 386 下会 panic（unaligned 64-bit atomic operation）。

## 运行方式

| 命令 | 说明 |
|---|---|
| `winmq.exe` | 图形界面：托盘 + 小窗口（状态、速率、流量；可改端口、启停、重启、打开管理页） |
| `winmq.exe -install` | 安装为 Windows 服务（自动启动 + 崩溃自动重启），**需管理员权限** |
| `winmq.exe -uninstall` | 停止并卸载服务，需管理员权限 |
| `winmq.exe -start` / `-stop` / `-status` | 服务启停与状态查询 |
| `winmq.exe -mode=headless` | 无界面前台运行（Ctrl+C 退出），适合自测或临时后台跑 |
| `winmq.exe -mode=bench -queue=q1 -count=10000 -concurrency=32` | 命令行 HTTP 全链路压测 |

### 开机自启，且无需用户登录

注册表 `HKCU\...\Run`（界面上的「登录后自启」）**必须用户登录后才会执行**，服务器停在登录界面时不会启动。要做到无人登录也随机器启动，请用服务方式：

```powershell
# 以管理员身份执行
D:\winMQ\winmq.exe -install
D:\winMQ\winmq.exe -start
```

之后重启机器、不登录任何用户，服务也会自动起来，管理页 `http://服务器IP:6166` 直接可用。

- 服务以 `LocalSystem` 运行，数据目录必须对该账户可写，不要放在用户私有目录或无权限的网络盘
- 服务在 session 0 运行，**没有窗口和托盘图标**（Windows 会话隔离），管理页与 API 不受影响
- exe 换了目录后再执行一次 `-install` 即可自动纠正服务路径，无需先卸载
- 图形界面与服务共用同一份配置与数据目录，因此同一时刻只能运行一个：GUI 启动时若检测到服务在运行，会自动不再启动本地实例，仅用于查看状态

## 配置

`config.json` 与 exe 同目录，**首次运行自动生成**（含随机 `accessKeySecret`）：

```json
{
  "httpAddr": ":6166",
  "dataDir": "D:\\winMQ\\data",
  "accessKeyId": "winmq",
  "accessKeySecret": "由程序自动生成",
  "snapshotIntervalSec": 30
}
```

`accessKeySecret` 同时是管理控制台的登录口令（`/admin/login`），请勿提交到版本库（已在 `.gitignore` 中排除）。

## API

认证：请求头 `X-Api-Key: <key>`（在控制台「API Key 管理」中创建，可设过期时间），也支持 `?key=<key>`。

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/queues` | 创建队列 `{"queueName":"订单"}`（幂等） |
| GET | `/api/queues` | 队列列表（含累计计数） |
| GET | `/api/queues/{name}` | 队列详情 |
| DELETE | `/api/queues/{name}` | 删除队列及其全部消息 |
| POST | `/api/queues/{name}/messages` | 发送消息 `{"body":"..."}` |
| POST | `/api/queues/{name}/messages/batch` | 批量发送 `{"bodies":["..",".."]}`（≤16 条） |
| GET | `/api/queues/{name}/messages?num=1&wait=5` | **接收（即删除）**，`wait` 为长轮询秒数 |
| GET | `/api/queues/{name}/messages/peek` | 查看队首消息（不消费） |
| POST | `/api/queues/{name}/peek-batch` | 分页查看 `{"offset":0,"limit":10}`（不消费） |

队列名支持中文、字母、数字、`_`、`-`，长度 1–64 个字符。调用示例：

```bash
curl -X POST http://127.0.0.1:6166/api/queues \
  -H "X-Api-Key: <你的key>" -H "Content-Type: application/json" \
  -d '{"queueName":"订单"}'

curl -X POST http://127.0.0.1:6166/api/queues/order/messages \
  -H "X-Api-Key: <你的key>" -H "Content-Type: application/json" \
  -d '{"body":"hello"}'

# 接收即删除；wait=5 表示没有消息时最多挂起 5 秒
curl "http://127.0.0.1:6166/api/queues/order/messages?num=1&wait=5" \
  -H "X-Api-Key: <你的key>"
```

> PowerShell 5.1 用 `-Body` 传字符串时会按非 UTF-8 编码，中文会被弄坏；请用 `[Text.Encoding]::UTF8.GetBytes($json)` 传字节。

## 管理控制台

浏览器打开 `http://127.0.0.1:6166`，输入 `accessKeySecret` 登录。包含全局总览、今日/近 7 天统计、队列管理（消息分页查看、磁盘回收、删除）、API Key 管理、内置引擎级压测。

## 实测性能

| 场景 | 速率 |
|---|---|
| 入队（引擎级，128 并发，每条 fsync） | 27,765 条/秒 |
| 入队（HTTP 单条接口） | 6,631 条/秒（受磁盘 fsync 限制） |
| 消费（引擎级，1 亿条堆积，128 并发） | 74,876 条/秒，连续 5 分钟每 2 秒采样最低 67k，无卡顿 |
| 消费（HTTP，1 亿条堆积） | 39,640 条/秒（300 万条，0 失败） |

内存占用与堆积量无关，1 亿条堆积下稳定在 220–280 MB。

## 工具

| 工具 | 用途 |
|---|---|
| `winmq-bench.exe` | 压测 GUI：收发全链路 / 仅发送 / 仅接收（即删除），可调并发、条数、消息体大小 |
| `go run ./tools/mkdata -queue big -count 100000000 -data D:\bigtest` | 造数：直接写 Pebble，亿级堆积分钟级完成（用于验证启动与消费） |
| `go run ./tools/memtest -mode get -queue big -count 100000000 -concurrency 128 -data D:\bigtest -fresh=false` | 引擎级入队/消费压测 + 内存观测，每 2 秒打印速率与内存 |
| `go run ./tools/diag send 1000000` / `recv 1000000` | 命令行 HTTP 压测（send / recv / both） |

## 存储设计

所有键都是定长、ASCII 安全的前缀，队列名不直接参与键拼接：

```
m:{id}{seq:016X}   消息体
q:{id}             队列名（UTF-8 原文，队列注册表）
s:{id}             队列累计计数 JSON
d:{YYYY-MM-DD}     每日统计
k:{key}            API Key
```

其中 `id = MD5(队列名) 取前 8 字节 → 16 位大写十六进制`（`engine.QueueID`）。这样做的收益：

- 任意字符（中文、符号）都不会破坏键或键范围；键里只有 `[0-9A-F]`
- 前缀定长，范围扫描 `[m:{id}, m:{id}\xff)` **严格只覆盖本队列**，不会串到 `订单2` 这类前缀同名的队列
- 长队列名反而比原名入键更省空间

磁盘回收：消息删除只写墓碑，磁盘由压实回收。为避免与消费抢 IO，全范围压实**只在队列消费空时自动触发**（或手动点「回收」），绝不在消费过程中执行——这正是早期版本"消费速率忽快忽 0"的根因。

## 目录结构

```
main.go                  入口：GUI / 服务 / 无界面 / 压测 / 服务管理命令
cmd/benchgui/            压测 GUI
internal/engine/         引擎：队列、Pebble 存储、消息、计数、API Key、每日统计
internal/server/         HTTP 服务 + 管理 API + 内嵌控制台前端（web/）
internal/app/            服务生命周期管理（GUI 与服务共用）
internal/gui/            Windows 图形界面（walk）与「登录后自启」
internal/winsvc/         Windows 服务接入（安装/卸载/启停/SCM 事件处理）
internal/benchcore/      压测核心（HTTP 客户端逻辑，GUI 与 CLI 共用）
tools/mkdata/            造数
tools/memtest/           引擎级压测与内存观测
tools/diag/              命令行 HTTP 压测
```

## 限制与注意

- **语义为"接收即删除"**：消费失败的消息不会重投，也没有 ack 接口。需要重试请在上层做补偿。
- 队列只有名字，没有延迟、可见性超时、TTL、消息大小上限等属性。
- 键布局与消息格式随重构变更过，**旧版本的数据目录不兼容**：启动时若检测到旧格式队列会打印告警，请清空数据目录重新导入。
- 单机版本，无集群/多副本/跨机同步。
