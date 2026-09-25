# YggSpeedTest

高效、易用、无需 Root/TUN 权限的 Yggdrasil 节点网络并发测速工具。

仓库里有两个独立的可执行文件，共用同一份测量核心（`internal/engine`），因此**命令行和 Web 界面测出来的数字不可能不一致**：

| 二进制 | 源码 | 用途 |
|---|---|---|
| `yggspeedtest` | `cmd/yggspeedtest` | 命令行一次性批测，终端表格 / JSON / CSV / Markdown 输出 |
| `yggspeedtest-web` | `cmd/yggspeedtest-web` | Web 仪表盘 + 定时任务，内嵌离线 UI，无需前端构建 |

> ### ⚠️ 测速 URL 必须指向 Yggdrasil 网内（200::/7）
>
> 本工具在 gVisor 用户态协议栈里只注册了 Yggdrasil 的 `200::/7` 路由，**没有通向公网的路由**。
> `-url` / 配置里的 `test_url` 必须是 Yggdrasil 网络内部的地址（例如 `http://[201:xxxx::x]:8080/file.bin` 这种由 ygg 节点提供的下载地址）。
> 指向普通互联网站点（如 `speed.cloudflare.com`）时，**每个 peer 都会在下载阶段失败**——程序现在会直接拒绝这类目标并说明原因，而不再给出难懂的 `network is unreachable`。

---

## ✨ 核心特性 (Features)

### 测量核心

- **免 TUN 驱动运行**：基于 gVisor 用户态 TCP/IP 协议栈，无需 root/管理员或 TUN 虚拟网卡权限即可完成 Yggdrasil IPv6 节点链路测速。
- **高并发与内存池优化**：彻底解决并发情况下的网络帧竞态与内存分配瓶颈，引入 `sync.Pool` 缓存复用。
- **多流并发测速 (`-streams N`)**：支持单 Peer 开启多个 HTTP 连接并发下载，打满带宽测试节点极限速率。
- **实时带宽与峰值采样**：每 200ms 采样一次瞬时吞吐，准确计算平均下载速率 (Avg Mbps) 与峰值瞬间速率 (Peak Mbps)。
- **灵活的测速限制 (`-max-duration`, `-max-bytes`)**：可设定最大测速时长（如 10 秒）或最大流量消耗（如 `50MB`、`1.5MB`、`3kb`）；设为 `0` 表示该项不限（不限时时请留意对端是否真有无限大的文件）。
- **目标地址预检**：启动时解析测速 URL，若目标不在 `200::/7` 内会立即给出明确告警；单个节点在下载前也会做同样的检查，避免为注定失败的目标空等路由收敛。
- **一键在线公共节点测速 (`-public`)**：拉取上游 `yggdrasil-network/public-peers` 仓库的国家级 Markdown 节点列表，**合并全部数据源**（部分源失败会跳过并告警），跨区采样以便按路由质量排名。全部在线源不可用时回退到内置节点列表，并在日志中明确告警。
- **预检握手按协议实现**：TCP/TLS 真实握手、QUIC 带 ALPN 握手、WebSocket 完整 RFC 6455 握手（含 `Sec-WebSocket-Key`/`Version`，否则严格服务器拒绝升级）、**SOCKS/sockstls 走完整 SOCKS5 握手 + CONNECT（含用户名密码认证，URI 形如 `socks://user:pass@proxy:port/peer:port`）、unix 走 socket 路径连接**、KCP 只能做 UDP 可达性探测（不产出握手时长，结果里显示 `-`；当前 yggdrasil-go 已移除 KCP underlay，`core` 会把 `kcp://` 报成未知协议）。
- **路由探测不打扰被测服务器**：等待路由收敛改为轮询本节点的 `getPaths` 路由表，而不是反复真实 TCP 连接目标地址，避免测速前就把测量服务器的连接限额用掉。
- **路由 Ping 精确归属**：按 Peer URI 中的 `?key=` 节点 ID 在 `getPeers` 返回列表中匹配，不再取「列表中第一个延迟」而把别人的链路质量当成该节点的。
- **多样化的结果导出与多维度排序**：
  - 支持 **JSON** (`-out`)、**CSV** (`-out-csv`，带 UTF-8 BOM，Windows Excel 可直接打开中文表头)、**Markdown 表格** (`-out-md`) 格式导出；Web 端每次运行都能单独导出 CSV。
  - 支持按 **下载速度** (`speed`)、**峰值速度** (`peak`)、**路由 Ping** (`ping`) 或 **握手延迟** (`handshake`) 排序，采用稳定排序，速度相同的节点保持测试顺序，多次运行结果一致。
  - 支持 **速度门槛** (`-min-speed`) 与 **延迟门槛** (`-max-ping`) 结果过滤；设了门槛时，**没有测出对应数值的节点会被一并丢弃**——`-min-speed 50` 要的是「确实跑到 50 Mbps」的节点，把没测到的放过去等于让这两个参数在最需要它们的全失败运行里完全失效。测试失败的节点（有错误信息）始终保留，方便定位问题。
  - 未测得的数值显示为 `-` 而非 `0.00`，避免把「没测到」误读成「极快」。
- **增量落盘 (`-checkpoint`)**：每个节点测完立即以 JSONL 追加到文件并 `fsync`，进程被杀或 Ctrl+C 中断都不会丢掉已完成的结果；再次运行会续写而不是覆盖。
- **参数合法性前置校验**：`-c` / `-streams` 小于 1、`-sort` 拼写错误、`-timeout` / `-route-timeout` 非正数、`-max-duration` 为负、`-max-bytes` 格式错误、`-url` 不是合法的 http(s) 绝对地址，都会在启动时立即报错退出。`-sort` 拼错以前会静默退化成按速度排序，得到一份看起来正常但排错维度的榜单而毫无提示。
- **日志统一走 zap**：`netstack` 层原本的 `log.Println` 会在每个节点销毁时往 stderr 打一行 `Yggdrasil RWC read error: ErrClosed`，既绕过日志级别也不属于错误。现在改为走 zap Debug 级别，仅 `-debug` 时可见，正常输出不再被污染。
- **优雅中断 (Graceful Shutdown)**：`Ctrl+C` 自动停止后续排队的测试，保留已完成节点的全部数据并正常输出、保存。

### Web 与定时（`yggspeedtest-web`）

- **内嵌仪表盘**：UI 用 `go:embed` 编译进二进制，**零 CDN、零 `<link>`、零 `src=` 引用**，一台没有外网的机器上照样能打开页面（而这正是跑 Yggdrasil 的那类机器）。
- **实时进度**：Server-Sent Events 推送 `run_started` / `peer_done` / `run_finished` / `schedule`；晚连接的浏览器会先收到一帧 `hello`，随后 15 秒一个心跳，代理不会掐断长连接。服务关闭时事件流会立即结束，HTTP 优雅停机不会被开着的仪表盘标签页拖满超时。
- **内置排程器**：支持标准 5 字段 cron（`*`、`a-b`、`a,b,c`、`*/n`、`a-b/n`、月份与星期名字）和 `@every 30m` 之类的间隔写法，以及 `@hourly` / `@daily` / `@weekly` / `@monthly` / `@yearly` / `@midnight`。
  - 不需要任何新依赖，解析器是手写的。
  - cron 的「日 / 星期」字段遵守标准 **OR 规则**：两者都限定时，任一命中即触发。
  - `0 0 31 2 *` 这种不可能的日期会被扫描四年后放弃，不会让调度器空转（四年是最坏情况下相邻两个 2 月 29 日的间隔，`0 0 29 2 *` 因此可用）。
- **绝不重叠**：调度循环同步等待上一轮结束，一轮还没测完时到来的 tick 会被**计入 `skipped`** 而不是悄悄丢失——作业比间隔还慢时，运维能看见。
- **持久化**：`config.json` + `runs.jsonl`，均为明文 JSON，可手改可进版本库；写入走「临时文件 + rename」原子替换，写一半崩溃不会留下启动即报错的配置。历史文件里坏一行会被跳过，其余照常读取。

---

## 🚀 快速使用 (Usage)

### 命令行 `yggspeedtest`

下面的示例把 `http://[201:db8::1]:8080/1GB.bin` 用作下载目标占位——请替换成任一 **Yggdrasil 网内**的真实下载地址（测试 URL 的硬约束见顶部说明）。

**1. 单节点测试**
```bash
./yggspeedtest -peer "tls://ygg.mkg20001.io:443" -url "http://[201:db8::1]:8080/1GB.bin"
```

**2. 多线程多连接测试**
4 个并发 Peer，每个 4 条并发下载流，限时 10 秒：
```bash
./yggspeedtest -peer "tls://ygg.mkg20001.io:443" \
  -url "http://[201:db8::1]:8080/1GB.bin" \
  -c 4 -streams 4 -max-duration 10s
```

**3. 一键公共节点批量测速并导出**
```bash
./yggspeedtest -public -url "http://[201:db8::1]:8080/1GB.bin" \
  -c 5 -sort speed -out-md report.md -out-csv report.csv -out report.json
```

**4. 只测前 20 个节点，并留存增量结果**
```bash
./yggspeedtest -public -limit 20 \
  -url "http://[201:db8::1]:8080/500MB.bin" \
  -max-duration 8s -checkpoint results.jsonl
```

**5. 查看版本**
```bash
./yggspeedtest -version
```

### Web 服务 `yggspeedtest-web`

**1. 直接启动**（默认绑定 `127.0.0.1:8080`，数据文件落在当前目录）
```bash
./yggspeedtest-web
# 然后浏览器打开 http://127.0.0.1:8080
```

**2. 指定监听地址与数据目录**
```bash
./yggspeedtest-web -listen 0.0.0.0:9090 -data /opt/yggspeedtest
```
数据目录里会有两个文件：

```
/opt/yggspeedtest/config.json    # 测速参数 + 排程，明文 JSON，可手改
/opt/yggspeedtest/runs.jsonl     # 每次运行的记录，每行一条（默认保留最近 100 次，history_limit 设为 -1 表示全部保留）
```

**3. 定时测速**

在 UI 的「定时任务」卡片里填表达式并打开开关，或者改配置文件：

```json
{
  "schedule": "@every 6h",
  "enabled": true
}
```

也支持标准 cron：

| 表达式 | 含义 |
|---|---|
| `@every 30m` | 每 30 分钟（最小 1 分钟） |
| `@hourly` | 每小时整点 |
| `0 */6 * * *` | 每 6 小时 |
| `30 3 * * *` | 每天凌晨 3:30 |
| `0 9 * * 1-5` | 工作日 09:00 |
| `0 0 1 * *` | 每月 1 日零点 |

**4. 命令行操作 API**

```bash
# 状态（含下次触发时间、上次运行、已跳过次数）
curl -s localhost:8080/api/status | jq

# 立即触发一次运行（异步，返回 run id）
curl -s -X POST localhost:8080/api/run

# 跑临时指定的节点，不改保存的配置
curl -s -X POST localhost:8080/api/run -d '{"peer":["tls://ygg.mkg20001.io:443"]}'

# 中止正在运行的任务（返回 202）
curl -s -X POST localhost:8080/api/runs/RUN_ID/cancel

# 运行历史（最新在前）
curl -s localhost:8080/api/runs | jq

# 最近一次运行的完整结果，按下载速率从大到小排序（失败排最后）
curl -s localhost:8080/api/latest | jq

# 单次运行的完整结果
curl -s localhost:8080/api/runs/RUN_ID

# 导出 CSV / 删除记录
curl -OJ localhost:8080/api/runs/RUN_ID/csv
curl -s -X DELETE localhost:8080/api/runs/RUN_ID

# 实时监控（SSE）
curl -N localhost:8080/api/events
```

正在运行时再发 `POST /api/run` 会返回 `409`，所以不会排队出两次。
`POST /api/runs/{id}/cancel` 只会中止它自己的那一次运行，不影响之后的定时任务；
已经测完的节点仍然会被写入历史，并在 `error` 字段里标出 `cancelled`。
任务已经结束之后再发一次取消返回 `404`——那是"已经没有任务了"，不是出错。

---

## 🛠️ 参数说明 (Command Line Flags)

### `yggspeedtest`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `-peer string` | `""` | 单个测试目标 Yggdrasil Peer URI (如 `tcp://...`, `tls://...`, `quic://...`, `wss://...`) |
| `-file string` | `""` | 批量测试节点列表文件路径 (每行一个 Peer URI，`#` 开头为注释) |
| `-public` | `false` | 自动在线拉取上游公共节点列表进行测试 |
| `-limit int` | `0` | 最多测试多少个节点 (0 = 不限) |
| `-url string` | `""` | 用于测速的 HTTP/HTTPS 下载目标 URL (必填，必须是 Yggdrasil 网内 200::/7 的地址) |
| `-c int` | `1` | 并发测试的 Peer 节点数量 |
| `-streams int` | `1` | 每个 Peer 测速时开启的并发 HTTP 下载流数量 |
| `-max-duration string` | `"10s"` | 单节点测速最大持续时间 (如 `5s`, `10s`, `30s`；`0` = 不限时) |
| `-max-bytes string` | `""` | 单节点测速最大下载数据量 (如 `10MB`, `1.5MB`, `3kb`) |
| `-timeout string` | `"5s"` | 底层协议（TCP/TLS/QUIC/WS/SOCKS）预检握手超时时间 |
| `-route-timeout string` | `"15s"` | Yggdrasil 组网路由收敛等待超时时间 |
| `-sort string` | `"speed"` | 结果排序依据：`speed` (平均速率), `peak` (峰值速率), `ping` (路由延迟), `handshake` (握手时间) |
| `-min-speed float` | `0` | 只保留下载速率达到此 Mbps 的节点；**未能测出速率的节点会被一并丢弃** |
| `-max-ping float` | `0` | 只保留路由延迟低于此 ms 的节点；**未能测出延迟的节点会被一并丢弃** |
| `-out string` | `""` | 导出 JSON 格式结果文件路径 |
| `-out-csv string` | `""` | 导出 CSV 格式结果文件路径 (带 UTF-8 BOM) |
| `-out-md string` | `""` | 导出 Markdown 表格结果文件路径 |
| `-checkpoint string` | `""` | 每个节点完成后追加一行 JSONL 到该文件，防止中断丢结果 |
| `-dns string` | `""` | 自定义 DNS 服务器 (如 `127.0.0.1:53`) |
| `-host string` | `""` | 自定义 HTTP Host 头 |
| `-sni string` | `""` | 自定义 TLS SNI |
| `-key string` | `""` | 私钥文件路径 (仅 `-c=1` 时生效，首次运行会自动生成) |
| `-debug` | `false` | 开启详细 Debug 调试日志 |
| `-quiet` | `false` | 静音模式，仅输出错误信息与最终排行榜表格 |
| `-version` | `false` | 打印版本号后退出 |

### `yggspeedtest-web`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `-listen string` | 配置里的 `listen_addr`，再否则 `127.0.0.1:8080` | HTTP 监听地址 |
| `-data string` | 当前目录 | 数据目录，`config.json` 与 `runs.jsonl` 都在这里 |
| `-config string` | `<data>/config.json` | 配置文件路径 |
| `-runs string` | `<data>/runs.jsonl` | 运行历史文件路径 |
| `-debug` | `false` | 开启详细 Debug 调试日志（命令行优先；否则 UI 里保存的「调试日志」开关会生效） |
| `-quiet` | `false` | 静音模式，仅输出错误日志 |
| `-version` | `false` | 打印版本号后退出 |

Web 端其余参数（节点列表、下载 URL、并发、流数、超时、排序、门槛、排程）都在配置文件或 UI 里改，`PUT /api/config` 会先校验、再原子落盘、再热生效，**配置文件和进程状态不会不一致**。UI 里的「调试日志」开关同样热生效，无需重启。

> **安全提示**：API 与仪表盘默认没有任何鉴权，且响应带 `Access-Control-Allow-Origin: *`。默认绑定 `127.0.0.1` 时这是安全的；一旦用 `-listen 0.0.0.0:…` 暴露到公网/局域网，**任何网页都能借你的浏览器驱动测速、删除历史**。请自行通过防火墙、反向代理认证或 SSH 隧道保护。

---

## 🔨 交叉编译 (Build)

```bash
bash scripts/build.sh             # 用代码里的默认版本号
bash scripts/build.sh v2.1.0      # 版本号写入二进制（-version 会打印）
```

脚本会先跑 `go mod tidy` 与 `go build ./...` 做一次健全性检查，然后为**两个**二进制交叉编译到 `bin/`：

- `linux/amd64`、`linux/arm64`、`linux/arm/7`、`linux/mipsle/softfloat`
- `darwin/amd64`、`darwin/arm64`
- `windows/amd64`、`windows/arm64`

全部 `CGO_ENABLED=0`、`-ldflags="-s -w"`，纯静态、体积压到最小。GitHub Actions（`.github/workflows/build.yml`）走同一套平台矩阵，先跑 `go vet` + `go test`，通过后才打包。

---

## ✅ 质量校验

```bash
gofmt -l . && go vet ./... && go test -count=1 ./... && go build ./...
```

测试覆盖：

- **engine**：字节大小解析（含小数与 int64 溢出）、端口分配区间、节点去重、WebSocket Key 长度与随机性、CSV/Markdown 导出内容与 BOM、CSV 写错误上报、路由表轮询（假 admin socket，含空表超时与取消）、节点延迟按 key 匹配、速度/延迟门槛过滤（含「未测得则不通过」）、checkpoint JSONL 追加格式、稳定排序的并列保持、CJK 表头列宽对齐、UTF-8 截断、Peer URI 从 Markdown 中提取、200::/7 地址判定、测试 URL 形态校验、公共节点多源合并与全失败回退、SOCKS5/sockstls 预检握手（本地假代理，含认证与缺端口拒绝）、kcp 探测不产出假握手时长、终端表格缺测数值显示 `-` 而非 `0.000`。
- **schedule**：`@every` 解析与最小间隔、全部速记写法、5 字段表达式的 26 个正反用例（越界、空列表项、`5-2`）、区间展开、真实星期数下的 `Match`、cron 日/星期 OR 规则、`NextAfter` 与不可能的日期（`0 0 31 2 *`）、闰日表达式跨四年命中、调度器不重叠 / 忙时拒绝 / 跳过计数 / 停止语义。
- **store**：缺文件给默认值、`history_limit` 默认 100 与 `-1` 全保留、往返序列化、非法 JSON 拒绝、覆盖参数与空列表语义、坏 duration 拒绝、覆盖不别名原切片、历史追加与截断、坏行跳过、原子写入不残留临时文件、`CountRuns` 行数计数。
- **web**：`/healthz`、仪表盘自包含（断言无 `<link>` / `src=` / CDN 引用）、状态与配置形状、配置持久化与 7 类拒绝用例、被拒配置不落盘、运行生命周期（`202` → 历史记录 → 详情）、忙时 `409` 冲突、坏请求体 `400`、`404`、CSV 导出（含 BOM 与失败行）、删除幂等、历史最新在前、SSE 首帧 `hello`、`Cancel` 会立刻结束事件流、排程热切换、摘要计算、run id 唯一性、`Cancel` 停止语义。

---

## 📁 目录结构

```
cmd/
  yggspeedtest/        命令行前端
  yggspeedtest-web/    Web 前端（监听地址、数据目录、信号处理）
internal/
  engine/              全部测量逻辑，两个二进制共用
  schedule/            cron / @every 解析器与调度器
  store/               config.json 与 runs.jsonl 持久化
  web/                 HTTP API、SSE、内嵌仪表盘
    webui/index.html   单文件离线 UI
netstack/              gVisor 相关封装
scripts/build.sh       多平台交叉编译
```
