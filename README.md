# YggSpeedTest

高效、易用、无需 Root/TUN 权限的 Yggdrasil 节点网络并发测速工具。

---

## ✨ 核心特性 (Features)

- **免 TUN 驱动运行**：基于 gVisor 用户态 TCP/IP 协议栈，无需 root/管理员或 TUN 虚拟网卡权限即可完成 Yggdrasil IPv6 节点链路测速。
- **高并发与内存池优化**：彻底解决并发情况下的网络帧竞态与内存分配瓶颈，引入 `sync.Pool` 缓存复用。
- **多流并发测速 (`-streams N`)**：支持单 Peer 开启多个 HTTP 连接并发下载，打满带宽测试节点极限速率。
- **实时带宽与峰值采样**：准确计算平均下载速率 (Avg Mbps) 与峰值瞬间速率 (Peak Mbps)。
- **灵活的测速限制 (`-max-duration`, `-max-bytes`)**：可设定最大测速时长（如 10 秒）或最大流量消耗（如 50MB），避免大文件下载阻塞。
- **一键在线公共节点测速 (`-public`)**：无须手动收集节点，一键拉取最新公共 Yggdrasil 节点自动进行批量对比测速。
- **多样化结果导出与多维度排序**：
  - 支持 **JSON** (`-out`)、**CSV** (`-out-csv`)、**Markdown 表格** (`-out-md`) 格式导出。
  - 支持按 **下载速度** (`speed`)、**峰值速度** (`peak`)、**路由 Ping** (`ping`) 或 **握手延迟** (`handshake`) 灵活排序。
  - 支持 **速度门槛** (`-min-speed`) 与 **延迟门槛** (`-max-ping`) 结果过滤。
- **优雅中断 (Graceful Shutdown)**：运行中按 `Ctrl+C` 自动终止后续测试，并立即输出与保存已完成节点的测试数据。

---

## 🚀 快速使用 (Usage)

### 1. 单节点测试
测试单个 Yggdrasil Peer 的连接与下载速率：
```bash
./yggspeedtest -peer "tls://ygg.mkg20001.io:443" -url "http://[200:1234:5678::1]/100MB.bin"
```

### 2. 多线程多连接测试 (高级加速)
开启 4 个并发 Peer 测试，每个 Peer 建立 4 条并发 HTTP 下载流，限时 10 秒：
```bash
./yggspeedtest -peer "tls://ygg.mkg20001.io:443" -url "http://[200:1234:5678::1]/test.bin" -c 4 -streams 4 -max-duration 10s
```

### 3. 一键公共节点批量测速并导出 Markdown / CSV / JSON
```bash
./yggspeedtest -public -url "http://[200:1234:5678::1]/test.bin" -c 5 -sort speed -out-md report.md -out-csv report.csv -out report.json
```

---

## 🛠️ 参数说明 (Command Line Flags)

| 参数 | 默认值 | 说明 |
|---|---|---|
| `-peer string` | `""` | 单个测试目标 Yggdrasil Peer URI (如 `tcp://...`, `tls://...`, `quic://...`, `wss://...`) |
| `-file string` | `""` | 批量测试节点列表文件路径 (每行一个 Peer URI) |
| `-public` | `false` | 自动在线拉取最新 Yggdrasil 公共节点列表进行测试 |
| `-url string` | `""` | 用于测速的 HTTP/HTTPS 下载目标 URL |
| `-c int` | `1` | 并发测试的 Peer 节点数量 |
| `-streams int` | `1` | 每个 Peer 测速时开启的并发 HTTP 下载流数量 |
| `-max-duration string` | `"10s"` | 单节点测速最大持续时间 (如 `5s`, `10s`, `30s`) |
| `-max-bytes string` | `""` | 单节点测速最大下载数据量 (如 `10MB`, `50MB`, `100MB`) |
| `-timeout string` | `"5s"` | 底层协议（TCP/TLS/WS/QUIC）握手探测超时时间 |
| `-route-timeout string` | `"15s"` | Yggdrasil 组网路由收敛等待超时时间 |
| `-sort string` | `"speed"` | 结果排序依据：`speed` (平均速率), `peak` (峰值速率), `ping` (路由延迟), `handshake` (握手时间) |
| `-min-speed float` | `0` | 过滤丢弃下载速率低于此 Mbps 门槛的节点 |
| `-max-ping float` | `0` | 过滤丢弃路由延迟高于此 ms 门槛的节点 |
| `-out string` | `""` | 导出 JSON 格式结果文件路径 |
| `-out-csv string` | `""` | 导出 CSV 格式结果文件路径 |
| `-out-md string` | `""` | 导出 Markdown 表格格式结果文件路径 |
| `-debug` | `false` | 开启详细 Debug 调试日志 |
| `-quiet` | `false` | 静音模式，仅输出错误信息与最终排行榜表格 |

---

## 🔨 交叉编译 (Build)

运行脚本自动编译多平台静态二进制可执行文件：
```bash
bash scripts/build.sh
```
编译产物将存放在 `bin/` 目录下，包含 Windows, Linux (amd64/arm64/armv7/mipsle), macOS 等平台的独立可执行文件。
