package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/gologme/log"
	"github.com/yggdrasil-network/yggdrasil-go/src/admin"
	"github.com/yggdrasil-network/yggdrasil-go/src/config"
	"github.com/yggdrasil-network/yggdrasil-go/src/core"
	"yggspeedtest/netstack"
)

var zlog *zap.Logger
var globalOutJSON string
var nextAdminPort int32

type PingFloat float64

func (pf PingFloat) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%.2f", pf)), nil
}

type SpeedFloat float64

func (sf SpeedFloat) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%.3f", sf)), nil
}

type SpeedResult struct {
	Peer         string      `json:"peer"`
	TestTime     string      `json:"test_time"`
	HandshakeMs  *PingFloat  `json:"handshake_ms,omitempty"`
	PingMs       *PingFloat  `json:"ping_ms,omitempty"`
	DownloadMbps *SpeedFloat `json:"download_mbps,omitempty"`
	Error        string      `json:"error,omitempty"`
}

func init() {
	// 初始化 Admin Port 起始值為 20000~30000 之間的隨機數，避免每次啟動衝突
	nextAdminPort = int32(20000 + (time.Now().UnixNano() % 10000))
}

func initZapLogger(debug bool) {
	var zapConfig zap.Config
	if debug {
		zapConfig = zap.NewDevelopmentConfig()
		zapConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		zapConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	} else {
		zapConfig = zap.NewProductionConfig()
		zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		zapConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}
	zapConfig.OutputPaths = []string{"stdout"}
	zapConfig.ErrorOutputPaths = []string{"stderr"}

	var err error
	zlog, err = zapConfig.Build()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize zap logger: %v", err))
	}
}

func main() {
	peerURI := flag.String("peer", "", "Target Yggdrasil peer URI (single test)")
	peersFile := flag.String("file", "", "Text file containing one peer URI per line (batch test)")
	outJSON := flag.String("out", "", "Output sorted JSON results to this file path")
	
	concurrency := flag.Int("c", 1, "Number of concurrent tests (if > 1, -key flag is ignored)")

	testURL := flag.String("url", "", "URL to test download speed")
	customHost := flag.String("host", "", "Custom HTTP Host header")
	customSNI := flag.String("sni", "", "Custom TLS SNI")
	customDNS := flag.String("dns", "", "Custom DNS server (e.g., 127.0.0.1:53)")
	keyFile := flag.String("key", "", "Path to private key file (optional, only effective if -c=1)")
	debugLog := flag.Bool("debug", false, "Enable detailed debug logs")
	flag.Parse()

	globalOutJSON = *outJSON
	initZapLogger(*debugLog)
	defer zlog.Sync()

	zlog.Info("Starting YggSpeedTest Batch Mode", zap.String("version", "4.3-clean-json"))

	if *testURL == "" {
		outputError("Missing -url argument", nil, "")
	}

	var peers []string
	if *peersFile != "" {
		filePeers, err := readPeersFromFile(*peersFile)
		if err != nil {
			outputError("Failed to read peers file", err, "")
		}
		peers = append(peers, filePeers...)
	}
	if *peerURI != "" {
		peers = append(peers, *peerURI)
	}

	if len(peers) == 0 {
		outputError("No peers specified. Use -peer or -file.", nil, "")
	}

	var globalCfg *config.NodeConfig
	if *concurrency == 1 {
		globalCfg = config.GenerateConfig()
		if *keyFile != "" {
			if _, err := os.Stat(*keyFile); os.IsNotExist(err) {
				privHex := hex.EncodeToString(globalCfg.PrivateKey)
				os.WriteFile(*keyFile, []byte(privHex), 0600)
			} else {
				keyBytes, err := os.ReadFile(*keyFile)
				if err != nil {
					outputError("Failed to read private key", err, "")
				}
				minConf := fmt.Sprintf(`{"PrivateKey": "%s"}`, strings.TrimSpace(string(keyBytes)))
				globalCfg.ReadFrom(strings.NewReader(minConf))
			}
		}
	} else {
		zlog.Warn(fmt.Sprintf("Concurrency is set to %d. Ignoring -key parameter. Generating temporary in-memory keys for each test.", *concurrency))
	}

	zlog.Info(fmt.Sprintf("Ready to test %d peers with concurrency %d", len(peers), *concurrency))

	var results []SpeedResult
	var mu sync.Mutex
	var wg sync.WaitGroup

	// 控制併發數量的 Semaphore
	sem := make(chan struct{}, *concurrency)

	for i, p := range peers {
		wg.Add(1)
		sem <- struct{}{} // 獲取信號量

		go func(index int, peer string) {
			defer wg.Done()
			defer func() { <-sem }() // 釋放信號量

			zlog.Info(fmt.Sprintf("=== [%d/%d] Testing peer ===", index+1, len(peers)), zap.String("peer", peer))
			
			// 如果是併發模式，為每個測試分配獨立的 NodeConfig 與 IP
			cfg := globalCfg
			if *concurrency > 1 {
				cfg = config.GenerateConfig()
			}

			res := runPeerTest(peer, cfg, *testURL, *customHost, *customSNI, *customDNS, *debugLog)
			
			mu.Lock()
			results = append(results, res)
			mu.Unlock()

			zlog.Debug("Peer test finished", zap.String("peer", peer))
		}(i, p)
	}

	// 等待所有測試完成
	wg.Wait()

	// 排序結果 (由快到慢)
	sort.Slice(results, func(i, j int) bool {
		var m1, m2 float64
		if results[i].DownloadMbps != nil { m1 = float64(*results[i].DownloadMbps) }
		if results[j].DownloadMbps != nil { m2 = float64(*results[j].DownloadMbps) }
		return m1 > m2
	})

	if *outJSON != "" {
		outData, _ := json.MarshalIndent(results, "", "  ")
		if err := os.WriteFile(*outJSON, outData, 0644); err != nil {
			zlog.Error("Failed to write JSON output", zap.Error(err))
		} else {
			zlog.Info("Results successfully saved to JSON file", zap.String("file", *outJSON))
		}
	} else {
		fmt.Println("\n========================= 測速排行榜 (由快到慢) =========================")
		for i, r := range results {
			if r.Error != "" {
				fmt.Printf("%2d. [0.000 Mbps] | FAIL: %s | %s\n", i+1, truncate(r.Error, 30), r.Peer)
			} else {
				fmt.Printf("%2d. [%5.3f Mbps] | Handshake: %6.2fms | Ping: %6.2fms | %s\n", 
					i+1, float64(*r.DownloadMbps), float64(*r.HandshakeMs), float64(*r.PingMs), r.Peer)
			}
		}
		fmt.Println("=========================================================================")
	}
}

// 獨立的底層協議握手探測函數 (修正 Yggdrasil ALPN 與 WS 子協議兼容性)
func measureHandshake(peerURI string, customSNI string, timeout time.Duration) (float64, error) {
	u, err := url.Parse(peerURI)
	if err != nil {
		return 0, err
	}

	host := u.Hostname()
	port := u.Port()
	if port == "" {
		switch u.Scheme {
		case "tcp", "ws":
			port = "80"
		case "tls", "wss", "quic":
			port = "443"
		}
	}
	address := net.JoinHostPort(host, port)

	sni := host
	if customSNI != "" {
		sni = customSNI
	} else if qsni := u.Query().Get("sni"); qsni != "" {
		sni = qsni
	}

	zlog.Debug("Initiating underlay handshake",
		zap.String("scheme", u.Scheme),
		zap.String("address", address),
		zap.String("sni", sni))

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	switch u.Scheme {
	case "tcp":
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", address)
		if err != nil {
			return 0, err
		}
		zlog.Debug("TCP Handshake OK", zap.String("remote", conn.RemoteAddr().String()))
		conn.Close()

	case "tls":
		var d net.Dialer
		conn, err := tls.DialWithDialer(&d, "tcp", address, &tls.Config{
			InsecureSkipVerify: true, // 強制跳過驗證
			ServerName:         sni,
			// 修正: 官方標準 TLS 不強制指定 ALPN，留空以確保最高相容性
			NextProtos:         nil, 
		})
		if err != nil {
			return 0, err
		}

		cn := "unknown"
		state := conn.ConnectionState()
		if len(state.PeerCertificates) > 0 {
			cn = state.PeerCertificates[0].Subject.CommonName
		}

		zlog.Debug("TLS Handshake OK",
			zap.String("remote", conn.RemoteAddr().String()),
			zap.String("cert_CN", cn),
			zap.String("alpn", state.NegotiatedProtocol))
		conn.Close()

	case "ws", "wss":
		scheme := "http"
		if u.Scheme == "wss" {
			scheme = "https"
		}
		reqURL := fmt.Sprintf("%s://%s/", scheme, address)
		req, _ := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		// Yggdrasil 强制要求 ygg-ws 子协议
		req.Header.Set("Sec-WebSocket-Protocol", "ygg-ws") 

		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				ServerName:         sni,
				NextProtos:         []string{"http/1.1"}, // WSS 走标准 HTTP/1.1 协议升级
			},
			DialContext: (&net.Dialer{}).DialContext,
		}
		client := &http.Client{Transport: tr}
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()

		// 检查 WebSocket 升级是否成功 (正常必须是 101)
		if resp.StatusCode != http.StatusSwitchingProtocols {
			return 0, fmt.Errorf("WS upgrade failed: expected 101, got %d %s", resp.StatusCode, resp.Status)
		}

		cn := "N/A (Cleartext)"
		negotiatedALPN := ""
		if resp.TLS != nil {
			if len(resp.TLS.PeerCertificates) > 0 {
				cn = resp.TLS.PeerCertificates[0].Subject.CommonName
			}
			negotiatedALPN = resp.TLS.NegotiatedProtocol
		}

		// 获取 Server 端确认回传的 Subprotocol
		respSubproto := resp.Header.Get("Sec-WebSocket-Protocol")

		zlog.Debug("WebSocket Handshake OK",
			zap.String("status", resp.Status),
			zap.String("cert_CN", cn),
			zap.String("alpn", negotiatedALPN),
			zap.String("subproto", respSubproto))

	case "quic":
		tlsConf := &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         sni,
			// 修正: QUIC 必須帶 ALPN，官方 Yggdrasil 的 QUIC Listeners 驗證的值為 "yggdrasil"
			NextProtos:         []string{"yggdrasil"}, 
		}
		conn, err := quic.DialAddr(ctx, address, tlsConf, nil)
		if err != nil {
			return 0, err
		}

		cn := "unknown"
		certs := conn.ConnectionState().TLS.PeerCertificates
		if len(certs) > 0 {
			cn = certs[0].Subject.CommonName
		}

		zlog.Debug("QUIC Handshake OK",
			zap.String("remote", conn.RemoteAddr().String()),
			zap.String("cert_CN", cn))
		conn.CloseWithError(0, "")

	case "kcp":
		var d net.Dialer
		conn, err := d.DialContext(ctx, "udp", address)
		if err != nil {
			return 0, err
		}
		conn.Write([]byte{0})
		zlog.Debug("KCP (UDP) Initial packet sent", zap.String("remote", address))
		conn.Close()

	default:
		return 0, fmt.Errorf("unsupported protocol for handshake: %s", u.Scheme)
	}

	return float64(time.Since(start).Milliseconds()), nil
}

func runPeerTest(originalURI string, cfg *config.NodeConfig, testURL, customHost, customSNI, customDNS string, debugLog bool) SpeedResult {
	finalPeerURI := originalURI
	if customSNI != "" {
		u, err := url.Parse(finalPeerURI)
		if err == nil && (u.Scheme == "tls" || u.Scheme == "quic" || u.Scheme == "wss") {
			q := u.Query()
			q.Set("sni", customSNI)
			u.RawQuery = q.Encode()
			finalPeerURI = u.String()
		}
	}

	hsMs, hsErr := measureHandshake(finalPeerURI, customSNI, 5*time.Second)
	if hsErr != nil {
		zlog.Debug("Pre-flight handshake failed, aborting Yggdrasil core init", zap.Error(hsErr))
		return genErrorResult(originalURI, fmt.Sprintf("Handshake failed: %v", hsErr))
	}

	var yggLogger *log.Logger
	if debugLog {
		yggLogger = log.New(os.Stdout, "[YGG-CORE] ", log.Flags())
		yggLogger.EnableLevel("error")
		yggLogger.EnableLevel("warn")
		yggLogger.EnableLevel("info")
		yggLogger.EnableLevel("debug")
	} else {
		yggLogger = log.New(io.Discard, "", 0)
	}

	coreOptions := []core.SetupOption{
		core.NodeInfoPrivacy(true),
		core.Peer{URI: finalPeerURI},
	}

	zlog.Debug("Initializing Yggdrasil core...")
	ygg, err := core.New(cfg.Certificate, yggLogger, coreOptions...)
	if err != nil {
		return genErrorResult(originalURI, fmt.Sprintf("Init core failed: %v", err))
	}
	defer ygg.Stop()

	// 使用 Atomic 取得獨立的 Admin Port，避免併發時的 Port 衝突
	adminPort := getUniqueAdminPort()
	adminOptions := []admin.SetupOption{
		admin.ListenAddress(fmt.Sprintf("tcp://127.0.0.1:%d", adminPort)),
	}

	zlog.Debug("Initializing Yggdrasil Admin Socket...", zap.Int("port", adminPort))
	adm, err := admin.New(ygg, yggLogger, adminOptions...)
	if err != nil {
		return genErrorResult(originalURI, fmt.Sprintf("Init admin failed: %v", err))
	}
	adm.SetupAdminHandlers()
	defer adm.Stop()

	zlog.Debug("Mounting user-space Netstack...")
	ns, err := netstack.CreateYggdrasilNetstack(ygg)
	if err != nil {
		return genErrorResult(originalURI, fmt.Sprintf("Init netstack failed: %v", err))
	}

	parsedURL, err := url.Parse(testURL)
	if err != nil {
		return genErrorResult(originalURI, "Invalid test URL")
	}

	targetHost := parsedURL.Hostname()
	targetPort := parsedURL.Port()
	if targetPort == "" {
		if parsedURL.Scheme == "https" { targetPort = "443" } else { targetPort = "80" }
	}

	var targetAddr string

	if net.ParseIP(targetHost) == nil {
		var resolver *net.Resolver
		if customDNS != "" {
			dnsAddr := customDNS
			if _, _, err := net.SplitHostPort(dnsAddr); err != nil {
				dnsAddr = net.JoinHostPort(dnsAddr, "53")
			}
			zlog.Debug("Resolving DNS via custom server", zap.String("dns", dnsAddr), zap.String("host", targetHost))
			resolver = &net.Resolver{
				PreferGo: true,
				Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
					dnsHost, _, _ := net.SplitHostPort(dnsAddr)
					ip := net.ParseIP(dnsHost)
					if ip != nil && ip.To4() == nil {
						dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
						defer cancel()
						conn, err := ns.DialContext(dialCtx, network, dnsAddr)
						if err == nil { return conn, nil }
					}
					var d net.Dialer
					return d.DialContext(ctx, network, dnsAddr)
				},
			}
		} else {
			zlog.Debug("Resolving DNS via system default", zap.String("host", targetHost))
			resolver = net.DefaultResolver
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		ips, err := resolver.LookupIPAddr(ctx, targetHost)
		if err != nil {
			return genErrorResult(originalURI, fmt.Sprintf("DNS resolution failed: %v", err))
		}
		if len(ips) == 0 {
			return genErrorResult(originalURI, "No IP records found")
		}

		resolvedIP := ips[0].IP.String()
		for _, ip := range ips {
			if ip.IP.To4() == nil {
				resolvedIP = ip.IP.String()
				break
			}
		}
		zlog.Debug("DNS resolved successfully", zap.String("ip", resolvedIP))

		if customHost == "" { customHost = targetHost }
		if customSNI == "" { customSNI = targetHost }

		parsedURL.Host = net.JoinHostPort(resolvedIP, targetPort)
		testURL = parsedURL.String()
		targetAddr = net.JoinHostPort(resolvedIP, targetPort)
	} else {
		targetAddr = net.JoinHostPort(targetHost, targetPort)
		zlog.Debug("Direct IP provided, skipped DNS", zap.String("ip", targetHost))
	}

	zlog.Debug("Waiting for Yggdrasil route convergence...", zap.String("target_addr", targetAddr))
	if err := waitForRoute(ns, targetAddr, 25*time.Second); err != nil {
		zlog.Warn("Route unreachable", zap.Error(err))
		return genErrorResult(originalURI, fmt.Sprintf("Route unreachable: %v", err))
	}
	zlog.Debug("Yggdrasil Route established!")

	pingMs := fetchPeerPing(adminPort)
	zlog.Debug("Internal Ping fetched", zap.Float64("ping_ms", pingMs))

	zlog.Debug("Starting HTTP download test...", zap.String("url", testURL))
	mbps, err := runDownloadTest(ns, testURL, customHost, customSNI)
	if err != nil {
		zlog.Warn("Download test failed", zap.Error(err))
		return genErrorResult(originalURI, fmt.Sprintf("Speedtest failed: %v", err))
	}
	zlog.Debug("Download test complete", zap.Float64("mbps", mbps))

	h := PingFloat(hsMs)
	p := PingFloat(pingMs)
	m := SpeedFloat(mbps)
	return SpeedResult{
		Peer:         originalURI,
		TestTime:     time.Now().Format(time.RFC3339),
		HandshakeMs:  &h,
		PingMs:       &p,
		DownloadMbps: &m,
	}
}

func genErrorResult(peer, errMsg string) SpeedResult {
	return SpeedResult{
		Peer:     peer,
		TestTime: time.Now().Format(time.RFC3339),
		Error:    errMsg,
	}
}

func readPeersFromFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil { return nil, err }
	defer file.Close()

	var peers []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			peers = append(peers, line)
		}
	}
	return peers, scanner.Err()
}

func fetchPeerPing(adminPort int) float64 {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", adminPort), 2*time.Second)
	if err != nil { return 0 }
	defer conn.Close()

	conn.Write([]byte(`{"request":"getPeers"}`))
	var response map[string]interface{}
	json.NewDecoder(conn).Decode(&response)

	latency := findLatencyField(response)
	if latency > 0 && latency < 5.0 {
		return latency * 1000
	}
	return latency
}

func findLatencyField(v interface{}) float64 {
	switch val := v.(type) {
	case map[string]interface{}:
		for k, v := range val {
			if k == "latency" || k == "ping" {
				if f, ok := v.(float64); ok { return f }
			}
			if f := findLatencyField(v); f > 0 { return f }
		}
	case []interface{}:
		for _, v := range val {
			if f := findLatencyField(v); f > 0 { return f }
		}
	}
	return 0
}

func waitForRoute(ns *netstack.YggdrasilNetstack, addr string, timeout time.Duration) error {
	start := time.Now()
	for time.Since(start) < timeout {
		dialTimeout := 10 * time.Second
		if remaining := timeout - time.Since(start); remaining < dialTimeout {
			dialTimeout = remaining
		}
		ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
		conn, err := ns.DialContext(ctx, "tcp", addr)
		cancel()

		if err == nil && conn != nil {
			conn.Close()
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for route to %s", addr)
}

func runDownloadTest(ns *netstack.YggdrasilNetstack, testURL string, customHost string, customSNI string) (float64, error) {
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	if customSNI != "" { tlsConfig.ServerName = customSNI }

	transport := &http.Transport{
		DialContext:         ns.DialContext,
		DisableKeepAlives:   true,
		TLSClientConfig:     tlsConfig,
		TLSHandshakeTimeout: 15 * time.Second,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second,
	}

	req, err := http.NewRequest("GET", testURL, nil)
	if err != nil { return 0, err }
	req.Header.Set("User-Agent", "YggSpeedTest-Batch/4.3")

	if customHost != "" { req.Host = customHost }

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil { return 0, err }
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP status %d", resp.StatusCode)
	}

	written, err := io.Copy(io.Discard, resp.Body)
	if err != nil { return 0, err }

	duration := time.Since(start).Seconds()
	if duration == 0 { return 0, nil }

	return (float64(written) * 8.0 / 1000000.0) / duration, nil
}

func getUniqueAdminPort() int {
	// 使用 Atomic 遞增確保高併發時絕對不會取得重複的 Port
	return int(atomic.AddInt32(&nextAdminPort, 1))
}

func truncate(text string, maxLen int) string {
	if len(text) > maxLen { return text[:maxLen-3] + "..." }
	return text
}

func outputError(msg string, err error, peer string) {
	if zlog != nil && zlog.Core().Enabled(zap.ErrorLevel) {
		if err != nil {
			zlog.Error("Fatal Error", zap.String("msg", msg), zap.Error(err))
		} else {
			zlog.Error("Fatal Error", zap.String("msg", msg))
		}
	}

	errMsg := msg
	if err != nil { errMsg = fmt.Sprintf("%s: %v", msg, err) }

	if globalOutJSON == "" {
		res := SpeedResult{
			Peer:     peer,
			TestTime: time.Now().Format(time.RFC3339),
			Error:    errMsg,
		}
		out, _ := json.MarshalIndent([]SpeedResult{res}, "", "  ")
		fmt.Println(string(out))
	}
	os.Exit(1)
}