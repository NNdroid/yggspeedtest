package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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

var Version = "v1.0.20260801"
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
	Peer            string      `json:"peer"`
	TestTime        string      `json:"test_time"`
	HandshakeMs     *PingFloat  `json:"handshake_ms,omitempty"`
	PingMs          *PingFloat  `json:"ping_ms,omitempty"`
	DownloadMbps    *SpeedFloat `json:"download_mbps,omitempty"`
	PeakMbps        *SpeedFloat `json:"peak_mbps,omitempty"`
	BytesDownloaded int64       `json:"bytes_downloaded,omitempty"`
	DurationSec     float64     `json:"duration_sec,omitempty"`
	Error           string      `json:"error,omitempty"`
}

func init() {
	nextAdminPort = int32(20000 + (time.Now().UnixNano() % 10000))
}

func initZapLogger(debug bool, quiet bool) {
	var zapConfig zap.Config
	if debug {
		zapConfig = zap.NewDevelopmentConfig()
		zapConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		zapConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	} else if quiet {
		zapConfig = zap.NewProductionConfig()
		zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		zapConfig.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
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
	usePublicPeers := flag.Bool("public", false, "Automatically fetch active public peers list from public repository")
	outJSON := flag.String("out", "", "Output sorted JSON results to this file path")
	outCSV := flag.String("out-csv", "", "Output CSV results to this file path")
	outMD := flag.String("out-md", "", "Output Markdown table results to this file path")

	concurrency := flag.Int("c", 1, "Number of concurrent peer tests (if > 1, -key flag is ignored)")
	streams := flag.Int("streams", 1, "Number of parallel HTTP download streams per peer test")

	maxDurationStr := flag.String("max-duration", "10s", "Max download test duration per peer (e.g. 5s, 10s, 30s)")
	maxBytesStr := flag.String("max-bytes", "", "Max bytes downloaded per peer (e.g. 10MB, 50MB, 100MB)")

	timeoutStr := flag.String("timeout", "5s", "Pre-flight underlay handshake timeout")
	routeTimeoutStr := flag.String("route-timeout", "15s", "Yggdrasil route convergence timeout")

	sortBy := flag.String("sort", "speed", "Sort results by: speed, peak, ping, handshake")
	minSpeed := flag.Float64("min-speed", 0, "Filter out results with download speed below this Mbps threshold")
	maxPing := flag.Float64("max-ping", 0, "Filter out results with ping above this ms threshold")

	testURL := flag.String("url", "", "URL to test download speed")
	customHost := flag.String("host", "", "Custom HTTP Host header")
	customSNI := flag.String("sni", "", "Custom TLS SNI")
	customDNS := flag.String("dns", "", "Custom DNS server (e.g., 127.0.0.1:53)")
	keyFile := flag.String("key", "", "Path to private key file (optional, only effective if -c=1)")
	debugLog := flag.Bool("debug", false, "Enable detailed debug logs")
	quietLog := flag.Bool("quiet", false, "Suppress non-error logs")
	flag.Parse()

	globalOutJSON = *outJSON
	initZapLogger(*debugLog, *quietLog)
	defer zlog.Sync()

	zlog.Info("Starting YggSpeedTest Engine", zap.String("version", Version))

	if *testURL == "" {
		outputError("Missing -url argument", nil, "")
	}

	maxDuration, err := time.ParseDuration(*maxDurationStr)
	if err != nil {
		outputError("Invalid -max-duration format", err, "")
	}

	timeout, err := time.ParseDuration(*timeoutStr)
	if err != nil {
		outputError("Invalid -timeout format", err, "")
	}

	routeTimeout, err := time.ParseDuration(*routeTimeoutStr)
	if err != nil {
		outputError("Invalid -route-timeout format", err, "")
	}

	maxBytes, err := parseByteSize(*maxBytesStr)
	if err != nil {
		outputError("Invalid -max-bytes format", err, "")
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

	if *usePublicPeers || len(peers) == 0 {
		if *usePublicPeers || len(peers) == 0 {
			zlog.Info("Fetching public Yggdrasil peers from online repository...")
			pubPeers, err := fetchPublicPeers()
			if err != nil && len(peers) == 0 {
				outputError("Failed to fetch public peers and no -peer/-file provided", err, "")
			} else if err == nil {
				peers = append(peers, pubPeers...)
				zlog.Info("Successfully fetched public peers", zap.Int("count", len(pubPeers)))
			}
		}
	}

	// Deduplicate peers
	peers = deduplicatePeers(peers)

	if len(peers) == 0 {
		outputError("No valid peers specified or found.", nil, "")
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

	zlog.Info(fmt.Sprintf("Ready to test %d peers with concurrency=%d, streams=%d, maxDuration=%s", len(peers), *concurrency, *streams, maxDuration))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var results []SpeedResult
	var mu sync.Mutex
	var wg sync.WaitGroup

	sem := make(chan struct{}, *concurrency)
	completedCount := int32(0)
	totalPeers := len(peers)

	for i, p := range peers {
		select {
		case <-ctx.Done():
			zlog.Warn("Interrupt signal received, cancelling remaining scheduled peer tests...")
			break
		default:
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(index int, peer string) {
			defer wg.Done()
			defer func() { <-sem }()

			curr := atomic.AddInt32(&completedCount, 1)
			zlog.Info(fmt.Sprintf("[%d/%d] Testing peer...", curr, totalPeers), zap.String("peer", peer))

			cfg := globalCfg
			if *concurrency > 1 {
				cfg = config.GenerateConfig()
			}

			res := runPeerTest(ctx, peer, cfg, *testURL, *customHost, *customSNI, *customDNS, *streams, timeout, routeTimeout, maxDuration, maxBytes, *debugLog)

			mu.Lock()
			results = append(results, res)
			mu.Unlock()

			if res.Error != "" {
				zlog.Debug("Peer test failed", zap.String("peer", peer), zap.String("error", res.Error))
			} else {
				zlog.Debug("Peer test complete", zap.String("peer", peer),
					zap.Float64("avg_mbps", float64(*res.DownloadMbps)),
					zap.Float64("peak_mbps", float64(*res.PeakMbps)))
			}
		}(i, p)
	}

	wg.Wait()

	// Apply filtering if requested
	if *minSpeed > 0 || *maxPing > 0 {
		var filtered []SpeedResult
		for _, r := range results {
			if r.Error != "" {
				filtered = append(filtered, r)
				continue
			}
			if *minSpeed > 0 && r.DownloadMbps != nil && float64(*r.DownloadMbps) < *minSpeed {
				continue
			}
			if *maxPing > 0 && r.PingMs != nil && float64(*r.PingMs) > *maxPing {
				continue
			}
			filtered = append(filtered, r)
		}
		results = filtered
	}

	// Sort results according to flag
	sortResults(results, *sortBy)

	// Output formats
	if *outJSON != "" {
		outData, _ := json.MarshalIndent(results, "", "  ")
		if err := os.WriteFile(*outJSON, outData, 0644); err != nil {
			zlog.Error("Failed to write JSON output", zap.Error(err))
		} else {
			zlog.Info("Results successfully saved to JSON file", zap.String("file", *outJSON))
		}
	}

	if *outCSV != "" {
		if err := exportCSV(*outCSV, results); err != nil {
			zlog.Error("Failed to write CSV output", zap.Error(err))
		} else {
			zlog.Info("Results successfully saved to CSV file", zap.String("file", *outCSV))
		}
	}

	if *outMD != "" {
		if err := exportMarkdown(*outMD, results); err != nil {
			zlog.Error("Failed to write Markdown output", zap.Error(err))
		} else {
			zlog.Info("Results successfully saved to Markdown file", zap.String("file", *outMD))
		}
	}

	if *outJSON == "" {
		printConsoleTable(results)
	}
}

func measureHandshake(ctx context.Context, peerURI string, customSNI string, timeout time.Duration) (float64, error) {
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

	start := time.Now()
	hsCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	switch u.Scheme {
	case "tcp":
		var d net.Dialer
		conn, err := d.DialContext(hsCtx, "tcp", address)
		if err != nil {
			return 0, err
		}
		conn.Close()

	case "tls":
		var d net.Dialer
		conn, err := tls.DialWithDialer(&d, "tcp", address, &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         sni,
			NextProtos:         nil,
		})
		if err != nil {
			return 0, err
		}
		conn.Close()

	case "ws", "wss":
		scheme := "http"
		if u.Scheme == "wss" {
			scheme = "https"
		}
		reqURL := fmt.Sprintf("%s://%s/", scheme, address)
		req, _ := http.NewRequestWithContext(hsCtx, "GET", reqURL, nil)
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Sec-WebSocket-Protocol", "ygg-ws")

		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				ServerName:         sni,
				NextProtos:         []string{"http/1.1"},
			},
			DialContext: (&net.Dialer{}).DialContext,
		}
		client := &http.Client{Transport: tr}
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusSwitchingProtocols {
			return 0, fmt.Errorf("WS upgrade failed: expected 101, got %d %s", resp.StatusCode, resp.Status)
		}

	case "quic":
		tlsConf := &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         sni,
			NextProtos:         []string{"yggdrasil"},
		}
		conn, err := quic.DialAddr(hsCtx, address, tlsConf, nil)
		if err != nil {
			return 0, err
		}
		conn.CloseWithError(0, "")

	case "kcp":
		var d net.Dialer
		conn, err := d.DialContext(hsCtx, "udp", address)
		if err != nil {
			return 0, err
		}
		conn.Write([]byte{0})
		conn.Close()

	default:
		return 0, fmt.Errorf("unsupported protocol for handshake: %s", u.Scheme)
	}

	return float64(time.Since(start).Milliseconds()), nil
}

func runPeerTest(
	ctx context.Context,
	originalURI string,
	cfg *config.NodeConfig,
	testURL, customHost, customSNI, customDNS string,
	streams int,
	timeout, routeTimeout, maxDuration time.Duration,
	maxBytes int64,
	debugLog bool,
) SpeedResult {
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

	hsMs, hsErr := measureHandshake(ctx, finalPeerURI, customSNI, timeout)
	if hsErr != nil {
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

	ygg, err := core.New(cfg.Certificate, yggLogger, coreOptions...)
	if err != nil {
		return genErrorResult(originalURI, fmt.Sprintf("Init core failed: %v", err))
	}
	defer ygg.Stop()

	adminPort := getUniqueAdminPort()
	adminOptions := []admin.SetupOption{
		admin.ListenAddress(fmt.Sprintf("tcp://127.0.0.1:%d", adminPort)),
	}

	adm, err := admin.New(ygg, yggLogger, adminOptions...)
	if err != nil {
		return genErrorResult(originalURI, fmt.Sprintf("Init admin failed: %v", err))
	}
	adm.SetupAdminHandlers()
	defer adm.Stop()

	ns, err := netstack.CreateYggdrasilNetstack(ygg)
	if err != nil {
		return genErrorResult(originalURI, fmt.Sprintf("Init netstack failed: %v", err))
	}
	defer ns.Close()

	parsedURL, err := url.Parse(testURL)
	if err != nil {
		return genErrorResult(originalURI, "Invalid test URL")
	}

	targetHost := parsedURL.Hostname()
	targetPort := parsedURL.Port()
	if targetPort == "" {
		if parsedURL.Scheme == "https" {
			targetPort = "443"
		} else {
			targetPort = "80"
		}
	}

	var targetAddr string
	if net.ParseIP(targetHost) == nil {
		var resolver *net.Resolver
		if customDNS != "" {
			dnsAddr := customDNS
			if _, _, err := net.SplitHostPort(dnsAddr); err != nil {
				dnsAddr = net.JoinHostPort(dnsAddr, "53")
			}
			resolver = &net.Resolver{
				PreferGo: true,
				Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
					dnsHost, _, _ := net.SplitHostPort(dnsAddr)
					ip := net.ParseIP(dnsHost)
					if ip != nil && ip.To4() == nil {
						dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
						defer cancel()
						conn, err := ns.DialContext(dialCtx, network, dnsAddr)
						if err == nil {
							return conn, nil
						}
					}
					var d net.Dialer
					return d.DialContext(ctx, network, dnsAddr)
				},
			}
		} else {
			resolver = net.DefaultResolver
		}

		dnsCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()

		ips, err := resolver.LookupIPAddr(dnsCtx, targetHost)
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

		if customHost == "" {
			customHost = targetHost
		}
		if customSNI == "" {
			customSNI = targetHost
		}

		parsedURL.Host = net.JoinHostPort(resolvedIP, targetPort)
		testURL = parsedURL.String()
		targetAddr = net.JoinHostPort(resolvedIP, targetPort)
	} else {
		targetAddr = net.JoinHostPort(targetHost, targetPort)
	}

	if err := waitForRoute(ctx, ns, targetAddr, routeTimeout); err != nil {
		return genErrorResult(originalURI, fmt.Sprintf("Route unreachable: %v", err))
	}

	pingMs := fetchPeerPing(adminPort)

	avgMbps, peakMbps, bytesDownloaded, durationSec, err := runDownloadTestMultiStream(
		ctx, ns, testURL, customHost, customSNI, streams, maxDuration, maxBytes,
	)
	if err != nil {
		return genErrorResult(originalURI, fmt.Sprintf("Speedtest failed: %v", err))
	}

	h := PingFloat(hsMs)
	p := PingFloat(pingMs)
	m := SpeedFloat(avgMbps)
	pk := SpeedFloat(peakMbps)

	return SpeedResult{
		Peer:            originalURI,
		TestTime:        time.Now().Format(time.RFC3339),
		HandshakeMs:     &h,
		PingMs:          &p,
		DownloadMbps:    &m,
		PeakMbps:        &pk,
		BytesDownloaded: bytesDownloaded,
		DurationSec:     durationSec,
	}
}

func runDownloadTestMultiStream(
	ctx context.Context,
	ns *netstack.YggdrasilNetstack,
	testURL string,
	customHost string,
	customSNI string,
	streams int,
	maxDuration time.Duration,
	maxBytes int64,
) (avgMbps float64, peakMbps float64, totalBytes int64, durationSec float64, err error) {
	if streams < 1 {
		streams = 1
	}

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	if customSNI != "" {
		tlsConfig.ServerName = customSNI
	}

	transport := &http.Transport{
		DialContext:         ns.DialContext,
		DisableKeepAlives:   true,
		TLSClientConfig:     tlsConfig,
		TLSHandshakeTimeout: 15 * time.Second,
	}

	client := &http.Client{
		Transport: transport,
	}

	downloadCtx, cancel := context.WithTimeout(ctx, maxDuration)
	defer cancel()

	var downloadedAtomic int64
	var peakBits uint64

	start := time.Now()

	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		var lastBytes int64
		lastTime := start

		for {
			select {
			case <-downloadCtx.Done():
				return
			case t := <-ticker.C:
				currentBytes := atomic.LoadInt64(&downloadedAtomic)
				bytesInInterval := currentBytes - lastBytes
				intervalSec := t.Sub(lastTime).Seconds()
				if intervalSec > 0 && bytesInInterval > 0 {
					currentMbps := (float64(bytesInInterval) * 8.0 / 1e6) / intervalSec
					for {
						oldBits := atomic.LoadUint64(&peakBits)
						oldVal := math.Float64frombits(oldBits)
						if currentMbps <= oldVal {
							break
						}
						newBits := math.Float64bits(currentMbps)
						if atomic.CompareAndSwapUint64(&peakBits, oldBits, newBits) {
							break
						}
					}
				}
				lastBytes = currentBytes
				lastTime = t
			}
		}
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, streams)

	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			req, err := http.NewRequestWithContext(downloadCtx, "GET", testURL, nil)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			req.Header.Set("User-Agent", "YggSpeedTest-Batch/5.0")
			if customHost != "" {
				req.Host = customHost
			}

			resp, err := client.Do(req)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
				select {
				case errCh <- fmt.Errorf("HTTP status %d", resp.StatusCode):
				default:
				}
				return
			}

			buf := make([]byte, 32*1024)
			for {
				select {
				case <-downloadCtx.Done():
					return
				default:
				}

				if maxBytes > 0 && atomic.LoadInt64(&downloadedAtomic) >= maxBytes {
					cancel()
					return
				}

				n, rErr := resp.Body.Read(buf)
				if n > 0 {
					currTotal := atomic.AddInt64(&downloadedAtomic, int64(n))
					if maxBytes > 0 && currTotal >= maxBytes {
						cancel()
						return
					}
				}
				if rErr != nil {
					if rErr != io.EOF && !errors.Is(rErr, context.Canceled) && !errors.Is(rErr, context.DeadlineExceeded) {
						select {
						case errCh <- rErr:
						default:
						}
					}
					return
				}
			}
		}()
	}

	wg.Wait()
	cancel()
	<-sampleDone

	totalBytes = atomic.LoadInt64(&downloadedAtomic)
	durationSec = time.Since(start).Seconds()

	if totalBytes == 0 {
		select {
		case err = <-errCh:
			return 0, 0, 0, durationSec, err
		default:
			return 0, 0, 0, durationSec, fmt.Errorf("no data downloaded")
		}
	}

	if durationSec > 0 {
		avgMbps = (float64(totalBytes) * 8.0 / 1e6) / durationSec
	}

	peakMbps = math.Float64frombits(atomic.LoadUint64(&peakBits))
	if peakMbps < avgMbps {
		peakMbps = avgMbps
	}

	return avgMbps, peakMbps, totalBytes, durationSec, nil
}

func fetchPublicPeers() ([]string, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	urls := []string{
		"https://raw.githubusercontent.com/yggdrasil-network/public-peers/master/other/peers.json",
		"https://publicnodes.yggdrasil.link/peers.json",
	}

	var peers []string
	peerSet := make(map[string]bool)

	for _, u := range urls {
		resp, err := client.Get(u)
		if err != nil {
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}

		var jsonPeers []string
		if err := json.Unmarshal(body, &jsonPeers); err == nil {
			for _, p := range jsonPeers {
				p = strings.TrimSpace(p)
				if p != "" && !peerSet[p] {
					peerSet[p] = true
					peers = append(peers, p)
				}
			}
			if len(peers) > 0 {
				return peers, nil
			}
		}

		re := regexp.MustCompile(`(tcp|tls|quic|ws|wss)://[^\s"'<>]+`)
		matches := re.FindAllString(string(body), -1)
		for _, m := range matches {
			m = strings.TrimSpace(m)
			if m != "" && !peerSet[m] {
				peerSet[m] = true
				peers = append(peers, m)
			}
		}
		if len(peers) > 0 {
			return peers, nil
		}
	}

	// Fallback standard public peer list if network request fails
	fallbackPeers := []string{
		"tcp://51.15.204.214:12345",
		"tls://ygg.mkg20001.io:443",
		"tls://ygg.tomasz-kuzemko.pl:443",
		"tcp://193.107.20.123:64319",
	}

	return fallbackPeers, nil
}

func deduplicatePeers(input []string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, p := range input {
		p = strings.TrimSpace(p)
		if p != "" && !seen[p] {
			seen[p] = true
			result = append(result, p)
		}
	}
	return result
}

func sortResults(results []SpeedResult, sortBy string) {
	sort.Slice(results, func(i, j int) bool {
		// Place errored items at the end
		if results[i].Error != "" && results[j].Error == "" {
			return false
		}
		if results[i].Error == "" && results[j].Error != "" {
			return true
		}
		if results[i].Error != "" && results[j].Error != "" {
			return results[i].Peer < results[j].Peer
		}

		switch strings.ToLower(sortBy) {
		case "ping":
			var p1, p2 float64 = 999999, 999999
			if results[i].PingMs != nil {
				p1 = float64(*results[i].PingMs)
			}
			if results[j].PingMs != nil {
				p2 = float64(*results[j].PingMs)
			}
			return p1 < p2

		case "handshake":
			var h1, h2 float64 = 999999, 999999
			if results[i].HandshakeMs != nil {
				h1 = float64(*results[i].HandshakeMs)
			}
			if results[j].HandshakeMs != nil {
				h2 = float64(*results[j].HandshakeMs)
			}
			return h1 < h2

		case "peak":
			var pk1, pk2 float64 = 0, 0
			if results[i].PeakMbps != nil {
				pk1 = float64(*results[i].PeakMbps)
			}
			if results[j].PeakMbps != nil {
				pk2 = float64(*results[j].PeakMbps)
			}
			return pk1 > pk2

		case "speed":
			fallthrough
		default:
			var m1, m2 float64 = 0, 0
			if results[i].DownloadMbps != nil {
				m1 = float64(*results[i].DownloadMbps)
			}
			if results[j].DownloadMbps != nil {
				m2 = float64(*results[j].DownloadMbps)
			}
			return m1 > m2
		}
	})
}

func exportCSV(filePath string, results []SpeedResult) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	_ = writer.Write([]string{"Rank", "Peer", "TestTime", "HandshakeMs", "PingMs", "AvgMbps", "PeakMbps", "DownloadedMB", "DurationSec", "Status"})

	for i, r := range results {
		rank := strconv.Itoa(i + 1)
		hsStr, pingStr, avgStr, peakStr, mbStr, durStr := "-", "-", "-", "-", "-", "-"
		status := "OK"

		if r.HandshakeMs != nil {
			hsStr = fmt.Sprintf("%.2f", float64(*r.HandshakeMs))
		}
		if r.PingMs != nil {
			pingStr = fmt.Sprintf("%.2f", float64(*r.PingMs))
		}
		if r.DownloadMbps != nil {
			avgStr = fmt.Sprintf("%.3f", float64(*r.DownloadMbps))
		}
		if r.PeakMbps != nil {
			peakStr = fmt.Sprintf("%.3f", float64(*r.PeakMbps))
		}
		if r.BytesDownloaded > 0 {
			mbStr = fmt.Sprintf("%.2f", float64(r.BytesDownloaded)/1024/1024)
		}
		if r.DurationSec > 0 {
			durStr = fmt.Sprintf("%.2f", r.DurationSec)
		}
		if r.Error != "" {
			status = "FAIL: " + r.Error
		}

		_ = writer.Write([]string{rank, r.Peer, r.TestTime, hsStr, pingStr, avgStr, peakStr, mbStr, durStr, status})
	}
	return nil
}

func exportMarkdown(filePath string, results []SpeedResult) error {
	var sb strings.Builder
	sb.WriteString("# YggSpeedTest 测速报告\n\n")
	sb.WriteString(fmt.Sprintf("测试时间: %s\n\n", time.Now().Format("2006-01-02 15:04:05")))
	sb.WriteString("| 序号 | Peer 地址 | 平均速率 (Mbps) | 峰值速率 (Mbps) | 握手 (ms) | Ping (ms) | 下载数据 | 状态 |\n")
	sb.WriteString("|---|---|---|---|---|---|---|---|\n")

	for i, r := range results {
		hsStr, pingStr, avgStr, peakStr, mbStr := "-", "-", "-", "-", "-"
		status := "✅ 成功"

		if r.HandshakeMs != nil {
			hsStr = fmt.Sprintf("%.2f ms", float64(*r.HandshakeMs))
		}
		if r.PingMs != nil {
			pingStr = fmt.Sprintf("%.2f ms", float64(*r.PingMs))
		}
		if r.DownloadMbps != nil {
			avgStr = fmt.Sprintf("%.3f Mbps", float64(*r.DownloadMbps))
		}
		if r.PeakMbps != nil {
			peakStr = fmt.Sprintf("%.3f Mbps", float64(*r.PeakMbps))
		}
		if r.BytesDownloaded > 0 {
			mbStr = fmt.Sprintf("%.2f MB", float64(r.BytesDownloaded)/1024/1024)
		}
		if r.Error != "" {
			status = fmt.Sprintf("❌ 失败 (%s)", r.Error)
		}

		sb.WriteString(fmt.Sprintf("| %d | `%s` | %s | %s | %s | %s | %s | %s |\n",
			i+1, r.Peer, avgStr, peakStr, hsStr, pingStr, mbStr, status))
	}

	return os.WriteFile(filePath, []byte(sb.String()), 0644)
}

func printConsoleTable(results []SpeedResult) {
	fmt.Println("\n=========================================================================================================")
	fmt.Printf("                                      YggSpeedTest 测速排行榜 (%s)\n", Version)
	fmt.Println("=========================================================================================================")
	fmt.Printf(" %-4s | %-15s | %-15s | %-12s | %-12s | %-10s | %-s\n",
		"序号", "平均速率(Mbps)", "峰值速率(Mbps)", "握手延迟(ms)", "路由Ping(ms)", "已下载", "Peer 地址")
	fmt.Println("------+-----------------+-----------------+--------------+--------------+------------+-------------------")

	for i, r := range results {
		if r.Error != "" {
			fmt.Printf(" %4d | %-15s | %-15s | %-12s | %-12s | %-10s | %s (FAIL: %s)\n",
				i+1, "0.000 Mbps", "0.000 Mbps", "-", "-", "-", r.Peer, truncate(r.Error, 25))
		} else {
			hsStr := fmt.Sprintf("%.2f ms", float64(*r.HandshakeMs))
			pingStr := fmt.Sprintf("%.2f ms", float64(*r.PingMs))
			avgStr := fmt.Sprintf("%.3f Mbps", float64(*r.DownloadMbps))
			peakStr := fmt.Sprintf("%.3f Mbps", float64(*r.PeakMbps))
			mbStr := fmt.Sprintf("%.2f MB", float64(r.BytesDownloaded)/1024/1024)

			fmt.Printf(" %4d | %15s | %15s | %12s | %12s | %10s | %s\n",
				i+1, avgStr, peakStr, hsStr, pingStr, mbStr, r.Peer)
		}
	}
	fmt.Println("=========================================================================================================")
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
	if err != nil {
		return nil, err
	}
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
	if err != nil {
		return 0
	}
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
				if f, ok := v.(float64); ok {
					return f
				}
			}
			if f := findLatencyField(v); f > 0 {
				return f
			}
		}
	case []interface{}:
		for _, v := range val {
			if f := findLatencyField(v); f > 0 {
				return f
			}
		}
	}
	return 0
}

func waitForRoute(ctx context.Context, ns *netstack.YggdrasilNetstack, addr string, timeout time.Duration) error {
	start := time.Now()
	for time.Since(start) < timeout {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		dialTimeout := 5 * time.Second
		if remaining := timeout - time.Since(start); remaining < dialTimeout {
			dialTimeout = remaining
		}
		dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
		conn, err := ns.DialContext(dialCtx, "tcp", addr)
		cancel()

		if err == nil && conn != nil {
			conn.Close()
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for route to %s", addr)
}

func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" || s == "0" {
		return 0, nil
	}
	multiplier := int64(1)
	if strings.HasSuffix(s, "KB") || strings.HasSuffix(s, "K") {
		multiplier = 1024
		s = strings.TrimRight(s, "KKB")
	} else if strings.HasSuffix(s, "MB") || strings.HasSuffix(s, "M") {
		multiplier = 1024 * 1024
		s = strings.TrimRight(s, "MMB")
	} else if strings.HasSuffix(s, "GB") || strings.HasSuffix(s, "G") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimRight(s, "GGB")
	}
	val, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, err
	}
	return val * multiplier, nil
}

func getUniqueAdminPort() int {
	return int(atomic.AddInt32(&nextAdminPort, 1))
}

func truncate(text string, maxLen int) string {
	if len(text) > maxLen {
		return text[:maxLen-3] + "..."
	}
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
	if err != nil {
		errMsg = fmt.Sprintf("%s: %v", msg, err)
	}

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
