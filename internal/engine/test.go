package engine

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/gologme/log"
	"github.com/quic-go/quic-go"
	"github.com/yggdrasil-network/yggdrasil-go/src/admin"
	"github.com/yggdrasil-network/yggdrasil-go/src/config"
	"github.com/yggdrasil-network/yggdrasil-go/src/core"
	"go.uber.org/zap"
	"yggspeedtest/netstack"
)

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
		req, err := http.NewRequestWithContext(hsCtx, "GET", reqURL, nil)
		if err != nil {
			return 0, err
		}
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Sec-WebSocket-Protocol", "ygg-ws")
		// RFC 6455 requires both headers. Servers that enforce the RFC refuse
		// the upgrade without them, so ws/wss peers were reported as failing
		// the pre-flight even when reachable.
		wsKey, err := generateWebSocketKey()
		if err != nil {
			return 0, err
		}
		req.Header.Set("Sec-WebSocket-Version", "13")
		req.Header.Set("Sec-WebSocket-Key", wsKey)

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
		// A write to a UDP socket proves nothing about the peer, so this is a
		// reachability probe with a read afterwards, not a handshake: no number
		// printed here can be called a handshake time.
		return kcpReachabilityProbe(hsCtx, address)

	default:
		return 0, fmt.Errorf("unsupported protocol for handshake: %s", u.Scheme)
	}

	// Sub-millisecond precision: the result is a float printed with two
	// decimals, so truncating to whole ms threw away real resolution.
	return time.Since(start).Seconds() * 1000, nil
}

// generateWebSocketKey returns a valid Sec-WebSocket-Key per RFC 6455, which
// is the base64 encoding of 16 random bytes.
func generateWebSocketKey() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate websocket key: %w", err)
	}
	return base64.StdEncoding.EncodeToString(b[:]), nil
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
		admin.ListenAddress(adminListenAddr(adminPort)),
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

	if err := waitForRoute(ctx, adminPort, targetAddr, finalPeerURI, routeTimeout); err != nil {
		return genErrorResult(originalURI, fmt.Sprintf("Route unreachable: %v", err))
	}

	pingMs, pingOK := fetchPeerPing(finalPeerURI, adminPort)
	if !pingOK {
		// A missing latency used to be silently reported as 0.00 ms.
		zlog.Debug("No peer latency available from the admin socket",
			zap.String("peer", finalPeerURI), zap.Int("admin_port", adminPort))
	}

	avgMbps, peakMbps, bytesDownloaded, durationSec, err := runDownloadTestMultiStream(
		ctx, ns, testURL, customHost, customSNI, streams, maxDuration, maxBytes,
	)
	if err != nil {
		return genErrorResult(originalURI, fmt.Sprintf("Speedtest failed: %v", err))
	}

	h := PingFloat(hsMs)
	m := SpeedFloat(avgMbps)
	pk := SpeedFloat(peakMbps)

	res := SpeedResult{
		Peer:            originalURI,
		TestTime:        time.Now().Format(time.RFC3339),
		HandshakeMs:     &h,
		DownloadMbps:    &m,
		PeakMbps:        &pk,
		BytesDownloaded: bytesDownloaded,
		DurationSec:     durationSec,
	}
	if pingOK {
		p := PingFloat(pingMs)
		res.PingMs = &p
	}
	return res
}
