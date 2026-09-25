package engine

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gologme/log"
	"github.com/quic-go/quic-go"
	"github.com/yggdrasil-network/yggdrasil-go/src/admin"
	"github.com/yggdrasil-network/yggdrasil-go/src/config"
	"github.com/yggdrasil-network/yggdrasil-go/src/core"
	"go.uber.org/zap"
	"yggspeedtest/netstack"
)

// measureHandshake probes a peer's underlay reachability the way yggdrasil's
// own link layer would dial it, and reports how long the probe took. ok is
// false when the number cannot honestly be called a handshake time (the kcp
// reachability probe), so callers leave the field out instead of printing a
// made-up measurement.
func measureHandshake(ctx context.Context, peerURI string, customSNI string, timeout time.Duration) (ms float64, ok bool, err error) {
	u, err := url.Parse(peerURI)
	if err != nil {
		return 0, false, err
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
			return 0, false, err
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
			return 0, false, err
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
			return 0, false, err
		}
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Sec-WebSocket-Protocol", "ygg-ws")
		// RFC 6455 requires both headers. Servers that enforce the RFC refuse
		// the upgrade without them, so ws/wss peers were reported as failing
		// the pre-flight even when reachable.
		wsKey, err := generateWebSocketKey()
		if err != nil {
			return 0, false, err
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
			return 0, false, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusSwitchingProtocols {
			return 0, false, fmt.Errorf("WS upgrade failed: expected 101, got %d %s", resp.StatusCode, resp.Status)
		}

	case "quic":
		tlsConf := &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         sni,
			NextProtos:         []string{"yggdrasil"},
		}
		conn, err := quic.DialAddr(hsCtx, address, tlsConf, nil)
		if err != nil {
			return 0, false, err
		}
		conn.CloseWithError(0, "")

	case "socks", "sockstls":
		// Same path yggdrasil's socks link dials: SOCKS5 CONNECT through the
		// proxy to the peer, then TLS for sockstls.
		if err := socksHandshake(hsCtx, u, sni); err != nil {
			return 0, false, err
		}

	case "kcp":
		// A write to a UDP socket proves nothing about the peer, so this is a
		// reachability probe with a read afterwards, not a handshake: no number
		// printed here can be called a handshake time.
		if err := kcpReachabilityProbe(hsCtx, address); err != nil {
			return 0, false, err
		}
		return 0, false, nil

	case "unix":
		// yggdrasil dials url.Path as a unix socket (core/link_unix.go).
		if u.Path == "" {
			return 0, false, fmt.Errorf("unix URI needs the socket path: unix:///path/to/socket")
		}
		var d net.Dialer
		conn, err := d.DialContext(hsCtx, "unix", u.Path)
		if err != nil {
			return 0, false, err
		}
		conn.Close()

	default:
		return 0, false, fmt.Errorf("unsupported protocol for handshake: %s", u.Scheme)
	}

	// Sub-millisecond precision: the result is a float printed with two
	// decimals, so truncating to whole ms threw away real resolution.
	return time.Since(start).Seconds() * 1000, true, nil
}

// socksHandshake performs the SOCKS5 greeting, optional username/password
// authentication and CONNECT that yggdrasil's socks link performs, plus the TLS
// layer for sockstls. The URI shape matches core/link_socks.go:
// socks://[user:pass@]proxyhost:proxyport/peerhost:peerport
func socksHandshake(ctx context.Context, u *url.URL, sni string) error {
	if u.Port() == "" {
		return fmt.Errorf("socks URI %q needs an explicit proxy port", u.Host)
	}
	target := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(target) == 0 || target[0] == "" {
		return fmt.Errorf("socks URI needs the peer address in its path: socks://proxy:port/peer:port")
	}
	peerAddr := target[0]

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", u.Host)
	if err != nil {
		return fmt.Errorf("dial socks proxy: %w", err)
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return err
		}
	}

	if err := socks5Connect(conn, peerAddr, u.User); err != nil {
		return err
	}

	if u.Scheme == "sockstls" {
		// yggdrasil sets the SNI to the proxy hostname unless ?sni= says
		// otherwise; measureHandshake resolved sni the same way.
		tlsConn := tls.Client(conn, &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         sni,
			NextProtos:         []string{"http/1.1"},
			MinVersion:         tls.VersionTLS12,
		})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			return fmt.Errorf("sockstls handshake: %w", err)
		}
	}
	return nil
}

// socks5Connect speaks just enough of RFC 1928 for a peering pre-flight:
// method negotiation, username/password subnegotiation when the URI carries
// credentials, and CONNECT to target.
func socks5Connect(conn net.Conn, target string, auth *url.Userinfo) error {
	methods := []byte{0x00} // no authentication
	user, pass := "", ""
	if auth != nil && auth.Username() != "" {
		methods = []byte{0x00, 0x02}
		user = auth.Username()
		pass, _ = auth.Password()
	}
	greeting := append([]byte{0x05, byte(len(methods))}, methods...)
	if _, err := conn.Write(greeting); err != nil {
		return err
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return err
	}
	if reply[0] != 0x05 {
		return fmt.Errorf("unexpected SOCKS version %d", reply[0])
	}
	switch reply[1] {
	case 0x00:
	case 0x02:
		if user == "" {
			return errors.New("socks proxy requires authentication; use socks://user:pass@proxy:port/peer:port")
		}
		req := append([]byte{0x01, byte(len(user))}, user...)
		req = append(req, byte(len(pass)))
		req = append(req, pass...)
		if _, err := conn.Write(req); err != nil {
			return err
		}
		rep := make([]byte, 2)
		if _, err := io.ReadFull(conn, rep); err != nil {
			return err
		}
		if rep[1] != 0x00 {
			return errors.New("socks authentication failed")
		}
	default:
		return fmt.Errorf("socks proxy rejected the offered methods (0x%02x)", reply[1])
	}

	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return fmt.Errorf("socks peer address %q: %w", target, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid port in socks peer address %q", target)
	}

	req := []byte{0x05, 0x01, 0x00}
	if ip := net.ParseIP(host); ip != nil {
		if ip4 := ip.To4(); ip4 != nil {
			req = append(req, 0x01)
			req = append(req, ip4...)
		} else {
			req = append(req, 0x04)
			req = append(req, ip.To16()...)
		}
	} else {
		req = append(req, 0x03, byte(len(host)))
		req = append(req, host...)
	}
	req = append(req, byte(port>>8), byte(port))
	if _, err := conn.Write(req); err != nil {
		return err
	}

	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return err
	}
	if head[0] != 0x05 {
		return fmt.Errorf("unexpected SOCKS version %d", head[0])
	}
	if head[1] != 0x00 {
		return fmt.Errorf("socks CONNECT failed (code 0x%02x)", head[1])
	}
	switch head[3] {
	case 0x01:
		if _, err := io.CopyN(io.Discard, conn, 4+2); err != nil {
			return err
		}
	case 0x04:
		if _, err := io.CopyN(io.Discard, conn, 16+2); err != nil {
			return err
		}
	case 0x03:
		lenByte := make([]byte, 1)
		if _, err := io.ReadFull(conn, lenByte); err != nil {
			return err
		}
		if _, err := io.CopyN(io.Discard, conn, int64(lenByte[0])+2); err != nil {
			return err
		}
	default:
		return fmt.Errorf("socks proxy replied with unknown address type 0x%02x", head[3])
	}
	return nil
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

	hsMs, hsOK, hsErr := measureHandshake(ctx, finalPeerURI, customSNI, timeout)
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

	// The gVisor netstack only routes 200::/7, so a target outside the
	// Yggdrasil range can never be downloaded. Failing here names the real
	// cause; without it every peer burned its route-convergence window and
	// died with an opaque "network is unreachable" from the dial instead.
	addrHost, _, _ := net.SplitHostPort(targetAddr)
	if !isYggdrasilAddr(net.ParseIP(addrHost)) {
		return genErrorResult(originalURI, fmt.Sprintf(
			"download target %s is outside the Yggdrasil range 200::/7: the built-in userspace netstack has no route beyond the Yggdrasil network, so the test URL must point at a Yggdrasil-internal address",
			targetAddr))
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
		DownloadMbps:    &m,
		PeakMbps:        &pk,
		BytesDownloaded: bytesDownloaded,
		DurationSec:     durationSec,
	}
	// The kcp probe cannot honestly produce a handshake time, so the field
	// stays unset and renders as "-" rather than a fabricated 0.00 ms.
	if hsOK {
		res.HandshakeMs = &h
	}
	if pingOK {
		p := PingFloat(pingMs)
		res.PingMs = &p
	}
	return res
}
