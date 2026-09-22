package engine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

// peerEntry mirrors the shape admin's getPeers reply uses for one entry.
// `remote` is the URI the node dialed and `key` is the peer's public key hex;
// latency is reported in seconds.
type peerEntry struct {
	Key     string  `json:"key"`
	URI     string  `json:"remote"`
	Latency float64 `json:"latency"`
}

// adminListenAddr binds the admin socket to loopback only. Each test spawns its
// own admin service, and every one listens on a different port in the
// [adminPortBase, adminPortBase+adminPortCount) range.
func adminListenAddr(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// adminCall issues one request against the local admin socket and returns the
// decoded `response` payload. It never returns a typed error: an admin service
// that is not up yet is the normal case during route convergence, and callers
// retry on a nil result.
func adminCall(adminPort int, request string) []byte {
	conn, err := net.DialTimeout("tcp", adminListenAddr(adminPort), 2*time.Second)
	if err != nil {
		return nil
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return nil
	}

	payload, err := json.Marshal(map[string]string{"request": request})
	if err != nil {
		return nil
	}
	if _, err := conn.Write(payload); err != nil {
		return nil
	}

	var reply struct {
		Status   string          `json:"status"`
		Error    string          `json:"error"`
		Response json.RawMessage `json:"response"`
	}
	if err := json.NewDecoder(conn).Decode(&reply); err != nil {
		return nil
	}
	if reply.Status != "success" {
		return nil
	}
	return []byte(reply.Response)
}

// peerNodeID extracts the 64-hex node key from a peer URI's ?key= query
// parameter, lowercased. That key is the only thing that distinguishes two
// entries advertising the same host, so a lookup that ignored it matched the
// wrong node. Anything that is not exactly 64 hex chars is reported as absent.
func peerNodeID(peerURI string) string {
	u, err := url.Parse(peerURI)
	if err != nil {
		return ""
	}
	key := strings.ToLower(strings.TrimSpace(u.Query().Get("key")))
	if len(key) != 64 {
		return ""
	}
	if _, err := hex.DecodeString(key); err != nil {
		return ""
	}
	return key
}

// stripKeyQuery drops the identifying ?key= parameter so two URIs for the same
// node compare equal by host and port. Unrelated parameters such as sni are
// left untouched.
func stripKeyQuery(peerURI string) string {
	u, err := url.Parse(peerURI)
	if err != nil {
		return peerURI
	}
	q := u.Query()
	if !q.Has("key") {
		return peerURI
	}
	q.Del("key")
	u.RawQuery = q.Encode()
	return u.String()
}

// latencyToMs normalizes a latency value from the admin socket. getPeers
// reports seconds, so a 350 ms round trip arrives as 0.35 and a 7 second one
// as 7.0. Anything under 100 is treated as seconds; a millisecond reading is
// already at least 100, which is the only realistic place to split the two
// units without a scale hint from the caller.
func latencyToMs(v float64) float64 {
	if v > 0 && v < 100 {
		return v * 1000
	}
	return v
}

// fetchPeerPing reports the dialed peer's own latency in milliseconds. getPeers
// returns every peer the node holds, so the first latency found was not
// necessarily this one; peerLatencyMs matches by node key instead.
func fetchPeerPing(peerURI string, adminPort int) (float64, bool) {
	body := adminCall(adminPort, "getPeers")
	if len(body) == 0 {
		return 0, false
	}

	var payload struct {
		Peers []peerEntry `json:"peers"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0, false
	}
	return peerLatencyMs(peerURI, payload.Peers)
}

// peerLatencyMs looks the peer up in the getPeers list. The node key wins when
// it is present, and otherwise the advertised URI is matched.
func peerLatencyMs(peerURI string, peers []peerEntry) (float64, bool) {
	if id := peerNodeID(peerURI); id != "" {
		for _, p := range peers {
			if strings.ToLower(strings.TrimSpace(p.Key)) == id {
				return latencyToMs(p.Latency), true
			}
		}
	}

	want := stripKeyQuery(peerURI)
	for _, p := range peers {
		if p.URI != "" && stripKeyQuery(p.URI) == want {
			return latencyToMs(p.Latency), true
		}
	}
	return 0, false
}

// hasRoute answers from the routing table rather than by dialing the target.
// getPaths addresses routes by the peer's public key, and 127.0.0.1 never
// appears in that table, so a dial-based check reported a working route as
// unreachable.
func hasRoute(body []byte, addr, targetKey string) bool {
	want := net.ParseIP(addr)
	targetKey = strings.ToLower(strings.TrimSpace(targetKey))
	if want == nil && targetKey == "" {
		return false
	}

	var payload struct {
		Paths []struct {
			Address string `json:"address"`
			Key     string `json:"key"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}

	for _, p := range payload.Paths {
		if targetKey != "" && strings.ToLower(strings.TrimSpace(p.Key)) == targetKey {
			return true
		}
		if want != nil && p.Address != "" {
			if a := net.ParseIP(p.Address); a != nil && a.Equal(want) {
				return true
			}
		}
	}
	return false
}

// waitForRoute polls the routing table until the measured target is reachable.
// The target's address is used for a route check and the peer's key for the
// same check: either one matching is enough.
func waitForRoute(ctx context.Context, adminPort int, addr, peerURI string, timeout time.Duration) error {
	targetKey := peerNodeID(peerURI)
	deadline := time.After(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		if hasRoute(adminCall(adminPort, "getPaths"), addr, targetKey) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("route to %s not available after %v", addr, timeout)
		case <-ticker.C:
		}
	}
}

// kcpReachabilityProbe reports reachability for a kcp:// peer. A write to a
// UDP socket proves nothing by itself, so the probe sends a packet and waits
// for any reply. No number printed from here can be called a handshake time.
func kcpReachabilityProbe(ctx context.Context, addr string) (float64, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return 0, err
	}
	if _, err := conn.Write([]byte("YggSpeedTest-probe")); err != nil {
		return 0, err
	}

	buf := make([]byte, 512)
	if _, err := conn.Read(buf); err != nil {
		return 0, fmt.Errorf("no reply from %s: %w", addr, err)
	}
	return 0, nil
}
