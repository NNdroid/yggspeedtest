package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{"", 0, false},
		{"0", 0, false},
		{"100", 100, false},
		{"100K", 100 << 10, false},
		{"100KB", 100 << 10, false},
		{"1M", 1 << 20, false},
		{"1MB", 1 << 20, false},
		{"1G", 1 << 30, false},
		{"1GB", 1 << 30, false},
		// Decimals were rejected entirely before ParseFloat replaced ParseInt.
		{"1.5MB", 1572864, false},
		{"2.5GB", 2684354560, false},
		{"  3 kb  ", 3 << 10, false},
		{"0.001MB", 1048, false},
		{"-1MB", 0, true},
		{"abc", 0, true},
		{"999999999999999999999999MB", 0, true},
		{"MB", 0, true},
		{"1.5.5MB", 0, true},
	}

	for _, tc := range tests {
		got, err := ParseByteSize(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("ParseByteSize(%q) error = %v, wantErr %v", tc.in, err, tc.wantErr)
			continue
		}
		if err == nil && got != tc.want {
			t.Errorf("ParseByteSize(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// Values that fit must parse, and values that overflow int64 have to be
// rejected rather than silently wrapping to a small positive size.
func TestParseByteSizeOverflowBoundary(t *testing.T) {
	if got, err := ParseByteSize("8GB"); err != nil {
		t.Fatalf("ParseByteSize(8GB) unexpected error: %v", err)
	} else if got != 8<<30 {
		t.Errorf("ParseByteSize(8GB) = %d, want %d", got, int64(8)<<30)
	}

	if _, err := ParseByteSize("16GB"); err != nil {
		t.Errorf("16GB should fit in int64, got error: %v", err)
	}
	if _, err := ParseByteSize("10000000000GB"); err == nil {
		t.Error("10000000000GB should overflow int64 and error")
	}
	if _, err := ParseByteSize("99999999999999999999GB"); err == nil {
		t.Error("huge value should overflow int64 and error")
	}
	_ = math.MaxInt64
}

func TestGetUniqueAdminPortInRange(t *testing.T) {
	for i := 0; i < adminPortCount*2; i++ {
		p := getUniqueAdminPort()
		if p < adminPortBase || p >= adminPortBase+adminPortCount {
			t.Fatalf("port %d out of range [%d, %d)", p, adminPortBase, adminPortBase+adminPortCount)
		}
	}
}

func TestDeduplicatePeers(t *testing.T) {
	in := []string{" tcp://a:1 ", "tcp://a:1", "", "tls://b:2", "\t", "tls://b:2"}
	out := deduplicatePeers(in)
	want := []string{"tcp://a:1", "tls://b:2"}
	if len(out) != len(want) {
		t.Fatalf("deduplicatePeers = %v, want %v", out, want)
	}
	for i := range want {
		if out[i] != want[i] {
			t.Fatalf("deduplicatePeers = %v, want %v", out, want)
		}
	}
}

func TestGenerateWebSocketKey(t *testing.T) {
	key, err := generateWebSocketKey()
	if err != nil {
		t.Fatalf("generateWebSocketKey: %v", err)
	}
	// 16 random bytes -> 24 base64 chars.
	if len(key) != 24 {
		t.Errorf("key length = %d, want 24: %q", len(key), key)
	}
	other, _ := generateWebSocketKey()
	if key == other {
		t.Error("two consecutive keys are identical; randomness looks broken")
	}
}

func TestExporters(t *testing.T) {
	results := []SpeedResult{
		{Peer: "tls://good:443", TestTime: "2026-09-21T12:00:00Z",
			HandshakeMs: pf(12.34), PingMs: pf(88.1), DownloadMbps: sf(123.456),
			PeakMbps: sf(200.001), BytesDownloaded: 1 << 20, DurationSec: 1.5},
		{Peer: "tcp://bad:1", TestTime: "2026-09-21T12:00:01Z",
			Error: "Handshake failed: i/o timeout"},
	}

	dir := t.TempDir()

	csvPath := dir + string(os.PathSeparator) + "out.csv"
	if err := ExportCSVFile(csvPath, results); err != nil {
		t.Fatalf("exportCSV: %v", err)
	}
	csvBytes, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	csvText := string(csvBytes)
	// Excel needs the BOM before the header to detect UTF-8; a header that
	// begins with "Rank" would fail this check.
	if !strings.HasPrefix(csvText, "\uFEFFRank") {
		t.Errorf("csv does not start with a UTF-8 BOM before the header: %q", csvText[:16])
	}
	for _, want := range []string{"Rank,Peer,TestTime", "tls://good:443", "123.456", "FAIL: Handshake failed: i/o timeout"} {
		if !strings.Contains(csvText, want) {
			t.Errorf("csv missing %q in:\n%s", want, csvText)
		}
	}
	if lines := strings.Count(csvText, "\n"); lines != 3 {
		t.Errorf("csv line count = %d, want 3 (header + 2 rows)", lines)
	}

	mdPath := dir + string(os.PathSeparator) + "out.md"
	if err := ExportMarkdownFile(mdPath, results); err != nil {
		t.Fatalf("exportMarkdown: %v", err)
	}
	mdBytes, err := os.ReadFile(mdPath)
	if err != nil {
		t.Fatalf("read md: %v", err)
	}
	mdText := string(mdBytes)
	for _, want := range []string{"123.456 Mbps", "200.001 Mbps", "❌ 失败", "✅ 成功"} {
		if !strings.Contains(mdText, want) {
			t.Errorf("markdown missing %q", want)
		}
	}
}

// exportCSV must surface write failures rather than swallowing them, since a
// truncated report is worse than a loud failure.
func TestExportCSVReportsWriteError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory-as-file is not reliably an error on Windows")
	}
	dir := t.TempDir()
	if err := ExportCSVFile(dir, nil); err == nil {
		t.Error("exportCSV writing into a directory should fail")
	}
}

// Regression guard: PingMs is optional, and the console table used to
// dereference it unconditionally. A missing rate must render as "-" like the
// other missing measurements, not as a fake "0.000 Mbps".
func TestPrintConsoleTableWithMissingMeasurements(t *testing.T) {
	results := []SpeedResult{
		{Peer: "tls://no-data:443", TestTime: "2026-09-21T12:00:00Z"}, // all pointers nil
		{Peer: "tcp://failed:1", Error: "Handshake failed: i/o timeout"},
		{Peer: "tls://good:443", TestTime: "2026-09-21T12:00:00Z",
			HandshakeMs: pf(12), PingMs: pf(88), DownloadMbps: sf(1), PeakMbps: sf(2)},
	}
	var out strings.Builder
	PrintConsoleTable(&out, results)

	got := out.String()
	for _, want := range []string{"YggSpeedTest 测速排行榜", "tls://good:443", "tls://no-data:443", "Handshake failed"} {
		if !strings.Contains(got, want) {
			t.Errorf("table missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "0.000 Mbps") {
		t.Errorf("a missing rate must not print as 0.000 Mbps:\n%s", got)
	}
}

// CJK headers must align with the ASCII data rows in terminal columns.
func TestRenderRowAlignsCJKHeaders(t *testing.T) {
	header := renderRow([]consoleCell{{"平均速率(Mbps)", 15, false}})
	row := renderRow([]consoleCell{{"123.456 Mbps", 15, false}})
	if w := displayWidth(header); w != 15 {
		t.Errorf("header display width = %d, want 15", w)
	}
	if w := displayWidth(row); w != 15 {
		t.Errorf("row display width = %d, want 15", w)
	}
}

func TestTruncateDoesNotSplitUTF8(t *testing.T) {
	in := "失败失败失败失败失败失败失败失败" // 16 CJK runes
	out := truncate(in, 10)
	if !utf8.ValidString(out) {
		t.Fatalf("truncate produced invalid UTF-8: %q", out)
	}
	if got := len([]rune(out)); got != 10 {
		t.Errorf("truncate length = %d runes, want 10", got)
	}
	for _, r := range out {
		if r == utf8.RuneError {
			t.Errorf("truncate produced U+FFFD, meaning a rune was split: %q", out)
			break
		}
	}
	if got := truncate("short", 10); got != "short" {
		t.Errorf("short input changed: %q", got)
	}
	if got := truncate("abcdefghij", 0); got != "" {
		t.Errorf("maxLen 0 = %q, want empty", got)
	}
}

func pf(f float64) *PingFloat  { v := PingFloat(f); return &v }
func sf(f float64) *SpeedFloat { v := SpeedFloat(f); return &v }

// The peer URI's ?key= is the only thing that identifies the operator: two
// entries for the same host can be different nodes.
func TestPeerNodeID(t *testing.T) {
	const id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	tests := []struct {
		in   string
		want string
	}{
		{fmt.Sprintf("tls://peer.example:443?key=%s", id), id},
		{"tls://peer.example?key=" + strings.ToUpper(id), id}, // case-insensitive
		{"tcp://host:1", ""},
		{"tcp://host:1?key=abc", ""},
		{"tcp://host:1?key=" + id + "0", ""},
		{"tcp://host:1?key=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg", ""},
		{"tcp://host:1?priority=5&key=" + id, id},
	}
	for _, tc := range tests {
		if got := peerNodeID(tc.in); got != tc.want {
			t.Errorf("peerNodeID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// peerLatencyMs is the bug fix in isolation: getPeers returns every peer the
// node holds, so the first latency found was not necessarily the peer's.
func TestPeerLatencyMsMatchesByID(t *testing.T) {
	const want = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	peers := []peerEntry{
		{Key: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", Latency: 0.30},
		{Key: want, URI: "tls://peer.example:443", Latency: 0.35},
	}

	ms, ok := peerLatencyMs("tls://peer.example:443?key="+want, peers)
	if !ok {
		t.Fatal("expected a match on node ID")
	}
	if ms != 350 {
		t.Errorf("latency = %.1f ms, want 350 (matched by key, not the first entry)", ms)
	}

	// Matched by advertised URI when no key is present.
	uriOnly, ok := peerLatencyMs("tls://peer.example:443", peers)
	if !ok || uriOnly != 350 {
		t.Errorf("URI match = (%.1f, %v), want (350, true)", uriOnly, ok)
	}

	if _, ok := peerLatencyMs("tcp://unknown:1", peers); ok {
		t.Error("an absent peer must not report a latency")
	}
}

// The admin API reports seconds; the old 5s cutoff turned a 7s peer into 7ms.
func TestLatencyToMs(t *testing.T) {
	if got := latencyToMs(0.32); got != 320 {
		t.Errorf("0.32s = %.1f, want 320", got)
	}
	if got := latencyToMs(7.0); got != 7000 {
		t.Errorf("7s = %.1f, want 7000", got)
	}
	if got := latencyToMs(120); got != 120 {
		t.Errorf("120 already in ms = %.1f, want 120", got)
	}
	if got := latencyToMs(0); got != 0 {
		t.Errorf("zero = %.1f, want 0", got)
	}
}

func TestStripKeyQuery(t *testing.T) {
	const id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if got := stripKeyQuery("tls://peer.example:443?key=" + id); got != "tls://peer.example:443" {
		t.Errorf("stripKeyQuery = %q", got)
	}
	if got := stripKeyQuery("tls://peer.example:443?sni=x.com"); got != "tls://peer.example:443?sni=x.com" {
		t.Errorf("unrelated query was dropped: %q", got)
	}
}

func TestHasRoute(t *testing.T) {
	const peerKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	base := fmt.Sprintf(`{"paths":[{"address":"%s","key":"%s"}]}`, "::ffff:127.0.0.1", peerKey)

	// The route table keys routes on the peer's public key, not on a local IP.
	if !hasRoute([]byte(base), "", peerKey) {
		t.Error("expected a key match")
	}
	if hasRoute([]byte(base), "", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff") {
		t.Error("a different key must not match")
	}

	byIP := `{"paths":[{"address":"::ffff:127.0.0.1","key":""}]}`
	if !hasRoute([]byte(byIP), "127.0.0.1", "") {
		t.Error("expected an IP match")
	}
	if hasRoute([]byte(byIP), "2001:db8::1", "") {
		t.Error("an unrelated IP must not match")
	}
	if hasRoute([]byte(`{"paths":[]}`), "127.0.0.1", "") {
		t.Error("an empty route table must not match")
	}
	if hasRoute([]byte(`not json`), "127.0.0.1", "") {
		t.Error("a malformed reply must not match")
	}
}

// SliceStable keeps the order the peers were tested in, so an interrupted run
// and a full run rank their common peers the same way.
func TestStableSortKeepsTieOrder(t *testing.T) {
	same := sf(12.0)
	results := []SpeedResult{
		{Peer: "tls://a:443", DownloadMbps: same},
		{Peer: "tls://b:443", DownloadMbps: same},
		{Peer: "tcp://broken:1", Error: "handshake failed"},
		{Peer: "tls://c:443", DownloadMbps: sf(99)},
	}
	stableSort(results, "speed")

	want := []string{"tls://c:443", "tls://a:443", "tls://b:443", "tcp://broken:1"}
	for i, w := range want {
		if results[i].Peer != w {
			t.Fatalf("position %d = %s, want %s (order: %v)", i, results[i].Peer, w, peersOnly(results))
		}
	}
}

// A peer without a measured download must not masquerade as the fastest.
func TestStableSortPutsUnmeasuredLastAmongSuccesses(t *testing.T) {
	results := []SpeedResult{
		{Peer: "tls://none:443", TestTime: "t"}, // all measurements nil
		{Peer: "tls://fast:443", DownloadMbps: sf(100), PeakMbps: sf(100)},
		{Peer: "tls://slow:443", DownloadMbps: sf(10), PeakMbps: sf(10)},
	}
	stableSort(results, "speed")
	want := []string{"tls://fast:443", "tls://slow:443", "tls://none:443"}
	for i, w := range want {
		if results[i].Peer != w {
			t.Fatalf("speed order = %v, want %v", peersOnly(results), want)
		}
	}

	stableSort(results, "peak")
	wantPeak := []string{"tls://fast:443", "tls://slow:443", "tls://none:443"}
	for i, w := range wantPeak {
		if results[i].Peer != w {
			t.Fatalf("peak order = %v, want %v", peersOnly(results), wantPeak)
		}
	}
}

// A threshold the operator asked for must exclude peers that have no
// measurement to compare against. Silently passing them through made
// -min-speed 50 and -max-ping 100 into no-ops on any run where the download
// never completed, which is exactly the run they exist to trim.
func TestPassesFilters(t *testing.T) {
	tests := []struct {
		name     string
		res      SpeedResult
		minSpeed float64
		maxPing  float64
		want     bool
	}{
		{
			name: "no thresholds accepts everything",
			res:  SpeedResult{Peer: "p"}, // no measurements at all
			want: true,
		},
		{
			name:     "download above threshold passes",
			res:      SpeedResult{Peer: "p", DownloadMbps: sf(60), PeakMbps: sf(60)},
			minSpeed: 50,
			want:     true,
		},
		{
			name:     "download below threshold is excluded",
			res:      SpeedResult{Peer: "p", DownloadMbps: sf(49.9), PeakMbps: sf(49.9)},
			minSpeed: 50,
			want:     false,
		},
		{
			name:     "download exactly at threshold passes",
			res:      SpeedResult{Peer: "p", DownloadMbps: sf(50), PeakMbps: sf(50)},
			minSpeed: 50,
			want:     true,
		},
		{
			name:     "missing download fails a set speed threshold",
			res:      SpeedResult{Peer: "p", PingMs: pf(10)},
			minSpeed: 50,
			want:     false,
		},
		{
			name:    "ping under threshold passes",
			res:     SpeedResult{Peer: "p", PingMs: pf(99)},
			maxPing: 100,
			want:    true,
		},
		{
			name:    "ping over threshold is excluded",
			res:     SpeedResult{Peer: "p", PingMs: pf(101)},
			maxPing: 100,
			want:    false,
		},
		{
			name:    "missing ping fails a set ping threshold",
			res:     SpeedResult{Peer: "p", DownloadMbps: sf(500), PeakMbps: sf(500)},
			maxPing: 100,
			want:    false,
		},
		{
			name:     "both thresholds applied",
			res:      SpeedResult{Peer: "p", PingMs: pf(50), DownloadMbps: sf(500), PeakMbps: sf(500)},
			minSpeed: 50,
			maxPing:  100,
			want:     true,
		},
		{
			name:     "both thresholds applied, ping fails",
			res:      SpeedResult{Peer: "p", PingMs: pf(500), DownloadMbps: sf(500), PeakMbps: sf(500)},
			minSpeed: 50,
			maxPing:  100,
			want:     false,
		},
		{
			name:     "both thresholds applied, speed fails",
			res:      SpeedResult{Peer: "p", PingMs: pf(50), DownloadMbps: sf(10), PeakMbps: sf(10)},
			minSpeed: 50,
			maxPing:  100,
			want:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := passesFilters(&tc.res, tc.minSpeed, tc.maxPing); got != tc.want {
				t.Fatalf("passesFilters(%v, %v, %v) = %v, want %v", tc.res.Peer, tc.minSpeed, tc.maxPing, got, tc.want)
			}
		})
	}
}

// finishRun is the last step before Run returns: apply the thresholds, then
// sort. It used to be reachable only through a full network run.
func TestFinishRunFiltersThenSorts(t *testing.T) {
	mk := func() []SpeedResult {
		return []SpeedResult{
			{Peer: "slow", DownloadMbps: sf(1), PeakMbps: sf(1), PingMs: pf(10)},
			{Peer: "fast", DownloadMbps: sf(900), PeakMbps: sf(900), PingMs: pf(10)},
			{Peer: "mid", DownloadMbps: sf(60), PeakMbps: sf(60), PingMs: pf(20)},
			{Peer: "slow-ping", DownloadMbps: sf(800), PeakMbps: sf(800), PingMs: pf(900)},
			{Peer: "failed", Error: "boom"},
			{Peer: "unmeasured"},
		}
	}
	peers := func(got []SpeedResult) []string {
		out := make([]string, 0, len(got))
		for _, r := range got {
			out = append(out, r.Peer)
		}
		return out
	}
	equals := func(got, want []string) bool {
		if len(got) != len(want) {
			return false
		}
		for i := range want {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}

	tests := []struct {
		name string
		cfg  RunConfig
		want []string
	}{
		{
			name: "thresholds drop below-min-speed and above-max-ping but keep failures",
			cfg:  RunConfig{MinSpeed: 50, MaxPing: 500, SortBy: "speed"},
			// slow fails min_speed, slow-ping fails max_ping, unmeasured has no
			// numbers at all; failed is a failure and failures always survive.
			want: []string{"fast", "mid", "failed"},
		},
		{
			name: "no thresholds keep everything, sorted fastest first with failures last",
			cfg:  RunConfig{SortBy: "speed"},
			want: []string{"fast", "slow-ping", "mid", "slow", "unmeasured", "failed"},
		},
		{
			name: "ping sort reorders and keeps the input order for ties",
			cfg:  RunConfig{SortBy: "ping"},
			// slow and fast both measure 10 ms; slow appears first in the input,
			// and a stable sort must leave that alone.
			want: []string{"slow", "fast", "mid", "slow-ping", "unmeasured", "failed"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := peers(finishRun(mk(), tt.cfg))
			if !equals(got, tt.want) {
				t.Errorf("finishRun = %v, want %v", got, tt.want)
			}
		})
	}
}

// Run hands finishRun's output straight to the persisted record, so nil would
// land in the API as "results": null instead of [].
func TestFinishRunNeverReturnsNil(t *testing.T) {
	got := finishRun(nil, RunConfig{SortBy: "speed"})
	if got == nil {
		t.Error("finishRun(nil) = nil, want an empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("finishRun(nil) = %v, want an empty slice", got)
	}

	// Every peer filtered out by the thresholds is empty as well.
	got = finishRun([]SpeedResult{{Peer: "slow", DownloadMbps: sf(1), PingMs: pf(10)}},
		RunConfig{MinSpeed: 50, SortBy: "speed"})
	if got == nil || len(got) != 0 {
		t.Errorf("finishRun(all filtered out) = %v, want an empty slice", got)
	}
}

func TestStableSortByPingAndHandshake(t *testing.T) {
	results := []SpeedResult{
		{Peer: "p:1", PingMs: pf(40)},
		{Peer: "p:2", PingMs: pf(10)},
		{Peer: "p:3", HandshakeMs: pf(3)},
		{Peer: "p:4", HandshakeMs: pf(1)},
	}
	stableSort(results, "ping")
	if results[0].Peer != "p:2" {
		t.Errorf("lowest ping first, got %v", peersOnly(results))
	}
	stableSort(results, "handshake")
	if results[0].Peer != "p:4" {
		t.Errorf("lowest handshake first, got %v", peersOnly(results))
	}
}

func peersOnly(results []SpeedResult) []string {
	out := make([]string, len(results))
	for i, r := range results {
		out[i] = r.Peer
	}
	return out
}

// A fake admin socket, so the routing-table polling can be exercised without
// booting a Yggdrasil core.
func newFakeAdmin(t *testing.T, replies map[string]string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
				buf := make([]byte, 256)
				n, _ := c.Read(buf)
				var req struct {
					Name string `json:"request"`
				}
				if n > 0 {
					_ = json.Unmarshal(buf[:n], &req)
				}
				reply, ok := replies[req.Name]
				if !ok {
					reply = `{"status":"error","error":"unknown command"}`
				}
				_, _ = c.Write([]byte(reply))
			}(conn)
		}
	}()
	return ln
}

func adminPortOf(ln net.Listener) int {
	port, err := strconv.Atoi(strings.TrimPrefix(ln.Addr().String(), "127.0.0.1:"))
	if err != nil {
		return 0
	}
	return port
}

// waitForRoute must answer from the routing table: given a reply that holds
// the peer's key it returns immediately, without opening a single real TCP
// connection to the measurement target.
func TestWaitForRoutePollsRoutingTable(t *testing.T) {
	const peerKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	pathsReply := fmt.Sprintf(
		`{"status":"success","response":{"paths":[{"address":"::ffff:10.0.0.1","key":"%s"}]}}`, peerKey)

	ln := newFakeAdmin(t, map[string]string{"getPaths": pathsReply})
	defer ln.Close()

	done := make(chan error, 1)
	go func() {
		done <- waitForRoute(context.Background(), adminPortOf(ln), "[2606:4700:7::da]:443", "tls://peer:443?key="+peerKey, 3*time.Second)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("route reported as missing: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waitForRoute did not return promptly")
	}
}

func TestWaitForRouteTimesOutOnEmptyTable(t *testing.T) {
	ln := newFakeAdmin(t, map[string]string{"getPaths": `{"status":"success","response":{"paths":[]}}`})
	defer ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	if err := waitForRoute(ctx, adminPortOf(ln), "[2606:4700:7::da]:443", "", 15*time.Second); err == nil {
		t.Error("expected a timeout with an empty routing table")
	}
}

func TestWaitForRouteHonoursCancellation(t *testing.T) {
	ln := newFakeAdmin(t, map[string]string{"getPaths": `{"status":"success","response":{"paths":[]}}`})
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := waitForRoute(ctx, adminPortOf(ln), "[2606:4700:7::da]:443", "", 15*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("cancellation error = %v, want context.Canceled", err)
	}
}

// fetchPeerPing must return the dialed peer's own latency, not the first one
// in the list.
func TestFetchPeerPing(t *testing.T) {
	const target = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	peersReply := fmt.Sprintf(
		`{"status":"success","response":{"peers":[`+
			`{"remote":"tls://other:443","key":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","latency":0.1},`+
			`{"remote":"tls://target:443","key":"%s","latency":0.35}]}}`, target)

	ln := newFakeAdmin(t, map[string]string{"getPeers": peersReply})
	defer ln.Close()

	ms, ok := fetchPeerPing("tls://target:443?key="+target, adminPortOf(ln))
	if !ok || ms != 350 {
		t.Errorf("fetchPeerPing = (%.1f, %v), want (350, true)", ms, ok)
	}
	if _, ok := fetchPeerPing("tls://unknown:443", adminPortOf(ln)); ok {
		t.Error("a peer absent from the list must not report a latency")
	}

	dead := adminPortOf(ln)
	ln.Close()
	if _, ok := fetchPeerPing("tls://target:443?key="+target, dead); ok {
		t.Error("an unreachable admin socket must yield ok=false")
	}
}

// The upstream public-peers pages wrap every URI in backticks; a delimiter
// that slips through becomes part of the port and the whole peer dies on
// url.Parse.
func TestPeerURIRegexHandlesEmbeddedURIs(t *testing.T) {
	body := "# United States Peers\n" +
		"* San Francisco, operated by [marioaugustorama](https://github.com/marioaugustorama)\n" +
		"  * `tcp://165.227.17.198:9002`\n" +
		"  * `tls://redcatho.de:9494`\n" +
		"  * `quic://ip6.nerdvm.mywire.org:443?key=00000000c61d731961a290d127cd3fc03a4c5f3f35b9083559d4c81d48d65854`\n" +
		"| tls://node.0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef@host:443 | 0.1 | 0.0 |\n" +
		"<code>ws://wshost:80</code>\n" +
		"* `kcp://kcphost:7000`\n" +
		"  * `socks://sockshost:443`\n" +
		"plain prose with no peer\n"

	got := peerURIRe.FindAllString(body, -1)
	want := []string{
		"tcp://165.227.17.198:9002",
		"tls://redcatho.de:9494",
		"quic://ip6.nerdvm.mywire.org:443?key=00000000c61d731961a290d127cd3fc03a4c5f3f35b9083559d4c81d48d65854",
		"tls://node.0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef@host:443",
		"ws://wshost:80",
		"kcp://kcphost:7000",
		"socks://sockshost:443",
	}
	if len(got) != len(want) {
		t.Fatalf("extracted %d peer URIs, want %d:\n%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("position %d = %q, want %q", i, got[i], want[i])
		}
	}
	for _, p := range got {
		if _, err := url.Parse(p); err != nil {
			t.Errorf("extracted URI %q is not parseable: %v", p, err)
		}
	}
}

// A checkpoint file is the only copy of a long batch's progress, so each line
// has to be independently readable JSON.
func TestWriteCheckpointLineIsJSONL(t *testing.T) {
	if err := writeCheckpointLine(nil, SpeedResult{}); err == nil {
		t.Error("writing to a nil file should fail")
	}

	results := []SpeedResult{
		{Peer: "tls://a:443", TestTime: "t", HandshakeMs: pf(12.345), PingMs: pf(88.1),
			DownloadMbps: sf(123.456), PeakMbps: sf(200.001), BytesDownloaded: 1 << 20, DurationSec: 1.5},
		{Peer: "tcp://b:1", Error: "handshake failed"},
	}

	dir := t.TempDir()
	path := dir + string(os.PathSeparator) + "run.jsonl"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range results {
		if err := writeCheckpointLine(f, r); err != nil {
			t.Fatalf("writeCheckpointLine: %v", err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	if len(lines) != len(results) {
		t.Fatalf("checkpoint has %d lines, want %d:\n%s", len(lines), len(results), body)
	}
	for i, line := range lines {
		var got SpeedResult
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatalf("line %d is not JSON: %v\n%s", i, err, line)
		}
		if got.Peer != results[i].Peer {
			t.Errorf("line %d peer = %q, want %q", i, got.Peer, results[i].Peer)
		}
	}
}

func TestIsYggdrasilAddr(t *testing.T) {
	cases := []struct {
		ip   string
		want bool
	}{
		{"200::1", true},
		{"201:1234:5678::1", true},
		{"203:0:0::1", true},
		{"2606:4700:4700::1111", false}, // clearnet IPv6
		{"1.2.3.4", false},              // IPv4 can never route here
		{"::1", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := isYggdrasilAddr(net.ParseIP(tc.ip)); got != tc.want {
			t.Errorf("isYggdrasilAddr(%q) = %v, want %v", tc.ip, got, tc.want)
		}
	}
}

func TestValidateRejectsBadTestURL(t *testing.T) {
	base := RunConfig{
		TestURL:      "http://[201:1::1]/file.bin",
		SortBy:       "speed",
		Concurrency:  1,
		Streams:      1,
		Timeout:      time.Second,
		RouteTimeout: time.Second,
		MaxDuration:  time.Second,
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	for name, url := range map[string]string{
		"missing":   "",
		"relative":  "file.bin",
		"no scheme": "[201:1::1]/file.bin",
		"ftp":       "ftp://[201:1::1]/file.bin",
		"no host":   "http://",
	} {
		cfg := base
		cfg.TestURL = url
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: URL %q should have been rejected", name, url)
		}
	}
}

// fetchPublicPeers must merge every configured source and tolerate a failing
// one: an earlier version returned after the first region that yielded peers,
// quietly reducing the cross-region sample to a single country.
func TestFetchPublicPeersMergesAllSources(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/a.md", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "## Europe\n`tls://a1.example:443`\n`tls://a2.example:443`")
	})
	mux.HandleFunc("/b.md", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "`tls://b1.example:443`")
	})
	mux.HandleFunc("/c.md", func(w http.ResponseWriter, r *http.Request) {
		// A source that yields nothing is a failure, not an empty success.
		fmt.Fprint(w, "no addresses here")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	orig := publicPeerSources
	publicPeerSources = []string{
		srv.URL + "/a.md",
		srv.URL + "/b.md",
		srv.URL + "/c.md",
		srv.URL + "/missing.md",
	}
	defer func() { publicPeerSources = orig }()

	peers, err := fetchPublicPeers(context.Background())
	if err != nil {
		t.Fatalf("fetchPublicPeers: %v", err)
	}
	for _, want := range []string{"tls://a1.example:443", "tls://a2.example:443", "tls://b1.example:443"} {
		found := false
		for _, p := range peers {
			if p == want {
				found = true
			}
		}
		if !found {
			t.Errorf("merged list missing %s: %v", want, peers)
		}
	}
	if len(peers) != 3 {
		t.Errorf("merged list = %v, want 3 deduplicated peers", peers)
	}
}

// With no source producing anything, the built-in fallback list is returned
// alongside an error so the caller can warn about the stale sample.
func TestFetchPublicPeersFallsBackWhenAllSourcesFail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	orig := publicPeerSources
	publicPeerSources = []string{srv.URL + "/a.md", srv.URL + "/b.md"}
	defer func() { publicPeerSources = orig }()

	peers, err := fetchPublicPeers(context.Background())
	if err == nil {
		t.Error("an all-failed fetch must report an error alongside the fallback list")
	}
	if len(peers) == 0 {
		t.Error("fallback peers must still be returned so a run can start")
	}
}

// A minimal SOCKS5 server: method negotiation (optionally username/password),
// CONNECT, then a silent open socket. The pre-flight must come out the other
// side with a measured handshake time.
type socksTestServer struct {
	listener net.Listener
	wantAuth bool
	authOK   chan struct{}
}

func serveSocks(t *testing.T, wantAuth bool) *socksTestServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &socksTestServer{listener: l, wantAuth: wantAuth, authOK: make(chan struct{})}
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		buf := make([]byte, 2)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return
		}
		nMethods := int(buf[1])
		methods := make([]byte, nMethods)
		if _, err := io.ReadFull(conn, methods); err != nil {
			return
		}
		chosen := byte(0x00)
		if wantAuth {
			chosen = 0x02
		}
		conn.Write([]byte{0x05, chosen})
		if wantAuth {
			hdr := make([]byte, 2)
			if _, err := io.ReadFull(conn, hdr); err != nil {
				return
			}
			ulen := int(hdr[1])
			uname := make([]byte, ulen)
			if _, err := io.ReadFull(conn, uname); err != nil {
				return
			}
			plen := make([]byte, 1)
			if _, err := io.ReadFull(conn, plen); err != nil {
				return
			}
			pass := make([]byte, int(plen[0]))
			if _, err := io.ReadFull(conn, pass); err != nil {
				return
			}
			if string(uname) != "user" || string(pass) != "pass" {
				conn.Write([]byte{0x01, 0x01})
				return
			}
			conn.Write([]byte{0x01, 0x00})
		}

		req := make([]byte, 4)
		if _, err := io.ReadFull(conn, req); err != nil {
			return
		}
		switch req[3] {
		case 0x01:
			rest := make([]byte, 6)
			_, _ = io.ReadFull(conn, rest)
		case 0x04:
			rest := make([]byte, 18)
			_, _ = io.ReadFull(conn, rest)
		case 0x03:
			ln := make([]byte, 1)
			if _, err := io.ReadFull(conn, ln); err != nil {
				return
			}
			rest := make([]byte, int(ln[0])+2)
			_, _ = io.ReadFull(conn, rest)
		}
		conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		close(s.authOK)
	}()
	return s
}

func TestMeasureHandshakeSocks(t *testing.T) {
	t.Run("plain", func(t *testing.T) {
		s := serveSocks(t, false)
		defer s.listener.Close()

		ms, ok, err := measureHandshake(context.Background(),
			fmt.Sprintf("socks://%s/[201:dead::beef]:443", s.listener.Addr()), "", 5*time.Second)
		if err != nil || !ok || ms < 0 {
			t.Errorf("ms=%v ok=%v err=%v", ms, ok, err)
		}
		select {
		case <-s.authOK:
		default:
			t.Error("server never completed CONNECT")
		}
	})

	t.Run("auth", func(t *testing.T) {
		s := serveSocks(t, true)
		defer s.listener.Close()

		ms, ok, err := measureHandshake(context.Background(),
			fmt.Sprintf("socks://user:pass@%s/[201:dead::beef]:443", s.listener.Addr()), "", 5*time.Second)
		if err != nil || !ok || ms < 0 {
			t.Errorf("ms=%v ok=%v err=%v", ms, ok, err)
		}
	})

	t.Run("missing port is rejected", func(t *testing.T) {
		if _, _, err := measureHandshake(context.Background(),
			"socks://proxy.example/[201:dead::beef]:443", "", 5*time.Second); err == nil {
			t.Error("a socks URI without an explicit proxy port should fail the pre-flight")
		}
	})
}

// A kcp probe measures reachability, not a handshake, so the result must not
// carry a fabricated handshake time.
func TestMeasureHandshakeKcpHasNoFakeDuration(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	go func() {
		buf := make([]byte, 512)
		for {
			_, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			pc.WriteTo(buf[:4], addr) // reply with something
		}
	}()

	_, ok, err := measureHandshake(context.Background(),
		fmt.Sprintf("kcp://%s", pc.LocalAddr()), "", 5*time.Second)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if ok {
		t.Error("a kcp reachability probe must not claim to be a handshake measurement")
	}
}
