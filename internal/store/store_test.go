package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"yggspeedtest/internal/engine"
)

func TestLoadMissingFileGivesDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "nope.json"))
	if err != nil {
		t.Fatalf("Load on a missing file should not error: %v", err)
	}
	if cfg.ListenAddr != "127.0.0.1:8080" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
	}
	if cfg.TestURL != "" {
		t.Errorf("TestURL = %q, want empty: the netstack can only download from Yggdrasil-internal addresses, so there is no honest default", cfg.TestURL)
	}
	if cfg.Concurrency != 1 || cfg.Streams != 4 {
		t.Errorf("Concurrency=%d Streams=%d, want 1 and 4", cfg.Concurrency, cfg.Streams)
	}
	if cfg.MaxDuration != "15s" || cfg.Timeout != "30s" || cfg.RouteTimeout != "10s" {
		t.Errorf("durations = %q %q %q", cfg.MaxDuration, cfg.Timeout, cfg.RouteTimeout)
	}
	if cfg.SortBy != "speed" {
		t.Errorf("SortBy = %q", cfg.SortBy)
	}
}

func TestLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	in := Config{
		ListenAddr:   "0.0.0.0:9090",
		HistoryLimit: 500,
		Peers:        []string{"tls://a:443", "tls://b:443"},
		PeersFile:    "/etc/peers.txt",
		PublicPeers:  true,
		Limit:        5,
		TestURL:      "https://example.com/big.bin",
		CustomSNI:    "example.com",
		Concurrency:  2,
		Streams:      8,
		MaxDuration:  "1m",
		MaxBytes:     "128MB",
		Timeout:      "45s",
		RouteTimeout: "20s",
		SortBy:       "ping",
		MinSpeed:     10,
		MaxPing:      900,
		KeyFile:      "/etc/key",
		Debug:        true,
		Schedule:     "@every 1h",
		Enabled:      true,
	}
	if err := in.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	out, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := assertEqualConfig(t, in, out); err != nil {
		t.Error(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The file must stay valid, indented JSON that a human can edit.
	var generic map[string]any
	if err := json.Unmarshal(data, &generic); err != nil {
		t.Fatalf("saved config is not valid JSON: %v\n%s", err, data)
	}
	if !strings.Contains(string(data), "\n  ") {
		t.Error("saved config is not indented")
	}
}

func assertEqualConfig(t *testing.T, a, b Config) error {
	t.Helper()
	if a.ListenAddr != b.ListenAddr || a.HistoryLimit != b.HistoryLimit {
		return fmt.Errorf("listen/history differ: %+v vs %+v", a, b)
	}
	if a.TestURL != b.TestURL || a.CustomSNI != b.CustomSNI {
		return fmt.Errorf("url/sni differ")
	}
	if a.Concurrency != b.Concurrency || a.Streams != b.Streams {
		return fmt.Errorf("concurrency/streams differ")
	}
	if a.MaxDuration != b.MaxDuration || a.MaxBytes != b.MaxBytes || a.Timeout != b.Timeout || a.RouteTimeout != b.RouteTimeout {
		return fmt.Errorf("durations differ: %+v vs %+v", a, b)
	}
	if a.SortBy != b.SortBy || a.MinSpeed != b.MinSpeed || a.MaxPing != b.MaxPing {
		return fmt.Errorf("filters differ")
	}
	if a.Debug != b.Debug || a.Schedule != b.Schedule || a.Enabled != b.Enabled {
		return fmt.Errorf("debug/schedule/enabled differ")
	}
	if a.PublicPeers != b.PublicPeers || a.PeersFile != b.PeersFile || a.Limit != b.Limit || a.KeyFile != b.KeyFile {
		return fmt.Errorf("peer sources differ")
	}
	if len(a.Peers) != len(b.Peers) {
		return fmt.Errorf("peer count %d vs %d", len(a.Peers), len(b.Peers))
	}
	for i := range a.Peers {
		if a.Peers[i] != b.Peers[i] {
			return fmt.Errorf("peer %d differs", i)
		}
	}
	return nil
}

func TestLoadRejectsInvalidJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Error("Load should fail on invalid JSON")
	}
}

func TestToRunConfigUsesSavedPeers(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Peers = []string{"tls://saved:443"}
	rc, err := cfg.ToRunConfig(nil)
	if err != nil {
		t.Fatalf("ToRunConfig: %v", err)
	}
	if len(rc.Peers) != 1 || rc.Peers[0] != "tls://saved:443" {
		t.Errorf("Peers = %v", rc.Peers)
	}
	if rc.MaxDuration != 15*time.Second || rc.Timeout != 30*time.Second || rc.RouteTimeout != 10*time.Second {
		t.Errorf("durations = %v %v %v", rc.MaxDuration, rc.Timeout, rc.RouteTimeout)
	}
	if rc.SortBy != "speed" || rc.Concurrency != 1 || rc.Streams != 4 {
		t.Errorf("misc = %+v", rc)
	}
}

func TestToRunConfigHonoursOverrides(t *testing.T) {
	cfg, _ := Load(filepath.Join(t.TempDir(), "missing.json"))
	cfg.Peers = []string{"tls://saved:443"}
	rc, err := cfg.ToRunConfig([]string{"tls://override:443"})
	if err != nil {
		t.Fatalf("ToRunConfig: %v", err)
	}
	if len(rc.Peers) != 1 || rc.Peers[0] != "tls://override:443" {
		t.Errorf("overrides were ignored: %v", rc.Peers)
	}

	// An empty override is a real empty list, not "fall back to saved".
	rc, err = cfg.ToRunConfig([]string{})
	if err != nil {
		t.Fatalf("ToRunConfig: %v", err)
	}
	if len(rc.Peers) != 0 {
		t.Errorf("empty override gave %v", rc.Peers)
	}
}

func TestToRunConfigParsesMaxBytes(t *testing.T) {
	cfg, _ := Load(filepath.Join(t.TempDir(), "missing.json"))
	cfg.MaxBytes = "128MB"
	rc, err := cfg.ToRunConfig(nil)
	if err != nil {
		t.Fatalf("ToRunConfig: %v", err)
	}
	if rc.MaxBytes != 128<<20 {
		t.Errorf("MaxBytes = %d", rc.MaxBytes)
	}

	cfg.MaxBytes = "1x"
	if _, err := cfg.ToRunConfig(nil); err == nil {
		t.Error("an invalid max_bytes must be reported")
	}
}

func TestToRunConfigRejectsBadDurations(t *testing.T) {
	for _, bad := range []string{"soon", "ten", "1.2.3s", ""} {
		for _, field := range []string{"max_duration", "timeout", "route_timeout"} {
			cfg, _ := Load(filepath.Join(t.TempDir(), "missing.json"))
			switch field {
			case "max_duration":
				cfg.MaxDuration = bad
			case "timeout":
				cfg.Timeout = bad
			case "route_timeout":
				cfg.RouteTimeout = bad
			}
			if _, err := cfg.ToRunConfig(nil); err == nil {
				t.Errorf("ToRunConfig accepted %s=%q", field, bad)
			}
		}
	}
}

func TestToRunConfigDoesNotAliasSavedPeers(t *testing.T) {
	cfg, _ := Load(filepath.Join(t.TempDir(), "missing.json"))
	cfg.Peers = []string{"tls://a:443"}
	rc, _ := cfg.ToRunConfig(nil)
	rc.Peers[0] = "mutated"
	if cfg.Peers[0] == "mutated" {
		t.Error("mutating the RunConfig rewrote the saved config's peer list")
	}
}

func TestRunHistoryAppendAndTrim(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.jsonl")

	mk := func(i int) RunRecord {
		return RunRecord{
			ID:        fmtID(i),
			StartedAt: time.Date(2026, 9, 1, 0, i, 0, 0, time.UTC),
			Duration:  5 * time.Second,
			Trigger:   "scheduled",
			Peers:     3,
			AvgMbps:   100 + float64(i),
			MaxMbps:   200,
			AvgPingMs: 33.5,
			Results:   []engine.SpeedResult{{Peer: "tls://a:443", TestTime: "t"}},
		}
	}

	limit := 3
	for i := 0; i < 5; i++ {
		if err := AppendRun(path, mk(i), limit); err != nil {
			t.Fatalf("AppendRun %d: %v", i, err)
		}
	}

	runs, err := LoadRuns(path)
	if err != nil {
		t.Fatalf("LoadRuns: %v", err)
	}
	if len(runs) != limit {
		t.Fatalf("runs = %d, want %d after trimming", len(runs), limit)
	}
	// The oldest are dropped, the newest kept: runs 2, 3 and 4 remain.
	if runs[0].AvgMbps != 102 {
		t.Errorf("trimmed set starts at %v, want the third run", runs[0].AvgMbps)
	}
	if runs[limit-1].AvgMbps != 104 {
		t.Errorf("newest run missing: %v", runs[limit-1].AvgMbps)
	}
	for i := 1; i < len(runs); i++ {
		if runs[i].StartedAt.Before(runs[i-1].StartedAt) {
			t.Errorf("runs are not in chronological order at %d", i)
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(data), "\n"); got != limit {
		t.Errorf("file line count = %d, want %d", got, limit)
	}
}

func TestFindRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.jsonl")
	mk := func(id string) RunRecord { return RunRecord{ID: id, Trigger: "manual", Peers: 1} }
	for _, id := range []string{"a", "b", "c"} {
		if err := AppendRun(path, mk(id), 0); err != nil {
			t.Fatal(err)
		}
	}

	rec, ok, err := FindRun(path, "b")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || rec.ID != "b" {
		t.Errorf("FindRun = %+v, %v; want b", rec, ok)
	}
	if _, ok, _ := FindRun(path, "nope"); ok {
		t.Error("FindRun reported a hit for an id that was never written")
	}
	if _, ok, _ := FindRun(path, "a"); !ok {
		t.Error("FindRun missed an existing id")
	}
	// An empty file is an empty answer, not an error.
	empty := filepath.Join(t.TempDir(), "none.jsonl")
	if _, ok, err := FindRun(empty, "a"); err != nil || ok {
		t.Errorf("FindRun on a missing file = %v, %v", err, ok)
	}
}

func TestLatestRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.jsonl")
	if _, ok, err := LatestRun(path); err != nil || ok {
		t.Fatalf("LatestRun on a missing file = %v, %v; want an empty answer", err, ok)
	}

	mk := func(id string) RunRecord { return RunRecord{ID: id, Trigger: "manual"} }
	for _, id := range []string{"oldest", "middle", "newest"} {
		if err := AppendRun(path, mk(id), 0); err != nil {
			t.Fatal(err)
		}
	}
	rec, ok, err := LatestRun(path)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || rec.ID != "newest" {
		t.Errorf("LatestRun = %+v, %v; want newest", rec, ok)
	}
}

func TestDeleteRunKeepsUnrelatedRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.jsonl")
	mk := func(id string) RunRecord { return RunRecord{ID: id, Trigger: "manual", Peers: 1} }
	for _, id := range []string{"a", "b", "c"} {
		if err := AppendRun(path, mk(id), 0); err != nil {
			t.Fatal(err)
		}
	}

	found, err := DeleteRun(path, "b")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("DeleteRun reported no deletion")
	}

	runs, err := LoadRuns(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 {
		t.Fatalf("runs = %d after deleting one, want 2", len(runs))
	}
	for _, rec := range runs {
		if rec.ID == "b" {
			t.Error("b survived the delete")
		}
	}
	if _, ok, _ := FindRun(path, "a"); !ok {
		t.Error("a was erased by a delete of b")
	}
	if _, ok, _ := FindRun(path, "c"); !ok {
		t.Error("c was erased by a delete of b")
	}

	// Deleting an unknown id is an empty answer, and it must not rewrite the
	// file at all.
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if found, err := DeleteRun(path, "nope"); err != nil || found {
		t.Errorf("DeleteRun on an unknown id = %v, %v", err, found)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Error("deleting an unknown id rewrote the history file")
	}
}

func TestLoadRunsWithMalformedLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.jsonl")
	good := `{"id":"good","started_at":"2026-09-01T00:00:00Z","peers":2,"avg_mbps":50}`
	body := strings.Join([]string{good, `{broken`, good, ``, good}, "\n")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	runs, err := LoadRuns(path)
	if err != nil {
		t.Fatalf("LoadRuns: %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("runs = %d, want 3 (one malformed line skipped)", len(runs))
	}
	for i, r := range runs {
		if r.ID != "good" || r.Peers != 2 || r.AvgMbps != 50 {
			t.Errorf("run %d = %+v", i, r)
		}
	}
}

func TestLoadRunsMissingFile(t *testing.T) {
	runs, err := LoadRuns(filepath.Join(t.TempDir(), "absent.jsonl"))
	if err != nil {
		t.Fatalf("LoadRuns on a missing file should not error: %v", err)
	}
	if len(runs) != 0 {
		t.Errorf("runs = %v", runs)
	}
}

func TestWriteRunsRoundTripsPointers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.jsonl")
	pf := func(v float64) *engine.PingFloat { x := engine.PingFloat(v); return &x }
	sf := func(v float64) *engine.SpeedFloat { x := engine.SpeedFloat(v); return &x }

	in := []RunRecord{{
		ID: "r1", StartedAt: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC),
		Duration: 7 * time.Second, Trigger: "manual", Peers: 1,
		Results: []engine.SpeedResult{{
			Peer: "tls://x:443", TestTime: "2026-09-02T12:00:00Z",
			HandshakeMs: pf(12.5), PingMs: pf(40), DownloadMbps: sf(123.456), PeakMbps: sf(200),
			BytesDownloaded: 1 << 20, DurationSec: 1.5,
		}},
	}}
	if err := WriteRuns(path, in); err != nil {
		t.Fatalf("WriteRuns: %v", err)
	}
	out, err := LoadRuns(path)
	if err != nil {
		t.Fatalf("LoadRuns: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("runs = %d", len(out))
	}
	r := out[0]
	if r.ID != "r1" || r.Peers != 1 || r.Trigger != "manual" {
		t.Errorf("record = %+v", r)
	}
	if len(r.Results) != 1 {
		t.Fatalf("results = %v", r.Results)
	}
	res := r.Results[0]
	if res.Peer != "tls://x:443" || res.BytesDownloaded != 1<<20 {
		t.Errorf("result = %+v", res)
	}
	if res.DownloadMbps == nil || float64(*res.DownloadMbps) != 123.456 {
		t.Errorf("DownloadMbps = %v", res.DownloadMbps)
	}
	if res.PingMs == nil || float64(*res.PingMs) != 40 {
		t.Errorf("PingMs = %v", res.PingMs)
	}
	if res.PeakMbps == nil || float64(*res.PeakMbps) != 200 {
		t.Errorf("PeakMbps = %v", res.PeakMbps)
	}
	if r.Duration != 7*time.Second {
		t.Errorf("Duration = %v", r.Duration)
	}
}

func TestLoadOrCreateConfigWritesDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	cfg, err := LoadOrCreateConfig(path)
	if err != nil {
		t.Fatalf("LoadOrCreateConfig: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("config file was not created: %v", err)
	}
	if cfg.ListenAddr == "" {
		t.Error("defaults missing")
	}
	// A second call must read the file rather than overwrite it.
	if _, err := LoadOrCreateConfig(path); err != nil {
		t.Fatalf("second LoadOrCreateConfig: %v", err)
	}
}

func TestWriteFileAtomicLeavesNoTemp(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "nested", "out.json")
	if err := writeFileAtomic(target, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("writeFileAtomic: %v", err)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != `{"ok":true}` {
		t.Errorf("content = %q", got)
	}

	entries, err := os.ReadDir(filepath.Join(dir, "nested"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "out.json" {
			t.Errorf("stray file %q left behind", e.Name())
		}
	}
}

func fmtID(i int) string {
	return fmt.Sprintf("run-%04d", i)
}

// The dashboard polls /api/status every few seconds; the count must come from
// a line scan rather than a full JSON decode of every record.
func TestCountRuns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.jsonl")

	n, err := CountRuns(path)
	if err != nil || n != 0 {
		t.Fatalf("missing file: n=%d err=%v, want 0 nil", n, err)
	}

	if err := AppendRun(path, RunRecord{ID: "a"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := AppendRun(path, RunRecord{ID: "b"}, 0); err != nil {
		t.Fatal(err)
	}
	n, err = CountRuns(path)
	if err != nil || n != 2 {
		t.Fatalf("after two appends: n=%d err=%v, want 2 nil", n, err)
	}
}

// A zero history limit picks up a bounded default (the runs file would
// otherwise grow without end); a negative limit means keep everything.
func TestHistoryLimitDefaultAndUnlimited(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "nope.json"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HistoryLimit != 100 {
		t.Errorf("HistoryLimit default = %d, want 100", cfg.HistoryLimit)
	}

	path := filepath.Join(t.TempDir(), "runs.jsonl")
	for _, id := range []string{"a", "b", "c", "d"} {
		if err := AppendRun(path, RunRecord{ID: id}, -1); err != nil {
			t.Fatal(err)
		}
	}
	runs, err := LoadRuns(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 4 {
		t.Errorf("negative limit kept %d runs, want all 4", len(runs))
	}
}
