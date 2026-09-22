package web

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"yggspeedtest/internal/engine"
	"yggspeedtest/internal/store"
)

// failPeer is a peer that cannot connect anywhere: an unsupported scheme makes
// the handshake return before any network setup, so these tests are fast and
// work with no connectivity.
const failPeer = "ftp://unreachable.invalid"

func TestMain(m *testing.M) {
	// engine.Run logs through the process logger, which panics if unset.
	engine.InitLogger(false, true)
	os.Exit(m.Run())
}

func newTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	return newServerWith(t, "1s")
}

// newServerWith lengthens the handshake deadline so a test can hold a run open
// long enough to reach it while it is still in flight.
func newServerWith(t *testing.T, timeout string) (*Server, *httptest.Server) {
	t.Helper()
	dir := t.TempDir()
	cfg := store.Config{
		ListenAddr:   "127.0.0.1:18080",
		Peers:        []string{failPeer},
		TestURL:      "https://example.invalid/file.bin",
		Concurrency:  1,
		Streams:      1,
		MaxDuration:  "1s",
		Timeout:      timeout,
		RouteTimeout: "1s",
		SortBy:       "speed",
	}
	cfg.Normalise()

	s, err := New(cfg, Paths{
		Config: filepath.Join(dir, "config.json"),
		Runs:   filepath.Join(dir, "runs.jsonl"),
	}, "127.0.0.1:18080")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, httptest.NewServer(s.Handler())
}

func mustJSON(t *testing.T, r *httptest.ResponseRecorder, into any) {
	t.Helper()
	if r.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", r.Code, r.Body.String())
	}
}

func getJSON(t *testing.T, url string, into any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s: status %d, body %s", url, resp.StatusCode, body)
	}
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatal(err)
	}
}

func pPing(f float64) *engine.PingFloat   { v := engine.PingFloat(f); return &v }
func pSpeed(f float64) *engine.SpeedFloat { v := engine.SpeedFloat(f); return &v }

func TestHealthz(t *testing.T) {
	_, ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"status":"ok"`) {
		t.Errorf("body = %s", body)
	}
}

// The dashboard ships inside the binary, so it must arrive without a CDN and
// without any external asset, or an offline box sees a broken page.
func TestIndexServesSelfContainedDashboard(t *testing.T) {
	_, ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Errorf("Content-Type = %q", got)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{"YggSpeedTest", "立即测速", "/api/events", "cron"} {
		if !strings.Contains(text, want) {
			t.Errorf("dashboard missing %q", want)
		}
	}
	for _, banned := range []string{"<link", "src=", "cdn.", "googleapis.com", "unpkg.com"} {
		if strings.Contains(text, banned) {
			t.Errorf("dashboard references an external asset %q", banned)
		}
	}
}

func TestStatusShape(t *testing.T) {
	_, ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	type shape struct {
		Version    string         `json:"version"`
		ListenAddr string         `json:"listen_addr"`
		Running    bool           `json:"running"`
		Runs       int            `json:"runs"`
		Schedule   statusSchedule `json:"schedule"`
	}
	var got shape
	if err := decode(body, &got); err != nil {
		t.Fatalf("decode: %v\n%s", err, body)
	}
	if got.Version == "" || got.ListenAddr == "" {
		t.Errorf("shape = %+v", got)
	}
	if got.Running || got.Runs != 0 {
		t.Errorf("fresh server should be idle with no runs: %+v", got)
	}
	if got.Schedule.Enabled || got.Schedule.Valid {
		t.Errorf("no schedule was configured, got %+v", got.Schedule)
	}
}

func TestGetConfig(t *testing.T) {
	s, ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/config")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if got := strings.TrimSpace(string(body)); got != strings.TrimSpace(marshal(t, s.Config())) {
		t.Errorf("GET /api/config mismatch:\n%s\nvs\n%s", got, marshal(t, s.Config()))
	}
}

func TestSetConfigPersists(t *testing.T) {
	_, ts := newTestServer(t)
	defer ts.Close()

	in := `{
		"listen_addr":"127.0.0.1:18090",
		"peer":["tls://a:443","tls://b:443"],
		"test_url":"https://example.invalid/x.bin",
		"concurrency":2,
		"streams":8,
		"max_duration":"30s",
		"timeout":"1m",
		"route_timeout":"20s",
		"sort":"ping",
		"min_speed":12,
		"max_ping":900,
		"schedule":"@every 1h",
		"enabled":false,
		"public_peers":true
	}`
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/config", strings.NewReader(in))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}

	// The answer is the canonical form, which is what the UI edits next time.
	got, err := http.Get(ts.URL + "/api/config")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	body, _ := io.ReadAll(got.Body)
	text := string(body)
	for _, want := range []string{
		`"listen_addr":"127.0.0.1:18090"`, `"tls://a:443"`, `"concurrency":2`,
		`"streams":8`, `"max_duration":"30s"`, `"sort":"ping"`, `"schedule":"@every 1h"`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("saved config missing %q in %s", want, text)
		}
	}
}

func TestSetConfigRejects(t *testing.T) {
	_, ts := newTestServer(t)
	defer ts.Close()

	cases := []struct {
		name string
		body string
		code int
	}{
		{"bad json", `{nope`, http.StatusBadRequest},
		{"bad schedule", `{"schedule":"not a cron"}`, http.StatusUnprocessableEntity},
		{"bad sort", `{"sort":"bogus"}`, http.StatusUnprocessableEntity},
		{"bad timeout", `{"timeout":"ten"}`, http.StatusUnprocessableEntity},
		{"bad max bytes", `{"max_bytes":"1x"}`, http.StatusUnprocessableEntity},
		{"negative limit", `{"limit":-1}`, http.StatusUnprocessableEntity},
		{"negative max duration", `{"max_duration":"-1s"}`, http.StatusUnprocessableEntity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/config", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.code {
				body, _ := io.ReadAll(resp.Body)
				t.Errorf("status = %d, want %d; body = %s", resp.StatusCode, tc.code, body)
			}
		})
	}
}

// SetConfig must not persist a config that fails to load back.
func TestSetConfigRejectedConfigDoesNotPersist(t *testing.T) {
	_, ts := newTestServer(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/api/config", strings.NewReader(`{"sort":"bogus"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	got, err := http.Get(ts.URL + "/api/config")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	body, _ := io.ReadAll(got.Body)
	if strings.Contains(string(body), "bogus") {
		t.Errorf("a rejected config leaked into the live state: %s", body)
	}
}

// A run triggered over HTTP must come back as a run record, including one that
// cannot reach its peer. This exercises the whole path: handler, engine, store.
func TestRunLifecycle(t *testing.T) {
	_, ts := newTestServer(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/run", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	// The run is asynchronous: poll the history until it lands.
	var runs []runBrief
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		got, err := http.Get(ts.URL + "/api/runs")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(got.Body)
		got.Body.Close()
		if err := decode(body, &runs); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(runs) == 1 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if len(runs) != 1 {
		t.Fatalf("run did not land in the history")
	}
	rec := runs[0]
	if rec.Trigger != "manual" || rec.Peers != 1 {
		t.Errorf("run = %+v", rec)
	}
	if rec.Error == "" {
		t.Error("a peer that cannot be reached should record an error")
	}
	if rec.Duration <= 0 {
		t.Errorf("duration = %d", rec.Duration)
	}

	// The record is retrievable by id with its results attached.
	one, err := http.Get(ts.URL + "/api/runs/" + rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer one.Body.Close()
	oneBody, _ := io.ReadAll(one.Body)
	if !strings.Contains(string(oneBody), rec.ID) || !strings.Contains(string(oneBody), "Handshake failed") {
		t.Errorf("run detail = %s", oneBody)
	}
}

// The peer sits on a blackhole address, so its dial blocks until the deadline:
// that is what guarantees the run is still in flight when the cancel arrives,
// rather than leaving the overlap to luck.
func TestCancelRun(t *testing.T) {
	_, ts := newServerWith(t, "3s")
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/run",
		strings.NewReader(`{"peer":["tcp://10.255.255.1:443"]}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, body)
	}
	var started struct {
		ID string `json:"id"`
	}
	if err := decode(body, &started); err != nil {
		t.Fatal(err)
	}
	if started.ID == "" {
		t.Fatalf("no run id in %s", body)
	}

	cancel := func() int {
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/runs/"+started.ID+"/cancel", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if code := cancel(); code != http.StatusAccepted {
		t.Fatalf("cancel status = %d, want 202", code)
	}

	rec, err := waitForRun(ts.URL, started.ID, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// A cancelled run keeps whatever it measured and says so: without the
	// marker it would be indistinguishable from a run that stopped early.
	if !strings.Contains(rec.Error, "cancelled") {
		t.Errorf("cancelled run not marked: error = %q", rec.Error)
	}
	// The batch was interrupted, not failed: the summary must not claim every
	// peer came back with an error, since the peers that never got to start
	// did not.
	if strings.Contains(rec.Error, "all ") {
		t.Errorf("cancelled run claims every peer failed: error = %q", rec.Error)
	}
	// The dial would have burned the whole 3s deadline on its own. A shorter
	// duration is the proof that the cancel reached the in-flight test rather
	// than merely being recorded.
	if rec.Duration >= 2*time.Second {
		t.Errorf("duration = %v, want well under the 3s dial deadline", rec.Duration)
	}

	// Now that the run is over, cancelling it again finds nothing in flight.
	if code := cancel(); code != http.StatusNotFound {
		t.Errorf("cancel after the run finished = %d, want 404", code)
	}
}

func TestCancelRunRefusesUnknownID(t *testing.T) {
	_, ts := newTestServer(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/runs/nope/cancel", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// A batch interrupted halfway must not be reported as "all 49 peer(s) failed":
// that would claim the 44 peers that never got to start failed as well.
func TestCancelSummary(t *testing.T) {
	two := []engine.SpeedResult{{Peer: "a", Error: "x"}, {Peer: "b", Error: "x"}}
	tests := []struct {
		name    string
		results []engine.SpeedResult
		planned int
		want    string
	}{
		{name: "nothing measured", results: nil, planned: 49, want: "cancelled"},
		{name: "stopped before the batch ran out", results: two, planned: 49,
			want: "cancelled: 2 of 49 peers completed"},
		{name: "discovery ran, so the plan is unknown", results: two, planned: 0,
			want: "cancelled: 2 peer(s) completed"},
		{name: "plan and completion agree", results: two, planned: 2,
			want: "cancelled: 2 peer(s) completed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cancelSummary(tt.results, tt.planned); got != tt.want {
				t.Errorf("cancelSummary = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPlannedPeers(t *testing.T) {
	tests := []struct {
		name string
		rc   engine.RunConfig
		want int
	}{
		{name: "configured peers", rc: engine.RunConfig{Peers: []string{"a", "b", "c"}}, want: 3},
		{name: "limit caps the list", rc: engine.RunConfig{Peers: []string{"a", "b", "c"}, Limit: 2}, want: 2},
		{name: "limit above the list", rc: engine.RunConfig{Peers: []string{"a"}, Limit: 5}, want: 1},
		{name: "public peers stay unknown", rc: engine.RunConfig{Peers: []string{"a"}, PublicPeers: true}, want: 0},
		{name: "a peer file stays unknown", rc: engine.RunConfig{PeersFile: "peers.txt", Peers: []string{"a"}}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := plannedPeers(tt.rc); got != tt.want {
				t.Errorf("plannedPeers = %d, want %d", got, tt.want)
			}
		})
	}
}

// waitForRun polls /api/runs/{id} until the record is recorded or the deadline
// passes.
func waitForRun(base, id string, timeout time.Duration) (store.RunRecord, error) {
	var rec store.RunRecord
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/api/runs/" + id)
		if err != nil {
			return rec, err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			if err := decode(body, &rec); err != nil {
				return rec, err
			}
			return rec, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return rec, fmt.Errorf("run %s did not land in the history within %s", id, timeout)
}

func TestScheduledRunIsRecordedAsScheduled(t *testing.T) {
	s, ts := newTestServer(t)
	defer ts.Close()

	id, done, err := s.StartScheduledRun(context.Background())
	if err != nil {
		t.Fatalf("StartScheduledRun: %v", err)
	}
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("scheduled run did not finish")
	}

	runs, err := store.LoadRuns(s.paths.Runs)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].ID != id {
		t.Fatalf("runs = %+v, want exactly one record for %s", runs, id)
	}
	if runs[0].Trigger != "scheduled" {
		t.Errorf("trigger = %q, want %q", runs[0].Trigger, "scheduled")
	}
}

func TestRunBusyConflict(t *testing.T) {
	s, ts := newTestServer(t)
	defer ts.Close()

	// Hold the in-flight slot the way a live run would.
	s.jobMu.Lock()
	s.busy = true
	s.jobMu.Unlock()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/run", strings.NewReader(`{}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status = %d, want 409", resp.StatusCode)
	}
}

func TestStartRunRefusesWhenBusy(t *testing.T) {
	s, _ := newTestServer(t)
	s.jobMu.Lock()
	s.busy = true
	s.jobMu.Unlock()

	_, _, err := s.StartRun(context.Background(), nil)
	if !errors.Is(err, ErrBusy) {
		t.Errorf("StartRun = %v, want ErrBusy", err)
	}
}

func TestRunDetailNotFound(t *testing.T) {
	_, ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/runs/nope")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestRunCSV(t *testing.T) {
	s, ts := newTestServer(t)
	defer ts.Close()

	// Seed a record directly; the handler reads the same file the engine writes.
	rec := store.RunRecord{
		ID:        "test-run-1",
		StartedAt: time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC),
		Duration:  4 * time.Second,
		Trigger:   "manual",
		Peers:     1,
		Results:   []engine.SpeedResult{{Peer: failPeer, Error: "Handshake failed: boom"}},
	}
	if err := store.AppendRun(s.paths.Runs, rec, 0); err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(ts.URL + "/api/runs/test-run-1/csv")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/csv") {
		t.Errorf("Content-Type = %q", resp.Header.Get("Content-Type"))
	}
	if !strings.Contains(resp.Header.Get("Content-Disposition"), "test-run-1.csv") {
		t.Errorf("Content-Disposition = %q", resp.Header.Get("Content-Disposition"))
	}
	body, _ := io.ReadAll(resp.Body)
	// The BOM is what makes Excel detect UTF-8.
	if !strings.HasPrefix(string(body), "\uFEFFRank") {
		t.Errorf("csv = %q", string(body[:40]))
	}
	if !strings.Contains(string(body), "FAIL: Handshake failed: boom") {
		t.Errorf("csv missing the failure row: %s", body)
	}
}

func TestDeleteRun(t *testing.T) {
	s, ts := newTestServer(t)
	defer ts.Close()

	mk := func(id string) store.RunRecord {
		return store.RunRecord{ID: id, StartedAt: time.Now(), Trigger: "manual", Peers: 1}
	}
	for _, id := range []string{"r1", "r2", "r3"} {
		if err := store.AppendRun(s.paths.Runs, mk(id), 0); err != nil {
			t.Fatal(err)
		}
	}

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/api/runs/r2", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}

	got, err := http.Get(ts.URL + "/api/runs")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	body, _ := io.ReadAll(got.Body)
	if strings.Contains(string(body), "r2") {
		t.Errorf("r2 was not deleted: %s", body)
	}
	if !strings.Contains(string(body), "r1") || !strings.Contains(string(body), "r3") {
		t.Errorf("other runs disappeared: %s", body)
	}

	// Deleting the same id twice is a 404, not a silent success.
	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/api/runs/r2", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("second delete = %d, want 404", resp.StatusCode)
	}
}

func TestRunsAreNewestFirst(t *testing.T) {
	s, ts := newTestServer(t)
	defer ts.Close()

	for i, id := range []string{"old", "newer", "newest"} {
		rec := store.RunRecord{ID: id, StartedAt: time.Now().Add(time.Duration(i) * time.Second), Trigger: "manual"}
		if err := store.AppendRun(s.paths.Runs, rec, 0); err != nil {
			t.Fatal(err)
		}
	}

	got, err := http.Get(ts.URL + "/api/runs")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	body, _ := io.ReadAll(got.Body)
	text := string(body)
	iNewest, iNewer, iOld := strings.Index(text, "newest"), strings.Index(text, "newer"), strings.Index(text, "old")
	if iNewest < 0 || iNewer < 0 || iOld < 0 {
		t.Fatalf("missing runs in %s", text)
	}
	if !(iNewest < iNewer && iNewer < iOld) {
		t.Errorf("runs are not newest-first: newest=%d newer=%d old=%d", iNewest, iNewer, iOld)
	}
}

// The endpoint's whole point is a stable contract: the newest run's peers,
// fastest first, failures last. The server is deliberately saved with
// sort=ping so that a pass-through implementation would return a ping-ordered
// list here and fail, which pins the re-sort in the handler.
func TestLatestReturnsNewestSortedBySpeedDesc(t *testing.T) {
	dir := t.TempDir()
	cfg := store.Config{
		ListenAddr:   "127.0.0.1:18080",
		Peers:        []string{failPeer},
		TestURL:      "https://example.invalid/file.bin",
		Concurrency:  1,
		Streams:      1,
		MaxDuration:  "1s",
		Timeout:      "1s",
		RouteTimeout: "1s",
		SortBy:       "ping", // deliberately not the sort the endpoint honours
	}
	cfg.Normalise()
	s, err := New(cfg, Paths{
		Config: filepath.Join(dir, "config.json"),
		Runs:   filepath.Join(dir, "runs.jsonl"),
	}, "127.0.0.1:18080")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	newest := store.RunRecord{
		ID:        "run-2",
		StartedAt: time.Date(2026, 9, 22, 11, 0, 0, 0, time.UTC),
		Duration:  4 * time.Second,
		Trigger:   "manual",
		Peers:     4,
		Results: []engine.SpeedResult{
			{Peer: "peer-slowest", PingMs: pPing(1), DownloadMbps: pSpeed(2)},
			{Peer: "peer-fastest", PingMs: pPing(300), DownloadMbps: pSpeed(500)},
			{Peer: "peer-middle", PingMs: pPing(20), DownloadMbps: pSpeed(3)},
			{Peer: "peer-failed", Error: "Handshake failed: boom"},
		},
	}
	older := store.RunRecord{ID: "run-1", StartedAt: time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC), Trigger: "manual"}
	for _, rec := range []store.RunRecord{older, newest} {
		if err := store.AppendRun(s.paths.Runs, rec, 0); err != nil {
			t.Fatal(err)
		}
	}

	var got store.RunRecord
	getJSON(t, ts.URL+"/api/latest", &got)
	if got.ID != "run-2" {
		t.Fatalf("latest id = %q, want the newest run run-2", got.ID)
	}
	want := []string{"peer-fastest", "peer-middle", "peer-slowest", "peer-failed"}
	if len(got.Results) != len(want) {
		t.Fatalf("results = %d entries, want %d", len(got.Results), len(want))
	}
	for i, p := range want {
		if got.Results[i].Peer != p {
			t.Errorf("results[%d] = %q, want %q (order must be speed-descending, failures last)", i, got.Results[i].Peer, p)
		}
	}

	// The re-sort must not persist: a second request re-reads the file and still
	// comes back fastest-first.
	getJSON(t, ts.URL+"/api/latest", &got)
	if got.Results[0].Peer != "peer-fastest" {
		t.Errorf("second call: results[0] = %q, want peer-fastest", got.Results[0].Peer)
	}

	// /api/runs/{id} is unchanged: the persisted record keeps whatever order the
	// engine stored, so the ordering is this endpoint's, not the file's.
	var byID store.RunRecord
	getJSON(t, ts.URL+"/api/runs/run-2", &byID)
	if byID.Results[0].Peer != "peer-slowest" {
		t.Errorf("detail endpoint reordered results: got %q, want the stored order", byID.Results[0].Peer)
	}
}

func TestLatestEmptyHistoryIsNotFound(t *testing.T) {
	_, ts := newTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/latest")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestEventsStream(t *testing.T) {
	_, ts := newTestServer(t)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req = req.WithContext(context.Background())

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q", got)
	}

	// A late subscriber gets a hello frame immediately.
	rd := bufio.NewReader(resp.Body)
	line, err := rd.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if line != "event: hello\n" {
		t.Errorf("first frame = %q, want event: hello", line)
	}
	data, _ := rd.ReadString('\n')
	if !strings.HasPrefix(data, "data: ") || !strings.Contains(data, `"version"`) {
		t.Errorf("hello payload = %q", data)
	}
}

func TestScheduleReconciles(t *testing.T) {
	s, ts := newTestServer(t)
	defer ts.Close()

	if s.sched != nil {
		t.Fatal("no schedule was configured, so nothing should be scheduled")
	}

	cfg := s.Config()
	cfg.Schedule = "@every 1h"
	cfg.Enabled = true
	if err := s.SetConfig(cfg); err != nil {
		t.Fatalf("SetConfig: %v", err)
	}
	s.schedMu.Lock()
	enabled, hasSched := s.enabled, s.sched != nil
	s.schedMu.Unlock()
	if !enabled || !hasSched {
		t.Errorf("scheduler did not start: enabled=%v sched=%v", enabled, hasSched)
	}

	// Turning it off again must stop it.
	cfg.Enabled = false
	if err := s.SetConfig(cfg); err != nil {
		t.Fatalf("SetConfig: %v", err)
	}
	s.schedMu.Lock()
	enabled = s.enabled
	s.schedMu.Unlock()
	if enabled {
		t.Error("scheduler should be disabled now")
	}

	// And the status endpoint reflects it.
	got, err := http.Get(ts.URL + "/api/status")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	body, _ := io.ReadAll(got.Body)
	if !strings.Contains(string(body), `"enabled":false`) {
		t.Errorf("status = %s", body)
	}
}

func TestCancelStopsSchedulerAndRuns(t *testing.T) {
	s, ts := newTestServer(t)
	defer ts.Close()

	cfg := s.Config()
	cfg.Schedule = "@every 1h"
	cfg.Enabled = true
	if err := s.SetConfig(cfg); err != nil {
		t.Fatalf("SetConfig: %v", err)
	}

	select {
	case <-s.runCtx.Done():
		t.Fatal("run context cancelled before Cancel was called")
	default:
	}

	s.Cancel()
	select {
	case <-s.runCtx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Cancel did not cancel the run context")
	}
	// A cancelled run context must also stop the scheduler's loop.
	time.Sleep(50 * time.Millisecond)
	if s.Running() {
		t.Error("a run should not be in flight after Cancel")
	}
}

func TestFailureSummary(t *testing.T) {
	cases := []struct {
		name    string
		results []engine.SpeedResult
		want    string
	}{
		{"all measured", []engine.SpeedResult{{}, {}}, ""},
		{"one of two failed", []engine.SpeedResult{{Error: "boom"}, {}}, "1 of 2 peers failed: boom"},
		{"all failed", []engine.SpeedResult{{Error: "boom"}, {Error: "other"}}, "all 2 peer(s) failed: boom"},
		{"empty", []engine.SpeedResult{}, ""},
	}
	for _, c := range cases {
		if got := failureSummary(c.results); got != c.want {
			t.Errorf("%s: failureSummary = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestSummarise(t *testing.T) {
	pf := func(v float64) *engine.PingFloat { x := engine.PingFloat(v); return &x }
	sf := func(v float64) *engine.SpeedFloat { x := engine.SpeedFloat(v); return &x }

	avg, max, ping := summarise([]engine.SpeedResult{
		{DownloadMbps: sf(100), PeakMbps: sf(200), PingMs: pf(30)},
		{DownloadMbps: sf(300), PeakMbps: sf(150), PingMs: pf(50)},
		{Error: "failed"}, // excluded from the averages, counted nowhere
		{},                // no measurements at all
	})
	if avg != 200 {
		t.Errorf("avg = %v, want 200", avg)
	}
	if max != 200 {
		t.Errorf("max = %v, want 200", max)
	}
	if ping != 40 {
		t.Errorf("avg ping = %v, want 40", ping)
	}

	if a, m, p := summarise(nil); a != 0 || m != 0 || p != 0 {
		t.Errorf("empty summarise = %v %v %v", a, m, p)
	}
	if a, m, p := summarise([]engine.SpeedResult{{Error: "x"}}); a != 0 || m != 0 || p != 0 {
		t.Errorf("all-failed summarise = %v %v %v", a, m, p)
	}
}

func TestDirOf(t *testing.T) {
	for path, want := range map[string]string{
		"/var/lib/ygg/config.json": "/var/lib/ygg",
		"config.json":              "config.json",
		"C:\\data\\runs.jsonl":     "C:\\data",
	} {
		if got := dirOf(path); got != want {
			t.Errorf("dirOf(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestRunIDUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 2000; i++ {
		id := newRunID()
		if seen[id] {
			t.Fatalf("duplicate run id %q", id)
		}
		seen[id] = true
	}
}

// TestStatusReflectsSavedSchedule walks the reconciliation matrix. Expression
// must mirror whatever the operator saved on every change, Enabled must follow
// the switch, and Valid must describe the expression alone — a merely switched
// off plan is still a valid one. Before the fix, expression came from a cached
// parse and survived a disable, so status reported the schedule it had stopped
// running as if it were still armed.
func TestStatusReflectsSavedSchedule(t *testing.T) {
	s, ts := newTestServer(t)
	defer ts.Close()

	cfg := s.Config()

	set := func(schedule string, enabled bool) {
		t.Helper()
		cfg = s.Config()
		cfg.Schedule = schedule
		cfg.Enabled = enabled
		if err := s.SetConfig(cfg); err != nil {
			t.Fatalf("SetConfig(%q, %v): %v", schedule, enabled, err)
		}
	}

	// Nothing saved at all: no plan, nothing armed, nothing valid.
	st := s.scheduleStatus()
	if st.Expression != "" || st.Enabled || st.Valid {
		t.Fatalf("empty config reports %+v", st)
	}

	set("@every 10m", true)
	st = s.scheduleStatus()
	if !st.Enabled || !st.Valid || st.Expression != "@every 10m" {
		t.Fatalf("armed schedule reports %+v", st)
	}
	if st.Next == nil {
		t.Errorf("an armed schedule must report a next firing: %+v", st)
	}

	// Switched off: the saved plan stays visible, but it is no longer armed and
	// nothing is due.
	set("@every 10m", false)
	st = s.scheduleStatus()
	if st.Enabled {
		t.Errorf("a disabled schedule reports enabled: %+v", st)
	}
	if !st.Valid {
		t.Errorf("a merely disabled schedule is still a valid expression: %+v", st)
	}
	if st.Expression != "@every 10m" {
		t.Errorf("expression = %q, want the saved plan %q", st.Expression, "@every 10m")
	}
	if st.Next != nil {
		t.Errorf("a disarmed scheduler must not report a next firing: %+v", st)
	}

	// Editing the expression while disarmed must be reflected immediately — this
	// is the staleness the fix removes.
	set("@every 45m", false)
	if got := s.scheduleStatus().Expression; got != "@every 45m" {
		t.Errorf("expression after an edit while disarmed = %q, want %q", got, "@every 45m")
	}

	// A 5-field cron through the same path.
	set("0 9 * * 1-5", true)
	st = s.scheduleStatus()
	if !st.Enabled || st.Expression != "0 9 * * 1-5" || st.Next == nil {
		t.Errorf("cron schedule reports %+v", st)
	}

	// Clearing the expression clears the plan and the validity.
	set("", true)
	st = s.scheduleStatus()
	if st.Expression != "" || st.Enabled || st.Valid {
		t.Fatalf("cleared schedule reports %+v", st)
	}
}

// TestScheduleChangeRetimesLiveScheduler covers editing the expression while
// the scheduler is already armed. Before the fix reconcileScheduler called
// Start, which is a no-op on a running scheduler, so the operator's new
// interval was reported by /api/status while the old one kept firing — the UI
// said @every 5m and the machine ran @every 1m.
func TestScheduleChangeRetimesLiveScheduler(t *testing.T) {
	s, ts := newTestServer(t)
	defer ts.Close()

	expr := func() string {
		s.schedMu.Lock()
		defer s.schedMu.Unlock()
		if s.sched == nil {
			return ""
		}
		return s.sched.Spec().Raw
	}

	cfg := s.Config()
	cfg.Schedule = "@every 1m"
	cfg.Enabled = true
	if err := s.SetConfig(cfg); err != nil {
		t.Fatalf("SetConfig: %v", err)
	}
	if got := expr(); got != "@every 1m" {
		t.Fatalf("scheduler spec = %q, want %q", got, "@every 1m")
	}

	cfg = s.Config()
	cfg.Schedule = "@every 5m"
	if err := s.SetConfig(cfg); err != nil {
		t.Fatalf("SetConfig: %v", err)
	}
	if got := expr(); got != "@every 5m" {
		t.Errorf("scheduler spec after the change = %q, want %q; the old cadence is still firing", got, "@every 5m")
	}
	if got := s.scheduleStatus().Expression; got != "@every 5m" {
		t.Errorf("status expression = %q, want %q", got, "@every 5m")
	}
	if st := s.scheduleStatus(); !st.Enabled || st.Next == nil {
		t.Errorf("rearming lost the plan or armed it off: %+v", st)
	}
	s.Cancel()
}

func decode(body []byte, into any) error {
	return json.Unmarshal(body, into)
}

func marshal(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(data)
}
