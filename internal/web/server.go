// Package web is the HTTP front-end: a REST API, a server-sent event stream,
// and the embedded dashboard. It owns no measurement logic of its own; every
// run goes through internal/engine, so the CLI and the web UI cannot drift
// apart in what they measure.
package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"yggspeedtest/internal/engine"
	"yggspeedtest/internal/schedule"
	"yggspeedtest/internal/store"
)

// runSeq numbers run ids within one process.
var runSeq uint64

// ErrBusy is returned by StartRun when a run is already in flight. The web
// layer maps it to HTTP 409.
var ErrBusy = errors.New("a speed test is already running")

// The trigger recorded on every run record.
const (
	triggerManual    = "manual"
	triggerScheduled = "scheduled"
)

// Paths holds where the server keeps its state on disk.
type Paths struct {
	Config string
	Runs   string
}

// Server is one running web front-end.
type Server struct {
	paths      Paths
	listenAddr string
	started    time.Time
	hub        *hub

	// runCtx is the lifetime of every run this server starts, whether a
	// browser pressed the button or the scheduler fired. Cancel stops the
	// scheduler and asks in-flight downloads to unwind, which is what Ctrl-C
	// wants.
	runCtx    context.Context
	cancelRun context.CancelFunc

	cfgMu sync.Mutex
	cfg   store.Config

	jobMu         sync.Mutex
	busy          bool
	current       string
	currentCancel context.CancelFunc

	schedMu sync.Mutex
	sched   *schedule.Scheduler
	// spec mirrors the saved expression on every reconciliation, armed or not,
	// so status never reports a plan the operator already changed. planValid is
	// separate from enabled: a valid plan can be switched off, and conflating
	// the two made the UI label a merely-disabled schedule "invalid".
	spec      schedule.Spec
	planValid bool
	enabled   bool

	storeMu sync.Mutex // serialises access to the config and runs files

	// shutdownCh is closed by Cancel. The SSE handlers watch it: without it,
	// Server.Shutdown in the main would wait out its whole timeout on every
	// open dashboard tab, because an event stream never ends on its own.
	shutdownCh   chan struct{}
	shutdownOnce sync.Once
}

// Option tunes a Server before Run.
type Option func(*Server)

// New builds a Server from a persisted config.
func New(cfg store.Config, paths Paths, listenAddr string, opts ...Option) (*Server, error) {
	runCtx, cancelRun := context.WithCancel(context.Background())
	s := &Server{
		paths:      paths,
		listenAddr: listenAddr,
		started:    time.Now(),
		hub:        newHub(),
		cfg:        cfg,
		runCtx:     runCtx,
		cancelRun:  cancelRun,
		shutdownCh: make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	s.reconcileScheduler()
	return s, nil
}

// Cancel stops the scheduler and asks every in-flight run to stop, then
// releases the event streams so an HTTP shutdown is not stuck waiting on them.
// It does not wait for the run to finish; a bounded batch unwinds on its own.
func (s *Server) Cancel() {
	s.shutdownOnce.Do(func() { close(s.shutdownCh) })
	s.cancelRun()
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// Config returns a copy of the live configuration.
func (s *Server) Config() store.Config {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	return s.cfg
}

// SetConfig validates, persists and activates a configuration. It is the only
// way a config change reaches disk, so the file and the process never disagree.
func (s *Server) SetConfig(cfg store.Config) error {
	cfg.Normalise()

	// The run parameters must hold up, otherwise the next run fails for the
	// operator only after they pressed the button.
	rc, err := cfg.ToRunConfig(nil)
	if err != nil {
		return err
	}
	if err := rc.Validate(); err != nil {
		return err
	}

	if cfg.HasSchedule() {
		if _, err := schedule.Parse(cfg.Schedule); err != nil {
			return fmt.Errorf("invalid schedule %q: %w", cfg.Schedule, err)
		}
	}

	// The save and the in-memory swap happen under one lock acquisition, so
	// two concurrent saves cannot leave the file holding one config while the
	// process runs with another.
	s.storeMu.Lock()
	if err := cfg.Save(s.paths.Config); err != nil {
		s.storeMu.Unlock()
		return err
	}
	s.cfgMu.Lock()
	s.cfg = cfg
	s.cfgMu.Unlock()
	s.storeMu.Unlock()

	// The debug switch is applied to the running process, not just stored:
	// the UI toggle would otherwise be a write-only setting.
	engine.SetDebug(cfg.Debug)

	s.reconcileScheduler()
	s.broadcast("schedule", s.scheduleStatus())
	return nil
}

// ReconcileScheduler brings the scheduler in line with the saved config.
// The saved expression is parsed whether or not it is armed: scheduleStatus
// reports Expression from s.spec, so s.spec must always mirror the config the
// operator can actually see — leaving a stale one in place made status claim
// "@every 1m" for a plan the operator had already changed.
func (s *Server) reconcileScheduler() {
	s.schedMu.Lock()
	defer s.schedMu.Unlock()

	s.cfgMu.Lock()
	cfg := s.cfg
	s.cfgMu.Unlock()

	if !cfg.HasSchedule() {
		s.spec = schedule.Spec{}
		s.planValid = false
		s.enabled = false
		s.stopScheduler()
		return
	}

	spec, err := schedule.Parse(cfg.Schedule)
	if err != nil {
		// SetConfig rejects an unparsable expression, but the file can be edited
		// out from under us. Report it as invalid and keep the scheduler off
		// rather than guessing at the intent.
		s.spec = schedule.Spec{}
		s.planValid = false
		s.enabled = false
		s.stopScheduler()
		return
	}

	s.spec = spec
	s.planValid = true
	if !cfg.Enabled {
		s.enabled = false
		s.stopScheduler()
		return
	}

	s.enabled = true
	if s.sched == nil {
		s.sched = schedule.NewScheduler(spec, s.scheduledRun)
		s.sched.Start(s.runCtx)
		return
	}
	// Already armed. Start is a no-op on a running scheduler, so calling it here
	// would keep firing on the previous interval while status advertised the new
	// one — the plan nobody asked for. Swap it in and let the loop re-time.
	s.sched.SetSpec(spec)
}

// stopScheduler stops and drops the scheduler without touching the plan that is
// recorded for status, which must describe the saved expression either way.
func (s *Server) stopScheduler() {
	if s.sched != nil {
		s.sched.Stop()
		s.sched = nil
	}
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

// StartRun triggers a run. peers, when non-nil, replaces the saved peer list;
// it may be an empty slice, which runs with the saved peer file or the public
// list instead. It returns the run id and a channel closed when the run ends.
func (s *Server) StartRun(ctx context.Context, peers []string) (string, <-chan struct{}, error) {
	return s.startRun(triggerManual, ctx, peers)
}

// StartScheduledRun is StartRun on the scheduler's behalf: same rules, but the
// record says who asked for it, which is the only way to tell an automatic
// batch apart from one a person pressed the button on.
func (s *Server) StartScheduledRun(ctx context.Context) (string, <-chan struct{}, error) {
	return s.startRun(triggerScheduled, ctx, nil)
}

func (s *Server) startRun(trigger string, ctx context.Context, peers []string) (string, <-chan struct{}, error) {
	s.jobMu.Lock()
	if s.busy {
		s.jobMu.Unlock()
		return "", nil, ErrBusy
	}
	// The run gets its own cancellable context so CancelRun can stop it without
	// cancelling s.runCtx, which would abort every later run for life.
	runCtx, runCancel := context.WithCancel(ctx)

	s.busy = true
	id := newRunID()
	s.current = id
	s.currentCancel = runCancel
	s.jobMu.Unlock()

	done := make(chan struct{})
	started := time.Now()

	// finish is idempotent: the panic guard below must not append a second
	// record when execute had already persisted one before unwinding.
	finished := &atomic.Bool{}
	finish := func(rec store.RunRecord) {
		if finished.Swap(true) {
			return
		}
		s.finish(rec)
	}

	go func() {
		defer close(done)
		defer runCancel()
		defer s.releaseRun()
		defer func() {
			if r := recover(); r != nil {
				// A panic inside one measurement must not take the whole
				// server down; the run still leaves a visible, failed record.
				engine.LogError("Run panicked and was aborted",
					zap.String("id", id), zap.Any("panic", r), zap.Stack("stack"))
				finish(store.RunRecord{
					ID:        id,
					StartedAt: started,
					Duration:  time.Since(started).Round(time.Millisecond),
					Trigger:   trigger,
					Error:     fmt.Sprintf("internal error: %v", r),
					Results:   []engine.SpeedResult{},
				})
			}
		}()
		s.execute(id, trigger, runCtx, peers, started, finish)
	}()
	return id, done, nil
}

// scheduledRun is the scheduler's job. It runs to completion so the scheduler
// can never start a run on top of one that is still going.
func (s *Server) scheduledRun(ctx context.Context) error {
	_, done, err := s.StartScheduledRun(ctx)
	if err != nil {
		return err
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) releaseRun() {
	s.jobMu.Lock()
	s.busy = false
	s.current = ""
	s.currentCancel = nil
	s.jobMu.Unlock()
}

// cancelCurrent cancels the in-flight run. Caller must hold jobMu.
func (s *Server) cancelCurrent() {
	if s.currentCancel != nil {
		s.currentCancel()
		s.currentCancel = nil
	}
}

// Running reports whether a run is in flight.
func (s *Server) Running() bool {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()
	return s.busy
}

// CancelRun stops the run with id. It reports whether a run with that id was
// in flight: a finished run is not an error, it is just already over.
func (s *Server) CancelRun(id string) bool {
	s.jobMu.Lock()
	if s.busy && s.current == id {
		s.cancelCurrent()
		s.jobMu.Unlock()
		return true
	}
	s.jobMu.Unlock()
	return false
}

// execute is one batch, from parameters to a persisted run record. finish is
// supplied by the caller so the panic guard can share the once-only contract.
func (s *Server) execute(id, trigger string, ctx context.Context, peers []string, started time.Time, finish func(store.RunRecord)) {
	cfg := s.Config()
	rc, err := cfg.ToRunConfig(peers)

	rec := store.RunRecord{
		ID:        id,
		StartedAt: started,
		Trigger:   trigger,
		Results:   []engine.SpeedResult{},
	}

	s.broadcast("run_started", runStartedEvent{ID: id, Trigger: trigger, At: started})

	failFast := func(reason string) {
		rec.Error = reason
		rec.Duration = time.Since(started).Round(time.Millisecond)
		finish(rec)
	}

	if err != nil {
		failFast(err.Error())
		return
	}
	if err := rc.Validate(); err != nil {
		failFast(err.Error())
		return
	}

	// OnResult is called concurrently by one goroutine per peer, so it only
	// forwards: every write to shared state stays in this goroutine.
	rc.OnResult = func(res engine.SpeedResult) {
		s.broadcast("peer_done", peerEvent{ID: id, Result: res})
	}

	results, err := engine.Run(ctx, rc)
	if err != nil {
		rec.Error = err.Error()
	} else {
		rec.Error = failureSummary(results)
	}
	if cerr := ctx.Err(); cerr != nil {
		// engine.Run returns whatever it had measured, with err == nil, when its
		// context is cancelled, so a cancelled run would otherwise be
		// indistinguishable from one that simply stopped early.
		rec.Error = cancelSummary(results, plannedPeers(rc))
	}
	rec.Peers = len(results)
	rec.AvgMbps, rec.MaxMbps, rec.AvgPingMs = summarise(results)
	rec.Duration = time.Since(started).Round(time.Millisecond)
	rec.Results = results
	finish(rec)
}

// finish persists a run record and announces the outcome.
func (s *Server) finish(rec store.RunRecord) {
	s.storeMu.Lock()
	limit := s.cfg.HistoryLimit
	if err := store.AppendRun(s.paths.Runs, rec, limit); err != nil {
		engine.LogError("Failed to record run history", zap.Error(err), zap.String("id", rec.ID))
	}
	s.storeMu.Unlock()

	s.broadcast("run_finished", map[string]any{
		"id": rec.ID, "trigger": rec.Trigger, "peers": rec.Peers,
		"duration_ms": rec.Duration.Milliseconds(),
		"avg_mbps":    rec.AvgMbps, "max_mbps": rec.MaxMbps,
		"avg_ping_ms": rec.AvgPingMs,
		"error":       rec.Error,
	})
	s.broadcast("schedule", s.scheduleStatus())
}

// failureSummary turns per-peer failures into the one line the history list
// shows. engine.Run itself succeeds when individual peers fail, so without
// this the history would show a run that measured nothing and look healthy.
// cancelSummary is the marker a stopped run gets. It does not reuse
// failureSummary, which counts the peers that actually ran: on a batch
// interrupted halfway, "all 49 peer(s) failed" would be a lie about the 44
// that never got to start.
func cancelSummary(results []engine.SpeedResult, planned int) string {
	completed := len(results)
	if completed == 0 {
		return "cancelled"
	}
	if planned > completed {
		return fmt.Sprintf("cancelled: %d of %d peers completed", completed, planned)
	}
	return fmt.Sprintf("cancelled: %d peer(s) completed", completed)
}

// plannedPeers is how many peers a batch intended to test, when that can be
// answered from the config alone. Peer discovery goes over the network, so for
// a run built from public peers the number is unknown and stays zero.
func plannedPeers(rc engine.RunConfig) int {
	if rc.PeersFile != "" || rc.PublicPeers {
		return 0
	}
	planned := len(rc.Peers)
	if rc.Limit > 0 && planned > rc.Limit {
		planned = rc.Limit
	}
	return planned
}

// failureSummary reports what happened to a run that ran to the end.
func failureSummary(results []engine.SpeedResult) string {
	failed := 0
	var first string
	for _, r := range results {
		if r.Error != "" {
			failed++
			if first == "" {
				first = r.Error
			}
		}
	}
	switch {
	case failed == 0:
		return ""
	case failed == len(results):
		return fmt.Sprintf("all %d peer(s) failed: %s", failed, first)
	default:
		return fmt.Sprintf("%d of %d peers failed: %s", failed, len(results), first)
	}
}

// summarise collapses a result set into the three numbers the dashboard
// charts. Failed peers are excluded from the averages but not from the count.
func summarise(results []engine.SpeedResult) (avg, max, avgPing float64) {
	var speedSum, pingSum float64
	var speedN, pingN int
	for _, r := range results {
		if r.Error != "" {
			continue
		}
		if r.DownloadMbps != nil {
			speedSum += float64(*r.DownloadMbps)
			speedN++
		}
		if r.PeakMbps != nil && float64(*r.PeakMbps) > max {
			max = float64(*r.PeakMbps)
		}
		if r.PingMs != nil {
			pingSum += float64(*r.PingMs)
			pingN++
		}
	}
	if speedN > 0 {
		avg = round3(speedSum / float64(speedN))
	}
	if pingN > 0 {
		avgPing = round2(pingSum / float64(pingN))
	}
	return avg, round3(max), avgPing
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

// Handler builds the route table.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", s.handleIndex)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /api/status", s.handleStatus)
	mux.HandleFunc("GET /api/config", s.handleGetConfig)
	mux.HandleFunc("PUT /api/config", s.handleSetConfig)
	mux.HandleFunc("POST /api/run", s.handleRun)
	mux.HandleFunc("POST /api/runs/{id}/cancel", s.handleCancel)
	mux.HandleFunc("GET /api/latest", s.handleLatest)
	mux.HandleFunc("GET /api/runs", s.handleRuns)
	mux.HandleFunc("GET /api/runs/{id}", s.handleRunOne)
	mux.HandleFunc("DELETE /api/runs/{id}", s.handleDeleteRun)
	mux.HandleFunc("GET /api/runs/{id}/csv", s.handleRunCSV)
	mux.HandleFunc("GET /api/events", s.handleEvents)

	return withCORS(mux)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	data, err := ui.ReadFile("webui/index.html")
	if err != nil {
		http.Error(w, "dashboard missing from this build", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(data)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "version": engine.Version})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.jobMu.Lock()
	busy, current := s.busy, s.current
	s.jobMu.Unlock()

	status := statusResp{
		Version:    engine.Version,
		ListenAddr: s.listenAddr,
		DataDir:    dirOf(s.paths.Config),
		Running:    busy,
		CurrentRun: current,
		StartedAt:  s.started,
		Schedule:   s.scheduleStatus(),
	}
	if n, err := store.CountRuns(s.paths.Runs); err == nil {
		status.Runs = n
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Config())
}

func (s *Server) handleSetConfig(w http.ResponseWriter, r *http.Request) {
	var cfg store.Config
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&cfg); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := s.SetConfig(cfg); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.Config())
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Peers []string `json:"peer"`
	}
	// An absent body is a valid "run the saved config" request, but a body
	// that exists and does not parse is a client mistake: silently ignoring it
	// would start a run with the saved peers when the caller asked for
	// something else.
	data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	if len(strings.TrimSpace(string(data))) > 0 {
		if err := json.Unmarshal(data, &body); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
		for _, p := range body.Peers {
			if strings.TrimSpace(p) == "" {
				writeError(w, http.StatusBadRequest, "peer list contains an empty entry")
				return
			}
		}
	}

	// The run is a server-side job, not part of this request. Using the
	// server's own context instead of r.Context() means a browser that closes
	// the tab, or a proxy that times out the POST, cannot abort a measurement
	// the user deliberately started — with r.Context() the handler returns
	// first, the context arrives already cancelled, zero peers get dispatched,
	// and the history gains an empty record.
	id, _, err := s.StartRun(s.runCtx, body.Peers)
	if errors.Is(err, ErrBusy) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"id": id})
}

// handleCancel stops an in-flight run. A run that already finished is not an
// error, just already done, so it is a plain 404 rather than a 409.
func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.CancelRun(id) {
		writeError(w, http.StatusNotFound, "no run in progress")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"id": id})
}

// loadRunsOrError loads the history and writes the 500 itself, so the handlers
// that only read stay two lines instead of four.
func (s *Server) loadRunsOrError(w http.ResponseWriter) ([]store.RunRecord, bool) {
	runs, err := store.LoadRuns(s.paths.Runs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return nil, false
	}
	return runs, true
}

// handleLatest returns the most recent run together with its peer-level results
// sorted by download speed, fastest first. The sort is re-applied here instead
// of trusting the saved config: a client asking for "fastest first" must not
// receive a ping-ordered list because the operator happens to have sort=ping
// saved, so this endpoint's ordering is a contract rather than a pass-through.
// LatestRun decodes a fresh record per request, so sorting in place is safe.
func (s *Server) handleLatest(w http.ResponseWriter, r *http.Request) {
	rec, ok, err := store.LatestRun(s.paths.Runs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "no runs recorded yet")
		return
	}

	engine.SortResults(rec.Results, "speed")
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) handleRuns(w http.ResponseWriter, r *http.Request) {
	runs, ok := s.loadRunsOrError(w)
	if !ok {
		return
	}
	// Newest first, which is what a history list is for.
	outRuns := make([]runBrief, 0, len(runs))
	for i := len(runs) - 1; i >= 0; i-- {
		rec := runs[i]
		outRuns = append(outRuns, runBrief{
			ID: rec.ID, StartedAt: rec.StartedAt, Duration: rec.Duration.Milliseconds(),
			Trigger: rec.Trigger, Peers: rec.Peers,
			AvgMbps: rec.AvgMbps, MaxMbps: rec.MaxMbps, AvgPingMs: rec.AvgPingMs, Error: rec.Error,
		})
	}
	writeJSON(w, http.StatusOK, outRuns)
}

func (s *Server) handleRunOne(w http.ResponseWriter, r *http.Request) {
	rec, ok, err := store.FindRun(s.paths.Runs, r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) handleDeleteRun(w http.ResponseWriter, r *http.Request) {
	// DeleteRun does the load and the rewrite in one call, and the lock spans
	// both: the history file is rewritten whole, so a snapshot taken outside
	// the lock would erase a record that finish() appended in the meantime.
	s.storeMu.Lock()
	found, err := store.DeleteRun(s.paths.Runs, r.PathValue("id"))
	s.storeMu.Unlock()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRunCSV(w http.ResponseWriter, r *http.Request) {
	rec, ok, err := store.FindRun(s.paths.Runs, r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s.csv", rec.ID))
	if err := engine.ExportCSV(w, rec.Results); err != nil {
		engine.LogError("Failed to write CSV", zap.Error(err))
	}
}

// handleEvents streams run progress as server-sent events.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch := s.hub.subscribe()
	defer s.hub.unsubscribe(ch)

	// A late subscriber still gets the current shape of the world.
	writeSSE(w, "hello", map[string]any{"version": engine.Version, "time": time.Now()})
	flusher.Flush()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.shutdownCh:
			// The server is going down: end the stream so http.Server.Shutdown
			// is not left waiting out its timeout on every open dashboard tab.
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			writeSSE(w, msg.event, msg.data)
			flusher.Flush()
		case <-ticker.C:
			// Keeps proxies and browsers from closing an idle stream.
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// scheduleStatus is the scheduler's state as the UI reads it. Expression and
// Valid describe the saved plan; Enabled describes whether it is armed.
func (s *Server) scheduleStatus() statusSchedule {
	s.schedMu.Lock()
	defer s.schedMu.Unlock()

	st := statusSchedule{
		Enabled:    s.enabled,
		Expression: s.spec.Describe(),
		Valid:      s.planValid,
	}
	if s.sched == nil {
		return st
	}
	st.LastRun = s.sched.LastRun()
	st.Runs = s.sched.RunCount()
	st.Skipped = s.sched.SkippedCount()
	if next, ok := s.sched.Next(); ok {
		st.Next = &next
	}
	if err := s.sched.LastError(); err != nil {
		st.Error = err.Error()
	}
	return st
}

func (s *Server) broadcast(event string, v any) {
	s.hub.publish(event, v)
}

// dirOf reports the directory holding a state file, for display purposes.
func dirOf(path string) string {
	i := len(path) - 1
	for i > 0 && path[i] != '/' && path[i] != '\\' {
		i--
	}
	if i == 0 {
		return path
	}
	return path[:i]
}

// withCORS adds permissive headers for local browsing. The default bind is
// loopback, so this is not a network exposure by itself.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write(data)
	_, _ = w.Write([]byte("\n"))
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeSSE(w http.ResponseWriter, event string, data any) {
	payload, err := json.Marshal(data)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, payload)
}

func newRunID() string {
	// A counter plus a timestamp: readable, sortable, and unique for one
	// process. It avoids crypto/rand because a run label needs no secrecy.
	n := atomic.AddUint64(&runSeq, 1)
	return fmt.Sprintf("run-%s-%04x", time.Now().Format("20060102-150405"), n&0xFFFF)
}
