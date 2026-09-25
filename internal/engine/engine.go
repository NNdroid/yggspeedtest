package engine

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/yggdrasil-network/yggdrasil-go/src/config"
	"yggspeedtest/netstack"
)

var Version = "v2.0.20260922"
var zlog *zap.Logger
var nextAdminPort int32

// logLevel is the live handle on the process-wide log level. The web UI's
// "debug log" switch flips it at runtime through SetDebug, so the saved
// setting takes effect without a restart.
var logLevel zap.AtomicLevel

// logBase is the non-debug level the process was started with (Error for
// quiet, Info otherwise); turning debug back off returns to it.
var logBase = zapcore.InfoLevel

const (
	peakSampleInterval = 200 * time.Millisecond
	adminPortBase      = 20000
	adminPortCount     = 30000
)

// RunConfig carries everything a single batch speed test needs. The CLI flags
// and the web server's saved config both map onto it, so the two front-ends
// cannot drift apart in what they measure.
type RunConfig struct {
	Peers       []string
	PeersFile   string
	PublicPeers bool
	Limit       int

	TestURL    string
	CustomHost string
	CustomSNI  string
	CustomDNS  string

	Concurrency  int
	Streams      int
	MaxDuration  time.Duration
	MaxBytes     int64
	Timeout      time.Duration
	RouteTimeout time.Duration

	SortBy   string
	MinSpeed float64
	MaxPing  float64

	KeyFile    string
	Checkpoint string
	Quiet      bool

	// OnResult, when set, receives every result as soon as its peer finishes,
	// before filtering or sorting are applied. The web server uses it to push
	// progress over the event stream; the CLI leaves it nil.
	OnResult func(SpeedResult)
}

// Validate rejects combinations that would either deadlock, panic, or silently
// measure the wrong thing. The CLI parses these into strings first, so the
// message here is the one a user actually reads.
func (cfg *RunConfig) Validate() error {
	if cfg.TestURL == "" {
		return errors.New("missing test URL")
	}
	// Reject malformed URLs up front: an URL that only fails to parse inside
	// runPeerTest would otherwise spin up a full Yggdrasil node per peer before
	// discovering the problem.
	u, err := url.Parse(cfg.TestURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("invalid test URL %q: must be an absolute http(s) URL", cfg.TestURL)
	}
	if cfg.Concurrency < 1 {
		return errors.New("concurrency must be at least 1")
	}
	if cfg.Streams < 1 {
		return errors.New("streams must be at least 1")
	}
	switch strings.ToLower(cfg.SortBy) {
	case "speed", "peak", "ping", "handshake":
	default:
		return fmt.Errorf("invalid sort value %q, must be one of: speed, peak, ping, handshake", cfg.SortBy)
	}
	if cfg.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if cfg.RouteTimeout <= 0 {
		return errors.New("route timeout must be positive")
	}
	if cfg.MaxDuration < 0 {
		return errors.New("max duration must not be negative")
	}
	if cfg.MaxBytes < 0 {
		return errors.New("max bytes must not be negative")
	}
	if cfg.Limit < 0 {
		return errors.New("limit must not be negative")
	}
	return nil
}

// isYggdrasilAddr reports whether ip falls into 200::/7, the range the
// Yggdrasil network routes. The built-in gVisor netstack only has a route for
// this range, so a download target outside it can never be reached and every
// peer would fail with "network is unreachable" after wasting its full
// handshake and route-convergence budget.
func isYggdrasilAddr(ip net.IP) bool {
	b := ip.To16()
	if b == nil || ip.To4() != nil {
		return false
	}
	return b[0]&0xFE == 0x02
}

// warnIfUnreachableTarget gives an early, human-readable warning when the test
// URL points outside the Yggdrasil network. It is best effort on purpose: with
// a custom in-Yggdrasil DNS the per-peer resolution can legitimately differ
// from this one, so the check stays silent whenever it cannot be certain.
func warnIfUnreachableTarget(ctx context.Context, testURL, customDNS string) {
	if customDNS != "" {
		return
	}
	u, err := url.Parse(testURL)
	if err != nil || u.Hostname() == "" {
		return
	}
	if ip := net.ParseIP(u.Hostname()); ip != nil {
		if !isYggdrasilAddr(ip) {
			logUnreachableTarget(u.Hostname())
		}
		return
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ips, err := net.DefaultResolver.LookupIPAddr(lookupCtx, u.Hostname())
	if err != nil || len(ips) == 0 {
		return
	}
	for _, ip := range ips {
		if isYggdrasilAddr(ip.IP) {
			return
		}
	}
	logUnreachableTarget(u.Hostname())
}

func logUnreachableTarget(host string) {
	zlog.Warn("Test URL target is outside the Yggdrasil range 200::/7; every peer will fail",
		zap.String("host", host),
		zap.String("hint", "the built-in userspace netstack can only download from Yggdrasil-internal addresses; point -url at a host inside the Yggdrasil network"))
}

// PeerSources gathers and deduplicates the peer list from every configured
// source. The public list is fetched only when nothing was supplied, so an
// interrupted fetch never throws away the operator's own list.
func PeerSources(ctx context.Context, cfg RunConfig) ([]string, error) {
	var peers []string
	if cfg.PeersFile != "" {
		filePeers, err := readPeersFromFile(cfg.PeersFile)
		if err != nil {
			return nil, fmt.Errorf("read peers file: %w", err)
		}
		peers = append(peers, filePeers...)
	}
	peers = append(peers, cfg.Peers...)

	// Deduplicate before deciding whether the public list is needed, so
	// duplicates in -file don't count against -limit.
	peers = deduplicatePeers(peers)

	if cfg.PublicPeers || len(peers) == 0 {
		zlog.Info("Fetching public Yggdrasil peers from online repository")
		pubPeers, fetchErr := fetchPublicPeers(ctx)
		// A failed fetch is not fatal: the fallback list is returned alongside
		// it, so a run can still start. It just means the sample is small and
		// stale.
		if fetchErr != nil {
			zlog.Warn("Public peer sources unreachable, using the built-in fallback list",
				zap.Error(fetchErr), zap.Int("fallback_count", len(pubPeers)))
		}
		peers = deduplicatePeers(append(peers, pubPeers...))
		zlog.Info("Loaded public peers", zap.Int("count", len(pubPeers)))
	}

	return peers, nil
}

// loadOrGenerateKey pins this node's identity to keyFile when the operator
// asked for one, so re-running a batch measures the same node.
func loadOrGenerateKey(keyFile string, cfg *config.NodeConfig) error {
	if _, err := os.Stat(keyFile); errors.Is(err, os.ErrNotExist) {
		privHex := hex.EncodeToString(cfg.PrivateKey)
		if err := os.WriteFile(keyFile, []byte(privHex), 0600); err != nil {
			return fmt.Errorf("write private key: %w", err)
		}
		zlog.Info("Generated new private key", zap.String("file", keyFile))
		return nil
	}
	keyBytes, err := os.ReadFile(keyFile)
	if err != nil {
		return fmt.Errorf("read private key: %w", err)
	}
	minConf := fmt.Sprintf(`{"PrivateKey": "%s"}`, strings.TrimSpace(string(keyBytes)))
	if _, err := cfg.ReadFrom(strings.NewReader(minConf)); err != nil {
		return fmt.Errorf("parse private key file %s: %w", keyFile, err)
	}
	return nil
}

// InitLogger sets up the structured logger for the whole process.
func InitLogger(debug bool, quiet bool) {
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

	logBase = zapcore.InfoLevel
	if quiet {
		logBase = zapcore.ErrorLevel
	}
	logLevel = zapConfig.Level

	var err error
	zlog, err = zapConfig.Build()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize zap logger: %v", err))
	}
	// The netstack layer used to write raw gvisor logs straight to stdout,
	// which drowned the result table in RWC noise. Route it through zap so -debug
	// keeps the detail and normal runs stay clean.
	SetDebug(debug)
}

// SetDebug turns debug logging on or off at runtime. It is safe to call before
// InitLogger (it does nothing) and more than once.
func SetDebug(debug bool) {
	if debug {
		netstack.SetDebugLogger(zlog.Sugar().Debugf)
	} else {
		netstack.SetDebugLogger(nil)
	}
	if zlog == nil {
		return
	}
	if debug {
		logLevel.SetLevel(zapcore.DebugLevel)
	} else {
		logLevel.SetLevel(logBase)
	}
}

// SyncLogger flushes buffered log output. It is safe to call more than once.
func SyncLogger() {
	if zlog != nil {
		_ = zlog.Sync()
	}
}

// LogInfo, LogWarn, LogError and LogDebug are thin wrappers that keep the
// front-ends from depending on zap directly.
func LogInfo(msg string, fields ...zap.Field)  { logAt(zap.InfoLevel, msg, fields...) }
func LogWarn(msg string, fields ...zap.Field)  { logAt(zap.WarnLevel, msg, fields...) }
func LogError(msg string, fields ...zap.Field) { logAt(zap.ErrorLevel, msg, fields...) }
func LogDebug(msg string, fields ...zap.Field) { logAt(zap.DebugLevel, msg, fields...) }

func logAt(lvl zapcore.Level, msg string, fields ...zap.Field) {
	if zlog == nil {
		return
	}
	zlog.Log(lvl, msg, fields...)
}

// Run executes one batch speed test and returns the filtered, sorted results.
// ctx is honoured throughout: a cancel stops scheduling new peers and unwinds
// any in-flight download.
// Run measures every configured peer and returns the results sorted by
// cfg.SortBy. It is three phases, each readable and testable on its own:
// resolve the request into a plan, dispatch the measurements, then filter and
// sort what came back.
func Run(ctx context.Context, cfg RunConfig) ([]SpeedResult, error) {
	plan, err := resolveRun(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer plan.close()

	return finishRun(dispatch(ctx, plan), cfg), nil
}

// runPlan is everything a dispatch loop needs: the peers to test, the node
// config a serial run shares, and the checkpoint the results stream to.
type runPlan struct {
	cfg        RunConfig
	peers      []string
	globalCfg  *config.NodeConfig
	checkpoint *os.File
}

// close syncs then closes the checkpoint file. Sync first: an interrupted run
// must leave what it measured on disk, and fsync is what makes that true.
func (p runPlan) close() {
	if p.checkpoint == nil {
		return
	}
	if err := p.checkpoint.Sync(); err != nil {
		zlog.Warn("Failed to sync checkpoint file", zap.Error(err))
	}
	zlog.Info("Checkpoint file complete", zap.String("file", p.cfg.Checkpoint))
	if err := p.checkpoint.Close(); err != nil {
		zlog.Warn("Failed to close checkpoint file", zap.Error(err))
	}
}

// resolveRun validates the request and turns it into a plan.
func resolveRun(ctx context.Context, cfg RunConfig) (runPlan, error) {
	if err := cfg.Validate(); err != nil {
		return runPlan{}, err
	}
	warnIfUnreachableTarget(ctx, cfg.TestURL, cfg.CustomDNS)
	peers, err := PeerSources(ctx, cfg)
	if err != nil {
		return runPlan{}, err
	}
	if len(peers) == 0 {
		return runPlan{}, fmt.Errorf("no peers available: configure peers, a peers file, or public peers")
	}
	if cfg.Limit > 0 && len(peers) > cfg.Limit {
		zlog.Info("Limiting peer count", zap.Int("requested", len(peers)), zap.Int("limit", cfg.Limit))
		peers = peers[:cfg.Limit]
	}

	var globalCfg *config.NodeConfig
	if cfg.Concurrency == 1 {
		globalCfg = config.GenerateConfig()
		if cfg.KeyFile != "" {
			if err := loadOrGenerateKey(cfg.KeyFile, globalCfg); err != nil {
				return runPlan{}, err
			}
		}
	} else {
		zlog.Warn("Concurrency is greater than 1, so the key file is ignored and each test uses a temporary in-memory key",
			zap.Int("concurrency", cfg.Concurrency))
	}

	zlog.Info("Ready to test peers",
		zap.Int("peers", len(peers)),
		zap.Int("concurrency", cfg.Concurrency),
		zap.Int("streams", cfg.Streams),
		zap.Duration("max_duration", cfg.MaxDuration))

	var checkpoint *os.File
	if cfg.Checkpoint != "" {
		// Append rather than truncate: the point of a checkpoint file is that an
		// interrupted run leaves what it already measured, and the next run
		// extends it instead of discarding it.
		file, err := os.OpenFile(cfg.Checkpoint, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return runPlan{}, fmt.Errorf("open checkpoint file: %w", err)
		}
		checkpoint = file
		zlog.Info("Appending results to checkpoint file", zap.String("file", cfg.Checkpoint))
	}

	return runPlan{cfg: cfg, peers: peers, globalCfg: globalCfg, checkpoint: checkpoint}, nil
}

// dispatch runs every peer in the plan, bounded by cfg.Concurrency, and stops
// scheduling further peers once ctx is done — whatever was measured survives.
func dispatch(ctx context.Context, plan runPlan) []SpeedResult {
	cfg := plan.cfg

	var results []SpeedResult
	var mu sync.Mutex
	var wg sync.WaitGroup

	sem := make(chan struct{}, cfg.Concurrency)
	var completedCount, inFlightCount int32
	totalPeers := len(plan.peers)

	// A labelled break, not a bare `break`, is required in the loop below:
	// `break` inside a select only leaves the select, which is how Ctrl-C
	// used to be ignored and every remaining peer still got scheduled.
dispatch:
	for _, p := range plan.peers {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			zlog.Warn("Interrupted; not scheduling further peer tests, collecting results gathered so far",
				zap.Int("done", int(atomic.LoadInt32(&completedCount))),
				zap.Int("total", totalPeers))
			break dispatch
		}
		wg.Add(1)

		go func(peer string) {
			defer wg.Done()
			defer func() { <-sem }()
			defer atomic.AddInt32(&inFlightCount, -1)

			zlog.Info("Testing peer",
				zap.Int("running", int(atomic.AddInt32(&inFlightCount, 1))),
				zap.Int("done", int(atomic.LoadInt32(&completedCount))),
				zap.Int("total", totalPeers),
				zap.String("peer", peer))

			pc := plan.globalCfg
			if cfg.Concurrency > 1 {
				pc = config.GenerateConfig()
			}

			// Bound each test independently. Without this a peer that fails to
			// stop cleanly wedges the whole run, and a cancel only aborts the
			// download phase rather than the handshake and route stages.
			testCtx, testCancel := context.WithTimeout(ctx,
				cfg.Timeout+cfg.RouteTimeout+cfg.MaxDuration+20*time.Second)
			defer testCancel()

			res := runPeerTest(testCtx, peer, pc, cfg.TestURL, cfg.CustomHost, cfg.CustomSNI, cfg.CustomDNS,
				cfg.Streams, cfg.Timeout, cfg.RouteTimeout, cfg.MaxDuration, cfg.MaxBytes, zlog.Core().Enabled(zap.DebugLevel))

			mu.Lock()
			results = append(results, res)
			if plan.checkpoint != nil {
				if err := writeCheckpointLine(plan.checkpoint, res); err != nil {
					zlog.Error("Failed to write checkpoint line", zap.Error(err))
				}
			}
			mu.Unlock()

			if cfg.OnResult != nil {
				cfg.OnResult(res)
			}

			// Counting in both branches means an interrupted run still reports
			// how far it got, not just how many are outstanding.
			curr := int(atomic.AddInt32(&completedCount, 1))
			if res.Error != "" {
				zlog.Debug("Peer test failed", zap.String("peer", peer), zap.String("error", res.Error))
				if !cfg.Quiet {
					zlog.Info("Completed peer", zap.Int("done", curr), zap.Int("total", totalPeers),
						zap.String("peer", peer), zap.String("error", res.Error))
				}
			} else {
				zlog.Debug("Peer test complete", zap.String("peer", peer),
					zap.Float64("avg_mbps", float64(*res.DownloadMbps)),
					zap.Float64("peak_mbps", float64(*res.PeakMbps)))
				if !cfg.Quiet {
					zlog.Info("Completed peer", zap.Int("done", curr), zap.Int("total", totalPeers),
						zap.String("peer", peer),
						zap.Float64("avg_mbps", float64(*res.DownloadMbps)),
						zap.Float64("peak_mbps", float64(*res.PeakMbps)))
				}
			}
		}(p)
	}

	wg.Wait()
	return results
}

// finishRun applies the speed and ping thresholds, then sorts. Kept separate
// from the dispatch loop so the filter-and-sort contract can be tested without
// a network at all.
func finishRun(results []SpeedResult, cfg RunConfig) []SpeedResult {
	if cfg.MinSpeed > 0 || cfg.MaxPing > 0 {
		var filtered []SpeedResult
		for _, r := range results {
			if r.Error != "" {
				filtered = append(filtered, r)
				continue
			}
			if !passesFilters(&r, cfg.MinSpeed, cfg.MaxPing) {
				continue
			}
			filtered = append(filtered, r)
		}
		results = filtered
	}

	SortResults(results, cfg.SortBy)
	if results == nil {
		// An empty slice rather than nil: a run cancelled before it could test a
		// single peer would otherwise persist as "results": null, which reads as
		// a missing field instead of an empty list.
		return []SpeedResult{}
	}
	return results
}

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
	nextAdminPort = int32(time.Now().UnixNano() % adminPortCount)
}
