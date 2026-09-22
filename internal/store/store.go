// Package store persists the web server's configuration and its run history.
//
// Both files are plain JSON so an operator can edit them by hand and diff them
// in version control. Writes are atomic (temp file plus rename) so a crash
// mid-write cannot leave a half-parsed config that would refuse to start.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"yggspeedtest/internal/engine"
)

// DefaultConfigFile and DefaultRunsFile are the relative paths used when the
// caller does not pass any, resolved against the current working directory.
const (
	DefaultConfigFile = "config.json"
	DefaultRunsFile   = "runs.jsonl"
)

// A run record is what the dashboard remembers about one batch. Results are
// kept inline so a run stays readable without joining two files.
type RunRecord struct {
	ID        string               `json:"id"`
	StartedAt time.Time            `json:"started_at"`
	Duration  time.Duration        `json:"duration"`
	Trigger   string               `json:"trigger"` // "manual" or "scheduled"
	Peers     int                  `json:"peers"`
	AvgMbps   float64              `json:"avg_mbps"`
	MaxMbps   float64              `json:"max_mbps"`
	AvgPingMs float64              `json:"avg_ping_ms"`
	Error     string               `json:"error,omitempty"`
	Results   []engine.SpeedResult `json:"results"`
}

// Config is the web server's persisted state. Duration fields are strings
// rather than time.Duration so the file stays human-editable: a duration in
// JSON would otherwise be a nanosecond integer.
type Config struct {
	// ListenAddr is the HTTP bind address.
	ListenAddr string `json:"listen_addr"`
	// DataDir is where config and runs live. Empty means the working directory.
	DataDir string `json:"data_dir,omitempty"`
	// HistoryLimit caps how many runs are retained. Zero means keep everything.
	HistoryLimit int `json:"history_limit,omitempty"`

	Peers       []string `json:"peer,omitempty"`
	PeersFile   string   `json:"peer_file,omitempty"`
	PublicPeers bool     `json:"public_peers,omitempty"`
	Limit       int      `json:"limit,omitempty"`

	TestURL    string `json:"test_url"`
	CustomHost string `json:"custom_host,omitempty"`
	CustomSNI  string `json:"custom_sni,omitempty"`
	CustomDNS  string `json:"custom_dns,omitempty"`

	Concurrency  int    `json:"concurrency"`
	Streams      int    `json:"streams"`
	MaxDuration  string `json:"max_duration"`
	MaxBytes     string `json:"max_bytes,omitempty"`
	Timeout      string `json:"timeout"`
	RouteTimeout string `json:"route_timeout"`

	SortBy   string  `json:"sort"`
	MinSpeed float64 `json:"min_speed,omitempty"`
	MaxPing  float64 `json:"max_ping,omitempty"`

	KeyFile string `json:"key_file,omitempty"`
	Debug   bool   `json:"debug,omitempty"`

	// Schedule and Enabled together decide whether the scheduler runs. An
	// empty schedule leaves the scheduler off, so "save the config" never
	// starts surprise jobs.
	Schedule string `json:"schedule,omitempty"`
	Enabled  bool   `json:"enabled,omitempty"`
}

// Load reads the persisted configuration. A missing or empty file yields a
// default Config, which is what makes a first run work with no setup at all.
func Load(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) || len(data) == 0 {
		cfg.fillDefaults()
		return cfg, nil
	}
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %s: %w", path, err)
	}
	cfg.fillDefaults()
	return cfg, nil
}

// Save writes the configuration atomically.
func (cfg Config) Save(path string) error {
	data, err := json.MarshalIndent(&cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
	return writeFileAtomic(path, append(data, '\n'))
}

// Normalise applies the same defaults Load does, so a config edited by hand or
// posted by the UI ends up in the same shape either way.
func (cfg *Config) Normalise() { cfg.fillDefaults() }

// fillDefaults leaves any zero field on its safe default. It is deliberately
// lenient about values that Validate will still reject, so a half-edited file
// loads and the user sees the specific complaint instead of a blank screen.
func (cfg *Config) fillDefaults() {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:8080"
	}
	if cfg.TestURL == "" {
		cfg.TestURL = "https://speed.cloudflare.com/__down?bytes=250000000"
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.Streams < 1 {
		cfg.Streams = 4
	}
	if cfg.MaxDuration == "" {
		cfg.MaxDuration = "15s"
	}
	if cfg.Timeout == "" {
		cfg.Timeout = "30s"
	}
	if cfg.RouteTimeout == "" {
		cfg.RouteTimeout = "10s"
	}
	if cfg.SortBy == "" {
		cfg.SortBy = "speed"
	}
}

// ToRunConfig renders an engine.RunConfig for a batch, with the given number
// of peer overrides replacing any peers saved in the config. Overrides is
// non-nil to mean "use this list", including an empty one.
func (cfg Config) ToRunConfig(overrides []string) (engine.RunConfig, error) {
	out := engine.RunConfig{
		PeersFile:    cfg.PeersFile,
		PublicPeers:  cfg.PublicPeers,
		Limit:        cfg.Limit,
		TestURL:      cfg.TestURL,
		CustomHost:   cfg.CustomHost,
		CustomSNI:    cfg.CustomSNI,
		CustomDNS:    cfg.CustomDNS,
		Concurrency:  cfg.Concurrency,
		Streams:      cfg.Streams,
		MaxDuration:  parseDur(cfg.MaxDuration, "max_duration"),
		Timeout:      parseDur(cfg.Timeout, "timeout"),
		RouteTimeout: parseDur(cfg.RouteTimeout, "route_timeout"),
		SortBy:       cfg.SortBy,
		MinSpeed:     cfg.MinSpeed,
		MaxPing:      cfg.MaxPing,
		KeyFile:      cfg.KeyFile,
		Quiet:        true, // The web UI renders progress itself.
	}
	if overrides != nil {
		out.Peers = append([]string(nil), overrides...)
	} else {
		out.Peers = append([]string(nil), cfg.Peers...)
	}

	if out.MaxDuration == 0 {
		return out, fmt.Errorf("invalid max_duration %q", cfg.MaxDuration)
	}
	if out.Timeout == 0 {
		return out, fmt.Errorf("invalid timeout %q", cfg.Timeout)
	}
	if out.RouteTimeout == 0 {
		return out, fmt.Errorf("invalid route_timeout %q", cfg.RouteTimeout)
	}

	if cfg.MaxBytes != "" {
		n, err := engine.ParseByteSize(cfg.MaxBytes)
		if err != nil {
			return out, fmt.Errorf("max_bytes: %w", err)
		}
		out.MaxBytes = n
	}
	return out, nil
}

// HasSchedule reports whether a schedule expression has been saved.
func (cfg Config) HasSchedule() bool { return cfg.Schedule != "" }

// parseDur is the forgiving version used by ToRunConfig: it returns zero
// instead of an error so the caller can report all three problems at once.
func parseDur(value, field string) time.Duration {
	if value == "" {
		return 0
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return d
}

// LoadRuns reads the run history, newest last. A missing file yields an empty
// slice. Malformed lines are skipped with the next line tried, because one bad
// line should not erase the history that precedes and follows it.
func LoadRuns(path string) ([]RunRecord, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read runs: %w", err)
	}

	var runs []RunRecord
	start := 0
	for start < len(data) {
		end := start
		for end < len(data) && data[end] != '\n' {
			end++
		}
		line := data[start:end]
		start = end + 1
		if len(line) == 0 {
			continue
		}

		var r RunRecord
		if err := json.Unmarshal(line, &r); err != nil {
			continue
		}
		runs = append(runs, r)
	}
	return runs, nil
}

// AppendRun adds one record and keeps at most limit records, dropping the
// oldest. limit <= 0 keeps everything.
func AppendRun(path string, rec RunRecord, limit int) error {
	runs, err := LoadRuns(path)
	if err != nil {
		return err
	}
	runs = append(runs, rec)
	if limit > 0 && len(runs) > limit {
		runs = runs[len(runs)-limit:]
	}
	return WriteRuns(path, runs)
}

// FindRun returns the run with id.
func FindRun(path, id string) (RunRecord, bool, error) {
	runs, err := LoadRuns(path)
	if err != nil {
		return RunRecord{}, false, err
	}
	for _, rec := range runs {
		if rec.ID == id {
			return rec, true, nil
		}
	}
	return RunRecord{}, false, nil
}

// LatestRun returns the newest run, since LoadRuns keeps them newest-last.
func LatestRun(path string) (RunRecord, bool, error) {
	runs, err := LoadRuns(path)
	if err != nil {
		return RunRecord{}, false, err
	}
	if len(runs) == 0 {
		return RunRecord{}, false, nil
	}
	return runs[len(runs)-1], true, nil
}

// DeleteRun removes one run and reports whether it existed. The load and the
// rewrite are kept inside one call: the history file is rewritten whole, so a
// LoadRuns result taken before an unrelated write would silently erase the
// records that were appended in the meantime.
func DeleteRun(path, id string) (bool, error) {
	runs, err := LoadRuns(path)
	if err != nil {
		return false, err
	}
	kept := runs[:0:0]
	found := false
	for _, rec := range runs {
		if rec.ID == id {
			found = true
			continue
		}
		kept = append(kept, rec)
	}
	if !found {
		return false, nil
	}
	if err := WriteRuns(path, kept); err != nil {
		return false, err
	}
	return true, nil
}

// WriteRuns rewrites the history atomically.
func WriteRuns(path string, runs []RunRecord) error {
	var buf []byte
	for _, r := range runs {
		line, err := json.Marshal(&r)
		if err != nil {
			return fmt.Errorf("encode run %s: %w", r.ID, err)
		}
		buf = append(buf, line...)
		buf = append(buf, '\n')
	}
	return writeFileAtomic(path, buf)
}

// LoadOrCreateConfig returns the saved config, or a default one written to
// disk when nothing exists yet. Used at startup so the file always exists.
func LoadOrCreateConfig(path string) (Config, error) {
	cfg, err := Load(path)
	if err != nil {
		return Config{}, err
	}
	if _, statErr := os.Stat(path); errors.Is(statErr, os.ErrNotExist) {
		if saveErr := cfg.Save(path); saveErr != nil {
			return cfg, saveErr
		}
	}
	return cfg, nil
}

// writeFileAtomic writes to a temp file in the same directory and renames it
// over the target, so a reader never observes a partial file.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create directory %s: %w", dir, err)
		}
	}

	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Chmod(tmpName, 0o644); err != nil {
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	return nil
}
