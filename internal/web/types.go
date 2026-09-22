package web

import (
	"encoding/json"
	"time"

	"yggspeedtest/internal/engine"
)

// statusResp is GET /api/status.
type statusResp struct {
	Version    string         `json:"version"`
	ListenAddr string         `json:"listen_addr"`
	DataDir    string         `json:"data_dir"`
	Running    bool           `json:"running"`
	CurrentRun string         `json:"current_run,omitempty"`
	StartedAt  time.Time      `json:"started_at"`
	Runs       int            `json:"runs"`
	Schedule   statusSchedule `json:"schedule"`
}

// statusSchedule is the scheduler slice of /api/status. Next is a pointer so
// that "no upcoming firing" serialises as null rather than a zero date.
type statusSchedule struct {
	Enabled    bool       `json:"enabled"`
	Expression string     `json:"expression,omitempty"`
	Valid      bool       `json:"valid"`
	Error      string     `json:"error,omitempty"`
	Next       *time.Time `json:"next,omitempty"`
	LastRun    time.Time  `json:"last_run,omitempty"`
	Runs       int64      `json:"runs"`
	Skipped    int64      `json:"skipped"`
}

// peerEvent is one peer_done server-sent event.
type peerEvent struct {
	ID     string             `json:"id"`
	Result engine.SpeedResult `json:"result"`
}

// runStartedEvent is the run_started server-sent event.
type runStartedEvent struct {
	ID      string    `json:"id"`
	Trigger string    `json:"trigger"`
	At      time.Time `json:"started_at"`
}

// runBrief is one row of GET /api/runs: the summary a history table shows.
type runBrief struct {
	ID        string    `json:"id"`
	StartedAt time.Time `json:"started_at"`
	Duration  int64     `json:"duration_ms"`
	Trigger   string    `json:"trigger"`
	Peers     int       `json:"peers"`
	AvgMbps   float64   `json:"avg_mbps"`
	MaxMbps   float64   `json:"max_mbps"`
	AvgPingMs float64   `json:"avg_ping_ms"`
	Error     string    `json:"error,omitempty"`
}

// message is one hub delivery: an event name and its already-encoded body.
type message struct {
	event string
	data  json.RawMessage
}

func round2(v float64) float64 {
	if v < 0 {
		v = -v
	}
	return float64(int64(v*100+0.5)) / 100
}

func round3(v float64) float64 {
	if v < 0 {
		v = -v
	}
	return float64(int64(v*1000+0.5)) / 1000
}
