// Command yggspeedtest-web serves the web dashboard and the scheduled runner.
//
// It is a thin wrapper over internal/engine: every run it triggers uses the
// same measurement code as the CLI, so the two cannot disagree about a number.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"go.uber.org/zap"

	"yggspeedtest/internal/engine"
	"yggspeedtest/internal/schedule"
	"yggspeedtest/internal/store"
	"yggspeedtest/internal/web"
)

func main() {
	var (
		listen  = flag.String("listen", "", "HTTP listen address, e.g. 127.0.0.1:8080 (default from config)")
		dataDir = flag.String("data", "", "directory for config.json and runs.jsonl (default: current directory)")
		configF = flag.String("config", "", "config file (default: <data>/config.json)")
		runsF   = flag.String("runs", "", "run history file (default: <data>/runs.jsonl)")
		debug   = flag.Bool("debug", false, "debug logging")
		quiet   = flag.Bool("quiet", false, "error-level logging only")
		version = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *version {
		fmt.Println("YggSpeedTest", engine.Version)
		return
	}

	engine.InitLogger(*debug, *quiet)
	defer engine.SyncLogger()

	// Resolve the data directory once and derive both paths from it, so the
	// two files cannot end up in different places.
	if *dataDir == "" {
		*dataDir = "."
	}
	if *configF == "" {
		*configF = filepath.Join(*dataDir, store.DefaultConfigFile)
	}
	if *runsF == "" {
		*runsF = filepath.Join(*dataDir, store.DefaultRunsFile)
	}

	cfg, err := store.LoadOrCreateConfig(*configF)
	if err != nil {
		engine.LogError("Failed to load config", zap.Error(err), zap.String("path", *configF))
		os.Exit(1)
	}

	if *listen == "" {
		*listen = cfg.ListenAddr
	}

	// A saved schedule is only trusted if it parses. A hand-edited typo in the
	// config file should not leave the scheduler in a half-started state.
	if cfg.HasSchedule() {
		if _, err := schedule.Parse(cfg.Schedule); err != nil {
			engine.LogError("Saved schedule is invalid; the scheduler will stay off",
				zap.Error(err), zap.String("schedule", cfg.Schedule))
			cfg.Enabled = false
		}
	}

	srv, err := web.New(cfg, web.Paths{Config: *configF, Runs: *runsF}, *listen)
	if err != nil {
		engine.LogError("Failed to start web server", zap.Error(err))
		os.Exit(1)
	}

	httpServer := &http.Server{
		Addr:              *listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		engine.LogInfo("YggSpeedTest web starting",
			zap.String("listen", *listen),
			zap.String("config", *configF),
			zap.String("runs", *runsF),
			zap.String("schedule", cfg.Schedule),
			zap.Bool("enabled", cfg.Enabled),
		)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			engine.LogError("HTTP server stopped unexpectedly", zap.Error(err), zap.String("listen", *listen))
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	engine.LogInfo("Shutting down")

	// Ask the scheduler and any in-flight run to stop first, so the HTTP drain
	// below is only waiting on connections instead of on a download.
	srv.Cancel()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		engine.LogWarn("HTTP shutdown did not finish in time", zap.Error(err))
	}
	engine.LogInfo("Stopped")
}
