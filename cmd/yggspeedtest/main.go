// Command yggspeedtest runs a batch of Yggdrasil peer speed tests from the
// command line and prints or exports the ranked results.
//
// All measurement logic lives in internal/engine so this binary is a thin
// front-end; cmd/yggspeedtest-web drives the same Run function.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"yggspeedtest/internal/engine"
)

func main() {
	peerURI := flag.String("peer", "", "Target Yggdrasil peer URI (single test)")
	peersFile := flag.String("file", "", "Text file containing one peer URI per line (batch test)")
	usePublicPeers := flag.Bool("public", false, "Automatically fetch active public peers list from public repository")
	limit := flag.Int("limit", 0, "Maximum number of peers to test (0 = unlimited)")

	outJSON := flag.String("out", "", "Output sorted JSON results to this file path")
	outCSV := flag.String("out-csv", "", "Output CSV results to this file path")
	outMD := flag.String("out-md", "", "Output Markdown table results to this file path")
	checkpoint := flag.String("checkpoint", "", "Append one JSONL line per completed peer to this file")

	concurrency := flag.Int("c", 1, "Number of concurrent peer tests (if > 1, -key flag is ignored)")
	streams := flag.Int("streams", 1, "Number of parallel HTTP download streams per peer test")

	maxDurationStr := flag.String("max-duration", "10s", "Max download test duration per peer (e.g. 5s, 10s, 30s)")
	maxBytesStr := flag.String("max-bytes", "", "Max bytes downloaded per peer (e.g. 10MB, 50MB, 100MB)")

	timeoutStr := flag.String("timeout", "5s", "Pre-flight underlay handshake timeout")
	routeTimeoutStr := flag.String("route-timeout", "15s", "Yggdrasil route convergence timeout")

	sortBy := flag.String("sort", "speed", "Sort results by: speed, peak, ping, handshake")
	minSpeed := flag.Float64("min-speed", 0, "Filter out results with download speed below this Mbps threshold")
	maxPing := flag.Float64("max-ping", 0, "Filter out results with ping above this ms threshold")

	testURL := flag.String("url", "", "URL to test download speed")
	customHost := flag.String("host", "", "Custom HTTP Host header")
	customSNI := flag.String("sni", "", "Custom TLS SNI")
	customDNS := flag.String("dns", "", "Custom DNS server (e.g., 127.0.0.1:53)")
	keyFile := flag.String("key", "", "Path to private key file (optional, only effective if -c=1)")
	debugLog := flag.Bool("debug", false, "Enable detailed debug logs")
	quietLog := flag.Bool("quiet", false, "Suppress non-error logs")
	showVersion := flag.Bool("version", false, "Print version and exit")

	flag.Parse()

	if *showVersion {
		fmt.Printf("YggSpeedTest %s\n", engine.Version)
		return
	}

	engine.InitLogger(*debugLog, *quietLog)
	defer engine.SyncLogger()

	engine.LogInfo("Starting YggSpeedTest Engine", zap.String("version", engine.Version))

	maxDuration, err := time.ParseDuration(*maxDurationStr)
	if err != nil {
		fatal(*outJSON, "Invalid -max-duration format", err, "")
	}
	if maxDuration < 0 {
		fatal(*outJSON, "-max-duration must not be negative", nil, "")
	}

	timeout, err := time.ParseDuration(*timeoutStr)
	if err != nil {
		fatal(*outJSON, "Invalid -timeout format", err, "")
	}

	routeTimeout, err := time.ParseDuration(*routeTimeoutStr)
	if err != nil {
		fatal(*outJSON, "Invalid -route-timeout format", err, "")
	}

	maxBytes, err := engine.ParseByteSize(*maxBytesStr)
	if err != nil {
		fatal(*outJSON, "Invalid -max-bytes format", err, "")
	}

	cfg := engine.RunConfig{
		Peers:        []string{},
		PeersFile:    *peersFile,
		PublicPeers:  *usePublicPeers,
		Limit:        *limit,
		TestURL:      *testURL,
		CustomHost:   *customHost,
		CustomSNI:    *customSNI,
		CustomDNS:    *customDNS,
		Concurrency:  *concurrency,
		Streams:      *streams,
		MaxDuration:  maxDuration,
		MaxBytes:     maxBytes,
		Timeout:      timeout,
		RouteTimeout: routeTimeout,
		SortBy:       *sortBy,
		MinSpeed:     *minSpeed,
		MaxPing:      *maxPing,
		KeyFile:      *keyFile,
		Checkpoint:   *checkpoint,
		Quiet:        *quietLog,
	}
	if *peerURI != "" {
		cfg.Peers = []string{*peerURI}
	}

	// Ctrl-C stops scheduling further peers and unwinds any in-flight download
	// instead of killing the process and losing everything.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	results, err := engine.Run(ctx, cfg)
	if err != nil {
		fatal(*outJSON, err.Error(), nil, "")
	}
	if ctx.Err() != nil {
		engine.LogWarn("Run interrupted before completion",
			zap.Int("results", len(results)), zap.Error(ctx.Err()))
	}

	if *outJSON != "" {
		outData, _ := json.MarshalIndent(results, "", "  ")
		if err := os.WriteFile(*outJSON, outData, 0644); err != nil {
			engine.LogError("Failed to write JSON output", zap.Error(err))
		} else {
			engine.LogInfo("Results successfully saved to JSON file", zap.String("file", *outJSON))
		}
	}

	if *outCSV != "" {
		if err := engine.ExportCSVFile(*outCSV, results); err != nil {
			engine.LogError("Failed to write CSV output", zap.Error(err))
		} else {
			engine.LogInfo("Results successfully saved to CSV file", zap.String("file", *outCSV))
		}
	}

	if *outMD != "" {
		if err := engine.ExportMarkdownFile(*outMD, results); err != nil {
			engine.LogError("Failed to write Markdown output", zap.Error(err))
		} else {
			engine.LogInfo("Results successfully saved to Markdown file", zap.String("file", *outMD))
		}
	}

	if *outJSON == "" {
		engine.PrintConsoleTable(os.Stdout, results)
	}
}

// fatal reports a configuration or run error and exits. When no JSON output
// path was requested the error is printed as a result document on stdout, which
// is the shape a script piping this tool can already handle.
func fatal(outJSON, msg string, err error, peer string) {
	if err != nil {
		engine.LogError("Fatal Error", zap.String("msg", msg), zap.Error(err))
	} else {
		engine.LogError("Fatal Error", zap.String("msg", msg))
	}

	errMsg := msg
	if err != nil {
		errMsg = fmt.Sprintf("%s: %v", msg, err)
	}

	if outJSON == "" {
		res := engine.SpeedResult{
			Peer:     peer,
			TestTime: time.Now().Format(time.RFC3339),
			Error:    errMsg,
		}
		out, _ := json.MarshalIndent([]engine.SpeedResult{res}, "", "  ")
		fmt.Println(string(out))
	}
	os.Exit(1)
}
