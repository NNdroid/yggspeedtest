package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"go.uber.org/zap"
)

// peerURIRe pulls peer URIs out of the upstream Markdown peer lists. The
// delimiter class must exclude backticks and pipes: those pages wrap every URI
// in `code` and separate table cells with `|`, and a delimiter that slips
// through becomes part of the port, which kills the whole peer at url.Parse.
// https:// is deliberately absent so the operator links in the prose are not
// collected as peers.
var peerURIRe = regexp.MustCompile(`(?:tcp|tls|quic|ws|wss|kcp|socks|sockstls|unix)://[^\s"'<>|,` + "`" + `]+`)

var fallbackPeers = []string{
	"tls://ygg.mkg20001.io:443",
	"tls://yggdrasil.org:443",
}

// publicPeerSources are the per-country lists pulled from the upstream
// public-peers repository. They are Markdown rather than JSON, so peerURIRe
// extracts the URIs. Spreading across regions is deliberate: the node only
// reaches the whole network once it holds a low-latency path into a given
// region, and one region is not enough of a sample to rank peers by. The
// variable exists so tests can point the fetcher at a local server.
var publicPeerSources = []string{
	"https://raw.githubusercontent.com/yggdrasil-network/public-peers/master/europe/germany.md",
	"https://raw.githubusercontent.com/yggdrasil-network/public-peers/master/north-america/united-states.md",
	"https://raw.githubusercontent.com/yggdrasil-network/public-peers/master/europe/netherlands.md",
	"https://raw.githubusercontent.com/yggdrasil-network/public-peers/master/europe/france.md",
	"https://raw.githubusercontent.com/yggdrasil-network/public-peers/master/europe/sweden.md",
}

// fetchPublicPeers downloads the live public peer list. err is non-nil when the
// built-in fallback list was used instead; the previous code returned nil there,
// so a caller had no way to know it was testing four hardcoded peers from
// months ago.
//
// Every source is consulted and the results merged: earlier versions returned
// after the first region that yielded peers, which quietly reduced the
// "cross-region sample" to a single country. A source that fails is skipped and
// reported; only when nothing at all came back does the fallback list take over.
func fetchPublicPeers(ctx context.Context) ([]string, error) {
	client := &http.Client{Timeout: 10 * time.Second}

	var peers []string
	seen := make(map[string]bool)
	var failures []string

	for _, u := range publicPeerSources {
		batch, err := fetchPeerSource(ctx, client, u)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", u, err))
			continue
		}
		for _, p := range batch {
			if !seen[p] {
				seen[p] = true
				peers = append(peers, p)
			}
		}
	}

	if len(peers) == 0 {
		return fallbackPeers, fmt.Errorf("all public peer list sources failed (%s); using built-in fallback list",
			strings.Join(failures, "; "))
	}
	if len(failures) > 0 {
		LogWarn("Some public peer sources failed; continuing with the others",
			zap.Int("failed", len(failures)), zap.String("detail", strings.Join(failures, "; ")))
	}
	return peers, nil
}

// fetchPeerSource downloads and parses one upstream list. The body read is
// bounded: a hostile or misconfigured endpoint could otherwise hand back an
// unbounded response.
func fetchPeerSource(ctx context.Context, client *http.Client, u string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	var jsonPeers []string
	if err := json.Unmarshal(body, &jsonPeers); err == nil {
		var out []string
		for _, p := range jsonPeers {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
		if len(out) == 0 {
			return nil, errors.New("no peers found")
		}
		return out, nil
	}

	batch := peerURIRe.FindAllString(string(body), -1)
	if len(batch) == 0 {
		return nil, errors.New("no peers found")
	}
	return batch, nil
}

func deduplicatePeers(input []string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, p := range input {
		p = strings.TrimSpace(p)
		if p != "" && !seen[p] {
			seen[p] = true
			result = append(result, p)
		}
	}
	return result
}

// readPeersFromFile reads one peer URI per line, ignoring blanks and #
// comments so the file can be maintained by hand.
func readPeersFromFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var peers []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			peers = append(peers, line)
		}
	}
	return peers, scanner.Err()
}
