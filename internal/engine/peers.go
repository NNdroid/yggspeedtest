package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
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

// fetchPublicPeers downloads the live public peer list. err is non-nil when the
// built-in fallback list was used instead; the previous code returned nil there,
// so a caller had no way to know it was testing four hardcoded peers from
// months ago.
func fetchPublicPeers() ([]string, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	urls := []string{
		// Per-country lists from the upstream public-peers repository. They are
		// Markdown rather than JSON, so peerURIRe extracts the URIs. Spreading
		// across regions is deliberate: the node only reaches the whole network
		// once it holds a low-latency path into a given region, and one region
		// is not enough of a sample to rank peers by.
		"https://raw.githubusercontent.com/yggdrasil-network/public-peers/master/europe/germany.md",
		"https://raw.githubusercontent.com/yggdrasil-network/public-peers/master/north-america/united-states.md",
		"https://raw.githubusercontent.com/yggdrasil-network/public-peers/master/europe/netherlands.md",
		"https://raw.githubusercontent.com/yggdrasil-network/public-peers/master/europe/france.md",
		"https://raw.githubusercontent.com/yggdrasil-network/public-peers/master/europe/sweden.md",
	}

	var peers []string
	seen := make(map[string]bool)
	var failures []string

	for _, u := range urls {
		resp, err := client.Get(u)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", u, err))
			continue
		}

		// Bound the read: a hostile or misconfigured endpoint could otherwise
		// hand back an unbounded body.
		body, rErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if rErr != nil || resp.StatusCode != http.StatusOK {
			failures = append(failures, fmt.Sprintf("%s: HTTP %d", u, resp.StatusCode))
			continue
		}

		var batch []string
		var jsonPeers []string
		if err := json.Unmarshal(body, &jsonPeers); err == nil {
			for _, p := range jsonPeers {
				p = strings.TrimSpace(p)
				if p != "" {
					batch = append(batch, p)
				}
			}
		} else {
			batch = peerURIRe.FindAllString(string(body), -1)
		}
		for _, p := range batch {
			if !seen[p] {
				seen[p] = true
				peers = append(peers, p)
			}
		}
		if len(peers) > 0 {
			return peers, nil
		}
		failures = append(failures, fmt.Sprintf("%s: no peers found", u))
	}

	return fallbackPeers, fmt.Errorf("all public peer list sources failed (%s); using built-in fallback list",
		strings.Join(failures, "; "))
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
