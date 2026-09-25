package engine

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SortResults orders results for output. It is stable: ties keep the order the
// peers were tested in, so an interrupted run and a full run rank their common
// peers the same way.
func SortResults(results []SpeedResult, sortBy string) {
	stableSort(results, sortBy)
}

func stableSort(results []SpeedResult, sortBy string) {
	sort.SliceStable(results, func(i, j int) bool {
		return lessResult(results[i], results[j], strings.ToLower(sortBy))
	})
}

// lessResult keeps failure rows at the bottom and treats a missing measurement
// as the worst value, so a peer whose download never completed cannot be
// ranked above one that was actually measured.
func lessResult(a, b SpeedResult, sortBy string) bool {
	if a.Error != "" && b.Error != "" {
		return a.Peer < b.Peer
	}
	if a.Error != "" {
		return false
	}
	if b.Error != "" {
		return true
	}

	switch sortBy {
	case "ping":
		return pingOf(a.PingMs) < pingOf(b.PingMs)
	case "handshake":
		return pingOf(a.HandshakeMs) < pingOf(b.HandshakeMs)
	case "peak":
		return speedOf(a.PeakMbps) > speedOf(b.PeakMbps)
	default:
		return speedOf(a.DownloadMbps) > speedOf(b.DownloadMbps)
	}
}

// passesFilters applies the operator's thresholds. A missing measurement fails
// a set threshold: silently passing it through made -min-speed and -max-ping
// into no-ops on any run where the download never completed, which is exactly
// the run they exist to trim.
func passesFilters(r *SpeedResult, minSpeed, maxPing float64) bool {
	if minSpeed > 0 {
		if r.DownloadMbps == nil || float64(*r.DownloadMbps) < minSpeed {
			return false
		}
	}
	if maxPing > 0 {
		if r.PingMs == nil || float64(*r.PingMs) > maxPing {
			return false
		}
	}
	return true
}

// pingOf reports a measurement, defaulting to a value that sorts last.
func pingOf(p *PingFloat) float64 {
	if p == nil {
		return 1 << 30
	}
	return float64(*p)
}

// speedOf reports a rate, defaulting to zero so a missing measurement sorts
// last among the descending speed and peak comparisons.
func speedOf(s *SpeedFloat) float64 {
	if s == nil {
		return 0
	}
	return float64(*s)
}

// writeCheckpointLine appends one result as a line of JSON. Each line is
// independently parseable, which is the whole point of a checkpoint: a run
// killed mid-batch leaves what it already measured.
func writeCheckpointLine(f *os.File, r SpeedResult) error {
	if f == nil {
		return errors.New("checkpoint file is nil")
	}
	line, err := jsonMarshalLine(r)
	if err != nil {
		return err
	}
	if _, err := f.Write(line); err != nil {
		return err
	}
	// Sync per line: the file is the only durable copy of this batch's work.
	return f.Sync()
}

// jsonMarshalLine encodes one result as a newline-terminated JSON line.
func jsonMarshalLine(r SpeedResult) ([]byte, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// genErrorResult records a peer that could not be tested at all.
func genErrorResult(peer, errMsg string) SpeedResult {
	return SpeedResult{
		Peer:     peer,
		TestTime: time.Now().Format(time.RFC3339),
		Error:    errMsg,
	}
}

// ExportCSV writes a report Excel can open. The leading BOM is what makes Excel
// detect UTF-8; without it the header row renders as mojibake.
func ExportCSV(w io.Writer, results []SpeedResult) error {
	if _, err := w.Write([]byte{0xEF, 0xBB, 0xBF}); err != nil {
		return err
	}

	cw := csv.NewWriter(w)
	if err := cw.Write([]string{
		"Rank", "Peer", "TestTime", "HandshakeMs", "PingMs",
		"AvgMbps", "PeakMbps", "DownloadedMB", "DurationSec", "Status",
	}); err != nil {
		return err
	}

	for i, r := range results {
		record := []string{
			strconv.Itoa(i + 1),
			r.Peer,
			r.TestTime,
			pingField(r.HandshakeMs),
			pingField(r.PingMs),
			speedField(r.DownloadMbps),
			speedField(r.PeakMbps),
			mbField(r.BytesDownloaded),
			durationField(r.DurationSec),
			statusField(r),
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}

	cw.Flush()
	// A report that only partly made it to disk is worse than a loud failure,
	// so a write error is returned rather than swallowed.
	return cw.Error()
}

// ExportCSVFile writes a CSV report to a file path.
func ExportCSVFile(filePath string, results []SpeedResult) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	return ExportCSV(f, results)
}

// ExportMarkdown writes a human-readable report.
func ExportMarkdown(w io.Writer, results []SpeedResult) error {
	var sb strings.Builder
	sb.WriteString("# YggSpeedTest 测速报告\n\n")
	sb.WriteString(fmt.Sprintf("测试时间: %s\n\n", time.Now().Format("2006-01-02 15:04:05")))
	sb.WriteString("| 序号 | Peer 地址 | 平均速率 (Mbps) | 峰值速率 (Mbps) | 握手 (ms) | Ping (ms) | 下载数据 | 状态 |\n")
	sb.WriteString("|---|---|---|---|---|---|---|---|\n")

	for i, r := range results {
		sb.WriteString(fmt.Sprintf("| %d | `%s` | %s | %s | %s | %s | %s | %s |\n",
			i+1, r.Peer, speedMD(r.DownloadMbps), speedMD(r.PeakMbps),
			pingMD(r.HandshakeMs), pingMD(r.PingMs), mbMD(r.BytesDownloaded), statusMD(r)))
	}

	_, err := io.WriteString(w, sb.String())
	return err
}

// ExportMarkdownFile writes a Markdown report to a file path.
func ExportMarkdownFile(filePath string, results []SpeedResult) error {
	return os.WriteFile(filePath, []byte(RenderMarkdown(results)), 0644)
}

// RenderMarkdown returns the report as text, for callers that cannot hand the
// engine a writer.
func RenderMarkdown(results []SpeedResult) string {
	var sb strings.Builder
	_ = ExportMarkdown(&sb, results)
	return sb.String()
}

// pingField renders a latency for a CSV cell, or "-" when it was not measured.
func pingField(p *PingFloat) string {
	if p == nil {
		return "-"
	}
	return fmt.Sprintf("%.2f", float64(*p))
}

func speedField(s *SpeedFloat) string {
	if s == nil {
		return "-"
	}
	return fmt.Sprintf("%.3f", float64(*s))
}

func mbField(n int64) string {
	if n <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f", float64(n)/1024/1024)
}

func durationField(d float64) string {
	if d <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f", d)
}

func statusField(r SpeedResult) string {
	if r.Error != "" {
		return "FAIL: " + r.Error
	}
	return "OK"
}

func pingMD(p *PingFloat) string {
	if p == nil {
		return "-"
	}
	return fmt.Sprintf("%.2f ms", float64(*p))
}

func speedMD(s *SpeedFloat) string {
	if s == nil {
		return "-"
	}
	return fmt.Sprintf("%.3f Mbps", float64(*s))
}

func mbMD(n int64) string {
	if n <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f MB", float64(n)/1024/1024)
}

func statusMD(r SpeedResult) string {
	if r.Error != "" {
		return fmt.Sprintf("❌ 失败 (%s)", r.Error)
	}
	return "✅ 成功"
}

// consoleCell is one column of the terminal table: its rendered text, its
// display width, and whether the text is right-aligned.
type consoleCell struct {
	text  string
	width int
	right bool
}

// renderRow lays out one row, padding each cell to its display width so that
// CJK headers line up with the ASCII data underneath them.
func renderRow(cells []consoleCell) string {
	parts := make([]string, 0, len(cells))
	for _, c := range cells {
		if c.width < 0 {
			c.width = 0
		}
		text := truncateToDisplayWidth(c.text, c.width)
		pad := c.width - displayWidth(text)
		if pad < 0 {
			pad = 0
		}
		if c.right {
			parts = append(parts, padRight(text, pad))
		} else {
			parts = append(parts, padLeft(text, pad))
		}
	}
	return strings.Join(parts, " | ")
}

// displayWidth counts the columns a string occupies in a terminal, where a
// CJK character is two columns wide.
func displayWidth(s string) int {
	w := 0
	for _, r := range s {
		if isWide(r) {
			w += 2
		} else {
			w++
		}
	}
	return w
}

func isWide(r rune) bool {
	switch {
	case r >= 0x1100 && r <= 0x115F, // Hangul Jamo
		r >= 0x2E80 && r <= 0x303E,   // CJK radicals, Kangxi, CJK symbols and punctuation
		r >= 0x3041 && r <= 0x33FF,   // Hiragana, Katakana, Hangul compatibility, CJK enclosed
		r >= 0x3400 && r <= 0x4DBF,   // CJK unified ideographs extension A
		r >= 0x4E00 && r <= 0x9FFF,   // CJK unified ideographs
		r >= 0xA000 && r <= 0xA4CF,   // Yi
		r >= 0xAC00 && r <= 0xD7A3,   // Hangul syllables
		r >= 0xF900 && r <= 0xFAFF,   // CJK compatibility ideographs
		r >= 0xFE30 && r <= 0xFE4F,   // CJK compatibility forms
		r >= 0xFF00 && r <= 0xFF60,   // Fullwidth forms
		r >= 0xFFE0 && r <= 0xFFE6,   // Fullwidth signs
		r >= 0x20000 && r <= 0x3FFFD: // CJK unified ideographs extension B and later
		return true
	}
	return false
}

// truncateToDisplayWidth clips a string to maxW terminal columns without ever
// splitting a multi-byte character.
func truncateToDisplayWidth(text string, maxW int) string {
	if maxW <= 0 {
		return ""
	}
	if displayWidth(text) <= maxW {
		return text
	}
	w := 0
	for i, r := range text {
		rw := 1
		if isWide(r) {
			rw = 2
		}
		if w+rw > maxW {
			return text[:i]
		}
		w += rw
	}
	return text
}

func padRight(text string, pad int) string {
	return strings.Repeat(" ", pad) + text
}

func padLeft(text string, pad int) string {
	return text + strings.Repeat(" ", pad)
}

// PrintConsoleTable renders the result table. Every measurement is optional:
// a peer that failed before producing numbers must not panic the printer.
func PrintConsoleTable(w io.Writer, results []SpeedResult) {
	const rule = "============================================================================================================="
	fmt.Fprintln(w)
	fmt.Fprintln(w, rule)
	fmt.Fprintf(w, "                                        YggSpeedTest 测速排行榜 (%s)\n", Version)
	fmt.Fprintln(w, rule)

	fmt.Fprintln(w, renderRow([]consoleCell{
		{"序号", 4, true},
		{"平均速率(Mbps)", 15, false},
		{"峰值速率(Mbps)", 15, false},
		{"握手延迟(ms)", 12, false},
		{"路由Ping(ms)", 12, false},
		{"已下载", 10, false},
		{"Peer 地址", 40, false},
	}))
	fmt.Fprintln(w, "------+-----------------+-----------------+--------------+--------------+------------+----------------------------------------")

	for i, r := range results {
		fmt.Fprintln(w, renderRow([]consoleCell{
			{strconv.Itoa(i + 1), 4, true},
			{speedCell(r.DownloadMbps), 15, false},
			{speedCell(r.PeakMbps), 15, false},
			{pingCell(r.HandshakeMs), 12, false},
			{pingCell(r.PingMs), 12, false},
			{mbCell(r.BytesDownloaded), 10, false},
			{peerCell(r), 40, false},
		}))
	}
	fmt.Fprintln(w, rule)
}

// speedCell renders a rate, or a placeholder when it was not measured. A
// missing rate must not read as "0.000 Mbps", which would look like an
// extremely fast measurement of nothing.
func speedCell(s *SpeedFloat) string {
	if s == nil {
		return "-"
	}
	return fmt.Sprintf("%.3f Mbps", float64(*s))
}

// pingCell renders a latency, or "-" when it was not measured.
func pingCell(p *PingFloat) string {
	if p == nil {
		return "-"
	}
	return fmt.Sprintf("%.2f ms", float64(*p))
}

func mbCell(n int64) string {
	if n <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f MB", float64(n)/1024/1024)
}

// peerCell shows the peer URI, appending the failure reason when the test did
// not complete.
func peerCell(r SpeedResult) string {
	if r.Error != "" {
		return fmt.Sprintf("%s (FAIL: %s)", r.Peer, truncate(r.Error, 40))
	}
	return r.Peer
}

// truncate shortens text to at most maxLen runes, keeping the ellipsis inside
// that budget so callers can treat the result as a hard width limit.
func truncate(text string, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	runes := []rune(text)
	if len(runes) <= maxLen {
		return text
	}
	return string(runes[:maxLen-1]) + "…"
}
