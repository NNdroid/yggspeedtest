package schedule

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseEvery(t *testing.T) {
	tests := []struct {
		in   string
		want time.Duration
		bad  bool
	}{
		{"@every 5m", 5 * time.Minute, false},
		{"@every 1h", time.Hour, false},
		{"@every 90s", 90 * time.Second, false},
		{" @every 30m ", 30 * time.Minute, false},
		{"@every 1d", 24 * time.Hour, false},
		{"@every 2w", 14 * 24 * time.Hour, false},
		{"@every 1mo", 30 * 24 * time.Hour, false},
		{"@every 1y", 365 * 24 * time.Hour, false},
		// Sub-minute intervals would hammer the engine; refuse them.
		{"@every 100ms", 0, true},
		{"@every 0m", 0, true},
		{"@every -5m", 0, true},
		{"@every 5x", 0, true},
		{"@every abc", 0, true},
	}
	for _, tc := range tests {
		got, err := Parse(tc.in)
		if tc.bad {
			if err == nil {
				t.Errorf("Parse(%q) = %+v, want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Parse(%q) unexpected error: %v", tc.in, err)
		}
		if got.Kind != Interval {
			t.Errorf("Parse(%q).Kind = %v, want Interval", tc.in, got.Kind)
		}
		if got.Every != tc.want {
			t.Errorf("Parse(%q).Every = %v, want %v", tc.in, got.Every, tc.want)
		}
	}
}

func TestParseShorthands(t *testing.T) {
	for sh := range map[string]string{
		"@yearly":   "0 0 1 1 *",
		"@annually": "0 0 1 1 *",
		"@monthly":  "0 0 1 * *",
		"@weekly":   "0 0 * * 0",
		"@daily":    "0 0 * * *",
		"@midnight": "0 0 * * *",
		"@hourly":   "0 * * * *",
	} {
		got, err := Parse(sh)
		if err != nil {
			t.Fatalf("Parse(%q) unexpected error: %v", sh, err)
		}
		if got.Kind != Clock {
			t.Errorf("Parse(%q).Kind = %v, want Clock", sh, got.Kind)
		}
		// Raw keeps what the operator typed, so @monthly is easier to read in a
		// UI than the cron form it expands to.
		if got.Raw != sh {
			t.Errorf("Parse(%q).Raw = %q, want %q", sh, got.Raw, sh)
		}
		if next, ok := got.NextAfter(at(2026, 9, 22, 12, 0)); !ok {
			t.Errorf("Parse(%q) has no firing after 2026-09-22: %v ok=%v", sh, next, ok)
		}
	}
	// Each shorthand expands to the intended five-field form.
	if spec := mustSpec("@monthly"); !spec.Match(at(2026, 10, 1, 0, 0)) || spec.Match(at(2026, 10, 2, 0, 0)) {
		t.Error("@monthly must fire only on the first of the month")
	}
	if spec := mustSpec("@yearly"); !spec.Match(at(2026, 1, 1, 0, 0)) || spec.Match(at(2026, 2, 1, 0, 0)) {
		t.Error("@yearly must fire only on January 1")
	}
	// Case-insensitive: cron convention allows either.
	if _, err := Parse("@HOURLY"); err != nil {
		t.Errorf("Parse(@HOURLY) unexpected error: %v", err)
	}
}

func TestParseFiveFields(t *testing.T) {
	tests := []struct {
		in  string
		bad bool
	}{
		{"0 0 * * *", false},
		{"*/5 * * * *", false},
		{"0 9-17 * * 1-5", false},
		{"0,30 8,20 * * *", false},
		{"15 10 1,15 * *", false},
		{"0 0 1 jan *", false},
		{"0 0 * jan,jun *", false},
		{"0 0 * * mon-wed", false},
		{"0 0 * * 7", false}, // 7 == Sunday
		{"59 23 31 dec 5", false},
		{"", true},
		{"0 0 * *", true},
		{"0 0 * * * *", true},
		{"60 * * * *", true},
		{"* 24 * * *", true},
		{"* * 0 * *", true},
		{"* * 32 * *", true},
		{"* * * 13 *", true},
		{"* * * * 8", true},
		{"*-1 * * * *", true},
		{"1- * * * *", true},
		{"1--5 * * * *", true},
		{"*/0 * * * *", true},
		{"a * * * *", true},
		{"1,,3 * * * *", true},
		{"1, * * * *", true},
		{"*/a * * * *", true},
		{"5-2 * * * *", true},
	}
	for _, tc := range tests {
		_, err := Parse(tc.in)
		if tc.bad && err == nil {
			t.Errorf("Parse(%q) expected error, got nil", tc.in)
		}
		if !tc.bad && err != nil {
			t.Errorf("Parse(%q) unexpected error: %v", tc.in, err)
		}
	}
}

func TestParseFieldExpands(t *testing.T) {
	tests := []struct {
		in   string
		lo   int
		hi   int
		want []int
	}{
		{"*", 0, 5, []int{0, 1, 2, 3, 4, 5}},
		{"3", 0, 5, []int{3}},
		{"1-3", 0, 5, []int{1, 2, 3}},
		{"1,3,5", 0, 5, []int{1, 3, 5}},
		{"1-10/3", 0, 59, []int{1, 4, 7, 10}},
		{"*/2", 0, 5, []int{0, 2, 4}},
		{"1,3-5", 0, 5, []int{1, 3, 4, 5}},
		{"*/1", 0, 5, []int{0, 1, 2, 3, 4, 5}},
		{"30-59/15", 0, 59, []int{30, 45}},
		{"0", 0, 59, []int{0}},
		{"59", 0, 59, []int{59}},
	}
	for _, tc := range tests {
		got, err := parseField(tc.in, tc.lo, tc.hi, nil)
		if err != nil {
			t.Fatalf("parseField(%q) unexpected error: %v", tc.in, err)
		}
		if len(got) != len(tc.want) {
			t.Fatalf("parseField(%q) = %v, want %v", tc.in, keys(got), tc.want)
		}
		for _, v := range tc.want {
			if !got[v] {
				t.Errorf("parseField(%q) missing %d, got %v", tc.in, v, keys(got))
			}
		}
	}

	months, err := parseField("jan,mar-jun", 1, 12, nameMonths)
	if err != nil {
		t.Fatalf("parseField months: %v", err)
	}
	for _, v := range []int{1, 3, 4, 5, 6} {
		if !months[v] {
			t.Errorf("months missing %d: %v", v, keys(months))
		}
	}

	dow, err := parseField("mon-fri", 0, 7, nameWeekdays)
	if err != nil {
		t.Fatalf("parseField dow: %v", err)
	}
	for _, v := range []int{1, 2, 3, 4, 5} {
		if !dow[v] {
			t.Errorf("dow missing %d: %v", v, keys(dow))
		}
	}
	if dow[0] || dow[6] {
		t.Errorf("dow should exclude the weekend: %v", keys(dow))
	}
}

func keys(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func at(y, m, d, h, min int) time.Time {
	return time.Date(y, time.Month(m), d, h, min, 0, 0, time.UTC)
}

func TestMatch(t *testing.T) {
	// 2026-09-21 is a Monday, 2026-09-22 a Tuesday, 2026-09-20 a Sunday.
	cases := []struct {
		expr string
		when time.Time
		want bool
	}{
		{"0 0 * * *", at(2026, 9, 22, 0, 0), true},
		{"0 0 * * *", at(2026, 9, 22, 0, 1), false},
		{"*/5 * * * *", at(2026, 9, 22, 3, 15), true},
		{"*/5 * * * *", at(2026, 9, 22, 3, 17), false},
		{"0 9-17 * * 1-5", at(2026, 9, 21, 12, 0), true},  // Monday
		{"0 9-17 * * 1-5", at(2026, 9, 22, 12, 1), false}, // Tuesday, wrong minute
		{"0 12 * * 1", at(2026, 9, 21, 12, 0), true},      // Monday
		{"0 12 * * 1", at(2026, 9, 23, 12, 0), false},     // Wednesday
		{"0 0 1 * *", at(2026, 9, 1, 0, 0), true},
		{"0 0 1 * *", at(2026, 9, 2, 0, 0), false},
		{"0 0 1 jan *", at(2026, 1, 1, 0, 0), true},
		{"0 0 1 jan *", at(2026, 6, 1, 0, 0), false},
		{"0 0 * * 7", at(2026, 9, 20, 0, 0), true}, // Sunday via the 7 alias
		{"0 0 * * 7", at(2026, 9, 22, 0, 0), false},
		{"@hourly", at(2026, 9, 22, 15, 0), true},
		{"@hourly", at(2026, 9, 22, 15, 30), false},
	}
	for _, tc := range cases {
		spec, err := Parse(tc.expr)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.expr, err)
		}
		if got := spec.Match(tc.when); got != tc.want {
			t.Errorf("Match(%q, %v) = %v, want %v", tc.expr, tc.when, got, tc.want)
		}
	}
}

// The dom/dow OR rule is the single most misimplemented cron detail.
func TestMatchDayOfMonthOrDayOfWeek(t *testing.T) {
	spec := mustSpec("0 12 21 * 2") // day 21 OR Tuesday

	if !spec.Match(at(2026, 9, 21, 12, 0)) { // Monday, the 21st
		t.Error("day-of-month 21 must match")
	}
	if !spec.Match(at(2026, 9, 22, 12, 0)) { // Tuesday, not the 21st
		t.Error("the cron OR rule: day-of-week 2 must match")
	}
	if spec.Match(at(2026, 9, 23, 12, 0)) { // Wednesday, the 23rd
		t.Error("neither field matches, so the time must not match")
	}

	domOnly := mustSpec("0 12 21 * *")
	if !domOnly.Match(at(2026, 9, 21, 12, 0)) {
		t.Error("day-of-month 21 should match")
	}
	if domOnly.Match(at(2026, 9, 22, 12, 0)) {
		t.Error("day-of-month 21 must not match the 22nd")
	}
}

func TestNextAfter(t *testing.T) {
	spec := mustSpec("0 9 * * *")
	if got, ok := spec.NextAfter(at(2026, 9, 22, 9, 0)); !ok || !got.Equal(at(2026, 9, 23, 9, 0)) {
		t.Errorf("NextAfter(09:00) = %v ok=%v, want next day 09:00", got, ok)
	}
	// Strictly after: 08:59 still reaches today's slot.
	if got, ok := spec.NextAfter(at(2026, 9, 22, 8, 59)); !ok || !got.Equal(at(2026, 9, 22, 9, 0)) {
		t.Errorf("NextAfter(08:59) = %v ok=%v, want today 09:00", got, ok)
	}
	// Seconds are truncated, so 09:00:30 waits for the next day.
	if got, ok := spec.NextAfter(at(2026, 9, 22, 9, 0).Add(30 * time.Second)); !ok || !got.Equal(at(2026, 9, 23, 9, 0)) {
		t.Errorf("NextAfter(09:00:30) = %v ok=%v, want next day 09:00", got, ok)
	}

	five := mustSpec("*/5 * * * *")
	if got, ok := five.NextAfter(at(2026, 9, 22, 10, 7)); !ok || !got.Equal(at(2026, 9, 22, 10, 10)) {
		t.Errorf("*/5 NextAfter(10:07) = %v ok=%v, want 10:10", got, ok)
	}

	if _, ok := mustSpec("@monthly").NextAfter(at(2026, 9, 22, 12, 0)); !ok {
		t.Error("@monthly has no firing, which is wrong")
	}
}

// Interval schedules are anchored to the call time, not the next whole minute.
func TestNextAfterInterval(t *testing.T) {
	spec := mustSpec("@every 10m")
	base := at(2026, 9, 22, 12, 3)
	if got, ok := spec.NextAfter(base); !ok || !got.Equal(base.Add(10*time.Minute)) {
		t.Errorf("NextAfter = %v ok=%v, want %v", got, ok, base.Add(10*time.Minute))
	}
}

func TestDescribe(t *testing.T) {
	// Describe is a round trip of what the operator typed, shorthand and all,
	// so the UI can show the expression back to them verbatim.
	for _, expr := range []string{"@every 1h", "@every 30m", "@daily", "@hourly", "0 9 * * 1-5"} {
		spec, err := Parse(expr)
		if err != nil {
			t.Fatalf("Parse(%q): %v", expr, err)
		}
		if got := spec.Describe(); got != expr {
			t.Errorf("Describe(%q) = %q", expr, got)
		}
	}
	// Nothing parsed: the dashboard must show an empty plan, not "every 0s".
	if got := (Spec{}).Describe(); got != "" {
		t.Errorf("Describe of zero Spec = %q, want empty", got)
	}
}

// Sub-minute intervals are refused by Parse, so tests build a Spec directly.
func fastInterval(d time.Duration) Spec {
	return Spec{Raw: "test", Kind: Interval, Every: d}
}

func TestSchedulerNeverOverlaps(t *testing.T) {
	var calls int32
	release := make(chan struct{})
	job := func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		<-release
		return nil
	}

	s := NewScheduler(fastInterval(40*time.Millisecond), job)
	s.Start(context.Background())
	defer s.Stop()

	// Hold the one in-flight slot for roughly nine intervals. The loop is
	// blocked inside the job the whole time, so nothing else may start, and the
	// ticks it would have fired must surface as skipped.
	time.Sleep(400 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("job calls = %d while held, want 1: a tick that lands mid-run must not run", got)
	}
	if !s.Running() {
		t.Error("Running is false while the job is in flight")
	}

	close(release)
	s.Stop()

	if n := s.SkippedCount(); n < 1 {
		t.Errorf("SkippedCount = %d, want >= 1 for the eaten intervals", n)
	}
	if n := s.RunCount(); n < 1 {
		t.Errorf("RunCount = %d, want >= 1", n)
	}
	if s.Running() {
		t.Error("Running is still true after Stop")
	}
	if last := s.LastRun(); last.IsZero() {
		t.Error("LastRun is the zero time after a successful run")
	}
}

func TestSchedulerRunOnceRefusesWhileRunning(t *testing.T) {
	s := NewScheduler(fastInterval(time.Hour), func(context.Context) error { return nil })
	s.mu.Lock()
	s.running = true
	s.mu.Unlock()

	if err := s.RunOnce(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Errorf("RunOnce while running = %v, want ErrAlreadyRunning", err)
	}
	if s.RunCount() != 0 {
		t.Errorf("RunCount = %d after a refused run, want 0", s.RunCount())
	}
}

func TestSchedulerRunOnceRuns(t *testing.T) {
	ran := 0
	s := NewScheduler(fastInterval(time.Hour), func(context.Context) error {
		ran++
		return nil
	})
	if err := s.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if ran != 1 || s.RunCount() != 1 {
		t.Errorf("ran=%d RunCount=%d, want 1 each", ran, s.RunCount())
	}
	if s.Running() {
		t.Error("Running is still true after the job returned")
	}
}

func TestSchedulerNextIsAdvertised(t *testing.T) {
	s := NewScheduler(mustSpec("@daily"), func(context.Context) error { return nil })
	if _, ok := s.Next(); ok {
		t.Error("Next before Start should report no firing")
	}
	s.Start(context.Background())
	defer s.Stop()

	// The loop plans its first tick in another goroutine.
	var got time.Time
	var ok bool
	for i := 0; i < 50; i++ {
		if got, ok = s.Next(); ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ok {
		t.Fatal("Next after Start never reported a firing")
	}
	if !got.After(time.Now()) {
		t.Errorf("Next = %v, want a future time", got)
	}
}

func TestSchedulerRecordPeers(t *testing.T) {
	s := NewScheduler(mustSpec("@daily"), func(context.Context) error { return nil })
	s.RecordPeers(5)
	s.RecordPeers(3)
	if n := s.PeersRun(); n != 8 {
		t.Errorf("PeersRun = %d, want 8", n)
	}
}

// A date that never exists must not spin the scheduler.
func TestNextAfterImpossibleDate(t *testing.T) {
	if _, ok := mustSpec("0 0 31 2 *").NextAfter(time.Now()); ok {
		t.Error("Feb 31 has no firing, so NextAfter must report none")
	}
}

func TestSchedulerStopWithoutStart(t *testing.T) {
	s := NewScheduler(mustSpec("@daily"), func(context.Context) error { return nil })
	if s.Stop() {
		t.Error("Stop on an unstarted scheduler should return false")
	}
}

func TestSchedulerCancelsParent(t *testing.T) {
	// A cancel of the parent context must end the loop without a hang.
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{}, 1)
	s := NewScheduler(fastInterval(50*time.Millisecond), func(context.Context) error {
		entered <- struct{}{}
		return nil
	})
	s.Start(ctx)

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the job never ran")
	}

	cancel()
	if !s.Stop() {
		t.Error("Stop after Start should return true")
	}
}

func mustSpec(expr string) Spec {
	s, err := Parse(expr)
	if err != nil {
		panic(err)
	}
	return s
}

// Changing the plan while armed must change the cadence the scheduler actually
// fires on, not just what it reports. Before the fix, a caller that reconfigured
// the interval found the old timer still armed and the change did not take
// effect until the old interval expired.
func TestSchedulerSetSpecRetimes(t *testing.T) {
	var calls int32
	s := NewScheduler(fastInterval(30*time.Millisecond), func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	s.Start(context.Background())
	defer s.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&calls) < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := atomic.LoadInt32(&calls); n < 3 {
		t.Fatalf("job ran %d times, want >= 3 before the spec change", n)
	}

	s.SetSpec(fastInterval(time.Hour))

	// The new plan must already be visible: callers that change the interval
	// immediately read back the next firing.
	nxt, ok := s.Next()
	if !ok || time.Until(nxt) < 50*time.Second {
		t.Fatalf("Next = %v ok=%v after switching to a long interval", nxt, ok)
	}
	if got := s.Spec().Every; got != time.Hour {
		t.Errorf("Spec.Every = %v, want %v", got, time.Hour)
	}

	// Let any tick already due at the moment of the change settle, then the
	// quiet window must be quiet. At most one run can be in flight when the
	// change lands; anything more means the old timer is still firing.
	time.Sleep(50 * time.Millisecond)
	before := atomic.LoadInt32(&calls)
	time.Sleep(300 * time.Millisecond)
	if extra := atomic.LoadInt32(&calls) - before; extra > 1 {
		t.Errorf("job ran %d more times in a 300ms window after switching to a long interval, want <= 1", extra)
	}
}

// Wake signalling must be exactly-once. A stale wake observed twice parks the
// loop on an already-closed channel and it spins, which looks like the scheduler
// silently stopping.
func TestSchedulerSetSpecRepeatedKeepsFiring(t *testing.T) {
	var calls int32
	s := NewScheduler(fastInterval(30*time.Millisecond), func(context.Context) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	s.Start(context.Background())
	defer s.Stop()

	start := time.Now()
	for i := 0; i < 200; i++ {
		s.SetSpec(fastInterval(30 * time.Millisecond))
		if time.Since(start) > 3*time.Second {
			t.Fatal("SetSpec calls are taking too long")
		}
	}

	time.Sleep(100 * time.Millisecond)
	first := atomic.LoadInt32(&calls)
	time.Sleep(300 * time.Millisecond)
	second := atomic.LoadInt32(&calls)
	if second-first < 1 {
		t.Error("the scheduler stopped firing after repeated SetSpec calls")
	}
}

// A leap-day schedule just past Feb 29 has its next firing almost four years
// away; a two-year scan window (as an earlier version used) would have
// reported the expression as having no firing at all.
func TestNextAfterLeapDayAcrossFourYears(t *testing.T) {
	spec, err := Parse("0 0 29 2 *")
	if err != nil {
		t.Fatal(err)
	}
	// One minute after Feb 29, 2028; the next Feb 29 is in 2032.
	start := time.Date(2028, 2, 29, 0, 1, 0, 0, time.UTC)
	next, ok := spec.NextAfter(start)
	if !ok {
		t.Fatal("a leap-day schedule must still fire; the scan window is too short")
	}
	if want := time.Date(2032, 2, 29, 0, 0, 0, 0, time.UTC); !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}
