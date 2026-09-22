package schedule

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrAlreadyRunning is returned by RunOnce and claimed by a tick when a run is
// already in flight.
var ErrAlreadyRunning = errors.New("a run is already in progress")

// Job is one unattended speed test. ctx is cancelled when the scheduler stops,
// so a job that honours it returns promptly and Stop can complete.
type Job func(context.Context) error

// Scheduler runs one Job repeatedly on a Spec. The job never overlaps itself:
// a tick that arrives while the previous run is still going is dropped and
// counted as skipped, and RunOnce is refused while a run is in flight.
type Scheduler struct {
	spec Spec
	job  Job

	mu      sync.Mutex
	running bool
	stopped bool
	skipID  int64

	stopCh chan struct{}
	done   chan struct{}
	// replan wakes the loop so it drops a timer set for a superseded spec.
	// wakeLoop closes the channel the loop is parked on and installs a fresh
	// one; the loop does the same after waking, so it never parks on a closed
	// channel and a wake cannot fire twice.
	replan chan struct{}

	next     time.Time
	nextSet  bool
	lastRun  time.Time
	lastErr  error
	runs     int64
	skipped  int64
	peersRun int64
}

// NewScheduler binds a parsed schedule to a job. The job must be safe to call
// concurrently with itself only in the sense described above: it never is.
func NewScheduler(spec Spec, job Job) *Scheduler {
	if job == nil {
		job = func(context.Context) error { return nil }
	}
	return &Scheduler{spec: spec, job: job}
}

// Spec returns the parsed schedule this scheduler fires on.
func (s *Scheduler) Spec() Spec {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spec
}

// SetSpec adopts a new firing plan. It re-plans synchronously so a caller that
// immediately asks for the next firing gets the new one, and it wakes the loop
// so the timer the loop set for the old cadence is discarded — without the
// wake, a change would take effect when the old timer expired, which for a
// longer interval can be hours.
func (s *Scheduler) SetSpec(spec Spec) {
	s.mu.Lock()
	if s.stopCh == nil || s.stopped {
		s.spec = spec
		s.mu.Unlock()
		return
	}
	s.spec = spec
	s.nextSet = false
	s.mu.Unlock()

	s.planNext()
	s.wakeLoop()
}

// wakeLoop delivers exactly one wake to the loop: close the channel it is
// parked on and install a fresh one. The replacement is what keeps a wake from
// being observed twice — a closed channel always fires, so parking on one
// again would spin the loop. Each closed channel is closed exactly once,
// because it is always replaced before it is closed and a replaced channel is
// never the one parked on.
func (s *Scheduler) wakeLoop() {
	s.mu.Lock()
	old := s.replan
	s.replan = make(chan struct{})
	s.mu.Unlock()

	if old != nil {
		close(old)
	}
}

// Next reports the wall-clock time the next tick is planned for. The zero time
// means the scheduler is not scheduled, has no future firing, or has stopped.
func (s *Scheduler) Next() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || !s.nextSet {
		return time.Time{}, false
	}
	return s.next, true
}

// LastRun is the start time of the most recent run, or the zero time.
func (s *Scheduler) LastRun() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRun
}

// LastError is the most recent run's error.
func (s *Scheduler) LastError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

// RunCount is how many runs have started.
func (s *Scheduler) RunCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runs
}

// SkippedCount is how many ticks were dropped because a run was still going.
func (s *Scheduler) SkippedCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.skipped
}

// PeersRun is the total number of peers tested across all runs.
func (s *Scheduler) PeersRun() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peersRun
}

// RecordPeers adds peers counted by the caller's OnResult callback.
func (s *Scheduler) RecordPeers(n int64) {
	s.mu.Lock()
	s.peersRun += n
	s.mu.Unlock()
}

// Running reports whether a run is in flight.
func (s *Scheduler) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// Stop returns false when Start was never called or Stop was already called.
func (s *Scheduler) Stop() bool {
	s.mu.Lock()
	if s.stopCh == nil || s.stopped {
		s.mu.Unlock()
		return false
	}
	s.stopped = true
	close(s.stopCh)
	done := s.done
	s.mu.Unlock()

	<-done
	return true
}

// Start begins firing. It is a no-op if already started. The first firing is
// planned synchronously: the loop that would plan it runs in a fresh goroutine,
// so returning without it left Next reporting the zero time until that
// goroutine happened to run.
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	if s.stopCh != nil {
		s.mu.Unlock()
		return
	}
	s.stopCh = make(chan struct{})
	s.done = make(chan struct{})
	s.replan = make(chan struct{})
	s.mu.Unlock()

	s.planNext()
	go s.loop(ctx)
}

// RunOnce triggers a single run immediately, outside the schedule. It is
// refused with ErrAlreadyRunning if a scheduled or manual run is in flight.
func (s *Scheduler) RunOnce(ctx context.Context) error {
	if !s.claim() {
		return ErrAlreadyRunning
	}
	started := time.Now()
	err := s.job(ctx)
	s.finish(started, err)
	return err
}

// loop is the fire-and-wait cycle. It never returns until stopped, so Stop
// waits on done rather than polling.
func (s *Scheduler) loop(ctx context.Context) {
	defer s.awaken()

	for {
		fire, ok, wantStop := s.planNext()
		if wantStop {
			return
		}
		if !ok {
			// The schedule has no firing within two years: nothing to do, and
			// spinning would burn CPU. Wait for a stop.
			select {
			case <-s.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}

		remaining := time.Until(fire)
		if remaining <= 0 {
			remaining = time.Millisecond
		}
		timer := time.NewTimer(remaining)

		select {
		case <-s.stopCh:
			timer.Stop()
			return
		case <-ctx.Done():
			timer.Stop()
			return
		case <-s.replan:
			// The plan changed, so this timer was set for the old cadence.
			// Install a fresh channel: the one woken is closed, and parking on
			// it again would fire forever.
			timer.Stop()
			s.mu.Lock()
			s.replan = make(chan struct{})
			s.mu.Unlock()
			continue
		case <-timer.C:
		}

		if !s.claim() {
			s.mu.Lock()
			s.skipped++
			s.mu.Unlock()
			continue
		}

		// Advance before the job runs so a slow job cannot shift the cadence.
		s.advance(fire)

		started := time.Now()
		err := s.job(ctx)
		s.finish(started, err)
	}
}

// planNext computes and stores the next firing. It returns wantStop when the
// scheduler has been stopped.
func (s *Scheduler) planNext() (time.Time, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return time.Time{}, false, true
	}

	now := time.Now()
	if s.nextSet && s.next.After(now) {
		return s.next, true, false
	}

	fire, ok := s.spec.NextAfter(now)
	if !ok {
		s.nextSet = false
		return time.Time{}, false, false
	}
	s.next = fire
	s.nextSet = true
	return fire, true, false
}

// advance records the next planned tick after fire. A planned tick in the past
// is left alone: planNext recomputes it from now instead of firing in a burst.
func (s *Scheduler) advance(fire time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if nx, ok := s.spec.NextAfter(fire); ok {
		s.next = nx
		s.nextSet = true
	}
}

// claim takes the single in-flight slot.
func (s *Scheduler) claim() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running || s.stopped {
		return false
	}
	s.running = true
	return true
}

// finish releases the in-flight slot and records the outcome. An interval job
// that runs longer than the interval is the one real misconfiguration a
// schedule can have, so those eaten ticks are counted rather than hidden:
// otherwise an operator would see a run every ten minutes and never notice
// that it was really running every fifteen.
func (s *Scheduler) finish(started time.Time, err error) {
	elapsed := time.Since(started)

	s.mu.Lock()
	defer s.mu.Unlock()

	// The plan is read here under the lock: SetSpec may replace it while a job is
	// in flight, and the interval a run outlived is the criterion for eaten ticks.
	var missed int64
	if s.spec.Kind == Interval && s.spec.Every > 0 && elapsed > s.spec.Every {
		missed = int64(elapsed / s.spec.Every)
	}

	s.running = false
	s.lastRun = started
	s.lastErr = err
	s.runs++
	s.skipped += missed
}

// awaken closes done so Stop returns.
func (s *Scheduler) awaken() {
	s.mu.Lock()
	d := s.done
	s.mu.Unlock()
	if d != nil {
		close(d)
	}
}
