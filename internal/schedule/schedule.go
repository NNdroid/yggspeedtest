// Package schedule parses and drives cron-style schedules for unattended Yggdrasil
// speed tests.
//
// The parser is hand written rather than a dependency: it supports the five
// classic cron fields plus the @every shorthand, which is all a batch speed test
// needs, and keeps the web server free of a transitive dependency tree.
package schedule

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Kind distinguishes a fixed-interval schedule from a clock-based one.
type Kind int

const (
	// Interval repeats every N minutes, anchored to when it was scheduled.
	Interval Kind = iota
	// Clock fires at specific wall-clock times.
	Clock
)

// Spec is a parsed schedule expression.
type Spec struct {
	Raw   string
	Kind  Kind
	Every time.Duration

	minutes  map[int]bool
	hours    map[int]bool
	days     map[int]bool
	months   map[int]bool
	weekdays map[int]bool
	daysFree bool // day-of-month was "*"
	weekFree bool // day-of-week was "*"
}

var (
	everyRe = regexp.MustCompile(`^@every\s+(\d+)(ms|s|m|h|d|w|mo|y)$`)

	nameMonths = map[string]int{
		"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
		"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
	}
	nameWeekdays = map[string]int{
		"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
	}
	shorthands = map[string]string{
		"@yearly":   "0 0 1 1 *",
		"@annually": "0 0 1 1 *",
		"@monthly":  "0 0 1 * *",
		"@weekly":   "0 0 * * 0",
		"@daily":    "0 0 * * *",
		"@midnight": "0 0 * * *",
		"@hourly":   "0 * * * *",
	}
)

// Parse turns an expression into a Spec. It accepts the five-field cron form
// (minute hour day-of-month month day-of-week) and the @every / @daily style
// shorthands.
func Parse(expr string) (Spec, error) {
	var s Spec
	s.Raw = expr
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return s, errors.New("empty schedule expression")
	}

	if sh, ok := shorthands[strings.ToLower(expr)]; ok {
		expr = sh
	}

	if m := everyRe.FindStringSubmatch(expr); m != nil {
		n, err := strconv.Atoi(m[1])
		if err != nil || n < 1 {
			return s, fmt.Errorf("invalid @every value %q", m[1])
		}
		unit, err := everyUnit(m[2])
		if err != nil {
			return s, err
		}
		if time.Duration(n)*unit < time.Minute {
			return s, errors.New("@every interval must be at least 1 minute")
		}
		s.Kind = Interval
		s.Every = time.Duration(n) * unit
		return s, nil
	}

	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return s, fmt.Errorf("expected 5 fields, got %d: %q", len(fields), expr)
	}

	var err error
	if s.minutes, err = parseField(fields[0], 0, 59, nil); err != nil {
		return s, fmt.Errorf("minute: %w", err)
	}
	if s.hours, err = parseField(fields[1], 0, 23, nil); err != nil {
		return s, fmt.Errorf("hour: %w", err)
	}
	if s.days, err = parseField(fields[2], 1, 31, nil); err != nil {
		return s, fmt.Errorf("day-of-month: %w", err)
	}
	if s.months, err = parseField(fields[3], 1, 12, nameMonths); err != nil {
		return s, fmt.Errorf("month: %w", err)
	}
	if s.weekdays, err = parseField(fields[4], 0, 7, nameWeekdays); err != nil {
		return s, fmt.Errorf("day-of-week: %w", err)
	}
	// Cron accepts 7 as a synonym for Sunday.
	s.weekdays[0] = s.weekdays[0] || s.weekdays[7]
	delete(s.weekdays, 7)
	s.daysFree = fields[2] == "*"
	s.weekFree = fields[4] == "*"

	s.Kind = Clock
	return s, nil
}

// everyUnit maps an @every unit suffix to a duration. A month is 30 days and a
// year 365, because cron has no month arithmetic.
func everyUnit(u string) (time.Duration, error) {
	switch u {
	case "ms":
		return time.Millisecond, nil
	case "s":
		return time.Second, nil
	case "m":
		return time.Minute, nil
	case "h":
		return time.Hour, nil
	case "d":
		return 24 * time.Hour, nil
	case "w":
		return 7 * 24 * time.Hour, nil
	case "mo":
		return 30 * 24 * time.Hour, nil
	case "y":
		return 365 * 24 * time.Hour, nil
	}
	return 0, fmt.Errorf("unknown @every unit %q", u)
}

// parseField expands one cron field into a set of allowed values. It supports
// "*", lists, ranges, steps, and the month/day names.
func parseField(field string, lo, hi int, names map[string]int) (map[int]bool, error) {
	out := make(map[int]bool)
	for _, part := range strings.Split(field, ",") {
		if strings.TrimSpace(part) == "" {
			return nil, errors.New("empty list item")
		}

		step := 1
		if slash := strings.Index(part, "/"); slash >= 0 {
			n, err := strconv.Atoi(part[slash+1:])
			if err != nil || n < 1 {
				return nil, fmt.Errorf("invalid step in %q", part)
			}
			step = n
			part = part[:slash]
		}

		start, end, err := fieldBounds(part, lo, hi, names)
		if err != nil {
			return nil, err
		}

		for v := start; v <= end; v += step {
			if v < lo || v > hi {
				return nil, fmt.Errorf("value %d out of range [%d, %d]", v, lo, hi)
			}
			out[v] = true
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("field %q matched no values", field)
	}
	return out, nil
}

// fieldBounds resolves the low and high end of one cron list item.
func fieldBounds(part string, lo, hi int, names map[string]int) (int, int, error) {
	if part == "*" {
		return lo, hi, nil
	}
	if dash := strings.Index(part, "-"); dash >= 0 {
		a, err := fieldValue(part[:dash], lo, hi, names)
		if err != nil {
			return 0, 0, err
		}
		b, err := fieldValue(part[dash+1:], lo, hi, names)
		if err != nil {
			return 0, 0, err
		}
		if a > b {
			return 0, 0, fmt.Errorf("range %q is empty", part)
		}
		return a, b, nil
	}
	v, err := fieldValue(part, lo, hi, names)
	if err != nil {
		return 0, 0, err
	}
	return v, v, nil
}

// fieldValue converts a single token to an integer, honouring named values.
func fieldValue(tok string, lo, hi int, names map[string]int) (int, error) {
	if names != nil {
		if v, ok := names[strings.ToLower(tok)]; ok {
			return v, nil
		}
	}
	v, err := strconv.Atoi(tok)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", tok)
	}
	if v < lo || v > hi {
		return 0, fmt.Errorf("value %d out of range [%d, %d]", v, lo, hi)
	}
	return v, nil
}

// Match reports whether a clock time satisfies this schedule.
func (s Spec) Match(t time.Time) bool {
	if s.Kind == Interval {
		return true
	}
	if !s.months[int(t.Month())] || !s.hours[t.Hour()] || !s.minutes[t.Minute()] {
		return false
	}
	domOK := s.days[t.Day()]
	dowOK := s.weekdays[int(t.Weekday())]
	switch {
	case s.daysFree && s.weekFree:
		return true
	case s.daysFree:
		return dowOK
	case s.weekFree:
		return domOK
	}
	// Both restricted: cron matches when either one does.
	return domOK || dowOK
}

// NextAfter returns the first time strictly after t that this schedule fires,
// scanning forward at most four years. Four years is what a leap-day schedule
// needs in the worst case: just past Feb 29, the next one is almost four full
// years away, and a two-year window (as an earlier version used) would have
// reported `0 0 29 2 *` as having no firing at all.
func (s Spec) NextAfter(t time.Time) (time.Time, bool) {
	if s.Kind == Interval {
		return t.Add(s.Every), true
	}
	c := t.Truncate(time.Minute).Add(time.Minute)
	for i := 0; i < 4*366*24*60; i++ {
		if s.Match(c) {
			return c, true
		}
		c = c.Add(time.Minute)
	}
	return time.Time{}, false
}

// Describe reports the expression the operator typed. That is already the
// clearest summary — rendering an interval through time.Duration.String()
// would turn "@every 30m" into "every 30m0s". A zero Spec, which nothing
// was parsed into, reports as unset: Interval is the zero value of Kind, so a
// plain Spec{} would otherwise claim to be "every 0s".
func (s Spec) Describe() string {
	return s.Raw
}
