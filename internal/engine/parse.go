package engine

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
)

// ParseByteSize turns "1.5MB" into a byte count. Fractions are accepted and
// int64 overflow is rejected rather than silently wrapped, since a wrapped
// value would truncate a download that was supposed to be unlimited.
func ParseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" || s == "0" {
		return 0, nil
	}

	multiplier := int64(1)
	switch {
	case strings.HasSuffix(s, "KB") || strings.HasSuffix(s, "K"):
		multiplier = 1024
		s = strings.TrimRight(s, "KKB")
	case strings.HasSuffix(s, "MB") || strings.HasSuffix(s, "M"):
		multiplier = 1024 * 1024
		s = strings.TrimRight(s, "MMB")
	case strings.HasSuffix(s, "GB") || strings.HasSuffix(s, "G"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimRight(s, "GGB")
	}

	// TrimRight drops a trailing "0" only when the value was exactly "0",
	// which is handled above. Anything left here must be a number.
	trimmed := strings.TrimSpace(s)
	value, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", trimmed, err)
	}
	if value < 0 {
		return 0, errors.New("size must not be negative")
	}

	total := value * float64(multiplier)
	if total > math.MaxInt64 {
		return 0, fmt.Errorf("size %s overflows int64", trimmed)
	}
	return int64(total), nil
}

// getUniqueAdminPort hands out a free loopback port to each spawned admin
// service. The counter is monotonic and wraps inside the reserved range, so
// two concurrent tests never share a port and a long-running process does not
// eventually run out.
func getUniqueAdminPort() int {
	p := int(atomic.AddInt32(&nextAdminPort, 1))
	r := p % adminPortCount
	if r < 0 {
		r += adminPortCount
	}
	return adminPortBase + r
}
