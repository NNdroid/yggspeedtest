package web

import (
	"encoding/json"
	"sync"
)

// subscriberBuffer bounds how far a slow client can fall behind. A client that
// overflows it loses events, but the UI re-fetches the run list on demand, so
// it recovers rather than showing a permanently stale view.
const subscriberBuffer = 256

// hub fans broadcast events out to every connected event-stream client.
type hub struct {
	mu   sync.Mutex
	subs map[chan message]struct{}
}

func newHub() *hub {
	return &hub{subs: make(map[chan message]struct{})}
}

// subscribe opens a client channel. It must be paired with unsubscribe.
func (h *hub) subscribe() chan message {
	ch := make(chan message, subscriberBuffer)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

// unsubscribe closes and removes a client channel.
func (h *hub) unsubscribe(ch chan message) {
	h.mu.Lock()
	if _, ok := h.subs[ch]; ok {
		delete(h.subs, ch)
		close(ch)
	}
	h.mu.Unlock()
}

// publish sends one event to every subscriber, dropping it for any that are
// full. Dropping beats blocking: the publisher is a measurement goroutine, and
// a stuck browser must never be able to hold up a speed test.
func (h *hub) publish(event string, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	msg := message{event: event, data: data}

	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- msg:
		default:
		}
	}
}
