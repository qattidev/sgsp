package sgsp

import (
	"net"
	"sync"
	"time"
)

const (
	maxHandshakeSources = 4096
	handshakeSourceIdle = time.Minute
)

// handshakeLimiter bounds pre-authentication work by source IP. Source IP is
// a rate key only: it is not an identity or session-affinity mechanism.
type handshakeLimiter struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	now     func() time.Time
	sources map[string]handshakeBucket
}

type handshakeBucket struct {
	tokens float64
	seen   time.Time
}

func newHandshakeLimiter(rate, burst int, now func() time.Time) *handshakeLimiter {
	if now == nil {
		now = time.Now
	}
	return &handshakeLimiter{rate: float64(rate), burst: float64(burst), now: now, sources: make(map[string]handshakeBucket)}
}

func (l *handshakeLimiter) allow(address net.Addr) bool {
	if l == nil {
		return true
	}
	key := sourceIP(address)
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	bucket, exists := l.sources[key]
	if !exists {
		for candidate, existing := range l.sources {
			if now.Sub(existing.seen) >= handshakeSourceIdle {
				delete(l.sources, candidate)
			}
		}
		if len(l.sources) >= maxHandshakeSources {
			return false
		}
		bucket = handshakeBucket{tokens: l.burst, seen: now}
	} else {
		elapsed := now.Sub(bucket.seen).Seconds()
		if elapsed > 0 {
			bucket.tokens = min(l.burst, bucket.tokens+elapsed*l.rate)
		}
		bucket.seen = now
	}
	if bucket.tokens < 1 {
		l.sources[key] = bucket
		return false
	}
	bucket.tokens--
	l.sources[key] = bucket
	return true
}

func sourceIP(address net.Addr) string {
	if address == nil {
		return ""
	}
	if udp, ok := address.(*net.UDPAddr); ok {
		return udp.IP.String()
	}
	host, _, err := net.SplitHostPort(address.String())
	if err == nil {
		return host
	}
	return address.String()
}
