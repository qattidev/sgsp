package sgsp

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestHandshakeLimiterRateAndIdleEviction(t *testing.T) {
	now := time.Unix(100, 0)
	limiter := newHandshakeLimiter(2, 2, func() time.Time { return now })
	address := &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1234}
	if !limiter.allow(address) || !limiter.allow(address) || limiter.allow(address) {
		t.Fatal("burst was not enforced")
	}
	now = now.Add(500 * time.Millisecond)
	if !limiter.allow(address) {
		t.Fatal("token was not replenished")
	}
	for i := 0; i < maxHandshakeSources-1; i++ {
		address := &net.UDPAddr{IP: net.IPv4(198, 51, byte(i>>8), byte(i)), Port: 1}
		if !limiter.allow(address) {
			t.Fatalf("source %d unexpectedly rejected", i)
		}
	}
	if limiter.allow(&net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 1}) {
		t.Fatal("source table exceeded its bound")
	}
	now = now.Add(handshakeSourceIdle + time.Nanosecond)
	if !limiter.allow(&net.UDPAddr{IP: net.ParseIP("203.0.113.1"), Port: 1}) {
		t.Fatal("idle source was not evicted")
	}
}

func TestClosedGroupRetention(t *testing.T) {
	owner := Owner{ID: "owner"}
	server := &serverEndpoint{
		config:   ServerConfig{App: AppIdentity{ID: "app", Version: "1"}, CommitGroupClose: func(context.Context, AppIdentity, string, Owner) error { return nil }},
		owner:    owner,
		limits:   DefaultLimits(),
		groups:   make(map[string]*groupState),
		groupTTL: 10 * time.Millisecond,
	}
	if err := server.CloseGroup(context.Background(), "match"); err != nil {
		t.Fatal(err)
	}
	server.groupMu.Lock()
	group := server.groups["match"]
	server.groupMu.Unlock()
	if group == nil || !group.closed || !group.persisted {
		t.Fatalf("closed group was not retained: %#v", group)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		server.groupMu.Lock()
		_, retained := server.groups["match"]
		server.groupMu.Unlock()
		if !retained {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("durably closed group was not released after retention")
}

func TestTerminalIDCacheDistinguishesExpiredSessions(t *testing.T) {
	server := &serverEndpoint{
		limits:      DefaultLimits(),
		sessions:    make(map[SessionID]*sessionRecord),
		terminals:   make(map[SessionID]terminalSession),
		terminalTTL: 10 * time.Millisecond,
	}
	session := newSessionRecord(SessionID{1}, Owner{}, "", Principal{}, server.limits, false, nil)
	server.sessions[session.ID()] = session
	server.closeSession(session, SessionExpired)
	if _, code := server.lookupSession(session.ID()); code != SessionExpired {
		t.Fatalf("terminal cache code = %v", code)
	}
	time.Sleep(20 * time.Millisecond)
	if _, code := server.lookupSession(session.ID()); code != SessionNotFound {
		t.Fatalf("expired terminal cache code = %v", code)
	}
}

func TestTerminalIDCacheIsBounded(t *testing.T) {
	now := time.Now()
	server := &serverEndpoint{
		limits:      Limits{MaxSessions: 2},
		terminals:   make(map[SessionID]terminalSession),
		terminalTTL: time.Hour,
	}
	defer server.stopTerminalTimer()
	for index := 1; index <= 4; index++ {
		server.terminals[SessionID{byte(index)}] = terminalSession{code: SessionExpired, expires: now.Add(time.Duration(index) * time.Minute)}
	}
	newID := SessionID{5}
	server.mu.Lock()
	server.recordTerminalLocked(newID, SessionExpired)
	server.mu.Unlock()
	if got, want := len(server.terminals), 2*server.limits.MaxSessions; got != want {
		t.Fatalf("terminal cache entries = %d, want %d", got, want)
	}
	if _, retained := server.terminals[SessionID{1}]; retained {
		t.Fatal("terminal cache retained its earliest expiry after reaching capacity")
	}
	if _, retained := server.terminals[newID]; !retained {
		t.Fatal("terminal cache did not retain newest terminal ID")
	}
}
