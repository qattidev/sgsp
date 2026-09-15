package sgsp

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	goruntime "runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"qattidev/sgsp/internal/quictransport"
	"qattidev/sgsp/internal/runtime"
	"qattidev/sgsp/internal/transport"
	"qattidev/sgsp/internal/wire"
)

type serverEndpoint struct {
	config            ServerConfig
	owner             Owner
	limits            Limits
	mu                sync.RWMutex
	sessions          map[SessionID]*sessionRecord
	terminals         map[SessionID]terminalSession
	terminalTTL       time.Duration
	terminalTimer     *time.Timer
	pending           int
	groupMu           sync.Mutex
	groups            map[string]*groupState
	limiter           *handshakeLimiter
	groupTTL          time.Duration
	draining          bool
	served            bool
	observer          *observerQueue
	applicationBudget *runtime.Budget
	incoming          *incomingQueue
	changed           chan struct{}
	listener          *quictransport.Listener
	scheduler         *handlerScheduler
	workers           sync.WaitGroup
	started           chan struct{}
}
type groupState struct {
	closed    bool
	persisted bool
	timer     *time.Timer
	sessions  map[*sessionRecord]struct{}
}
type terminalSession struct {
	code    Code
	expires time.Time
}
type clientEndpoint struct {
	session           *sessionRecord
	connection        transport.Conn
	packet            net.PacketConn
	mode              DispatchMode
	incoming          *incomingQueue
	endpoint          Endpoint
	config            ClientConfig
	limits            Limits
	resumeSecret      string
	resumeGrace       time.Duration
	scheduler         *handlerScheduler
	applicationBudget *runtime.Budget
	observer          *observerQueue
	mu                sync.Mutex
	closed            bool
	reconnecting      bool
	closeOnce         sync.Once
}

func NewServer(config ServerConfig) (Server, error) {
	limits, err := normalizeLimits(config.Limits)
	if err != nil {
		return nil, err
	}
	if config.TLS == nil || config.TLS.InsecureSkipVerify || len(config.TLS.Certificates) == 0 && config.TLS.GetCertificate == nil || config.App.ID == "" || config.App.Version == "" || config.Owner.ID == "" || config.Owner.Endpoint.Address == "" || config.Owner.Endpoint.ServerName == "" || config.Auth == nil {
		return nil, ErrInvalidArgument
	}
	if config.Owner.Incarnation != (Incarnation{}) {
		return nil, ErrInvalidArgument
	}
	if config.Dispatch.Mode == Polling && config.Dispatch.Router != nil || config.Dispatch.Mode == Handlers && config.Dispatch.Router == nil {
		return nil, ErrInvalidArgument
	}
	owner := config.Owner
	if _, err := rand.Read(owner.Incarnation[:]); err != nil {
		return nil, err
	}
	config.Limits, config.Owner = limits, owner
	config.Dispatch.Router = config.Dispatch.Router.snapshot()
	server := &serverEndpoint{config: config, owner: owner, limits: limits, sessions: make(map[SessionID]*sessionRecord), terminals: make(map[SessionID]terminalSession), terminalTTL: 30 * time.Second, groups: make(map[string]*groupState), limiter: newHandshakeLimiter(limits.HandshakesPerIPPerSecond, limits.HandshakeBurstPerIP, nil), groupTTL: 35 * time.Second, observer: newObserverQueue(config.Observer, 4096, time.Second), applicationBudget: runtime.NewBudget(limits.GlobalApplicationBytes), changed: make(chan struct{}, 1), started: make(chan struct{})}
	if config.Dispatch.Mode == Polling {
		server.incoming = newIncomingQueue(limits.GlobalApplicationBytes, limits.QueueMessages*limits.MaxSessions, server.applicationBudget)
	}
	return server, nil
}

func (s *serverEndpoint) Owner() Owner { return s.owner }
func (s *serverEndpoint) observe(name, kind string, code Code, value float64) {
	if s != nil && s.observer != nil {
		s.observer.Observe(Observation{Name: name, Kind: kind, Code: code, Value: value})
	}
}

func (s *serverEndpoint) armExpiry(session *sessionRecord, expiresAt time.Time) {
	delay := time.Until(expiresAt)
	if delay < 0 {
		delay = 0
	}
	session.mu.Lock()
	if session.expiryTimer != nil {
		session.expiryTimer.Stop()
	}
	session.expiryTimer = time.AfterFunc(delay, func() { s.closeSession(session, AuthExpired) })
	session.mu.Unlock()
}
func (s *serverEndpoint) armGrace(session *sessionRecord) {
	session.mu.Lock()
	if session.state != Suspended {
		session.mu.Unlock()
		return
	}
	delay := s.limits.ResumeGrace
	if untilExpiry := time.Until(session.principal.ExpiresAt); untilExpiry < delay {
		delay = untilExpiry
	}
	if delay < 0 {
		delay = 0
	}
	if session.graceTimer != nil {
		session.graceTimer.Stop()
	}
	session.graceTimer = time.AfterFunc(delay, func() { s.closeSession(session, SessionExpired) })
	session.mu.Unlock()
}
func (s *serverEndpoint) closeSession(session *sessionRecord, code Code) {
	if session == nil {
		return
	}
	_ = session.Close(context.Background(), code)
	s.removeSession(session)
}
func (s *serverEndpoint) removeSession(session *sessionRecord) {
	if session == nil {
		return
	}
	session.mu.Lock()
	if session.expiryTimer != nil {
		session.expiryTimer.Stop()
	}
	if session.graceTimer != nil {
		session.graceTimer.Stop()
	}
	session.mu.Unlock()
	closed, terminalCode := session.State() == Closed, session.terminalCode()
	if group := session.GroupKey(); group != "" {
		s.groupMu.Lock()
		if state := s.groups[group]; state != nil {
			delete(state.sessions, session)
		}
		s.groupMu.Unlock()
	}
	s.mu.Lock()
	if s.sessions[session.id] == session {
		delete(s.sessions, session.id)
		if closed {
			s.recordTerminalLocked(session.id, terminalCode)
		}
		s.signalChanged()
	}
	s.mu.Unlock()
}
func (s *serverEndpoint) terminalCacheCapacity() int {
	maxInt := int(^uint(0) >> 1)
	if s.limits.MaxSessions > maxInt/2 {
		return maxInt
	}
	return 2 * s.limits.MaxSessions
}
func (s *serverEndpoint) recordTerminalLocked(id SessionID, code Code) {
	if s.terminals == nil {
		s.terminals = make(map[SessionID]terminalSession)
	}
	now := time.Now()
	s.expireTerminalsLocked(now)
	if _, exists := s.terminals[id]; !exists && len(s.terminals) >= s.terminalCacheCapacity() {
		var oldest SessionID
		var oldestExpiry time.Time
		for candidate, terminal := range s.terminals {
			if oldestExpiry.IsZero() || terminal.expires.Before(oldestExpiry) {
				oldest, oldestExpiry = candidate, terminal.expires
			}
		}
		delete(s.terminals, oldest)
	}
	ttl := s.terminalTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	s.terminals[id] = terminalSession{code: code, expires: now.Add(ttl)}
	s.scheduleTerminalExpiryLocked()
}
func (s *serverEndpoint) expireTerminalsLocked(now time.Time) {
	for id, terminal := range s.terminals {
		if !terminal.expires.After(now) {
			delete(s.terminals, id)
		}
	}
	s.scheduleTerminalExpiryLocked()
}
func (s *serverEndpoint) scheduleTerminalExpiryLocked() {
	if s.terminalTimer != nil {
		s.terminalTimer.Stop()
		s.terminalTimer = nil
	}
	var next time.Time
	for _, terminal := range s.terminals {
		if next.IsZero() || terminal.expires.Before(next) {
			next = terminal.expires
		}
	}
	if next.IsZero() {
		return
	}
	delay := time.Until(next)
	if delay < 0 {
		delay = 0
	}
	s.terminalTimer = time.AfterFunc(delay, func() {
		s.mu.Lock()
		s.expireTerminalsLocked(time.Now())
		s.mu.Unlock()
	})
}
func (s *serverEndpoint) lookupSession(id SessionID) (*sessionRecord, Code) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireTerminalsLocked(time.Now())
	if session := s.sessions[id]; session != nil {
		return session, Normal
	}
	if terminal, ok := s.terminals[id]; ok && terminal.code == SessionExpired {
		return nil, SessionExpired
	}
	return nil, SessionNotFound
}
func (s *serverEndpoint) stopTerminalTimer() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminalTimer != nil {
		s.terminalTimer.Stop()
		s.terminalTimer = nil
	}
}
func (s *serverEndpoint) signalChanged() {
	select {
	case s.changed <- struct{}{}:
	default:
	}
}
func (s *serverEndpoint) groupOpen(group string) bool {
	if group == "" {
		return true
	}
	s.groupMu.Lock()
	defer s.groupMu.Unlock()
	state := s.groups[group]
	return state == nil || !state.closed
}
func (s *serverEndpoint) retainClosedGroup(groupKey string, group *groupState) {
	if group == nil {
		return
	}
	s.groupMu.Lock()
	defer s.groupMu.Unlock()
	if s.groups[groupKey] != group || !group.closed || group.persisted {
		return
	}
	group.persisted = true
	ttl := s.groupTTL
	if ttl <= 0 {
		ttl = 35 * time.Second
	}
	group.timer = time.AfterFunc(ttl, func() {
		s.groupMu.Lock()
		defer s.groupMu.Unlock()
		if s.groups[groupKey] == group && group.closed && group.persisted {
			delete(s.groups, groupKey)
		}
	})
}
func (s *serverEndpoint) stopGroupTimers() {
	s.groupMu.Lock()
	defer s.groupMu.Unlock()
	for _, group := range s.groups {
		if group.timer != nil {
			group.timer.Stop()
		}
	}
}
func (s *serverEndpoint) finishConnection(session *sessionRecord, operations *connectionOperations) {
	if session.suspendIfCurrent(operations) {
		operations.deliverLifecycle(ConnectionLost, Normal)
		s.armGrace(session)
		return
	}
	if session.State() == Closed {
		s.removeSession(session)
	}
}
func (s *serverEndpoint) Serve(ctx context.Context, packet net.PacketConn) error {
	s.mu.Lock()
	if s.served {
		s.mu.Unlock()
		return ErrInvalidArgument
	}
	s.served = true
	if s.config.Dispatch.Mode == Handlers {
		s.scheduler = newHandlerScheduler(max(2, goruntime.GOMAXPROCS(0)), s.limits.MaxSessions)
	}
	s.mu.Unlock()
	listener, err := quictransport.Listen(packet, transportConfig(s.config.TLS, s.limits))
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	observeCtx, stopObserving := context.WithCancel(ctx)
	close(s.started)
	if s.incoming != nil {
		s.incoming.startObserving(ctx, s.observer)
	}
	if s.observer != nil {
		s.workers.Add(1)
		go func() {
			defer s.workers.Done()
			s.observeSessionStates(observeCtx)
		}()
	}
	defer func() {
		stopObserving()
		_ = listener.Close()
		for _, session := range s.Sessions() {
			s.closeSession(session.(*sessionRecord), ServerUnavailable)
		}
		_ = packet.Close()
		s.workers.Wait()
		if s.incoming != nil {
			s.incoming.Close()
		}
		if s.observer != nil {
			s.observer.Close()
		}
		s.scheduler.close()
		s.stopGroupTimers()
		s.stopTerminalTimer()
	}()
	for {
		connection, err := listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		s.observe("handshakes", "attempt", Normal, 1)
		if !s.limiter.allow(connection.RemoteAddr()) {
			s.observe("handshakes", "result", ResourceExhausted, 1)
			_ = connection.Close(uint64(ResourceExhausted), "handshake rate exceeded")
			continue
		}
		s.mu.Lock()
		if s.pending >= s.limits.MaxPendingHandshakes {
			s.mu.Unlock()
			s.observe("handshakes", "result", ResourceExhausted, 1)
			_ = connection.Close(uint64(ResourceExhausted), "too many pending handshakes")
			continue
		}
		s.pending++
		s.mu.Unlock()
		s.workers.Add(1)
		go func() {
			defer s.workers.Done()
			s.handleConnection(connection)
		}()
	}
}
func (s *serverEndpoint) handleConnection(connection transport.Conn) {
	var handshakeResult sync.Once
	completeHandshake := func(code Code) {
		handshakeResult.Do(func() { s.observe("handshakes", "result", code, 1) })
	}
	rejectHandshake := func(code Code, message string) {
		completeHandshake(code)
		_ = connection.Close(uint64(code), message)
	}
	var releasePending sync.Once
	finishPending := func() {
		releasePending.Do(func() { s.mu.Lock(); s.pending--; s.mu.Unlock() })
	}
	defer finishPending()
	ctx, cancel := context.WithTimeout(connection.Context(), s.limits.AuthTimeout)
	defer cancel()
	stream, err := connection.AcceptBidi(ctx)
	if err != nil {
		rejectHandshake(ProtocolViolation, "missing control stream")
		return
	}
	channel := newControlChannel(stream)
	control, err := channel.read(ctx, s.limits.ControlBytes)
	if err != nil {
		rejectHandshake(ProtocolViolation, "invalid control")
		return
	}
	if control.Op != "hello" {
		rejectHandshake(ProtocolViolation, "expected hello")
		return
	}
	hello, err := parseHello(control)
	if err != nil {
		rejectHandshake(ProtocolViolation, "invalid hello")
		return
	}
	supported := map[string]bool{}
	if connection.Stats().DatagramsEnabled {
		supported["datagrams"] = true
	}
	if err := wire.RequireCapabilities(control, supported); err != nil {
		rejectHandshake(UnsupportedCapability, "required capability unavailable")
		return
	}
	if len(hello.Group) > s.limits.MaxGroupKeyBytes {
		rejectHandshake(InvalidArgument, "group key too large")
		return
	}
	if err := validateLimitMessage(hello.Limits); err != nil {
		rejectHandshake(InvalidArgument, "invalid receive limits")
		return
	}
	if hello.Role != roleName(s.config.Role) || hello.App != s.config.App || !connection.Stats().DatagramsEnabled && s.config.Role == GameRole {
		rejectHandshake(UnsupportedCapability, "unsupported capability")
		return
	}
	credentialData, err := base64.RawURLEncoding.DecodeString(hello.Credential.Data)
	if err != nil {
		rejectHandshake(Unauthenticated, "authentication failed")
		return
	}
	authStarted := time.Now()
	principal, err := s.config.Auth.Authenticate(ctx, Credential{Scheme: hello.Credential.Scheme, Data: credentialData})
	authCode := Normal
	if err != nil || principal.Issuer == "" || principal.Subject == "" || !principal.ExpiresAt.After(time.Now()) {
		authCode = Unauthenticated
	}
	s.observe("auth", histogramKind("verify", time.Since(authStarted)), authCode, 1)
	if err != nil || principal.Issuer == "" || principal.Subject == "" || !principal.ExpiresAt.After(time.Now()) {
		rejectHandshake(Unauthenticated, "authentication failed")
		return
	}
	finishPending()
	if hello.Resume != nil {
		s.resumeConnection(connection, channel, hello, principal, completeHandshake)
		return
	}
	if err := s.authorizeAdmission(ctx, hello, principal); err != nil {
		rejectHandshake(Forbidden, "admission rejected")
		return
	}
	secret := make([]byte, 32)
	if s.config.Role == GameRole {
		if _, err := rand.Read(secret); err != nil {
			rejectHandshake(Internal, "random failure")
			return
		}
	}
	var group *groupState
	if hello.Group != "" {
		if s.config.CommitGroupClose == nil {
			rejectHandshake(InvalidArgument, "group closure is not configured")
			return
		}
		s.groupMu.Lock()
		group = s.groups[hello.Group]
		if group == nil {
			if len(s.groups) >= s.limits.MaxActiveGroups {
				s.groupMu.Unlock()
				rejectHandshake(ResourceExhausted, "group capacity reached")
				return
			}
			group = &groupState{sessions: make(map[*sessionRecord]struct{})}
			s.groups[hello.Group] = group
		}
		if group.closed {
			s.groupMu.Unlock()
			rejectHandshake(GroupClosed, "group closed")
			return
		}
	}
	s.mu.Lock()
	if s.draining || len(s.sessions) >= s.limits.MaxSessions {
		s.mu.Unlock()
		if group != nil {
			s.groupMu.Unlock()
		}
		rejectHandshake(ServerDraining, "server unavailable")
		return
	}
	var id SessionID
	if _, err := rand.Read(id[:]); err != nil {
		s.mu.Unlock()
		if group != nil {
			s.groupMu.Unlock()
		}
		rejectHandshake(Internal, "random failure")
		return
	}
	ops := &connectionOperations{connection: connection, control: channel, mode: s.config.Dispatch.Mode, incoming: s.incoming, scheduler: s.scheduler, applicationBudget: s.applicationBudget, observer: s.observer}
	session := newSessionRecord(id, s.owner, hello.Group, principal, s.limits, false, ops)
	ops.epoch = 1
	session.resumeHash = sha256.Sum256(secret)
	session.peerLimits = hello.Limits
	ops.session, ops.router = session, s.config.Dispatch.Router
	ops.onIncomingRefresh = func(ctx context.Context, control wire.Control) refreshResultMessage {
		return s.handleRefresh(ctx, session, control)
	}
	s.sessions[id] = session
	delete(s.terminals, id)
	s.signalChanged()
	s.mu.Unlock()
	if group != nil {
		group.sessions[session] = struct{}{}
		s.groupMu.Unlock()
	}
	s.armExpiry(session, principal.ExpiresAt)
	defer s.finishConnection(session, ops)
	welcome := welcomeMessage{Op: "welcome", SessionID: hex.EncodeToString(id[:]), Epoch: "1", Owner: ownerMessage{ID: s.owner.ID, Incarnation: hex.EncodeToString(s.owner.Incarnation[:]), Address: s.owner.Endpoint.Address, ServerName: s.owner.Endpoint.ServerName}, Principal: principalMessage{Issuer: principal.Issuer, Subject: principal.Subject, ExpiresMS: principal.ExpiresAt.UnixMilli()}, Limits: limitMessageFrom(s.limits), Capabilities: []string{"datagrams"}, Resumed: false, Resumable: s.config.Role == GameRole, ResumeGraceMS: int(s.limits.ResumeGrace / time.Millisecond), ResumeSecret: base64.RawURLEncoding.EncodeToString(secret)}
	if s.config.Role == BootstrapRole {
		welcome.Resumable, welcome.ResumeGraceMS, welcome.ResumeSecret = false, 0, ""
	}
	if err := channel.write(welcome); err != nil {
		rejectHandshake(Internal, "welcome failed")
		return
	}
	completeHandshake(Normal)
	ops.deliverLifecycle(Opened, Normal)
	ops.startReceive()
	ops.startControl()
	<-connection.Context().Done()
}

func (s *serverEndpoint) resumeConnection(connection transport.Conn, channel *controlChannel, hello helloMessage, principal Principal, completeHandshake func(Code)) {
	s.observe("resumes", "attempt", Normal, 1)
	reject := func(code Code, message string) {
		s.observe("resumes", "failure", code, 1)
		completeHandshake(code)
		_ = connection.Close(uint64(code), message)
	}
	if s.config.Role != GameRole || hello.Group != "" || hello.Admission != "" {
		reject(Forbidden, "invalid resume")
		return
	}
	resume := *hello.Resume
	id, err := parseSessionID(resume.SessionID)
	if err != nil || resume.OwnerID != s.owner.ID || resume.Incarnation != hex.EncodeToString(s.owner.Incarnation[:]) {
		reject(SessionNotFound, "unknown session")
		return
	}
	secret, err := base64.RawURLEncoding.DecodeString(resume.Secret)
	if err != nil || len(secret) != 32 {
		reject(Unauthenticated, "invalid resume secret")
		return
	}
	session, terminal := s.lookupSession(id)
	if session == nil {
		reject(terminal, "unknown session")
		return
	}
	groupKey := session.GroupKey()
	if groupKey != "" {
		if s.config.AuthorizeGroup == nil || s.config.AuthorizeGroup(connection.Context(), principal, groupKey) != nil {
			reject(Forbidden, "resume authorization rejected")
			return
		}
	}
	ops := &connectionOperations{connection: connection, control: channel, mode: s.config.Dispatch.Mode, incoming: s.incoming, scheduler: s.scheduler, applicationBudget: s.applicationBudget, observer: s.observer}
	hash := sha256.Sum256(secret)
	var group *groupState
	if groupKey != "" {
		s.groupMu.Lock()
		group = s.groups[groupKey]
		if group == nil || group.closed {
			s.groupMu.Unlock()
			reject(GroupClosed, "group closed")
			return
		}
	}
	session.mu.Lock()
	current := session.principal
	if session.state == Closed || !current.ExpiresAt.After(time.Now()) || current.Issuer != principal.Issuer || current.Subject != principal.Subject || session.peerLimits != hello.Limits || subtle.ConstantTimeCompare(session.resumeHash[:], hash[:]) != 1 {
		session.mu.Unlock()
		if group != nil {
			s.groupMu.Unlock()
		}
		reject(Unauthenticated, "resume rejected")
		return
	}
	if session.epoch == ^uint64(0) {
		session.mu.Unlock()
		if group != nil {
			s.groupMu.Unlock()
		}
		reject(ResourceExhausted, "epoch exhausted")
		return
	}
	previous, _ := session.operations.(*connectionOperations)
	session.epoch++
	epoch := session.epoch
	ops.epoch = epoch
	session.state = Active
	session.principal = copyPrincipal(principal)
	session.operations = ops
	if session.graceTimer != nil {
		session.graceTimer.Stop()
	}
	session.mu.Unlock()
	if group != nil {
		s.groupMu.Unlock()
	}
	ops.session, ops.router = session, s.config.Dispatch.Router
	ops.onIncomingRefresh = func(ctx context.Context, control wire.Control) refreshResultMessage {
		return s.handleRefresh(ctx, session, control)
	}
	if previous != nil {
		s.observe("resumes", "superseded", SessionSuperseded, 1)
		previous.stopEpoch()
		_ = previous.connection.Close(uint64(SessionSuperseded), "session superseded")
	}
	s.armExpiry(session, principal.ExpiresAt)
	defer s.finishConnection(session, ops)
	welcome := welcomeMessage{Op: "welcome", SessionID: hex.EncodeToString(id[:]), Epoch: strconv.FormatUint(epoch, 10), Owner: ownerMessage{ID: s.owner.ID, Incarnation: hex.EncodeToString(s.owner.Incarnation[:]), Address: s.owner.Endpoint.Address, ServerName: s.owner.Endpoint.ServerName}, Principal: principalMessage{Issuer: principal.Issuer, Subject: principal.Subject, ExpiresMS: principal.ExpiresAt.UnixMilli()}, Limits: limitMessageFrom(s.limits), Capabilities: []string{"datagrams"}, Resumed: true, Resumable: true, ResumeGraceMS: int(s.limits.ResumeGrace / time.Millisecond)}
	if err := channel.write(welcome); err != nil {
		reject(Internal, "resume welcome failed")
		return
	}
	s.observe("resumes", "success", Normal, 1)
	completeHandshake(Normal)
	ops.deliverLifecycle(Resumed, Normal)
	ops.startReceive()
	ops.startControl()
	<-connection.Context().Done()
}
func (s *serverEndpoint) authorizeAdmission(ctx context.Context, hello helloMessage, principal Principal) error {
	if s.config.Role == BootstrapRole {
		return nil
	}
	requires := s.config.RequireAdmission || hello.Group != ""
	if !requires {
		return nil
	}
	if s.config.Admission == nil || s.config.AuthorizeGroup == nil || hello.Admission == "" {
		return ErrForbidden
	}
	admission, err := s.config.Admission.Verify(ctx, hello.Admission)
	if err != nil {
		return err
	}
	if admission.App != s.config.App || admission.PrincipalIssuer != principal.Issuer || admission.Subject != principal.Subject || admission.Owner != s.owner || admission.GroupKey != hello.Group || !admission.ExpiresAt.After(time.Now()) {
		return ErrForbidden
	}
	return s.config.AuthorizeGroup(ctx, principal, hello.Group)
}
func (s *serverEndpoint) handleRefresh(ctx context.Context, session *sessionRecord, control wire.Control) refreshResultMessage {
	failure := refreshResultMessage{Op: "refresh_result", Code: uint32(Unauthenticated), Message: "authentication failed"}
	var message refreshMessage
	raw, err := json.Marshal(control.Fields)
	if err != nil || json.Unmarshal(raw, &message) != nil {
		return failure
	}
	data, err := base64.RawURLEncoding.DecodeString(message.Credential.Data)
	if err != nil {
		return failure
	}
	refreshCtx, cancel := context.WithTimeout(ctx, s.limits.AuthTimeout)
	defer cancel()
	refreshStarted := time.Now()
	principal, err := s.config.Auth.Authenticate(refreshCtx, Credential{Scheme: message.Credential.Scheme, Data: data})
	previous := session.Principal()
	refreshOK := err == nil && principal.Issuer == previous.Issuer && principal.Subject == previous.Subject && principal.ExpiresAt.After(time.Now())
	refreshCode := Normal
	if !refreshOK {
		refreshCode = Unauthenticated
	}
	s.observe("auth", histogramKind("refresh", time.Since(refreshStarted)), refreshCode, 1)
	if !refreshOK {
		return failure
	}
	session.setPrincipal(principal)
	s.armExpiry(session, principal.ExpiresAt)
	return refreshResultMessage{Op: "refresh_result", ExpiresMS: principal.ExpiresAt.UnixMilli()}
}
func (s *serverEndpoint) Next(ctx context.Context) (*Incoming, error) {
	if s.config.Dispatch.Mode != Polling {
		return nil, ErrWrongMode
	}
	incoming, err := s.incoming.Next(ctx)
	if err == nil {
		observeIncomingQueueAge(s.observer, incoming)
	}
	return incoming, err
}
func (s *serverEndpoint) Sessions() []Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sessions := make([]Session, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	return sessions
}
func (s *serverEndpoint) Revoke(ctx context.Context, issuer, subject string) error {
	s.mu.RLock()
	var sessions []*sessionRecord
	for _, session := range s.sessions {
		principal := session.Principal()
		if principal.Issuer == issuer && principal.Subject == subject {
			sessions = append(sessions, session)
		}
	}
	s.mu.RUnlock()
	for _, session := range sessions {
		_ = session.Close(ctx, Forbidden)
		s.removeSession(session)
	}
	return nil
}
func (s *serverEndpoint) CloseGroup(ctx context.Context, groupKey string) error {
	if groupKey == "" || len(groupKey) > s.limits.MaxGroupKeyBytes || s.config.CommitGroupClose == nil {
		return ErrInvalidArgument
	}
	s.groupMu.Lock()
	group := s.groups[groupKey]
	if group == nil {
		if len(s.groups) >= s.limits.MaxActiveGroups {
			s.groupMu.Unlock()
			return ErrResourceExhausted
		}
		group = &groupState{sessions: make(map[*sessionRecord]struct{})}
		s.groups[groupKey] = group
	}
	group.closed = true
	sessions := make([]*sessionRecord, 0, len(group.sessions))
	for session := range group.sessions {
		sessions = append(sessions, session)
	}
	s.groupMu.Unlock()
	for _, session := range sessions {
		s.closeSession(session, GroupClosed)
	}
	if err := s.config.CommitGroupClose(ctx, s.config.App, groupKey, s.owner); err != nil {
		return err
	}
	s.retainClosedGroup(groupKey, group)
	return nil
}
func (s *serverEndpoint) Drain(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		return ErrInvalidArgument
	}
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
	for {
		if len(s.Sessions()) == 0 {
			return nil
		}
		select {
		case <-s.changed:
		case <-ctx.Done():
			for _, session := range s.Sessions() {
				s.closeSession(session.(*sessionRecord), ServerDraining)
			}
			return ctx.Err()
		}
	}
}

func Dial(ctx context.Context, endpoint Endpoint, config ClientConfig) (Client, error) {
	limits, err := normalizeLimits(config.Limits)
	if err != nil {
		return nil, err
	}
	if config.TLS == nil || config.TLS.InsecureSkipVerify || config.Credentials == nil || config.App.ID == "" || config.App.Version == "" || endpoint.Address == "" || endpoint.ServerName == "" || len(config.GroupKey) > limits.MaxGroupKeyBytes || config.Dispatch.Mode == Polling && config.Dispatch.Router != nil || config.Dispatch.Mode == Handlers && config.Dispatch.Router == nil {
		return nil, ErrInvalidArgument
	}
	// Bound the complete client HELLO/WELCOME exchange even when the caller
	// supplied a context without a deadline. controlChannel.read applies this
	// deadline directly to the QUIC stream, avoiding a per-frame helper task.
	handshakeCtx, cancelHandshake := context.WithTimeout(ctx, limits.AuthTimeout)
	defer cancelHandshake()
	credential, err := config.Credentials(handshakeCtx)
	if err != nil {
		return nil, err
	}
	remote, err := net.ResolveUDPAddr("udp", endpoint.Address)
	if err != nil {
		return nil, err
	}
	packet, err := net.ListenPacket("udp", "[::]:0")
	if err != nil {
		return nil, err
	}
	tlsConfig := config.TLS.Clone()
	tlsConfig.ServerName = endpoint.ServerName
	connection, err := quictransport.Dial(handshakeCtx, packet, remote, transportConfig(tlsConfig, limits))
	if err != nil {
		_ = packet.Close()
		return nil, err
	}
	stream, err := connection.OpenBidi(handshakeCtx)
	if err != nil {
		_ = connection.Close(uint64(Internal), "control stream")
		_ = packet.Close()
		return nil, err
	}
	hello := helloMessage{Op: "hello", Role: roleName(config.Role), App: config.App, Required: []string{"datagrams"}, Limits: limitMessageFrom(limits), Credential: credentialMessage{Scheme: credential.Scheme, Data: base64.RawURLEncoding.EncodeToString(credential.Data)}, Group: config.GroupKey, Admission: config.AdmissionTicket}
	if config.Role == BootstrapRole {
		hello.Required = []string{}
	}
	channel := newControlChannel(stream)
	if err := channel.write(hello); err != nil {
		_ = connection.Close(uint64(Internal), "hello failed")
		_ = packet.Close()
		return nil, err
	}
	control, err := channel.read(handshakeCtx, limits.ControlBytes)
	if err != nil {
		_ = connection.Close(uint64(ProtocolViolation), "welcome failed")
		_ = packet.Close()
		return nil, err
	}
	if control.Op != "welcome" {
		_ = connection.Close(uint64(Unauthenticated), "rejected")
		_ = packet.Close()
		return nil, ErrUnauthenticated
	}
	welcome, err := parseWelcome(control)
	if err != nil {
		_ = connection.Close(uint64(ProtocolViolation), "invalid welcome")
		_ = packet.Close()
		return nil, err
	}
	if err := validateLimitMessage(welcome.Limits); err != nil {
		_ = connection.Close(uint64(ProtocolViolation), "invalid receive limits")
		_ = packet.Close()
		return nil, err
	}
	if config.Role == GameRole && (!connection.Stats().DatagramsEnabled || !hasCapability(welcome.Capabilities, "datagrams")) {
		_ = connection.Close(uint64(UnsupportedCapability), "datagram capability unavailable")
		_ = packet.Close()
		return nil, ErrUnsupportedCapability
	}
	if err := validateClientWelcome(welcome, config.Role, false); err != nil {
		_ = connection.Close(uint64(ProtocolViolation), "invalid welcome")
		_ = packet.Close()
		return nil, err
	}
	if config.ExpectedOwner != nil && (welcome.Owner.ID != config.ExpectedOwner.ID || welcome.Owner.toOwner().Incarnation != config.ExpectedOwner.Incarnation) {
		_ = connection.Close(uint64(Forbidden), "wrong owner")
		_ = packet.Close()
		return nil, ErrForbidden
	}
	id, err := parseSessionID(welcome.SessionID)
	if err != nil {
		_ = connection.Close(uint64(ProtocolViolation), "invalid session")
		_ = packet.Close()
		return nil, err
	}
	principal := Principal{Issuer: welcome.Principal.Issuer, Subject: welcome.Principal.Subject, ExpiresAt: time.UnixMilli(welcome.Principal.ExpiresMS)}
	applicationBudget := runtime.NewBudget(limits.GlobalApplicationBytes)
	observer := newObserverQueue(config.Observer, 4096, time.Second)
	var incoming *incomingQueue
	if config.Dispatch.Mode == Polling {
		incoming = newIncomingQueue(int64(limits.QueueBytes), limits.QueueMessages, applicationBudget)
	}
	var scheduler *handlerScheduler
	if config.Dispatch.Mode == Handlers {
		scheduler = newHandlerScheduler(1, 1)
	}
	operations := &connectionOperations{connection: connection, control: channel, mode: config.Dispatch.Mode, incoming: incoming, scheduler: scheduler, applicationBudget: applicationBudget, observer: observer}
	session := newSessionRecord(id, welcome.Owner.toOwner(), config.GroupKey, principal, limits, true, operations)
	session.peerLimits = welcome.Limits
	operations.epoch = 1
	operations.session, operations.router = session, config.Dispatch.Router.snapshot()
	operations.onRefresh = func(principal Principal) {
		current := session.Principal()
		current.ExpiresAt = principal.ExpiresAt
		session.setPrincipal(current)
	}
	client := &clientEndpoint{session: session, connection: connection, packet: packet, mode: config.Dispatch.Mode, incoming: incoming, endpoint: endpoint, config: config, limits: limits, resumeSecret: welcome.ResumeSecret, resumeGrace: time.Duration(welcome.ResumeGraceMS) * time.Millisecond, scheduler: scheduler, applicationBudget: applicationBudget, observer: observer}
	operations.onLost = client.transportLost
	operations.onTerminal = func() {
		if incoming != nil {
			incoming.Close()
		}
		scheduler.close()
		observer.Close()
	}
	operations.deliverLifecycle(Opened, Normal)
	operations.startReceive()
	operations.startControl()
	go client.watchConnection(operations)
	return client, nil
}
func (c *clientEndpoint) Session() Session { return c.session }
func (c *clientEndpoint) Next(ctx context.Context) (*Incoming, error) {
	if c.mode != Polling {
		return nil, ErrWrongMode
	}
	incoming, err := c.incoming.Next(ctx)
	if err == nil {
		observeIncomingQueueAge(c.observer, incoming)
	}
	return incoming, err
}
func (c *clientEndpoint) watchConnection(operations *connectionOperations) {
	<-operations.connection.Context().Done()
	if operations.onLost != nil {
		operations.onLost(operations)
	}
}
func (c *clientEndpoint) transportLost(operations *connectionOperations) {
	if !c.session.suspendIfCurrent(operations) {
		if c.session.State() == Closed {
			c.mu.Lock()
			c.closed = true
			packet := c.packet
			c.mu.Unlock()
			if packet != nil {
				_ = packet.Close()
			}
		}
		return
	}
	operations.deliverLifecycle(ConnectionLost, Normal)
	if value, ok := operations.connection.CloseCode(); ok && terminalCloseCode(Code(value)) {
		c.mu.Lock()
		c.closed = true
		if c.packet != nil {
			_ = c.packet.Close()
		}
		c.mu.Unlock()
		_ = c.session.Close(context.Background(), Code(value))
		return
	}
	c.mu.Lock()
	if c.packet != nil {
		_ = c.packet.Close()
	}
	if c.closed || c.config.DisableReconnect || c.resumeSecret == "" || c.resumeGrace <= 0 {
		c.closed = true
		c.mu.Unlock()
		_ = c.session.Close(context.Background(), SessionClosed)
		return
	}
	if c.reconnecting {
		c.mu.Unlock()
		return
	}
	c.reconnecting = true
	c.mu.Unlock()
	go c.reconnect()
}
func terminalCloseCode(code Code) bool {
	switch code {
	case ProtocolViolation, UnsupportedVersion, UnsupportedCapability, Unauthenticated, Forbidden, AuthExpired, SessionNotFound, SessionExpired, ServerUnavailable, ServerDraining, ResourceExhausted, SlowConsumer, GroupClosed, SessionClosed:
		return true
	default:
		return false
	}
}
func (c *clientEndpoint) reconnect() {
	deadline := time.Now().Add(c.resumeGrace)
	if expiresAt := c.session.Principal().ExpiresAt; expiresAt.Before(deadline) {
		deadline = expiresAt
	}
	for attempt := 0; ; attempt++ {
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if closed || time.Now().After(deadline) {
			break
		}
		attemptDeadline := time.Now().Add(c.limits.AuthTimeout)
		if attemptDeadline.After(deadline) {
			attemptDeadline = deadline
		}
		ctx, cancel := context.WithDeadline(context.Background(), attemptDeadline)
		attachment, err := c.openResume(ctx)
		cancel()
		if err == nil {
			err = c.attachResume(attachment)
		}
		if err == nil {
			c.mu.Lock()
			c.reconnecting = false
			c.mu.Unlock()
			return
		}
		if code, terminal := terminalResumeCode(err); terminal {
			c.finishReconnect(code)
			return
		}
		maximumDelay := c.limits.ReconnectMin << min(attempt, 3)
		if maximumDelay > c.limits.ReconnectMax {
			maximumDelay = c.limits.ReconnectMax
		}
		if remaining := time.Until(deadline); remaining < maximumDelay {
			maximumDelay = remaining
		}
		if maximumDelay <= 0 {
			break
		}
		timer := time.NewTimer(fullJitter(maximumDelay))
		select {
		case <-timer.C:
		case <-c.session.Context().Done():
			timer.Stop()
			c.mu.Lock()
			c.reconnecting = false
			c.mu.Unlock()
			return
		}
	}
	c.finishReconnect(SessionExpired)
}

// terminalResumeCode identifies a server-issued rejection that cannot be
// repaired by another network retry. Transport and deadline errors remain
// retryable until the advertised grace deadline.
func terminalResumeCode(err error) (Code, bool) {
	var protocol *Error
	if !errors.As(err, &protocol) || protocol == nil || !terminalCloseCode(protocol.Code) {
		return Normal, false
	}
	return protocol.Code, true
}

func (c *clientEndpoint) finishReconnect(code Code) {
	c.mu.Lock()
	c.reconnecting = false
	c.closed = true
	c.mu.Unlock()
	_ = c.session.Close(context.Background(), code)
}

func fullJitter(maximum time.Duration) time.Duration {
	if maximum <= 0 {
		return 0
	}
	value, err := rand.Int(rand.Reader, big.NewInt(int64(maximum)+1))
	if err != nil {
		return maximum / 2
	}
	return time.Duration(value.Int64())
}

type resumeAttachment struct {
	packet     net.PacketConn
	connection transport.Conn
	channel    *controlChannel
	welcome    welcomeMessage
}

func (c *clientEndpoint) openResume(ctx context.Context) (resumeAttachment, error) {
	credential, err := c.config.Credentials(ctx)
	if err != nil {
		return resumeAttachment{}, err
	}
	remote, err := net.ResolveUDPAddr("udp", c.endpoint.Address)
	if err != nil {
		return resumeAttachment{}, err
	}
	packet, err := net.ListenPacket("udp", "[::]:0")
	if err != nil {
		return resumeAttachment{}, err
	}
	fail := func(err error) (resumeAttachment, error) {
		_ = packet.Close()
		return resumeAttachment{}, err
	}
	tlsConfig := c.config.TLS.Clone()
	tlsConfig.ServerName = c.endpoint.ServerName
	connection, err := quictransport.Dial(ctx, packet, remote, transportConfig(tlsConfig, c.limits))
	if err != nil {
		return fail(err)
	}
	stream, err := connection.OpenBidi(ctx)
	if err != nil {
		_ = connection.Close(uint64(Internal), "resume control stream")
		return fail(err)
	}
	sessionID := c.session.ID()
	owner := c.session.Owner()
	resume := resumeMessage{SessionID: hex.EncodeToString(sessionID[:]), OwnerID: owner.ID, Incarnation: hex.EncodeToString(owner.Incarnation[:]), Secret: c.resumeSecret}
	hello := helloMessage{Op: "hello", Role: roleName(c.config.Role), App: c.config.App, Required: []string{"datagrams"}, Limits: limitMessageFrom(c.limits), Credential: credentialMessage{Scheme: credential.Scheme, Data: base64.RawURLEncoding.EncodeToString(credential.Data)}, Resume: &resume}
	channel := newControlChannel(stream)
	if err := channel.write(hello); err != nil {
		_ = connection.Close(uint64(Internal), "resume hello failed")
		return fail(err)
	}
	control, err := channel.read(ctx, c.limits.ControlBytes)
	if err != nil {
		// A stream read can wake just before quic-go publishes the connection's
		// application-close cause. Wait for that terminal state (or this attempt's
		// deadline) before deciding whether a server rejection is retryable.
		select {
		case <-connection.Context().Done():
		case <-ctx.Done():
		}
		if value, ok := connection.CloseCode(); ok && terminalCloseCode(Code(value)) {
			return fail(codeError(Code(value)))
		}
		_ = connection.Close(uint64(ProtocolViolation), "resume welcome failed")
		return fail(err)
	}
	if control.Op != "welcome" {
		_ = connection.Close(uint64(Unauthenticated), "resume rejected")
		return fail(ErrUnauthenticated)
	}
	welcome, err := parseWelcome(control)
	if err != nil || !welcome.Resumed {
		_ = connection.Close(uint64(ProtocolViolation), "invalid resume welcome")
		if err == nil {
			err = ErrProtocolViolation
		}
		return fail(err)
	}
	if err := validateLimitMessage(welcome.Limits); err != nil {
		_ = connection.Close(uint64(ProtocolViolation), "invalid resumed receive limits")
		return fail(err)
	}
	if c.config.Role == GameRole && (!connection.Stats().DatagramsEnabled || !hasCapability(welcome.Capabilities, "datagrams")) {
		_ = connection.Close(uint64(UnsupportedCapability), "datagram capability unavailable")
		return fail(ErrUnsupportedCapability)
	}
	if err := validateClientWelcome(welcome, c.config.Role, true); err != nil {
		_ = connection.Close(uint64(ProtocolViolation), "invalid resumed welcome")
		return fail(err)
	}
	return resumeAttachment{packet: packet, connection: connection, channel: channel, welcome: welcome}, nil
}
func (c *clientEndpoint) attachResume(attachment resumeAttachment) error {
	id, err := parseSessionID(attachment.welcome.SessionID)
	if err != nil || id != c.session.ID() || attachment.welcome.Owner.toOwner() != c.session.Owner() || attachment.welcome.Limits != c.session.peerLimitsSnapshot() {
		_ = attachment.connection.Close(uint64(ProtocolViolation), "invalid resume owner")
		_ = attachment.packet.Close()
		return ErrProtocolViolation
	}
	principal := Principal{Issuer: attachment.welcome.Principal.Issuer, Subject: attachment.welcome.Principal.Subject, ExpiresAt: time.UnixMilli(attachment.welcome.Principal.ExpiresMS)}
	current := c.session.Principal()
	if principal.Issuer != current.Issuer || principal.Subject != current.Subject || !principal.ExpiresAt.After(time.Now()) {
		_ = attachment.connection.Close(uint64(Unauthenticated), "invalid resumed principal")
		_ = attachment.packet.Close()
		return ErrUnauthenticated
	}
	epoch, err := strconv.ParseUint(attachment.welcome.Epoch, 10, 64)
	if err != nil || epoch == 0 {
		_ = attachment.connection.Close(uint64(ProtocolViolation), "invalid resumed epoch")
		_ = attachment.packet.Close()
		return ErrProtocolViolation
	}
	operations := &connectionOperations{connection: attachment.connection, control: attachment.channel, mode: c.mode, incoming: c.incoming, scheduler: c.scheduler, applicationBudget: c.applicationBudget, observer: c.observer}
	operations.session, operations.router = c.session, c.config.Dispatch.Router.snapshot()
	operations.onRefresh = func(principal Principal) {
		updated := c.session.Principal()
		updated.ExpiresAt = principal.ExpiresAt
		c.session.setPrincipal(updated)
	}
	previous, err := c.session.replaceConnectionAt(operations, principal, epoch)
	if err != nil {
		_ = attachment.connection.Close(uint64(Internal), "resume attach failed")
		_ = attachment.packet.Close()
		return err
	}
	if previous != nil {
		previous.stopEpoch()
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = attachment.connection.Close(uint64(SessionClosed), "client closed")
		_ = attachment.packet.Close()
		return ErrSessionClosed
	}
	c.connection, c.packet = attachment.connection, attachment.packet
	c.resumeGrace = time.Duration(attachment.welcome.ResumeGraceMS) * time.Millisecond
	c.mu.Unlock()
	operations.onLost = c.transportLost
	operations.onTerminal = func() {
		if c.incoming != nil {
			c.incoming.Close()
		}
		c.scheduler.close()
		c.observer.Close()
	}
	operations.deliverLifecycle(Resumed, Normal)
	operations.startReceive()
	operations.startControl()
	go c.watchConnection(operations)
	return nil
}
func (c *clientEndpoint) Close(ctx context.Context) error {
	var err error
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		packet := c.packet
		c.mu.Unlock()
		err = c.session.Close(ctx, Normal)
		if packet != nil {
			_ = packet.Close()
		}
	})
	return err
}

type connectionOperations struct {
	connection                                     transport.Conn
	control                                        *controlChannel
	onRefresh                                      func(Principal)
	onIncomingRefresh                              func(context.Context, wire.Control) refreshResultMessage
	refreshMu                                      sync.Mutex
	refreshStateMu                                 sync.Mutex
	refreshActive                                  bool
	refreshReply                                   chan refreshResultMessage
	controlStart                                   sync.Once
	closeMu                                        sync.Mutex
	closeAck                                       chan struct{}
	session                                        *sessionRecord
	router                                         *Router
	mode                                           DispatchMode
	incoming                                       *incomingQueue
	onLost                                         func(*connectionOperations)
	onTerminal                                     func()
	epoch                                          uint64
	epochMu                                        sync.Mutex
	epochCtx                                       context.Context
	stop                                           context.CancelFunc
	scheduler                                      *handlerScheduler
	applicationBudget                              *runtime.Budget
	observer                                       *observerQueue
	handlerMu                                      sync.Mutex
	handlerQueue                                   *runtime.Queue[*Incoming]
	handlerScheduled                               bool
	eventMu                                        sync.Mutex
	sequences                                      map[ChannelID]uint64
	received                                       map[ChannelID]uint64
	channelMu                                      sync.Mutex
	outbound                                       map[ChannelID]Delivery
	inbound                                        map[ChannelID]Delivery
	reliable                                       map[ChannelID]transport.SendStream
	slotMu                                         sync.Mutex
	inRequests, outRequests, inStreams, outStreams int
	rateMu                                         sync.Mutex
	rateTokens                                     float64
	rateAt                                         time.Time
	localDatagramDrops                             atomic.Uint64
	staleUpdatesDropped                            atomic.Uint64
}

// incomingQueue owns polling-mode envelopes until the application releases
// them. The queue bounds pending work while the accompanying budget continues
// to charge items after Next has returned, preventing retained payloads from
// escaping the endpoint-wide application-memory limit.
type incomingQueue struct {
	queue             *runtime.Queue[*Incoming]
	budget            *runtime.Budget
	applicationBudget *runtime.Budget
	observeOnce       sync.Once
	mu                sync.Mutex
	closed            bool
	terminal          []*Incoming
	nextMu            sync.Mutex
	nextBusy          bool
}

// reliableIncomingReservation holds queue and memory charges while a
// reliable reader is still waiting to read a declared payload. Commit hands
// those charges to the Incoming envelope; Cancel releases them without ever
// allocating the body.
type reliableIncomingReservation struct {
	queue         *runtime.Reservation[*Incoming]
	bytes         int
	releaseCharge func()
	mu            sync.Mutex
	done          bool
}

func (r *reliableIncomingReservation) Commit(incoming *Incoming) bool {
	if r == nil || incoming == nil || r.queue == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return false
	}
	previous := incoming.release
	incoming.enqueuedAt = time.Now()
	incoming.release = func() {
		if previous != nil {
			previous()
		}
		if r.releaseCharge != nil {
			r.releaseCharge()
		}
	}
	if !r.queue.Commit(runtime.Item[*Incoming]{Value: incoming, Bytes: r.bytes}) {
		incoming.release = previous
		r.done = true
		if r.releaseCharge != nil {
			r.releaseCharge()
		}
		return false
	}
	r.done = true
	return true
}

func (r *reliableIncomingReservation) Cancel() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return
	}
	r.done = true
	if r.queue != nil {
		r.queue.Cancel()
	}
	if r.releaseCharge != nil {
		r.releaseCharge()
	}
}

func newIncomingQueue(maxBytes int64, maxItems int, applicationBudget *runtime.Budget) *incomingQueue {
	if maxItems < 1 {
		maxItems = 1
	}
	maxInt := int64(^uint(0) >> 1)
	if maxBytes > maxInt {
		maxBytes = maxInt
	}
	return &incomingQueue{queue: runtime.NewQueue[*Incoming](int(maxBytes), maxItems), budget: runtime.NewBudget(maxBytes), applicationBudget: applicationBudget}
}
func (q *incomingQueue) Push(incoming *Incoming, bytes int) bool {
	if q == nil || incoming == nil || bytes < 0 || !q.acquire(int64(bytes)) {
		return false
	}
	q.attachCharge(incoming, int64(bytes))
	incoming.enqueuedAt = time.Now()
	q.mu.Lock()
	closed := q.closed
	if !closed && q.queue.TryPush(runtime.Item[*Incoming]{Value: incoming, Bytes: bytes}) {
		q.mu.Unlock()
		return true
	}
	q.mu.Unlock()
	incoming.Release()
	return false
}

// PushWait waits for both retained-payload budget and queue capacity. It is
// reserved for reliable input; ordinary unreliable traffic must use Push and
// be discarded under pressure rather than create a waiting reader.
func (q *incomingQueue) PushWait(ctx context.Context, incoming *Incoming, bytes int) error {
	if q == nil || incoming == nil || bytes < 0 {
		return ErrBackpressure
	}
	if err := q.acquireWait(ctx, int64(bytes)); err != nil {
		return err
	}
	q.attachCharge(incoming, int64(bytes))
	incoming.enqueuedAt = time.Now()
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed {
		incoming.Release()
		return context.Canceled
	}
	if err := q.queue.Push(ctx, runtime.Item[*Incoming]{Value: incoming, Bytes: bytes}); err != nil {
		incoming.Release()
		return err
	}
	return nil
}

// reserveReliable obtains polling queue and application-budget capacity
// before a reliable stream reader allocates its body.
func (q *incomingQueue) reserveReliable(ctx context.Context, bytes int) (*reliableIncomingReservation, error) {
	if q == nil || bytes < 0 {
		return nil, ErrBackpressure
	}
	if err := q.acquireWait(ctx, int64(bytes)); err != nil {
		return nil, err
	}
	reservation, err := q.queue.Reserve(ctx, bytes)
	if err != nil {
		q.budget.Release(int64(bytes))
		if q.applicationBudget != nil {
			q.applicationBudget.Release(int64(bytes))
		}
		return nil, err
	}
	var once sync.Once
	return &reliableIncomingReservation{queue: reservation, bytes: bytes, releaseCharge: func() {
		once.Do(func() {
			q.budget.Release(int64(bytes))
			if q.applicationBudget != nil {
				q.applicationBudget.Release(int64(bytes))
			}
		})
	}}, nil
}

func (q *incomingQueue) acquire(bytes int64) bool {
	if !q.budget.Acquire(bytes) {
		return false
	}
	if q.applicationBudget == nil || q.applicationBudget.Acquire(bytes) {
		return true
	}
	q.budget.Release(bytes)
	return false
}
func (q *incomingQueue) acquireWait(ctx context.Context, bytes int64) error {
	for {
		if q.budget.Acquire(bytes) {
			if q.applicationBudget == nil || q.applicationBudget.Acquire(bytes) {
				return nil
			}
			q.budget.Release(bytes)
			if err := q.applicationBudget.Wait(ctx); err != nil {
				return err
			}
			continue
		}
		if err := q.budget.Wait(ctx); err != nil {
			return err
		}
	}
}
func (q *incomingQueue) attachCharge(incoming *Incoming, bytes int64) {
	previous := incoming.release
	global := q.applicationBudget
	incoming.release = func() {
		if previous != nil {
			previous()
		}
		q.budget.Release(bytes)
		if global != nil {
			global.Release(bytes)
		}
	}
}

// startObserving samples one polling queue once per second. Server polling
// queues are started by Serve, while a client starts its own queue from its
// sole connection operation. sync.Once prevents per-session duplication when
// a server owns a shared polling queue.
func (q *incomingQueue) startObserving(ctx context.Context, observer *observerQueue) {
	if q == nil || observer == nil || ctx == nil {
		return
	}
	q.observeOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					q.observeUsage(observer)
				}
			}
		}()
	})
}

func (q *incomingQueue) observeUsage(observer *observerQueue) {
	if q == nil || observer == nil || q.queue == nil {
		return
	}
	items, bytes := q.queue.Len()
	observer.Observe(Observation{Name: "queue_bytes", Kind: "in_polling", Code: Normal, Value: float64(bytes)})
	observer.Observe(Observation{Name: "queue_items", Kind: "in_polling", Code: Normal, Value: float64(items)})
}

func (q *incomingQueue) Next(ctx context.Context) (*Incoming, error) {
	if q == nil {
		return nil, ErrSessionClosed
	}
	q.nextMu.Lock()
	if q.nextBusy {
		q.nextMu.Unlock()
		return nil, ErrInvalidArgument
	}
	q.nextBusy = true
	q.nextMu.Unlock()
	defer func() {
		q.nextMu.Lock()
		q.nextBusy = false
		q.nextMu.Unlock()
	}()
	q.mu.Lock()
	if len(q.terminal) > 0 {
		incoming := q.terminal[0]
		q.terminal = q.terminal[1:]
		q.mu.Unlock()
		return incoming, nil
	}
	closed := q.closed
	q.mu.Unlock()
	if closed {
		return nil, ErrSessionClosed
	}
	item, err := q.queue.WaitPop(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrSessionClosed
	}
	return item.Value, nil
}
func (q *incomingQueue) Close() {
	if q == nil {
		return
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	q.queue.Close()
	for {
		item, ok := q.queue.Pop()
		if !ok {
			break
		}
		if item.Value.Kind == LifecycleMessage && item.Value.Lifecycle != nil && item.Value.Lifecycle.Kind == SessionEnded {
			q.terminal = append(q.terminal, item.Value)
			continue
		}
		item.Value.Release()
	}
	q.mu.Unlock()
}

// discardEpoch removes ordinary messages belonging to an obsolete connection
// epoch while retaining lifecycle notices and work for other sessions sharing
// a server polling queue. Items already returned by Next remain application
// owned and continue to hold their charge until the caller releases them.
func (q *incomingQueue) discardEpoch(session Session, epoch uint64) {
	if q == nil || session == nil {
		return
	}
	for _, item := range q.queue.Remove(func(item runtime.Item[*Incoming]) bool {
		incoming := item.Value
		return incoming != nil && incoming.Kind != LifecycleMessage && incoming.Session == session && incoming.Epoch == epoch
	}) {
		if item.Value != nil {
			item.Value.Release()
		}
	}
}

func incomingCharge(incoming *Incoming) int {
	if incoming == nil {
		return 0
	}
	return len(incoming.Payload) + 32
}
func (o *connectionOperations) startEpoch() {
	o.epochMu.Lock()
	defer o.epochMu.Unlock()
	if o.epochCtx == nil {
		o.epochCtx, o.stop = context.WithCancel(o.session.Context())
	}
}
func (o *connectionOperations) stopEpoch() {
	o.epochMu.Lock()
	stop := o.stop
	o.epochMu.Unlock()
	if stop != nil {
		stop()
	}
	if o.control != nil {
		o.control.stopWriter()
	}
	o.dropQueuedHandlers()
	if o.mode == Polling {
		o.incoming.discardEpoch(o.session, o.epoch)
	}
}
func (o *connectionOperations) context() context.Context {
	o.epochMu.Lock()
	ctx := o.epochCtx
	o.epochMu.Unlock()
	if ctx != nil {
		return ctx
	}
	return o.session.Context()
}
func (o *connectionOperations) current() bool { return o.session.isCurrent(o) }

// protocolViolation closes only a still-active current epoch. An interrupted
// stream can wake before the server's connection-finish path has canceled its
// epoch context, so the transport context must also be checked. Ordinary
// connection loss remains a suspension/reconnect candidate, not malformed
// application input.
func (o *connectionOperations) protocolViolation() {
	if o == nil || o.connection == nil || o.connection.Context().Err() != nil || o.context().Err() != nil || !o.current() {
		return
	}
	o.observe("protocol_errors", "framing", ProtocolViolation, 1)
	_ = o.session.Close(context.Background(), ProtocolViolation)
}

// protocolViolationAfterStreamRead gives a concurrently closing transport a
// bounded chance to publish its cancellation before classifying an interrupted
// stream as malformed. QUIC can wake a stream read with its application-close
// error before the connection watcher runs. Treating that error as a framing
// failure would make an ordinary loss terminal, contrary to the epoch/resume
// contract. EOF remains an unsolicited FIN and is immediately a violation.
func (o *connectionOperations) protocolViolationAfterStreamRead(err error) {
	if err == nil || o == nil || o.connection == nil || errors.Is(err, io.EOF) {
		o.protocolViolation()
		return
	}
	if o.connection.Context().Err() != nil || o.context().Err() != nil {
		return
	}
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-o.connection.Context().Done():
		return
	case <-o.context().Done():
		return
	case <-timer.C:
		o.protocolViolation()
	}
}
func (o *connectionOperations) observe(name, kind string, code Code, value float64) {
	if o != nil && o.observer != nil {
		o.observer.Observe(Observation{Name: name, Kind: kind, Code: code, Value: value})
	}
}

// observeMessage keeps message counts and payload bytes distinct while using
// only fixed direction/delivery labels.
func (o *connectionOperations) observeMessage(kind string, payloadBytes int) {
	o.observe("messages", kind+"_count", Normal, 1)
	o.observe("messages", kind+"_bytes", Normal, float64(payloadBytes))
}

func boundedObservationCode(code Code) Code {
	if code > WrongMode {
		return Internal
	}
	return code
}

func observationErrorCode(err error) Code {
	if err == nil {
		return Normal
	}
	var protocol *Error
	if errors.As(err, &protocol) {
		return boundedObservationCode(protocol.Code)
	}
	switch {
	case errors.Is(err, context.Canceled):
		return Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return DeadlineExceeded
	default:
		return Internal
	}
}

func (o *connectionOperations) dropDatagram(kind string, code Code, stale bool) {
	if o == nil {
		return
	}
	o.localDatagramDrops.Add(1)
	if stale {
		o.staleUpdatesDropped.Add(1)
	}
	o.observe("datagram_drop", kind, code, 1)
}

func (o *connectionOperations) observeDuration(name, kind string, code Code, duration time.Duration) {
	o.observe(name, histogramKind(kind, duration), code, 1)
}

// reserveDirectionalBytes accounts for a short-lived library-owned payload
// allocation against both the session direction and endpoint-wide ledger.
// The session budget persists across a resume, so a replacement connection
// cannot temporarily exceed the logical session's directional allowance.
func (o *connectionOperations) reserveDirectionalBytes(direction *runtime.Budget, bytes int) (func(), error) {
	if o == nil || bytes < 0 {
		return nil, ErrBackpressure
	}
	if direction != nil && !direction.Acquire(int64(bytes)) {
		return nil, ErrBackpressure
	}
	if o.applicationBudget != nil && !o.applicationBudget.Acquire(int64(bytes)) {
		if direction != nil {
			direction.Release(int64(bytes))
		}
		return nil, ErrBackpressure
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			if o.applicationBudget != nil {
				o.applicationBudget.Release(int64(bytes))
			}
			if direction != nil {
				direction.Release(int64(bytes))
			}
		})
	}, nil
}

// reserveReplyDirectionalBytes first uses ordinary free capacity and falls
// back to the per-direction reply reserve. Because ordinary capacity excludes
// that reserve, the two ledgers together never exceed QueueBytes.
func (o *connectionOperations) reserveReplyDirectionalBytes(ordinary, reply *runtime.Budget, bytes int) (func(), error) {
	if o == nil || bytes < 0 {
		return nil, ErrBackpressure
	}
	used := ordinary
	if ordinary == nil || !ordinary.Acquire(int64(bytes)) {
		used = reply
		if reply != nil && !reply.Acquire(int64(bytes)) {
			return nil, ErrBackpressure
		}
	}
	if o.applicationBudget != nil && !o.applicationBudget.Acquire(int64(bytes)) {
		if used != nil {
			used.Release(int64(bytes))
		}
		return nil, ErrBackpressure
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			if o.applicationBudget != nil {
				o.applicationBudget.Release(int64(bytes))
			}
			if used != nil {
				used.Release(int64(bytes))
			}
		})
	}, nil
}

// reserveApplicationBytes covers decoded Call replies before their bytes are
// transferred to application ownership.
func (o *connectionOperations) reserveApplicationBytes(bytes int) (func(), error) {
	var direction *runtime.Budget
	if o != nil && o.session != nil {
		direction = o.session.incomingBudget
	}
	return o.reserveDirectionalBytes(direction, bytes)
}

func (o *connectionOperations) reserveReplyApplicationBytes(bytes int) (func(), error) {
	var ordinary, reply *runtime.Budget
	if o != nil && o.session != nil {
		ordinary, reply = o.session.incomingBudget, o.session.incomingReplyBudget
	}
	return o.reserveReplyDirectionalBytes(ordinary, reply, bytes)
}

// reserveOutgoing covers frames built from caller-owned payloads before the
// transport has accepted them.
func (o *connectionOperations) reserveOutgoing(bytes int) (func(), error) {
	var direction *runtime.Budget
	if o != nil && o.session != nil {
		direction = o.session.outgoingBudget
	}
	return o.reserveDirectionalBytes(direction, bytes)
}

func (o *connectionOperations) reserveOutgoingReply(bytes int) (func(), error) {
	var ordinary, reply *runtime.Budget
	if o != nil && o.session != nil {
		ordinary, reply = o.session.outgoingBudget, o.session.outgoingReplyBudget
	}
	return o.reserveReplyDirectionalBytes(ordinary, reply, bytes)
}
func (o *connectionOperations) dispatch(incoming *Incoming) bool {
	if !o.current() {
		return false
	}
	if o.mode == Polling {
		return o.incoming.Push(incoming, incomingCharge(incoming))
	}
	return o.enqueueHandler(incoming)
}

// chargeHandlerIncoming accounts for a queued or in-flight handler envelope
// in the endpoint-wide application budget. Its release is chained onto the
// incoming envelope so replacements, queue drops, and callback completion all
// return the exact same charge once.
func (o *connectionOperations) chargeHandlerIncoming(incoming *Incoming) bool {
	if o == nil || incoming == nil || o.applicationBudget == nil {
		return true
	}
	charge := int64(incomingCharge(incoming))
	if !o.applicationBudget.Acquire(charge) {
		return false
	}
	previous := incoming.release
	incoming.release = func() {
		if previous != nil {
			previous()
		}
		o.applicationBudget.Release(charge)
	}
	return true
}

// enqueueHandler retains an incoming item until a bounded endpoint worker
// invokes it. Sequenced datagrams replace work that has not begun dispatch,
// which bounds replaceable-state backlog without changing the wire sequence.
func (o *connectionOperations) enqueueHandler(incoming *Incoming) bool {
	if o == nil || incoming == nil || o.scheduler == nil {
		return false
	}
	if !o.chargeHandlerIncoming(incoming) {
		return false
	}
	o.handlerMu.Lock()
	if o.handlerQueue == nil {
		o.handlerQueue = runtime.NewQueue[*Incoming](o.session.limits.QueueBytes, o.session.limits.QueueMessages)
	}
	incoming.enqueuedAt = time.Now()
	item := runtime.Item[*Incoming]{Value: incoming, Bytes: incomingCharge(incoming)}
	accepted := false
	if incoming.Delivery == UnreliableSequenced {
		item.Key = uint64(incoming.Channel) + 1
		var previous runtime.Item[*Incoming]
		var replaced bool
		accepted, previous, replaced = o.handlerQueue.ReplaceWith(item)
		if accepted && replaced && previous.Value != nil {
			o.dropDatagram("coalesced", Normal, false)
			previous.Value.Release()
		}
	} else {
		accepted = o.handlerQueue.TryPush(item)
	}
	if !accepted {
		o.handlerMu.Unlock()
		return false
	}
	if o.handlerScheduled {
		o.handlerMu.Unlock()
		return true
	}
	// An unscheduled queue was empty before this item was accepted. If the
	// endpoint ready queue is full, remove exactly this item and report local
	// backpressure rather than leave work with no scheduling token.
	o.handlerScheduled = true
	if o.scheduler.enqueue(o) {
		o.handlerMu.Unlock()
		return true
	}
	item, ok := o.handlerQueue.Pop()
	o.handlerScheduled = false
	o.handlerMu.Unlock()
	if ok && item.Value != nil {
		item.Value.Release()
	}
	return false
}

// enqueueReliableHandler waits only for queue capacity. Reliable readers use
// it instead of discarding ordered input under transient handler pressure.
func (o *connectionOperations) enqueueReliableHandler(ctx context.Context, incoming *Incoming) error {
	if o == nil || incoming == nil || o.scheduler == nil {
		return ErrBackpressure
	}
	if !o.chargeHandlerIncoming(incoming) {
		return ErrResourceExhausted
	}
	o.handlerMu.Lock()
	if o.handlerQueue == nil {
		o.handlerQueue = runtime.NewQueue[*Incoming](o.session.limits.QueueBytes, o.session.limits.QueueMessages)
	}
	incoming.enqueuedAt = time.Now()
	item := runtime.Item[*Incoming]{Value: incoming, Bytes: incomingCharge(incoming)}
	if !o.handlerQueue.TryPush(item) {
		queue := o.handlerQueue
		o.handlerMu.Unlock()
		return queue.Push(ctx, item)
	}
	if o.handlerScheduled {
		o.handlerMu.Unlock()
		return nil
	}
	o.handlerScheduled = true
	if o.scheduler.enqueue(o) {
		o.handlerMu.Unlock()
		return nil
	}
	_, _ = o.handlerQueue.Pop() // This was the only item while unscheduled.
	o.handlerScheduled = false
	o.handlerMu.Unlock()
	return ErrBackpressure
}

// reserveReliableHandler holds the global byte charge and one handler-queue
// slot before a reliable reader allocates its payload.
func (o *connectionOperations) reserveReliableHandler(ctx context.Context, bytes int) (*reliableIncomingReservation, error) {
	if o == nil || o.scheduler == nil || bytes < 0 {
		return nil, ErrBackpressure
	}
	o.handlerMu.Lock()
	if o.handlerQueue == nil {
		o.handlerQueue = runtime.NewQueue[*Incoming](o.session.limits.QueueBytes, o.session.limits.QueueMessages)
	}
	queue := o.handlerQueue
	o.handlerMu.Unlock()
	if o.applicationBudget != nil {
		for !o.applicationBudget.Acquire(int64(bytes)) {
			if err := o.applicationBudget.Wait(ctx); err != nil {
				return nil, err
			}
		}
	}
	reservation, err := queue.Reserve(ctx, bytes)
	if err != nil {
		if o.applicationBudget != nil {
			o.applicationBudget.Release(int64(bytes))
		}
		return nil, err
	}
	var once sync.Once
	return &reliableIncomingReservation{queue: reservation, bytes: bytes, releaseCharge: func() {
		once.Do(func() {
			if o.applicationBudget != nil {
				o.applicationBudget.Release(int64(bytes))
			}
		})
	}}, nil
}

func (o *connectionOperations) enqueueReservedReliableHandler(incoming *Incoming, reservation *reliableIncomingReservation) error {
	if o == nil || incoming == nil || reservation == nil || o.scheduler == nil {
		return ErrBackpressure
	}
	o.handlerMu.Lock()
	if !reservation.Commit(incoming) {
		o.handlerMu.Unlock()
		return ErrBackpressure
	}
	if o.handlerScheduled {
		o.handlerMu.Unlock()
		return nil
	}
	o.handlerScheduled = true
	if o.scheduler.enqueue(o) {
		o.handlerMu.Unlock()
		return nil
	}
	item, _ := o.handlerQueue.Pop() // This was the only committed item while unscheduled.
	o.handlerScheduled = false
	o.handlerMu.Unlock()
	if item.Value != nil {
		item.Value.Release()
	}
	return ErrBackpressure
}

func (o *connectionOperations) reserveReliableIncoming(ctx context.Context, bytes int) (*reliableIncomingReservation, error) {
	if o == nil {
		return nil, ErrBackpressure
	}
	if o.mode == Polling {
		return o.incoming.reserveReliable(ctx, bytes)
	}
	return o.reserveReliableHandler(ctx, bytes)
}

func (o *connectionOperations) deliverReservedReliableEvent(event wire.Event, reservation *reliableIncomingReservation) error {
	o.observeMessage("in_reliable", len(event.Payload))
	incoming := &Incoming{Kind: Event, Session: o.session, Epoch: o.epoch, Type: MessageType(event.MessageType), Channel: ChannelID(event.Channel), Delivery: ReliableOrdered, Payload: event.Payload, ctx: o.context()}
	if o.mode == Polling {
		if o.incoming == nil || !reservation.Commit(incoming) {
			return ErrBackpressure
		}
		return nil
	}
	return o.enqueueReservedReliableHandler(incoming, reservation)
}

// deliverReliableEvent is retained for in-memory callers and tests that
// already own a decoded payload. Transport readers use the reserved form
// above so they never allocate a declared body before capacity is available.
func (o *connectionOperations) deliverReliableEvent(event wire.Event) error {
	ctx, cancel := context.WithTimeout(o.context(), o.session.limits.SlowConsumerTimeout)
	defer cancel()
	reservation, err := o.reserveReliableIncoming(ctx, incomingCharge(&Incoming{Payload: event.Payload}))
	if err != nil {
		return err
	}
	if err := o.deliverReservedReliableEvent(event, reservation); err != nil {
		reservation.Cancel()
		return err
	}
	return nil
}

func (o *connectionOperations) runHandlerQuantum(scheduler *handlerScheduler) {
	for count := 0; count < 32; count++ {
		o.handlerMu.Lock()
		if o.handlerQueue == nil {
			o.handlerScheduled = false
			o.handlerMu.Unlock()
			scheduler.work.Done()
			return
		}
		item, ok := o.handlerQueue.Pop()
		if !ok {
			o.handlerScheduled = false
			o.handlerMu.Unlock()
			scheduler.work.Done()
			return
		}
		o.handlerMu.Unlock()
		o.invoke(item.Value)
	}

	o.handlerMu.Lock()
	if o.handlerQueue == nil {
		o.handlerScheduled = false
		o.handlerMu.Unlock()
		scheduler.work.Done()
		return
	}
	items, _ := o.handlerQueue.Len()
	if items == 0 {
		o.handlerScheduled = false
		o.handlerMu.Unlock()
		scheduler.work.Done()
		return
	}
	// Preserve the existing scheduling token while yielding after one quantum.
	// It cannot be rejected while the scheduler is accepting: this worker just
	// removed one ready entry, and at most one token exists per session.
	scheduler.mu.Lock()
	accepting := scheduler.accepting
	scheduler.mu.Unlock()
	if accepting {
		o.handlerMu.Unlock()
		scheduler.ready <- o
		return
	}
	o.handlerScheduled = false
	o.handlerMu.Unlock()
	scheduler.work.Done()
}

func (o *connectionOperations) dropQueuedHandlers() {
	if o == nil {
		return
	}
	o.handlerMu.Lock()
	defer o.handlerMu.Unlock()
	if o.handlerQueue == nil {
		return
	}
	for {
		item, ok := o.handlerQueue.Pop()
		if !ok {
			return
		}
		if item.Value != nil {
			item.Value.Release()
		}
	}
}

func (o *connectionOperations) invoke(incoming *Incoming) {
	o.observeQueuedIncoming(incoming)
	defer func() {
		if incoming.after != nil {
			incoming.after()
		}
	}()
	defer incoming.Release()
	if !o.current() && incoming.Kind != LifecycleMessage {
		return
	}
	defer func() {
		if recover() != nil {
			o.observe("handler_panics", "panic", Internal, 1)
			_ = o.session.Close(context.Background(), Internal)
		}
	}()
	handler := o.router.handler(incoming)
	if handler == nil {
		if incoming.Kind == RequestMessage {
			_ = incoming.Fail(incoming.Context(), UnsupportedMessage, "unsupported message")
		}
		return
	}
	handler(incoming.Context(), incoming)
	if incoming.Kind == RequestMessage && !incoming.hasResponded() {
		_ = incoming.Fail(incoming.Context(), Internal, "handler returned without a response")
	}
}
func (o *connectionOperations) deliverLifecycle(kind LifecycleKind, reason Code) {
	o.deliverLifecycleAfter(kind, reason, nil)
}
func lifecycleObservationKind(kind LifecycleKind) string {
	switch kind {
	case Opened:
		return "opened"
	case ConnectionLost:
		return "connection_lost"
	case Resumed:
		return "resumed"
	case SessionEnded:
		return "ended"
	case AuthRefreshed:
		return "auth_refreshed"
	default:
		return "unknown"
	}
}

func sessionStateObservationKind(state State) string {
	switch state {
	case Connecting:
		return "state_connecting"
	case Authenticating:
		return "state_authenticating"
	case Active:
		return "state_active"
	case Suspended:
		return "state_suspended"
	case Closed:
		return "state_closed"
	default:
		return "state_unknown"
	}
}

// observeSessionStates emits current server-side logical-session gauges. It
// runs once per second outside transport and handler paths, and includes zero
// values so a host can distinguish an absent state from a missing sample.
func (s *serverEndpoint) observeSessionStates(ctx context.Context) {
	if s == nil || s.observer == nil {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		s.observeSessionStatesOnce()
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (s *serverEndpoint) observeSessionStatesOnce() {
	if s == nil || s.observer == nil {
		return
	}
	counts := map[State]uint64{
		Connecting:     0,
		Authenticating: 0,
		Active:         0,
		Suspended:      0,
		Closed:         0,
	}
	for _, session := range s.Sessions() {
		counts[session.State()]++
	}
	for _, state := range []State{Connecting, Authenticating, Active, Suspended, Closed} {
		s.observe("sessions", sessionStateObservationKind(state), Normal, float64(counts[state]))
	}
}

func (o *connectionOperations) deliverLifecycleAfter(kind LifecycleKind, reason Code, after func()) {
	o.observe("sessions", lifecycleObservationKind(kind), reason, 1)
	incoming := &Incoming{Kind: LifecycleMessage, Session: o.session, Epoch: o.epoch, Lifecycle: &Lifecycle{Kind: kind, Epoch: o.epoch, Reason: reason}, ctx: o.session.Context(), after: after}
	if o.mode == Polling {
		if o.incoming == nil || !o.incoming.Push(incoming, incomingCharge(incoming)) {
			incoming.Release()
		}
		if after != nil {
			after()
		}
		return
	}
	if !o.enqueueHandler(incoming) {
		incoming.Release()
		if after != nil {
			after()
		}
	}
}

func (o *connectionOperations) send(ctx context.Context, typ MessageType, payload []byte, options SendOptions) error {
	if !o.current() {
		return ErrSessionSuspended
	}
	release, err := o.reserveOutgoing(len(payload) + 32)
	if err != nil {
		return err
	}
	defer release()
	if options.Delivery == ReliableOrdered {
		frame, err := wire.EncodeReliableEvent(wire.Event{Channel: uint64(options.Channel), MessageType: uint64(typ), Payload: payload}, o.outboundMessageLimit())
		if err != nil {
			return mapWireError(err)
		}
		o.channelMu.Lock()
		defer o.channelMu.Unlock()
		stream, err := o.bindReliableSender(ctx, options.Channel)
		if err != nil {
			return err
		}
		_, err = stream.Write(frame)
		if err == nil {
			o.observeMessage("out_reliable", len(payload))
		}
		return err
	}
	o.channelMu.Lock()
	if err := o.bindOutbound(options.Channel, options.Delivery); err != nil {
		o.channelMu.Unlock()
		return err
	}
	o.eventMu.Lock()
	if o.sequences == nil {
		o.sequences = make(map[ChannelID]uint64)
	}
	sequence := uint64(0)
	kind := wire.UnreliableKind
	if options.Delivery == UnreliableSequenced {
		kind = wire.SequencedKind
		o.sequences[options.Channel]++
		sequence = o.sequences[options.Channel]
	}
	o.eventMu.Unlock()
	o.channelMu.Unlock()
	datagram, err := wire.EncodeDatagram(wire.Event{Kind: kind, Channel: uint64(options.Channel), MessageType: uint64(typ), Sequence: sequence, Payload: payload}, o.outboundDatagramLimit())
	if err != nil {
		return mapWireError(err)
	}
	err = o.connection.SendDatagram(ctx, datagram)
	if err == nil {
		kind := "out_unreliable"
		if options.Delivery == UnreliableSequenced {
			kind = "out_sequenced"
		}
		o.observeMessage(kind, len(payload))
	}
	return err
}

func (o *connectionOperations) bindOutbound(channel ChannelID, delivery Delivery) error {
	if o.outbound == nil {
		o.outbound = make(map[ChannelID]Delivery)
	}
	if bound, exists := o.outbound[channel]; exists {
		if bound != delivery {
			return ErrInvalidArgument
		}
		return nil
	}
	peer := o.session.peerLimitsSnapshot()
	limit := minimumAdvertisedLimit(o.session.limits.DatagramChannels, peer.DatagramChannels)
	if delivery == ReliableOrdered {
		limit = minimumAdvertisedLimit(o.session.limits.ReliableChannels, peer.ReliableChannels)
	}
	count := 0
	for _, bound := range o.outbound {
		if (bound == ReliableOrdered) == (delivery == ReliableOrdered) {
			count++
		}
	}
	if count >= limit {
		return ErrResourceExhausted
	}
	o.outbound[channel] = delivery
	return nil
}
func (o *connectionOperations) bindReliableSender(ctx context.Context, channel ChannelID) (transport.SendStream, error) {
	if err := o.bindOutbound(channel, ReliableOrdered); err != nil {
		return nil, err
	}
	if o.reliable != nil && o.reliable[channel] != nil {
		return o.reliable[channel], nil
	}
	stream, err := o.connection.OpenUni(ctx)
	if err != nil {
		return nil, err
	}
	header, err := wire.EncodeReliableChannelHeaderFor(uint64(channel))
	if err != nil {
		stream.Abort(uint64(ProtocolViolation))
		return nil, mapWireError(err)
	}
	if _, err := stream.Write(header); err != nil {
		stream.Abort(uint64(Internal))
		return nil, err
	}
	if o.reliable == nil {
		o.reliable = make(map[ChannelID]transport.SendStream)
	}
	o.reliable[channel] = stream
	return stream, nil
}
func (o *connectionOperations) bindInbound(channel ChannelID, delivery Delivery) bool {
	o.channelMu.Lock()
	defer o.channelMu.Unlock()
	if o.inbound == nil {
		o.inbound = make(map[ChannelID]Delivery)
	}
	if bound, exists := o.inbound[channel]; exists {
		return bound == delivery && delivery != ReliableOrdered
	}
	limit := o.session.limits.DatagramChannels
	if delivery == ReliableOrdered {
		limit = o.session.limits.ReliableChannels
	}
	count := 0
	for _, bound := range o.inbound {
		if (bound == ReliableOrdered) == (delivery == ReliableOrdered) {
			count++
		}
	}
	if count >= limit {
		return false
	}
	o.inbound[channel] = delivery
	return true
}
func (o *connectionOperations) acquireRequest(inbound bool) (func(), bool) {
	o.slotMu.Lock()
	limit := o.session.limits.Requests
	if !inbound {
		limit = minimumAdvertisedLimit(limit, o.session.peerLimitsSnapshot().Requests)
	}
	if inbound {
		if o.inRequests >= limit {
			o.slotMu.Unlock()
			return nil, false
		}
		o.inRequests++
	} else {
		if o.outRequests >= limit {
			o.slotMu.Unlock()
			return nil, false
		}
		o.outRequests++
	}
	o.slotMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			o.slotMu.Lock()
			if inbound {
				o.inRequests--
			} else {
				o.outRequests--
			}
			o.slotMu.Unlock()
		})
	}, true
}
func (o *connectionOperations) acquireStream(inbound bool) (func(), bool) {
	o.slotMu.Lock()
	limit := o.session.limits.Streams
	if !inbound {
		limit = minimumAdvertisedLimit(limit, o.session.peerLimitsSnapshot().Streams)
	}
	if inbound {
		if o.inStreams >= limit {
			o.slotMu.Unlock()
			return nil, false
		}
		o.inStreams++
	} else {
		if o.outStreams >= limit {
			o.slotMu.Unlock()
			return nil, false
		}
		o.outStreams++
	}
	o.slotMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			o.slotMu.Lock()
			if inbound {
				o.inStreams--
			} else {
				o.outStreams--
			}
			o.slotMu.Unlock()
		})
	}, true
}
func (o *connectionOperations) allowInboundMessage() bool {
	o.rateMu.Lock()
	defer o.rateMu.Unlock()
	now := time.Now()
	if o.rateAt.IsZero() {
		o.rateAt, o.rateTokens = now, float64(o.session.limits.MessageBurstPerSession)
	}
	o.rateTokens = min(float64(o.session.limits.MessageBurstPerSession), o.rateTokens+now.Sub(o.rateAt).Seconds()*float64(o.session.limits.MessagesPerSessionPerSecond))
	o.rateAt = now
	if o.rateTokens < 1 {
		return false
	}
	o.rateTokens--
	return true
}
func (o *connectionOperations) call(ctx context.Context, typ MessageType, payload []byte) ([]byte, error) {
	requestStarted := time.Now()
	resultCode := Internal
	defer func() {
		o.observe("requests", "outbound_result", resultCode, 1)
		o.observeDuration("requests", "outbound_latency", resultCode, time.Since(requestStarted))
	}()
	if !o.current() {
		resultCode = SessionSuspended
		return nil, ErrSessionSuspended
	}
	release, ok := o.acquireRequest(false)
	if !ok {
		resultCode = ResourceExhausted
		return nil, ErrResourceExhausted
	}
	defer release()
	releaseOutgoing, err := o.reserveOutgoing(len(payload) + 32)
	if err != nil {
		resultCode = observationErrorCode(err)
		return nil, err
	}
	defer releaseOutgoing()
	timeout := o.session.limits.RequestTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}
	if timeout <= 0 {
		resultCode = DeadlineExceeded
		return nil, &Error{Code: DeadlineExceeded, Cause: context.DeadlineExceeded}
	}
	timeoutMS := uint64(timeout / time.Millisecond)
	if timeoutMS == 0 {
		timeoutMS = 1
	}
	stream, err := o.connection.OpenBidi(ctx)
	if err != nil {
		resultCode = observationErrorCode(err)
		return nil, err
	}
	request, err := wire.EncodeRequest(wire.Request{MessageType: uint64(typ), TimeoutMS: timeoutMS, Payload: payload}, o.outboundMessageLimit())
	if err != nil {
		_ = stream.Close()
		mapped := mapWireError(err)
		resultCode = observationErrorCode(mapped)
		return nil, mapped
	}
	written, err := stream.Write(request)
	if err != nil {
		_ = stream.Close()
		if written > 0 {
			resultCode = OutcomeUnknown
			return nil, o.unknownRequestOutcome(err)
		}
		resultCode = observationErrorCode(err)
		return nil, err
	}
	if err = stream.CloseWrite(); err != nil {
		_ = stream.Close()
		resultCode = OutcomeUnknown
		return nil, o.unknownRequestOutcome(err)
	}
	result := make(chan struct {
		response wire.Response
		err      error
		complete bool
		release  func()
	})
	go func() {
		defer stream.Close()
		type responseResult struct {
			response wire.Response
			err      error
			complete bool
			release  func()
		}
		sendResult := func(value responseResult) {
			select {
			case result <- struct {
				response wire.Response
				err      error
				complete bool
				release  func()
			}{response: value.response, err: value.err, complete: value.complete, release: value.release}:
			case <-ctx.Done():
				if value.release != nil {
					value.release()
				}
			case <-o.context().Done():
				if value.release != nil {
					value.release()
				}
			}
		}
		reader := streamByteReader{Reader: stream}
		response, payloadBytes, readErr := wire.ReadResponseHeader(reader, o.session.limits.MessageBytes)
		if readErr != nil {
			sendResult(responseResult{err: readErr})
			return
		}
		releaseBody, reserveErr := o.reserveReplyApplicationBytes(payloadBytes + 32)
		if reserveErr != nil {
			sendResult(responseResult{err: reserveErr})
			return
		}
		response.Payload = make([]byte, payloadBytes)
		if _, readErr = io.ReadFull(reader, response.Payload); readErr != nil {
			releaseBody()
			sendResult(responseResult{err: readErr})
			return
		}
		var trailing [1]byte
		if count, trailingErr := reader.Read(trailing[:]); count != 0 {
			releaseBody()
			sendResult(responseResult{err: wire.ErrMalformed, complete: true})
			return
		} else if !errors.Is(trailingErr, io.EOF) {
			releaseBody()
			sendResult(responseResult{err: trailingErr})
			return
		}
		if response.Status != 0 && !utf8.Valid(response.Payload) {
			releaseBody()
			sendResult(responseResult{err: wire.ErrMalformed, complete: true})
			return
		}
		sendResult(responseResult{response: response, complete: true, release: releaseBody})
	}()
	select {
	case result := <-result:
		if result.release != nil {
			defer result.release()
		}
		if result.err != nil {
			if !result.complete {
				resultCode = OutcomeUnknown
				return nil, o.unknownRequestOutcome(result.err)
			}
			mapped := mapWireError(result.err)
			resultCode = observationErrorCode(mapped)
			return nil, mapped
		}
		if result.response.Status != 0 {
			resultCode = boundedObservationCode(Code(result.response.Status))
			return nil, &Error{Code: Code(result.response.Status), Message: string(result.response.Payload)}
		}
		resultCode = Normal
		return append([]byte(nil), result.response.Payload...), nil
	case <-ctx.Done():
		stream.Abort(uint64(Canceled))
		resultCode = OutcomeUnknown
		return nil, o.unknownRequestOutcome(ctx.Err())
	case <-o.context().Done():
		stream.Abort(uint64(SessionClosed))
		resultCode = OutcomeUnknown
		return nil, o.unknownRequestOutcome(context.Cause(o.context()))
	}
}
func (o *connectionOperations) unknownRequestOutcome(cause error) error {
	o.observe("requests", "unknown_outbound", OutcomeUnknown, 1)
	return unknownRequestOutcome(cause)
}
func unknownRequestOutcome(cause error) error {
	if cause == nil {
		cause = context.Canceled
	}
	return &Error{Code: OutcomeUnknown, OutcomeUnknown: true, Cause: cause}
}
func (o *connectionOperations) openStream(ctx context.Context, typ MessageType) (Stream, error) {
	if !o.current() {
		return nil, ErrSessionSuspended
	}
	release, ok := o.acquireStream(false)
	if !ok {
		return nil, ErrResourceExhausted
	}
	timeout := o.session.limits.RequestTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < timeout {
			timeout = remaining
		}
	}
	if timeout <= 0 {
		release()
		return nil, &Error{Code: DeadlineExceeded, Cause: context.DeadlineExceeded}
	}
	stream, err := o.connection.OpenBidi(ctx)
	if err != nil {
		release()
		return nil, err
	}
	if err := stream.SetDeadline(time.Now().Add(timeout)); err != nil {
		_ = stream.Close()
		release()
		return nil, err
	}
	header, err := wire.EncodeCustomStreamHeader(uint64(typ))
	if err != nil {
		_ = stream.Close()
		release()
		return nil, mapWireError(err)
	}
	if _, err := stream.Write(header); err != nil {
		_ = stream.Close()
		release()
		return nil, err
	}
	// Do not buffer here: after the acceptance varint, any prefetched bytes
	// would become an unaccounted per-stream allocation for the stream's life.
	reader := streamByteReader{Reader: stream}
	status, err := wire.ReadVarint(reader)
	if err != nil {
		stream.Abort(uint64(ProtocolViolation))
		release()
		return nil, mapWireError(err)
	}
	if status != 0 {
		diagnostic, err := readCustomDiagnostic(reader)
		if err != nil {
			stream.Abort(uint64(ProtocolViolation))
			release()
			return nil, mapWireError(err)
		}
		_ = stream.Close()
		release()
		return nil, &Error{Code: Code(status), Message: string(diagnostic)}
	}
	if err := stream.SetDeadline(time.Time{}); err != nil {
		_ = stream.Close()
		release()
		return nil, err
	}
	return &applicationStream{stream: stream, reader: reader, release: release}, nil
}
func (o *connectionOperations) refresh(ctx context.Context, credential Credential) error {
	o.refreshMu.Lock()
	defer o.refreshMu.Unlock()
	if o.control == nil || o.onRefresh == nil {
		return ErrWrongMode
	}
	if !o.current() {
		return ErrSessionSuspended
	}
	reply := make(chan refreshResultMessage, 1)
	o.refreshStateMu.Lock()
	if o.refreshActive {
		o.refreshStateMu.Unlock()
		return ErrResourceExhausted
	}
	o.refreshActive, o.refreshReply = true, reply
	o.refreshStateMu.Unlock()
	if err := o.control.write(refreshMessage{Op: "refresh", Credential: credentialMessage{Scheme: credential.Scheme, Data: base64.RawURLEncoding.EncodeToString(credential.Data)}}); err != nil {
		o.clearRefresh(reply)
		return err
	}
	select {
	case result := <-reply:
		if result.Code != 0 {
			return &Error{Code: Code(result.Code), Message: result.Message}
		}
		if result.ExpiresMS <= 0 {
			return ErrProtocolViolation
		}
		o.onRefresh(Principal{ExpiresAt: time.UnixMilli(result.ExpiresMS)})
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-o.context().Done():
		return ErrSessionClosed
	}
}
func (o *connectionOperations) clearRefresh(reply chan refreshResultMessage) {
	o.refreshStateMu.Lock()
	if o.refreshReply == reply {
		o.refreshActive, o.refreshReply = false, nil
	}
	o.refreshStateMu.Unlock()
}
func (o *connectionOperations) close(ctx context.Context, code Code) error {
	var acknowledged bool
	if o.control != nil {
		ack := make(chan struct{})
		o.closeMu.Lock()
		o.closeAck = ack
		o.closeMu.Unlock()
		if err := o.control.write(closeMessage{Op: "close", Code: uint32(code), Message: "session closed"}); err == nil {
			wait := o.session.limits.CloseTimeout
			if deadline, ok := ctx.Deadline(); ok {
				if remaining := time.Until(deadline); remaining < wait {
					wait = remaining
				}
			}
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-ack:
					acknowledged = true
				case <-ctx.Done():
				case <-timer.C:
				}
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
		}
		o.closeMu.Lock()
		if o.closeAck == ack {
			o.closeAck = nil
		}
		o.closeMu.Unlock()
	}
	o.stopEpoch()
	o.emitTerminal(code)
	err := o.connection.Close(uint64(code), "session closed")
	if err != nil {
		return err
	}
	if !acknowledged && ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}
func (o *connectionOperations) stats() Stats {
	if o == nil || o.connection == nil {
		return Stats{}
	}
	stats := o.connection.Stats()
	return Stats{RTT: stats.RTT, TransportStatsAvailable: true, BytesSent: stats.BytesSent, BytesReceived: stats.BytesReceived, LocalDatagramsDropped: o.localDatagramDrops.Load(), StaleUpdatesDropped: o.staleUpdatesDropped.Load()}
}
func (o *connectionOperations) startReceive() {
	if o.session == nil {
		return
	}
	o.startEpoch()
	if o.mode == Polling && o.incoming != nil {
		// A client polling queue survives a suspended epoch and is reused by a
		// resumed connection, so bind its sampler to the logical session.
		o.incoming.startObserving(o.session.Context(), o.observer)
	}
	go o.receiveDatagrams()
	go o.acceptReliableStreams()
	go o.acceptApplicationStreams()
	go o.observeTransport()
}

// observeTransport is one fixed task per active connection. It reports
// bounded transport counters, packet-loss gauge, and RTT histogram without
// putting endpoint or session identifiers into observation labels. QUIC packet
// loss is transport evidence only; it is not application-message loss.
func (o *connectionOperations) observeTransport() {
	if o == nil || o.connection == nil {
		return
	}
	previous := o.connection.Stats()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-o.context().Done():
			return
		case <-ticker.C:
			current := o.connection.Stats()
			o.observeTransportSample(previous, current)
			o.observeActiveOperations()
			o.observeHandlerQueueUsage()
			previous = current
		}
	}
}

func (o *connectionOperations) observeTransportSample(previous, current transport.Stats) {
	if current.RTT > 0 {
		o.observeDuration("transport", "rtt", Normal, current.RTT)
	}
	// quic-go's loss count can decrease when a packet declared lost later
	// arrives, so report it as a current gauge rather than a counter delta.
	o.observe("transport", "packets_lost", Normal, float64(current.PacketsLost))
	if current.BytesSent > previous.BytesSent {
		o.observe("transport", "bytes_sent", Normal, float64(current.BytesSent-previous.BytesSent))
	}
	if current.BytesReceived > previous.BytesReceived {
		o.observe("transport", "bytes_received", Normal, float64(current.BytesReceived-previous.BytesReceived))
	}
}

// observeActiveOperations samples bounded per-connection slot counters. The
// endpoint observer aggregates same-label samples across connections, so no
// session or application identifiers are exposed.
func (o *connectionOperations) observeActiveOperations() {
	if o == nil {
		return
	}
	o.slotMu.Lock()
	inRequests, outRequests := o.inRequests, o.outRequests
	inStreams, outStreams := o.inStreams, o.outStreams
	o.slotMu.Unlock()
	o.observe("requests", "active_inbound", Normal, float64(inRequests))
	o.observe("requests", "active_outbound", Normal, float64(outRequests))
	o.observe("streams", "request_inbound", Normal, float64(inRequests))
	o.observe("streams", "request_outbound", Normal, float64(outRequests))
	o.observe("streams", "custom_inbound", Normal, float64(inStreams))
	o.observe("streams", "custom_outbound", Normal, float64(outStreams))
}

func (o *connectionOperations) observeHandlerQueueUsage() {
	if o == nil {
		return
	}
	o.handlerMu.Lock()
	queue := o.handlerQueue
	o.handlerMu.Unlock()
	if queue == nil {
		return
	}
	items, bytes := queue.Len()
	o.observe("queue_bytes", "in_handler", Normal, float64(bytes))
	o.observe("queue_items", "in_handler", Normal, float64(items))
}

func (o *connectionOperations) observeQueuedIncoming(incoming *Incoming) {
	if o == nil {
		return
	}
	observeIncomingQueueAge(o.observer, incoming)
}

func observeIncomingQueueAge(observer *observerQueue, incoming *Incoming) {
	if observer == nil || incoming == nil || incoming.enqueuedAt.IsZero() {
		return
	}
	kind := "in_lifecycle"
	switch incoming.Kind {
	case Event:
		kind = "in_event"
	case RequestMessage:
		kind = "in_request"
	case StreamMessage:
		kind = "in_stream"
	}
	age := time.Since(incoming.enqueuedAt)
	if age < 0 {
		age = 0
	}
	observer.Observe(Observation{Name: "queue_age", Kind: histogramKind(kind, age), Code: Normal, Value: 1})
}
func (o *connectionOperations) startControl() {
	if o.session == nil || o.control == nil {
		return
	}
	o.controlStart.Do(func() {
		o.control.startWriter(o.connection.Context(), o.session.limits.ControlQueueBytes, o.session.limits.ControlQueueMessages, func() {
			o.abortControl(ResourceExhausted)
		})
		go o.controlLoop()
	})
}
func (o *connectionOperations) controlLoop() {
	for {
		control, err := o.control.read(o.connection.Context(), o.session.limits.ControlBytes)
		if err != nil {
			return
		}
		switch control.Op {
		case "refresh":
			if o.onIncomingRefresh == nil {
				o.abortControl(ProtocolViolation)
				return
			}
			if err := o.control.write(o.onIncomingRefresh(o.connection.Context(), control)); err != nil {
				return
			}
		case "refresh_result":
			var result refreshResultMessage
			raw, err := json.Marshal(control.Fields)
			if err != nil || json.Unmarshal(raw, &result) != nil {
				o.abortControl(ProtocolViolation)
				return
			}
			o.refreshStateMu.Lock()
			active, reply := o.refreshActive, o.refreshReply
			if active {
				o.refreshActive, o.refreshReply = false, nil
			}
			o.refreshStateMu.Unlock()
			if !active {
				o.abortControl(ProtocolViolation)
				return
			}
			reply <- result
		case "close":
			message, err := parseClose(control)
			if err != nil {
				o.abortControl(ProtocolViolation)
				return
			}
			if err := o.control.write(closeAckMessage{Op: "close_ack"}); err != nil {
				return
			}
			o.remoteClose(Code(message.Code))
			return
		case "close_ack":
			o.closeMu.Lock()
			ack := o.closeAck
			o.closeMu.Unlock()
			if ack == nil {
				o.abortControl(ProtocolViolation)
				return
			}
			select {
			case <-ack:
			default:
				close(ack)
			}
		default:
			o.abortControl(ProtocolViolation)
			return
		}
	}
}
func (o *connectionOperations) remoteClose(code Code) {
	if !o.session.closeRemote(code) {
		return
	}
	o.stopEpoch()
	o.emitTerminal(code)
}
func (o *connectionOperations) emitTerminal(code Code) {
	// Handler dispatch only enqueues bounded work, and polling only enqueues an
	// envelope, so terminal delivery need not escape to an untracked goroutine.
	// Keeping it synchronous ensures endpoint shutdown has scheduled terminal
	// lifecycle markers before it begins draining its worker pool.
	o.deliverLifecycleAfter(SessionEnded, code, o.onTerminal)
}
func (o *connectionOperations) abortControl(code Code) {
	o.remoteClose(code)
	_ = o.connection.Close(uint64(code), "invalid control")
}
func (o *connectionOperations) receiveDatagrams() {
	for {
		datagram, err := o.connection.ReceiveDatagram(o.connection.Context())
		if err != nil {
			return
		}
		event, err := wire.DecodeDatagram(datagram.Payload, o.session.limits.DatagramBytes)
		if err != nil {
			o.dropDatagram("invalid", ProtocolViolation, false)
			continue
		}
		if !o.allowInboundMessage() {
			o.dropDatagram("rate", ResourceExhausted, false)
			continue
		}
		delivery := Unreliable
		if event.Kind == wire.SequencedKind {
			delivery = UnreliableSequenced
		}
		if !o.bindInbound(ChannelID(event.Channel), delivery) {
			_ = o.session.Close(context.Background(), ProtocolViolation)
			return
		}
		if event.Kind == wire.SequencedKind {
			o.eventMu.Lock()
			if o.received == nil {
				o.received = make(map[ChannelID]uint64)
			}
			channel := ChannelID(event.Channel)
			stale := event.Sequence <= o.received[channel]
			if !stale {
				o.received[channel] = event.Sequence
			}
			o.eventMu.Unlock()
			if stale {
				o.dropDatagram("stale", Normal, true)
				continue
			}
		}
		o.deliverEvent(event)
	}
}
func (o *connectionOperations) acceptReliableStreams() {
	for {
		stream, err := o.connection.AcceptUni(o.connection.Context())
		if err != nil {
			return
		}
		go o.readReliableStream(stream)
	}
}

// streamByteReader deliberately does not buffer. Reliable readers must hold
// queue and application-budget capacity before consuming a declared body, so
// a buffered ReadByte implementation must not prefetch that body while it is
// waiting for pressure to clear.
type streamByteReader struct{ io.Reader }

func (r streamByteReader) ReadByte() (byte, error) {
	var value [1]byte
	if _, err := io.ReadFull(r.Reader, value[:]); err != nil {
		return 0, err
	}
	return value[0], nil
}

func (o *connectionOperations) readReliableStream(stream transport.ReceiveStream) {
	reader := streamByteReader{Reader: stream}
	kind, err := wire.ReadVarint(reader)
	if err != nil || kind != wire.ReliableEventKind {
		o.protocolViolationAfterStreamRead(err)
		return
	}
	channel, err := wire.ReadVarint(reader)
	if err != nil || channel > 1<<16-1 {
		o.protocolViolationAfterStreamRead(err)
		return
	}
	if !o.bindInbound(ChannelID(channel), ReliableOrdered) {
		_ = o.session.Close(context.Background(), ProtocolViolation)
		return
	}
	for {
		length, err := wire.ReadVarint(reader)
		if err != nil {
			o.protocolViolationAfterStreamRead(err)
			return
		}
		if length > uint64(o.session.limits.MessageBytes+32) {
			o.protocolViolation()
			return
		}
		event, payloadBytes, err := wire.ReadReliableEventHeader(reader, length, o.session.limits.MessageBytes)
		if err != nil || event.Channel != channel {
			o.protocolViolationAfterStreamRead(err)
			return
		}
		if !o.allowInboundMessage() {
			_ = o.session.Close(context.Background(), ResourceExhausted)
			return
		}
		pressureCtx, cancel := context.WithTimeout(o.context(), o.session.limits.SlowConsumerTimeout)
		reservation, err := o.reserveReliableIncoming(pressureCtx, payloadBytes+32)
		cancel()
		if err != nil {
			if o.context().Err() == nil && o.current() {
				_ = o.session.Close(context.Background(), SlowConsumer)
			}
			return
		}
		event.Payload = make([]byte, payloadBytes)
		if _, err := io.ReadFull(reader, event.Payload); err != nil {
			reservation.Cancel()
			o.protocolViolationAfterStreamRead(err)
			return
		}
		if err := o.deliverReservedReliableEvent(event, reservation); err != nil {
			reservation.Cancel()
			if o.context().Err() == nil && o.current() {
				_ = o.session.Close(context.Background(), SlowConsumer)
			}
			return
		}
	}
}
func (o *connectionOperations) acceptApplicationStreams() {
	for {
		stream, err := o.connection.AcceptBidi(o.connection.Context())
		if err != nil {
			return
		}
		go o.readApplicationStream(stream)
	}
}
func (o *connectionOperations) readApplicationStream(stream transport.BidiStream) {
	// Use an unbuffered header reader so a request body remains unread until
	// its exact application reservation has been acquired.
	reader := streamByteReader{Reader: stream}
	if err := stream.SetDeadline(time.Now().Add(o.session.limits.AuthTimeout)); err != nil {
		_ = stream.Close()
		return
	}
	kind, err := wire.ReadVarint(reader)
	if err != nil {
		_ = stream.Close()
		o.protocolViolationAfterStreamRead(err)
		return
	}
	switch kind {
	case wire.RequestStreamKind:
		if !o.allowInboundMessage() {
			frame, _ := wire.EncodeResponse(wire.Response{Status: uint64(ResourceExhausted), Payload: []byte("message rate exceeded")}, o.session.limits.MessageBytes)
			_, _ = stream.Write(frame)
			_ = stream.Close()
			_ = o.session.Close(context.Background(), ResourceExhausted)
			return
		}
		release, ok := o.acquireRequest(true)
		if !ok {
			frame, _ := wire.EncodeResponse(wire.Response{Status: uint64(ResourceExhausted), Payload: []byte("request capacity reached")}, o.session.limits.MessageBytes)
			_, _ = stream.Write(frame)
			_ = stream.Close()
			return
		}
		o.readRequestStream(stream, reader, release)
	case wire.CustomStreamKind:
		messageType, err := wire.ReadVarint(reader)
		if err != nil || messageType == 0 || messageType > 1<<32-1 {
			_ = stream.Close()
			o.protocolViolationAfterStreamRead(err)
			return
		}
		if !o.allowInboundMessage() {
			_ = writeCustomAcceptance(stream, ResourceExhausted, "message rate exceeded")
			_ = stream.Close()
			_ = o.session.Close(context.Background(), ResourceExhausted)
			return
		}
		if err := stream.SetDeadline(time.Time{}); err != nil {
			_ = stream.Close()
			return
		}
		release, ok := o.acquireStream(true)
		if !ok {
			_ = writeCustomAcceptance(stream, ResourceExhausted, "stream capacity reached")
			_ = stream.Close()
			return
		}
		o.readCustomStream(stream, stream, MessageType(messageType), release)
	default:
		stream.Abort(uint64(ProtocolViolation))
		o.protocolViolation()
	}
}
func (o *connectionOperations) readRequestStream(stream transport.BidiStream, reader interface {
	io.Reader
	io.ByteReader
}, release func()) {
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			_ = stream.Close()
			if release != nil {
				release()
			}
		})
	}
	handedOff := false
	defer func() {
		if !handedOff {
			cleanup()
		}
	}()
	request, payloadBytes, err := wire.ReadRequestHeader(reader, o.session.limits.MessageBytes)
	if err != nil {
		o.protocolViolationAfterStreamRead(err)
		return
	}
	pressureCtx, stopPressure := context.WithTimeout(o.context(), o.session.limits.SlowConsumerTimeout)
	reservation, err := o.reserveReliableIncoming(pressureCtx, payloadBytes+32)
	stopPressure()
	if err != nil {
		if o.context().Err() == nil && o.current() {
			_ = o.session.Close(context.Background(), SlowConsumer)
		}
		return
	}
	request.Payload = make([]byte, payloadBytes)
	if _, err := io.ReadFull(reader, request.Payload); err != nil {
		reservation.Cancel()
		o.protocolViolationAfterStreamRead(err)
		return
	}
	requestCtx, cancel := context.WithTimeout(o.context(), time.Duration(request.TimeoutMS)*time.Millisecond)
	requestStarted := time.Now()
	completed := make(chan struct{})
	var completeOnce sync.Once
	respond := func(ctx context.Context, status Code, payload []byte) error {
		resultCode := boundedObservationCode(status)
		defer func() {
			o.observe("requests", "inbound_result", resultCode, 1)
			o.observeDuration("requests", "inbound_latency", resultCode, time.Since(requestStarted))
		}()
		if err := ctx.Err(); err != nil {
			resultCode = observationErrorCode(err)
			return err
		}
		releaseOutgoing, err := o.reserveOutgoingReply(len(payload) + 32)
		if err != nil {
			resultCode = Backpressure
			return err
		}
		defer releaseOutgoing()
		frame, err := wire.EncodeResponse(wire.Response{Status: uint64(status), Payload: payload}, o.outboundMessageLimit())
		if err != nil {
			resultCode = Internal
			return mapWireError(err)
		}
		if _, err := stream.Write(frame); err != nil {
			resultCode = Internal
			return err
		}
		if err := stream.CloseWrite(); err != nil {
			resultCode = Internal
			return err
		}
		completeOnce.Do(func() { close(completed) })
		return nil
	}
	incoming := &Incoming{
		Kind:    RequestMessage,
		Session: o.session,
		Epoch:   o.epoch,
		Type:    MessageType(request.MessageType),
		Payload: request.Payload,
		ctx:     requestCtx,
		reply: func(ctx context.Context, payload []byte) error {
			return respond(ctx, Normal, payload)
		},
		fail: func(ctx context.Context, code Code, message string) error {
			return respond(ctx, code, []byte(message))
		},
	}
	if o.mode != Polling {
		// The worker owns the request context after this reader returns. Its
		// invocation releases the envelope (and therefore cancels the context)
		// only after the handler has replied or failed.
		incoming.release = func() {
			cancel()
			cleanup()
		}
		handedOff = true
		if err := o.enqueueReservedReliableHandler(incoming, reservation); err != nil {
			reservation.Cancel()
			_ = incoming.Fail(context.Background(), Backpressure, "request queue full")
			incoming.Release()
		}
		return
	}
	defer cancel()
	defer incoming.Release()
	if !reservation.Commit(incoming) {
		_ = incoming.Fail(context.Background(), Backpressure, "request queue full")
		return
	}
	select {
	case <-completed:
	case <-requestCtx.Done():
		if !incoming.hasResponded() {
			_ = incoming.Fail(context.Background(), DeadlineExceeded, "request deadline exceeded")
		}
	}
}
func (o *connectionOperations) readCustomStream(stream transport.BidiStream, reader io.Reader, typ MessageType, release func()) {
	application := &applicationStream{stream: stream, reader: reader, release: release}
	incoming := &Incoming{Kind: StreamMessage, Session: o.session, Epoch: o.epoch, Type: typ, Stream: application, ctx: o.context()}
	if o.mode == Polling {
		if !o.dispatch(incoming) {
			_ = writeCustomAcceptance(stream, Backpressure, "stream queue full")
			_ = stream.Close()
			application.releaseSlot()
			return
		}
		if err := writeCustomAcceptance(stream, Normal, ""); err != nil {
			incoming.Release()
			_ = stream.Close()
			application.releaseSlot()
		}
		return
	}
	handler := o.router.handler(incoming)
	if handler == nil {
		_ = writeCustomAcceptance(stream, UnsupportedMessage, "unsupported message")
		_ = stream.Close()
		application.releaseSlot()
		return
	}
	if err := writeCustomAcceptance(stream, Normal, ""); err != nil {
		_ = stream.Close()
		application.releaseSlot()
		return
	}
	// Active custom streams already hold a bounded stream slot. Their handlers
	// may legitimately block on stream I/O, so they run independently of the
	// ordinary per-session callback scheduler; otherwise one bulk transfer
	// would stall unrelated input, request, and lifecycle callbacks.
	go o.invoke(incoming)
}

type applicationStream struct {
	stream      transport.BidiStream
	reader      io.Reader
	release     func()
	releaseOnce sync.Once
}

func (s *applicationStream) Read(payload []byte) (int, error)  { return s.reader.Read(payload) }
func (s *applicationStream) Write(payload []byte) (int, error) { return s.stream.Write(payload) }
func (s *applicationStream) CloseWrite() error                 { return s.stream.CloseWrite() }
func (s *applicationStream) CloseRead() error                  { s.stream.CloseRead(); return nil }
func (s *applicationStream) Close() error {
	err := s.stream.CloseWrite()
	s.stream.CloseRead()
	s.releaseSlot()
	return err
}
func (s *applicationStream) SetDeadline(deadline time.Time) error {
	return s.stream.SetDeadline(deadline)
}
func (s *applicationStream) Abort(code Code) { s.stream.Abort(uint64(code)); s.releaseSlot() }
func (s *applicationStream) releaseSlot() {
	s.releaseOnce.Do(func() {
		if s.release != nil {
			s.release()
		}
	})
}

func writeCustomAcceptance(stream transport.BidiStream, status Code, diagnostic string) error {
	if status == Normal {
		frame, err := wire.AppendVarint(nil, 0)
		if err != nil {
			return err
		}
		_, err = stream.Write(frame)
		return err
	}
	frame, err := wire.EncodeResponse(wire.Response{Status: uint64(status), Payload: []byte(diagnostic)}, 256)
	if err != nil {
		return mapWireError(err)
	}
	_, err = stream.Write(frame)
	return err
}
func readCustomDiagnostic(reader interface {
	io.Reader
	io.ByteReader
}) ([]byte, error) {
	length, err := wire.ReadVarint(reader)
	if err != nil || length > 256 {
		return nil, wire.ErrMalformed
	}
	diagnostic := make([]byte, int(length))
	if _, err := io.ReadFull(reader, diagnostic); err != nil {
		return nil, err
	}
	if !utf8.Valid(diagnostic) {
		return nil, wire.ErrMalformed
	}
	return diagnostic, nil
}
func (o *connectionOperations) deliverEvent(event wire.Event) {
	kind := "in_unreliable"
	if event.Kind == wire.SequencedKind {
		kind = "in_sequenced"
	}
	o.observeMessage(kind, len(event.Payload))
	incoming := &Incoming{Kind: Event, Session: o.session, Epoch: o.epoch, Type: MessageType(event.MessageType), Channel: ChannelID(event.Channel), Payload: append([]byte(nil), event.Payload...), ctx: o.context()}
	switch event.Kind {
	case wire.UnreliableKind:
		incoming.Delivery = Unreliable
	case wire.SequencedKind:
		incoming.Delivery, incoming.Sequence = UnreliableSequenced, event.Sequence
	default:
		incoming.Delivery = ReliableOrdered
	}
	if !o.dispatch(incoming) {
		if o.current() {
			o.dropDatagram("pressure", Backpressure, false)
		}
		incoming.Release()
	}
}
func mapWireError(err error) error {
	if errors.Is(err, wire.ErrTooLarge) {
		return &Error{Code: TooLarge}
	}
	return &Error{Code: ProtocolViolation, Cause: err}
}

func (o *connectionOperations) outboundMessageLimit() int {
	return minimumAdvertisedLimit(o.session.limits.MessageBytes, o.session.peerLimitsSnapshot().MessageBytes)
}
func (o *connectionOperations) outboundDatagramLimit() int {
	return minimumAdvertisedLimit(o.session.limits.DatagramBytes, o.session.peerLimitsSnapshot().DatagramBytes)
}

func transportConfig(tlsConfig *tls.Config, limits Limits) quictransport.Config {
	return quictransport.Config{TLS: tlsConfig, HandshakeTimeout: limits.AuthTimeout, IdleTimeout: limits.IdleTimeout, KeepAlive: limits.KeepAlive, StreamReceiveWindow: limits.StreamReceiveWindow, ConnectionReceiveWindow: limits.ConnectionReceiveWindow, MaxIncomingBidi: int64(limits.Requests + limits.Streams + 1), MaxIncomingUni: int64(limits.ReliableChannels), EnableDatagrams: true}
}
func roleName(role Role) string {
	if role == BootstrapRole {
		return "bootstrap"
	}
	return "game"
}

type limitMessage struct {
	ControlBytes     int `json:"control_bytes"`
	MessageBytes     int `json:"message_bytes"`
	DatagramBytes    int `json:"datagram_bytes"`
	ReliableChannels int `json:"reliable_channels"`
	DatagramChannels int `json:"datagram_channels"`
	Requests         int `json:"requests"`
	Streams          int `json:"streams"`
}

func limitMessageFrom(limits Limits) limitMessage {
	return limitMessage{limits.ControlBytes, limits.MessageBytes, limits.DatagramBytes, limits.ReliableChannels, limits.DatagramChannels, limits.Requests, limits.Streams}
}
func validateLimitMessage(limits limitMessage) error {
	values := []int{limits.ControlBytes, limits.MessageBytes, limits.DatagramBytes, limits.ReliableChannels, limits.DatagramChannels, limits.Requests, limits.Streams}
	for _, value := range values {
		if value <= 0 {
			return ErrInvalidArgument
		}
	}
	return nil
}
func hasCapability(capabilities []string, expected string) bool {
	for _, capability := range capabilities {
		if capability == expected {
			return true
		}
	}
	return false
}
func validateClientWelcome(welcome welcomeMessage, role Role, resumed bool) error {
	if welcome.Resumed != resumed || !time.UnixMilli(welcome.Principal.ExpiresMS).After(time.Now()) {
		return ErrProtocolViolation
	}
	if !resumed && welcome.Epoch != "1" {
		return ErrProtocolViolation
	}
	if role == BootstrapRole {
		if welcome.Resumable || welcome.ResumeGraceMS != 0 || welcome.ResumeSecret != "" {
			return ErrProtocolViolation
		}
		return nil
	}
	if !welcome.Resumable || welcome.ResumeGraceMS <= 0 {
		return ErrProtocolViolation
	}
	if resumed {
		if welcome.ResumeSecret != "" {
			return ErrProtocolViolation
		}
		return nil
	}
	secret, err := base64.RawURLEncoding.DecodeString(welcome.ResumeSecret)
	if err != nil || len(secret) != 32 {
		return ErrProtocolViolation
	}
	return nil
}

type credentialMessage struct {
	Scheme string `json:"scheme"`
	Data   string `json:"data"`
}
type helloMessage struct {
	Op         string            `json:"op"`
	Role       string            `json:"role"`
	App        AppIdentity       `json:"-"`
	AppID      string            `json:"app"`
	AppVersion string            `json:"app_version"`
	Required   []string          `json:"required"`
	Limits     limitMessage      `json:"limits"`
	Credential credentialMessage `json:"credential"`
	Group      string            `json:"group,omitempty"`
	Admission  string            `json:"admission,omitempty"`
	Resume     *resumeMessage    `json:"resume,omitempty"`
}

type resumeMessage struct {
	SessionID   string `json:"session_id"`
	OwnerID     string `json:"owner_id"`
	Incarnation string `json:"incarnation"`
	Secret      string `json:"secret"`
}

func (h helloMessage) MarshalJSON() ([]byte, error) {
	type raw helloMessage
	h.AppID, h.AppVersion = h.App.ID, h.App.Version
	return json.Marshal(raw(h))
}

type ownerMessage struct {
	ID          string `json:"id"`
	Incarnation string `json:"incarnation"`
	Address     string `json:"address"`
	ServerName  string `json:"server_name"`
}

func (o ownerMessage) toOwner() Owner {
	var incarnation Incarnation
	decoded, _ := hex.DecodeString(o.Incarnation)
	copy(incarnation[:], decoded)
	return Owner{ID: o.ID, Incarnation: incarnation, Endpoint: Endpoint{Address: o.Address, ServerName: o.ServerName}}
}

type principalMessage struct {
	Issuer    string `json:"issuer"`
	Subject   string `json:"subject"`
	ExpiresMS int64  `json:"expires_unix_ms"`
}
type welcomeMessage struct {
	Op            string           `json:"op"`
	SessionID     string           `json:"session_id"`
	Epoch         string           `json:"epoch"`
	Owner         ownerMessage     `json:"owner"`
	Principal     principalMessage `json:"principal"`
	Limits        limitMessage     `json:"limits"`
	Capabilities  []string         `json:"capabilities"`
	Resumed       bool             `json:"resumed"`
	Resumable     bool             `json:"resumable"`
	ResumeGraceMS int              `json:"resume_grace_ms"`
	ResumeSecret  string           `json:"resume_secret,omitempty"`
}
type refreshMessage struct {
	Op         string            `json:"op"`
	Credential credentialMessage `json:"credential"`
}
type refreshResultMessage struct {
	Op        string `json:"op"`
	Code      uint32 `json:"code"`
	Message   string `json:"message"`
	ExpiresMS int64  `json:"expires_unix_ms,omitempty"`
}
type closeMessage struct {
	Op      string `json:"op"`
	Code    uint32 `json:"code"`
	Message string `json:"message"`
}
type closeAckMessage struct {
	Op string `json:"op"`
}

func parseHello(control wire.Control) (helloMessage, error) {
	var result helloMessage
	raw, err := json.Marshal(control.Fields)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return result, err
	}
	result.App = AppIdentity{ID: result.AppID, Version: result.AppVersion}
	data, err := base64.RawURLEncoding.DecodeString(result.Credential.Data)
	if err != nil {
		return result, err
	}
	result.Credential.Data = base64.RawURLEncoding.EncodeToString(data)
	return result, nil
}
func parseWelcome(control wire.Control) (welcomeMessage, error) {
	var result welcomeMessage
	raw, err := json.Marshal(control.Fields)
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(raw, &result)
	return result, err
}
func parseClose(control wire.Control) (closeMessage, error) {
	var result closeMessage
	raw, err := json.Marshal(control.Fields)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(raw, &result); err != nil || result.Op != "close" || len(result.Message) > 256 || !utf8.ValidString(result.Message) || Code(result.Code) > WrongMode {
		if err == nil {
			err = ErrProtocolViolation
		}
		return closeMessage{}, err
	}
	return result, nil
}
func parseSessionID(value string) (SessionID, error) {
	var id SessionID
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(id) {
		return id, errors.New("invalid session ID")
	}
	copy(id[:], decoded)
	return id, nil
}

type controlChannel struct {
	reader      *bufio.Reader
	writer      io.Writer
	setDeadline func(time.Time) error
	writeMu     sync.Mutex
	queueMu     sync.Mutex
	queue       *runtime.Queue[*queuedControlFrame]
	queueCtx    context.Context
	queueCancel context.CancelFunc
	closed      bool
	exhausted   func()
	exhaustOnce sync.Once
}

type queuedControlFrame struct {
	payload []byte
	done    chan error
}

func newControlChannel(stream interface {
	io.Reader
	io.Writer
}) *controlChannel {
	channel := &controlChannel{reader: bufio.NewReader(stream), writer: stream}
	if deadlines, ok := stream.(interface{ SetDeadline(time.Time) error }); ok {
		channel.setDeadline = deadlines.SetDeadline
	}
	return channel
}
func (c *controlChannel) read(ctx context.Context, max int) (wire.Control, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return wire.Control{}, err
	}
	if deadline, ok := ctx.Deadline(); ok && c.setDeadline != nil {
		if err := c.setDeadline(deadline); err != nil {
			return wire.Control{}, err
		}
		defer c.setDeadline(time.Time{})
	}
	return readControlReader(c.reader, max)
}
func (c *controlChannel) write(message any) error {
	payload, err := encodeControl(message)
	if err != nil {
		return err
	}
	c.queueMu.Lock()
	queue, queueCtx, closed := c.queue, c.queueCtx, c.closed
	c.queueMu.Unlock()
	if closed {
		return context.Canceled
	}
	if queue == nil {
		return c.writeFrame(payload)
	}
	frame := &queuedControlFrame{payload: payload, done: make(chan error, 1)}
	if !queue.TryPush(runtime.Item[*queuedControlFrame]{Value: frame, Bytes: len(payload)}) {
		c.exhaustOnce.Do(func() {
			if c.exhausted != nil {
				c.exhausted()
			}
		})
		return ErrBackpressure
	}
	select {
	case err := <-frame.done:
		return err
	case <-queueCtx.Done():
		return context.Canceled
	}
}

// startWriter enables the fixed per-connection control writer after the
// handshake. Its bounded queue prevents blocked transport writes from turning
// concurrent refresh/close paths into unbounded parked goroutines.
func (c *controlChannel) startWriter(ctx context.Context, maxBytes, maxItems int, exhausted func()) {
	if c == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if maxBytes < 1 {
		maxBytes = 1
	}
	if maxItems < 1 {
		maxItems = 1
	}
	c.queueMu.Lock()
	if c.closed || c.queue != nil {
		c.queueMu.Unlock()
		return
	}
	queueCtx, cancel := context.WithCancel(ctx)
	queue := runtime.NewQueue[*queuedControlFrame](maxBytes, maxItems)
	c.queue, c.queueCtx, c.queueCancel, c.exhausted = queue, queueCtx, cancel, exhausted
	c.queueMu.Unlock()
	go c.writeLoop(queueCtx, queue)
}

func (c *controlChannel) stopWriter() {
	if c == nil {
		return
	}
	c.queueMu.Lock()
	if c.closed {
		c.queueMu.Unlock()
		return
	}
	c.closed = true
	queue, cancel := c.queue, c.queueCancel
	c.queueMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if queue != nil {
		queue.Close()
		c.failQueuedControls(queue, context.Canceled)
	}
}

func (c *controlChannel) writeLoop(ctx context.Context, queue *runtime.Queue[*queuedControlFrame]) {
	defer c.failQueuedControls(queue, context.Canceled)
	for {
		frame, err := queue.WaitPop(ctx)
		if err != nil {
			return
		}
		select {
		case <-ctx.Done():
			c.finishControlFrame(frame.Value, context.Canceled)
			return
		default:
		}
		if frame.Value == nil {
			continue
		}
		c.finishControlFrame(frame.Value, c.writeFrame(frame.Value.payload))
	}
}

func (c *controlChannel) failQueuedControls(queue *runtime.Queue[*queuedControlFrame], err error) {
	if queue == nil {
		return
	}
	for {
		frame, ok := queue.Pop()
		if !ok {
			return
		}
		c.finishControlFrame(frame.Value, err)
	}
}

func (c *controlChannel) finishControlFrame(frame *queuedControlFrame, err error) {
	if frame == nil {
		return
	}
	select {
	case frame.done <- err:
	default:
	}
}

func (c *controlChannel) writeFrame(payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	written, err := c.writer.Write(payload)
	if err == nil && written != len(payload) {
		return io.ErrShortWrite
	}
	return err
}

func readControlReader(reader *bufio.Reader, max int) (wire.Control, error) {
	length, err := wire.ReadVarint(reader)
	if err != nil {
		return wire.Control{}, err
	}
	if length > uint64(max) {
		return wire.Control{}, ErrTooLarge
	}
	body := make([]byte, int(length))
	if _, err := io.ReadFull(reader, body); err != nil {
		return wire.Control{}, err
	}
	return wire.DecodeControlBody(body, max)
}
func writeControl(stream io.Writer, message any) error {
	encoded, err := encodeControl(message)
	if err != nil {
		return err
	}
	written, err := stream.Write(encoded)
	if err == nil && written != len(encoded) {
		return io.ErrShortWrite
	}
	return err
}
func encodeControl(message any) ([]byte, error) {
	raw, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	encoded, err := wire.EncodeControl(raw, wire.PreNegotiationControlBytes)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}
