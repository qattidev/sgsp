package sgsp

import (
	"context"
	"sync"
	"time"

	"qattidev/sgsp/internal/runtime"
)

// sessionOperations is the private seam between the concurrency-safe public
// Session and an admitted endpoint's transport/dispatcher. M4 supplies the
// authenticated QUIC implementation; no unauthenticated constructor is
// exported.
type sessionOperations interface {
	send(context.Context, MessageType, []byte, SendOptions) error
	call(context.Context, MessageType, []byte) ([]byte, error)
	openStream(context.Context, MessageType) (Stream, error)
	refresh(context.Context, Credential) error
	close(context.Context, Code) error
	stats() Stats
}

type sessionRecord struct {
	mu         sync.RWMutex
	id         SessionID
	owner      Owner
	group      string
	principal  Principal
	ctx        context.Context
	cancel     context.CancelFunc
	state      State
	epoch      uint64
	attachment any
	limits     Limits
	clientSide bool
	operations sessionOperations
	resumeHash [32]byte
	peerLimits limitMessage
	terminal   Code
	// Directional budgets persist across resumed connections. They cover
	// temporary library-owned frames and reply bodies; queued incoming work is
	// additionally charged by its dispatch queue. A full-size request reply has
	// a separate reserve so ordinary traffic cannot deadlock Call completion.
	incomingBudget, incomingReplyBudget *runtime.Budget
	outgoingBudget, outgoingReplyBudget *runtime.Budget
	expiryTimer                         *time.Timer
	graceTimer                          *time.Timer
}

func newSessionRecord(id SessionID, owner Owner, group string, principal Principal, limits Limits, clientSide bool, operations sessionOperations) *sessionRecord {
	ctx, cancel := context.WithCancel(context.Background())
	ordinaryBytes, replyBytes := directionalBudgetLimits(limits)
	return &sessionRecord{id: id, owner: owner, group: group, principal: copyPrincipal(principal), ctx: ctx, cancel: cancel, state: Active, epoch: 1, limits: limits, clientSide: clientSide, operations: operations, incomingBudget: runtime.NewBudget(ordinaryBytes), incomingReplyBudget: runtime.NewBudget(replyBytes), outgoingBudget: runtime.NewBudget(ordinaryBytes), outgoingReplyBudget: runtime.NewBudget(replyBytes)}
}

// directionalBudgetLimits leaves MessageBytes+32 bytes for one complete
// response in each direction. New endpoints have already normalized
// QueueBytes to at least twice that value. The fallback keeps direct internal
// fixtures with deliberately tiny limits usable without overstating capacity.
func directionalBudgetLimits(limits Limits) (ordinary, reply int64) {
	total := int64(limits.QueueBytes)
	reserve := int64(limits.MessageBytes + 32)
	if total >= 2*reserve {
		return total - reserve, reserve
	}
	return total, 0
}

func copyPrincipal(principal Principal) Principal {
	copy := principal
	if principal.Attributes != nil {
		copy.Attributes = make(map[string]string, len(principal.Attributes))
		for key, value := range principal.Attributes {
			copy.Attributes[key] = value
		}
	}
	return copy
}

func (s *sessionRecord) ID() SessionID    { return s.id }
func (s *sessionRecord) Owner() Owner     { return s.owner }
func (s *sessionRecord) GroupKey() string { return s.group }
func (s *sessionRecord) State() State     { s.mu.RLock(); defer s.mu.RUnlock(); return s.state }
func (s *sessionRecord) Epoch() uint64    { s.mu.RLock(); defer s.mu.RUnlock(); return s.epoch }
func (s *sessionRecord) Principal() Principal {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return copyPrincipal(s.principal)
}
func (s *sessionRecord) Context() context.Context { return s.ctx }
func (s *sessionRecord) Attachment() any          { s.mu.RLock(); defer s.mu.RUnlock(); return s.attachment }
func (s *sessionRecord) SetAttachment(value any)  { s.mu.Lock(); s.attachment = value; s.mu.Unlock() }

func (s *sessionRecord) Send(ctx context.Context, typ MessageType, payload []byte, options SendOptions) error {
	return s.send(ctx, typ, payload, options)
}
func (s *sessionRecord) TrySend(typ MessageType, payload []byte, options SendOptions) error {
	return s.send(context.Background(), typ, payload, options)
}
func (s *sessionRecord) send(ctx context.Context, typ MessageType, payload []byte, options SendOptions) error {
	if typ == 0 || options.Delivery > UnreliableSequenced {
		return ErrInvalidArgument
	}
	limit := s.outboundMessageLimit()
	if options.Delivery != ReliableOrdered {
		limit = s.outboundDatagramLimit()
	}
	if len(payload) > limit {
		return &Error{Code: TooLarge, MaxPayload: limit}
	}
	s.mu.RLock()
	state, operations := s.state, s.operations
	s.mu.RUnlock()
	if state == Suspended {
		return ErrSessionSuspended
	}
	if state != Active || operations == nil {
		return ErrSessionClosed
	}
	// Admitted transport operations copy or frame payload while this call is in
	// flight. Keeping the caller's slice here lets that layer reserve its
	// application-budget charge before allocating the encoded outgoing frame.
	return operations.send(ctx, typ, payload, options)
}
func (s *sessionRecord) Call(ctx context.Context, typ MessageType, payload []byte) ([]byte, error) {
	if typ == 0 || len(payload) > s.outboundMessageLimit() {
		return nil, ErrInvalidArgument
	}
	s.mu.RLock()
	state, operations := s.state, s.operations
	s.mu.RUnlock()
	if state == Suspended {
		return nil, ErrSessionSuspended
	}
	if state != Active || operations == nil {
		return nil, ErrSessionClosed
	}
	result, err := operations.call(ctx, typ, payload)
	return append([]byte(nil), result...), err
}

func (s *sessionRecord) outboundMessageLimit() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return minimumAdvertisedLimit(s.limits.MessageBytes, s.peerLimits.MessageBytes)
}
func (s *sessionRecord) outboundDatagramLimit() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return minimumAdvertisedLimit(s.limits.DatagramBytes, s.peerLimits.DatagramBytes)
}
func (s *sessionRecord) peerLimitsSnapshot() limitMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peerLimits
}

func minimumAdvertisedLimit(local, peer int) int {
	if peer <= 0 {
		return local
	}
	return min(local, peer)
}
func (s *sessionRecord) OpenStream(ctx context.Context, typ MessageType) (Stream, error) {
	if typ == 0 {
		return nil, ErrInvalidArgument
	}
	s.mu.RLock()
	state, operations := s.state, s.operations
	s.mu.RUnlock()
	if state == Suspended {
		return nil, ErrSessionSuspended
	}
	if state != Active || operations == nil {
		return nil, ErrSessionClosed
	}
	return operations.openStream(ctx, typ)
}
func (s *sessionRecord) RefreshAuth(ctx context.Context, credential Credential) error {
	s.mu.RLock()
	state, clientSide, operations := s.state, s.clientSide, s.operations
	s.mu.RUnlock()
	if !clientSide || state != Active || operations == nil {
		return ErrWrongMode
	}
	return operations.refresh(ctx, Credential{Scheme: credential.Scheme, Data: append([]byte(nil), credential.Data...)})
}
func (s *sessionRecord) Close(ctx context.Context, code Code) error {
	if code == SessionSuperseded {
		return ErrInvalidArgument
	}
	s.mu.Lock()
	if s.state == Closed {
		s.mu.Unlock()
		return ErrSessionClosed
	}
	s.state = Closed
	s.terminal = code
	operations := s.operations
	s.cancel()
	s.mu.Unlock()
	if operations == nil {
		return nil
	}
	return operations.close(ctx, code)
}

func (s *sessionRecord) terminalCode() Code {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.terminal
}

// closeRemote commits a peer-initiated logical close without trying to send a
// second close frame. The control reader acknowledges before calling it.
func (s *sessionRecord) closeRemote(code Code) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == Closed {
		return false
	}
	s.state = Closed
	s.terminal = code
	s.cancel()
	return true
}
func (s *sessionRecord) Stats() Stats {
	s.mu.RLock()
	state, operations := s.state, s.operations
	s.mu.RUnlock()
	if state != Active || operations == nil {
		return Stats{}
	}
	return operations.stats()
}

// setState and setPrincipal are endpoint-only transitions. They deliberately
// remain unexported so applications cannot forge lifecycle changes.
func (s *sessionRecord) setState(state State, epoch uint64) {
	s.mu.Lock()
	s.state, s.epoch = state, epoch
	if state == Closed {
		s.cancel()
	}
	s.mu.Unlock()
}
func (s *sessionRecord) setPrincipal(principal Principal) {
	s.mu.Lock()
	s.principal = copyPrincipal(principal)
	s.mu.Unlock()
}

// suspendIfCurrent detaches only the operation that still owns the active
// epoch. A late close from a superseded QUIC connection therefore cannot turn
// a newer attachment back into Suspended.
func (s *sessionRecord) suspendIfCurrent(operations *connectionOperations) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.operations.(*connectionOperations)
	if !ok || current != operations || s.state != Active {
		return false
	}
	s.state = Suspended
	operations.stopEpoch()
	return true
}

func (s *sessionRecord) isCurrent(operations *connectionOperations) bool {
	if s == nil || operations == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	current, ok := s.operations.(*connectionOperations)
	return ok && current == operations && s.state == Active && s.epoch == operations.epoch
}

func (s *sessionRecord) replaceConnection(operations *connectionOperations, principal Principal) (*connectionOperations, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == Closed {
		return nil, 0, ErrSessionExpired
	}
	if s.epoch == ^uint64(0) {
		return nil, 0, ErrResourceExhausted
	}
	previous, _ := s.operations.(*connectionOperations)
	s.epoch++
	operations.epoch = s.epoch
	s.state = Active
	s.principal = copyPrincipal(principal)
	s.operations = operations
	return previous, s.epoch, nil
}

// replaceConnectionAt installs an owner-issued epoch observed in a resumed
// WELCOME. It permits a gap when a prior resume committed at the owner but
// its WELCOME was lost before this SDK could observe that epoch.
func (s *sessionRecord) replaceConnectionAt(operations *connectionOperations, principal Principal, epoch uint64) (*connectionOperations, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == Closed {
		return nil, ErrSessionExpired
	}
	if epoch <= s.epoch || epoch == 0 {
		return nil, ErrProtocolViolation
	}
	previous, _ := s.operations.(*connectionOperations)
	s.epoch = epoch
	operations.epoch = epoch
	s.state = Active
	s.principal = copyPrincipal(principal)
	s.operations = operations
	return previous, nil
}
