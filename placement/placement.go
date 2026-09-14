// Package placement implements optional group-to-owner placement and the
// bootstrap request exchange. It deliberately depends only on SGSP's public
// API so games may use it with their own registry and durable store.
package placement

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"qattidev/sgsp"
)

var ErrAssignmentNotFound = errors.New("sgsp placement: assignment not found")

type GroupID struct {
	App sgsp.AppIdentity
	Key string
}
type Assignment struct {
	Group  GroupID
	Owner  sgsp.Owner
	Closed bool
}
type OwnerStatus struct {
	Owner                          sgsp.Owner
	Healthy, Draining, HasCapacity bool
}
type AssignmentStore interface {
	Assign(context.Context, GroupID, sgsp.Owner) (Assignment, error)
	Get(context.Context, GroupID) (Assignment, error)
	Close(context.Context, GroupID, sgsp.Owner) error
}
type Registry interface {
	Snapshot(context.Context) ([]OwnerStatus, error)
}
type Selector interface {
	SelectOwner(context.Context, sgsp.Principal, GroupID, []OwnerStatus) (sgsp.Owner, error)
}
type SelectorFunc func(context.Context, sgsp.Principal, GroupID, []OwnerStatus) (sgsp.Owner, error)

func (f SelectorFunc) SelectOwner(ctx context.Context, principal sgsp.Principal, group GroupID, candidates []OwnerStatus) (sgsp.Owner, error) {
	return f(ctx, principal, group, candidates)
}

type AdmissionSigner interface {
	Sign(context.Context, sgsp.Admission) (string, error)
}
type Placement struct {
	Owner                     sgsp.Owner
	GroupKey, AdmissionTicket string
	ExpiresAt                 time.Time
}
type BootstrapConfig struct {
	App            sgsp.AppIdentity
	Store          AssignmentStore
	Registry       Registry
	Selector       Selector
	Signer         AdmissionSigner
	AuthorizeGroup sgsp.GroupAuthorizer
}
type Bootstrap struct{ config BootstrapConfig }

func NewBootstrap(config BootstrapConfig) (*Bootstrap, error) {
	if config.App.ID == "" || config.App.Version == "" || config.Registry == nil || config.Signer == nil {
		return nil, sgsp.ErrInvalidArgument
	}
	return &Bootstrap{config: config}, nil
}

// Resolve picks or reads the durable group assignment, verifies that its
// owner is currently admissible, and creates a short-lived owner-bound ticket.
func (b *Bootstrap) Resolve(ctx context.Context, principal sgsp.Principal, groupKey string) (Placement, error) {
	if b == nil || b.config.Registry == nil || b.config.Signer == nil || principal.Issuer == "" || principal.Subject == "" || !principal.ExpiresAt.After(time.Now()) {
		return Placement{}, sgsp.ErrInvalidArgument
	}
	if groupKey != "" && b.config.AuthorizeGroup != nil {
		if err := b.config.AuthorizeGroup(ctx, principal, groupKey); err != nil {
			return Placement{}, err
		}
	}
	statuses, err := b.config.Registry.Snapshot(ctx)
	if err != nil {
		return Placement{}, err
	}
	group := GroupID{App: b.config.App, Key: groupKey}
	var owner sgsp.Owner
	if groupKey != "" {
		if b.config.Store == nil {
			return Placement{}, sgsp.ErrInvalidArgument
		}
		assignment, err := b.config.Store.Get(ctx, group)
		if errors.Is(err, ErrAssignmentNotFound) {
			candidate, selectErr := selectOwner(ctx, principal, group, statuses, b.config.Selector)
			if selectErr != nil {
				return Placement{}, selectErr
			}
			assignment, err = b.config.Store.Assign(ctx, group, candidate)
		}
		if err != nil {
			return Placement{}, err
		}
		if assignment.Closed {
			return Placement{}, sgsp.ErrGroupClosed
		}
		owner = assignment.Owner
	} else {
		selectionKey := make([]byte, 16)
		if _, err := rand.Read(selectionKey); err != nil {
			return Placement{}, err
		}
		group.Key = string(selectionKey)
		owner, err = selectOwner(ctx, principal, group, statuses, b.config.Selector)
		if err != nil {
			return Placement{}, err
		}
	}
	status, ok := matchingStatus(owner, statuses)
	if !ok || !status.Healthy || !status.HasCapacity {
		return Placement{}, sgsp.ErrServerUnavailable
	}
	if status.Draining {
		return Placement{}, sgsp.ErrServerDraining
	}
	expiresAt := time.Now().Add(30 * time.Second)
	if principal.ExpiresAt.Before(expiresAt) {
		expiresAt = principal.ExpiresAt
	}
	if !expiresAt.After(time.Now()) {
		return Placement{}, sgsp.ErrUnauthenticated
	}
	ticket, err := b.config.Signer.Sign(ctx, sgsp.Admission{App: b.config.App, PrincipalIssuer: principal.Issuer, Subject: principal.Subject, GroupKey: groupKey, Owner: owner, ExpiresAt: expiresAt})
	if err != nil {
		return Placement{}, err
	}
	return Placement{Owner: owner, GroupKey: groupKey, AdmissionTicket: ticket, ExpiresAt: expiresAt}, nil
}

func selectOwner(ctx context.Context, principal sgsp.Principal, group GroupID, candidates []OwnerStatus, selector Selector) (sgsp.Owner, error) {
	if selector != nil {
		return selector.SelectOwner(ctx, principal, group, candidates)
	}
	eligible := make([]OwnerStatus, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Healthy && !candidate.Draining && candidate.HasCapacity {
			eligible = append(eligible, candidate)
		}
	}
	if len(eligible) == 0 {
		return sgsp.Owner{}, sgsp.ErrServerUnavailable
	}
	sort.Slice(eligible, func(i, j int) bool {
		left, right := rendezvous(group, eligible[i].Owner), rendezvous(group, eligible[j].Owner)
		if left != right {
			return string(left[:]) > string(right[:])
		}
		if eligible[i].Owner.ID != eligible[j].Owner.ID {
			return eligible[i].Owner.ID < eligible[j].Owner.ID
		}
		return string(eligible[i].Owner.Incarnation[:]) < string(eligible[j].Owner.Incarnation[:])
	})
	return eligible[0].Owner, nil
}
func rendezvous(group GroupID, owner sgsp.Owner) [32]byte {
	buf := make([]byte, 0, len(group.App.ID)+len(group.Key)+len(owner.ID)+32)
	for _, value := range []string{group.App.ID, group.Key, owner.ID} {
		buf = appendQUICVarint(buf, uint64(len(value)))
		buf = append(buf, value...)
	}
	buf = append(buf, owner.Incarnation[:]...)
	return sha256.Sum256(buf)
}
func appendQUICVarint(dst []byte, value uint64) []byte {
	switch {
	case value <= 63:
		return append(dst, byte(value))
	case value <= 16383:
		var encoded [2]byte
		binary.BigEndian.PutUint16(encoded[:], uint16(value)|0x4000)
		return append(dst, encoded[:]...)
	case value <= 1073741823:
		var encoded [4]byte
		binary.BigEndian.PutUint32(encoded[:], uint32(value)|0x80000000)
		return append(dst, encoded[:]...)
	default:
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], value|0xc000000000000000)
		return append(dst, encoded[:]...)
	}
}
func matchingStatus(owner sgsp.Owner, statuses []OwnerStatus) (OwnerStatus, bool) {
	for _, status := range statuses {
		if status.Owner.ID == owner.ID && status.Owner.Incarnation == owner.Incarnation {
			return status, true
		}
	}
	return OwnerStatus{}, false
}

// Register installs the type-1 bootstrap Resolve request on a router.
func (b *Bootstrap) Register(router *sgsp.Router) error {
	if b == nil || router == nil {
		return sgsp.ErrInvalidArgument
	}
	return router.OnRequest(1, func(ctx context.Context, incoming *sgsp.Incoming) {
		var request resolveRequest
		if json.Unmarshal(incoming.Payload, &request) != nil {
			_ = incoming.Fail(ctx, sgsp.InvalidArgument, "invalid resolve request")
			return
		}
		placement, err := b.Resolve(ctx, incoming.Session.Principal(), request.Group)
		if err != nil {
			_ = incoming.Fail(ctx, errorCode(err), "placement unavailable")
			return
		}
		payload, err := json.Marshal(resolveReply{Owner: ownerWireFrom(placement.Owner), Group: placement.GroupKey, Admission: placement.AdmissionTicket, ExpiresMS: placement.ExpiresAt.UnixMilli()})
		if err != nil {
			_ = incoming.Fail(ctx, sgsp.Internal, "encode placement")
			return
		}
		_ = incoming.Reply(ctx, payload)
	})
}

type ResolveConfig struct {
	App         sgsp.AppIdentity
	TLS         *tls.Config
	Credentials sgsp.CredentialProvider
	GroupKey    string
}

func Resolve(ctx context.Context, endpoint sgsp.Endpoint, config ResolveConfig) (Placement, error) {
	if config.App.ID == "" || config.App.Version == "" || config.TLS == nil || config.Credentials == nil {
		return Placement{}, sgsp.ErrInvalidArgument
	}
	client, err := sgsp.Dial(ctx, endpoint, sgsp.ClientConfig{Role: sgsp.BootstrapRole, TLS: config.TLS, App: config.App, Credentials: config.Credentials, DisableReconnect: true, Dispatch: sgsp.DispatchConfig{Mode: sgsp.Handlers, Router: sgsp.NewRouter()}})
	if err != nil {
		return Placement{}, err
	}
	defer client.Close(context.Background())
	payload, err := json.Marshal(resolveRequest{Group: config.GroupKey})
	if err != nil {
		return Placement{}, err
	}
	reply, err := client.Session().Call(ctx, 1, payload)
	if err != nil {
		return Placement{}, err
	}
	var decoded resolveReply
	if err := json.Unmarshal(reply, &decoded); err != nil {
		return Placement{}, err
	}
	owner, err := decoded.Owner.toOwner()
	if err != nil || decoded.Admission == "" || decoded.ExpiresMS <= time.Now().UnixMilli() {
		return Placement{}, sgsp.ErrProtocolViolation
	}
	return Placement{Owner: owner, GroupKey: decoded.Group, AdmissionTicket: decoded.Admission, ExpiresAt: time.UnixMilli(decoded.ExpiresMS)}, nil
}

type resolveRequest struct {
	Group string `json:"group"`
}
type resolveReply struct {
	Owner     ownerWire `json:"owner"`
	Group     string    `json:"group"`
	Admission string    `json:"admission"`
	ExpiresMS int64     `json:"expires_unix_ms"`
}
type ownerWire struct {
	ID          string `json:"id"`
	Incarnation string `json:"incarnation"`
	Address     string `json:"address"`
	ServerName  string `json:"server_name"`
}

func ownerWireFrom(owner sgsp.Owner) ownerWire {
	return ownerWire{ID: owner.ID, Incarnation: hex.EncodeToString(owner.Incarnation[:]), Address: owner.Endpoint.Address, ServerName: owner.Endpoint.ServerName}
}
func (o ownerWire) toOwner() (sgsp.Owner, error) {
	var incarnation sgsp.Incarnation
	decoded, err := hex.DecodeString(o.Incarnation)
	if err != nil || len(decoded) != len(incarnation) || o.ID == "" || o.Address == "" || o.ServerName == "" {
		return sgsp.Owner{}, errors.New("invalid placement owner")
	}
	copy(incarnation[:], decoded)
	return sgsp.Owner{ID: o.ID, Incarnation: incarnation, Endpoint: sgsp.Endpoint{Address: o.Address, ServerName: o.ServerName}}, nil
}
func errorCode(err error) sgsp.Code {
	var protocol *sgsp.Error
	if errors.As(err, &protocol) {
		return protocol.Code
	}
	for _, candidate := range []struct {
		err  error
		code sgsp.Code
	}{{sgsp.ErrGroupClosed, sgsp.GroupClosed}, {sgsp.ErrServerUnavailable, sgsp.ServerUnavailable}, {sgsp.ErrServerDraining, sgsp.ServerDraining}, {sgsp.ErrUnsupportedVersion, sgsp.UnsupportedVersion}, {sgsp.ErrForbidden, sgsp.Forbidden}} {
		if errors.Is(err, candidate.err) {
			return candidate.code
		}
	}
	return sgsp.Internal
}
