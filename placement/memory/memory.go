// Package memory provides the development-only in-memory placement adapters.
// They deliberately make no restart-safety promise; production owners should
// use the PostgreSQL adapter or another durable AssignmentStore.
package memory

import (
	"context"
	"sync"

	"qattidev/sgsp"
	"qattidev/sgsp/placement"
)

const DefaultMaxAssignments = 100000

type assignmentKey struct{ appID, group string }
type Store struct {
	mu          sync.RWMutex
	max         int
	assignments map[assignmentKey]placement.Assignment
}

func NewStore(maxRecords ...int) *Store {
	max := DefaultMaxAssignments
	if len(maxRecords) > 0 {
		max = maxRecords[0]
	}
	if max < 1 {
		max = 1
	}
	return &Store{max: max, assignments: make(map[assignmentKey]placement.Assignment)}
}
func key(group placement.GroupID) assignmentKey {
	return assignmentKey{appID: group.App.ID, group: group.Key}
}
func validGroup(group placement.GroupID) bool {
	return group.App.ID != "" && group.App.Version != "" && group.Key != ""
}
func validOwner(owner sgsp.Owner) bool {
	return owner.ID != "" && owner.Endpoint.Address != "" && owner.Endpoint.ServerName != ""
}
func (s *Store) Assign(ctx context.Context, group placement.GroupID, candidate sgsp.Owner) (placement.Assignment, error) {
	if err := ctx.Err(); err != nil {
		return placement.Assignment{}, err
	}
	if s == nil || !validGroup(group) || !validOwner(candidate) {
		return placement.Assignment{}, sgsp.ErrInvalidArgument
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.assignments[key(group)]; ok {
		if existing.Group.App.Version != group.App.Version {
			return placement.Assignment{}, sgsp.ErrUnsupportedVersion
		}
		if existing.Closed {
			return placement.Assignment{}, sgsp.ErrGroupClosed
		}
		return existing, nil
	}
	if len(s.assignments) >= s.max {
		return placement.Assignment{}, sgsp.ErrResourceExhausted
	}
	assignment := placement.Assignment{Group: group, Owner: candidate}
	s.assignments[key(group)] = assignment
	return assignment, nil
}
func (s *Store) Get(ctx context.Context, group placement.GroupID) (placement.Assignment, error) {
	if err := ctx.Err(); err != nil {
		return placement.Assignment{}, err
	}
	if s == nil || !validGroup(group) {
		return placement.Assignment{}, sgsp.ErrInvalidArgument
	}
	s.mu.RLock()
	assignment, ok := s.assignments[key(group)]
	s.mu.RUnlock()
	if !ok {
		return placement.Assignment{}, placement.ErrAssignmentNotFound
	}
	if assignment.Group.App.Version != group.App.Version {
		return placement.Assignment{}, sgsp.ErrUnsupportedVersion
	}
	return assignment, nil
}
func (s *Store) Close(ctx context.Context, group placement.GroupID, expected sgsp.Owner) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || !validGroup(group) || !validOwner(expected) {
		return sgsp.ErrInvalidArgument
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	assignment, ok := s.assignments[key(group)]
	if !ok {
		return placement.ErrAssignmentNotFound
	}
	if assignment.Group.App.Version != group.App.Version {
		return sgsp.ErrUnsupportedVersion
	}
	if assignment.Owner.ID != expected.ID || assignment.Owner.Incarnation != expected.Incarnation {
		return sgsp.ErrForbidden
	}
	assignment.Closed = true
	s.assignments[key(group)] = assignment
	return nil
}

type Registry struct {
	mu     sync.RWMutex
	owners map[ownerKey]placement.OwnerStatus
}
type ownerKey struct {
	id          string
	incarnation sgsp.Incarnation
}

func NewRegistry() *Registry { return &Registry{owners: make(map[ownerKey]placement.OwnerStatus)} }
func registryKey(owner sgsp.Owner) ownerKey {
	return ownerKey{id: owner.ID, incarnation: owner.Incarnation}
}
func (r *Registry) Register(ctx context.Context, owner sgsp.Owner) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || !validOwner(owner) {
		return sgsp.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.owners[registryKey(owner)] = placement.OwnerStatus{Owner: owner, Healthy: true, HasCapacity: true}
	return nil
}
func (r *Registry) SetStatus(ctx context.Context, owner sgsp.Owner, healthy, draining, hasCapacity bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || !validOwner(owner) {
		return sgsp.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := registryKey(owner)
	if _, ok := r.owners[key]; !ok {
		return placement.ErrAssignmentNotFound
	}
	r.owners[key] = placement.OwnerStatus{Owner: owner, Healthy: healthy, Draining: draining, HasCapacity: hasCapacity}
	return nil
}
func (r *Registry) Remove(ctx context.Context, owner sgsp.Owner) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r == nil || !validOwner(owner) {
		return sgsp.ErrInvalidArgument
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.owners, registryKey(owner))
	return nil
}
func (r *Registry) Snapshot(ctx context.Context) ([]placement.OwnerStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil {
		return nil, sgsp.ErrInvalidArgument
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]placement.OwnerStatus, 0, len(r.owners))
	for _, status := range r.owners {
		result = append(result, status)
	}
	return result, nil
}
