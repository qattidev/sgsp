package placement_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"qattidev/sgsp"
	"qattidev/sgsp/placement"
	"qattidev/sgsp/placement/memory"
)

type recordingSigner struct{ admissions []sgsp.Admission }

func (s *recordingSigner) Sign(_ context.Context, admission sgsp.Admission) (string, error) {
	s.admissions = append(s.admissions, admission)
	return fmt.Sprintf("ticket-%d", len(s.admissions)), nil
}

type recordingObserver struct{ values chan sgsp.Observation }

func (o *recordingObserver) Observe(observation sgsp.Observation) { o.values <- observation }

type unavailableStore struct{ err error }

func (s unavailableStore) Assign(context.Context, placement.GroupID, sgsp.Owner) (placement.Assignment, error) {
	return placement.Assignment{}, s.err
}
func (s unavailableStore) Get(context.Context, placement.GroupID) (placement.Assignment, error) {
	return placement.Assignment{}, s.err
}
func (s unavailableStore) Close(context.Context, placement.GroupID, sgsp.Owner) error { return s.err }

func bootstrapOwner(id byte) sgsp.Owner {
	return sgsp.Owner{ID: string(rune('a' + id)), Incarnation: sgsp.Incarnation{id}, Endpoint: sgsp.Endpoint{Address: "127.0.0.1:4444", ServerName: "owner"}}
}

func TestBootstrapResolve(t *testing.T) {
	registry, store, signer := memory.NewRegistry(), memory.NewStore(), &recordingSigner{}
	first, second := bootstrapOwner(0), bootstrapOwner(1)
	for _, owner := range []sgsp.Owner{first, second} {
		if err := registry.Register(context.Background(), owner); err != nil {
			t.Fatal(err)
		}
	}
	bootstrap, err := placement.NewBootstrap(placement.BootstrapConfig{App: sgsp.AppIdentity{ID: "app", Version: "1"}, Store: store, Registry: registry, Signer: signer})
	if err != nil {
		t.Fatal(err)
	}
	principal := sgsp.Principal{Issuer: "identity", Subject: "player", ExpiresAt: time.Now().Add(time.Minute)}
	firstPlacement, err := bootstrap.Resolve(context.Background(), principal, "match")
	if err != nil {
		t.Fatal(err)
	}
	secondPlacement, err := bootstrap.Resolve(context.Background(), principal, "match")
	if err != nil {
		t.Fatal(err)
	}
	if firstPlacement.Owner != secondPlacement.Owner || firstPlacement.GroupKey != "match" || firstPlacement.AdmissionTicket == "" {
		t.Fatalf("placements = %#v / %#v", firstPlacement, secondPlacement)
	}
	if len(signer.admissions) != 2 {
		t.Fatalf("signed admissions = %d", len(signer.admissions))
	}
	for _, admission := range signer.admissions {
		if admission.App != (sgsp.AppIdentity{ID: "app", Version: "1"}) || admission.PrincipalIssuer != "identity" || admission.Subject != "player" || admission.GroupKey != "match" || admission.Owner != firstPlacement.Owner || !admission.ExpiresAt.After(time.Now()) {
			t.Fatalf("bad admission = %#v", admission)
		}
	}
	if err := registry.SetStatus(context.Background(), firstPlacement.Owner, false, false, true); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrap.Resolve(context.Background(), principal, "match"); err != sgsp.ErrServerUnavailable {
		t.Fatalf("unhealthy winner = %v", err)
	}
}

func TestUnhealthyOwner(t *testing.T) {
	registry, store, signer := memory.NewRegistry(), memory.NewStore(), &recordingSigner{}
	first, second := bootstrapOwner(0), bootstrapOwner(1)
	if err := registry.Register(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	bootstrap, err := placement.NewBootstrap(placement.BootstrapConfig{
		App:      sgsp.AppIdentity{ID: "app", Version: "1"},
		Store:    store,
		Registry: registry,
		Signer:   signer,
	})
	if err != nil {
		t.Fatal(err)
	}
	principal := sgsp.Principal{Issuer: "identity", Subject: "player", ExpiresAt: time.Now().Add(time.Minute)}
	assigned, err := bootstrap.Resolve(context.Background(), principal, "owned-match")
	if err != nil {
		t.Fatal(err)
	}
	if assigned.Owner != first {
		t.Fatalf("initial owner = %#v, want %#v", assigned.Owner, first)
	}
	if err := registry.SetStatus(context.Background(), first, false, false, true); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrap.Resolve(context.Background(), principal, "owned-match"); err != sgsp.ErrServerUnavailable {
		t.Fatalf("unhealthy assigned owner resolution = %v, want ServerUnavailable", err)
	}
	newPlacement, err := bootstrap.Resolve(context.Background(), principal, "new-match")
	if err != nil {
		t.Fatal(err)
	}
	if newPlacement.Owner != second {
		t.Fatalf("new group owner = %#v, want healthy second owner %#v", newPlacement.Owner, second)
	}
}

func TestPlacementOutage(t *testing.T) {
	registry, signer := memory.NewRegistry(), &recordingSigner{}
	owner := bootstrapOwner(0)
	if err := registry.Register(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	bootstrap, err := placement.NewBootstrap(placement.BootstrapConfig{
		App:      sgsp.AppIdentity{ID: "app", Version: "1"},
		Store:    unavailableStore{err: context.DeadlineExceeded},
		Registry: registry,
		Signer:   signer,
	})
	if err != nil {
		t.Fatal(err)
	}
	principal := sgsp.Principal{Issuer: "identity", Subject: "player", ExpiresAt: time.Now().Add(time.Minute)}
	if _, err := bootstrap.Resolve(context.Background(), principal, "match"); err != context.DeadlineExceeded {
		t.Fatalf("group placement during store outage = %v, want explicit store error", err)
	}
	// Ungrouped resolution is a selector-only route and must not touch the
	// durable group store. It is analogous to a direct owner route that remains
	// usable while bootstrap storage is unavailable.
	placement, err := bootstrap.Resolve(context.Background(), principal, "")
	if err != nil {
		t.Fatalf("ungrouped placement during store outage = %v", err)
	}
	if placement.Owner != owner || placement.AdmissionTicket == "" {
		t.Fatalf("ungrouped placement = %#v", placement)
	}
}

func TestBootstrapPlacementObservations(t *testing.T) {
	registry, store, signer := memory.NewRegistry(), memory.NewStore(), &recordingSigner{}
	owner := bootstrapOwner(0)
	if err := registry.Register(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{values: make(chan sgsp.Observation, 32)}
	bootstrap, err := placement.NewBootstrap(placement.BootstrapConfig{
		App:      sgsp.AppIdentity{ID: "app", Version: "1"},
		Store:    store,
		Registry: registry,
		Signer:   signer,
		Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bootstrap.Close()
	principal := sgsp.Principal{Issuer: "identity", Subject: "player", ExpiresAt: time.Now().Add(time.Minute)}
	if _, err := bootstrap.Resolve(context.Background(), principal, "match"); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.CloseGroup(context.Background(), "match", owner); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"resolve": false, "registry": false, "read": false, "assign": false, "ticket": false, "close": false}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		complete := true
		for _, observed := range want {
			complete = complete && observed
		}
		if complete {
			return
		}
		select {
		case observation := <-observer.values:
			if observation.Name == "placement" && observation.Code == sgsp.Normal {
				if _, ok := want[observation.Kind]; ok {
					want[observation.Kind] = true
				}
			}
		case <-deadline.C:
			t.Fatalf("placement observations = %#v", want)
		}
	}
}
