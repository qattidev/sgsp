package memory

import (
	"context"
	"sync"
	"testing"

	"qattidev/sgsp"
	"qattidev/sgsp/placement"
)

func placementOwner(id byte) sgsp.Owner {
	return sgsp.Owner{ID: string(rune('a' + id)), Incarnation: sgsp.Incarnation{id}, Endpoint: sgsp.Endpoint{Address: "127.0.0.1:4444", ServerName: "owner"}}
}
func placementGroup(version string) placement.GroupID {
	return placement.GroupID{App: sgsp.AppIdentity{ID: "app", Version: version}, Key: "match"}
}

func TestConcurrentAssignment(t *testing.T) {
	store := NewStore()
	group := placementGroup("1")
	owners := []sgsp.Owner{placementOwner(0), placementOwner(1), placementOwner(2)}
	const attempts = 100
	results := make(chan placement.Assignment, attempts)
	errors := make(chan error, attempts)
	var workers sync.WaitGroup
	for attempt := 0; attempt < attempts; attempt++ {
		workers.Add(1)
		go func(candidate sgsp.Owner) {
			defer workers.Done()
			assignment, err := store.Assign(context.Background(), group, candidate)
			if err != nil {
				errors <- err
				return
			}
			results <- assignment
		}(owners[attempt%len(owners)])
	}
	workers.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	var winner sgsp.Owner
	for assignment := range results {
		if winner == (sgsp.Owner{}) {
			winner = assignment.Owner
		}
		if assignment.Owner != winner || assignment.Closed {
			t.Fatalf("assignment winner diverged: %#v, want %#v", assignment, winner)
		}
	}
	stored, err := store.Get(context.Background(), group)
	if err != nil || stored.Owner != winner {
		t.Fatalf("stored assignment = %#v, %v", stored, err)
	}
}

func TestAssignmentVersionAndClose(t *testing.T) {
	store := NewStore()
	group, owner := placementGroup("1"), placementOwner(1)
	if _, err := store.Assign(context.Background(), group, owner); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Assign(context.Background(), placementGroup("2"), owner); err != sgsp.ErrUnsupportedVersion {
		t.Fatalf("version conflict = %v", err)
	}
	if err := store.Close(context.Background(), group, placementOwner(2)); err != sgsp.ErrForbidden {
		t.Fatalf("wrong owner close = %v", err)
	}
	if err := store.Close(context.Background(), group, owner); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(context.Background(), group, owner); err != nil {
		t.Fatalf("idempotent close = %v", err)
	}
	assignment, err := store.Get(context.Background(), group)
	if err != nil || !assignment.Closed {
		t.Fatalf("closed assignment = %#v, %v", assignment, err)
	}
	if _, err := store.Assign(context.Background(), group, owner); err != sgsp.ErrGroupClosed {
		t.Fatalf("reopen = %v", err)
	}
}
