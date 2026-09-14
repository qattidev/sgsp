package postgresintegration

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"qattidev/sgsp"
	"qattidev/sgsp/placement"
	adapter "qattidev/sgsp/placement/postgres"
)

// integrationDB creates and later removes only a unique schema owned by this
// test run. A missing URL intentionally fails rather than passing a skipped
// test: the architecture treats a missing PostgreSQL service as incomplete
// evidence, not successful adapter verification.
func integrationDB(t *testing.T) *sql.DB {
	t.Helper()
	url := os.Getenv("SGSP_TEST_DATABASE_URL")
	if url == "" {
		t.Fatal("SGSP_TEST_DATABASE_URL is required for PostgreSQL integration; this is an incomplete check")
	}
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	schema := "sgsp_it_" + hex.EncodeToString(suffix)
	admin, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(`CREATE SCHEMA ` + quoteIdentifier(schema)); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = admin.Close()
		cleanup, err := sql.Open("pgx", url)
		if err == nil {
			_, _ = cleanup.Exec(`DROP SCHEMA IF EXISTS ` + quoteIdentifier(schema) + ` CASCADE`)
			_ = cleanup.Close()
		}
	})
	config, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	if config.RuntimeParams == nil {
		config.RuntimeParams = make(map[string]string)
	}
	// stdlib.OpenDB creates every pooled connection from this config, so the
	// search path is not accidentally limited to a one-off DB.Exec call.
	config.RuntimeParams["search_path"] = quoteIdentifier(schema)
	db := stdlib.OpenDB(*config)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if err := adapter.ApplyMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := adapter.ApplyMigrations(ctx, db); err != nil {
		t.Fatalf("migration reapplication = %v", err)
	}
	return db
}

func quoteIdentifier(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }

func TestConcurrentAssignment(t *testing.T) {
	db := integrationDB(t)
	store, err := adapter.New(db)
	if err != nil {
		t.Fatal(err)
	}
	group := placement.GroupID{App: sgsp.AppIdentity{ID: "integration", Version: "1"}, Key: "race"}
	owners := []sgsp.Owner{
		{ID: "a", Incarnation: sgsp.Incarnation{1}, Endpoint: sgsp.Endpoint{Address: "127.0.0.1:1", ServerName: "a"}},
		{ID: "b", Incarnation: sgsp.Incarnation{2}, Endpoint: sgsp.Endpoint{Address: "127.0.0.1:2", ServerName: "b"}},
		{ID: "c", Incarnation: sgsp.Incarnation{3}, Endpoint: sgsp.Endpoint{Address: "127.0.0.1:3", ServerName: "c"}},
	}
	results := make(chan placement.Assignment, 100)
	errs := make(chan error, 100)
	var groupWait sync.WaitGroup
	for index := 0; index < 100; index++ {
		groupWait.Add(1)
		go func(index int) {
			defer groupWait.Done()
			assignment, err := store.Assign(context.Background(), group, owners[index%len(owners)])
			if err != nil {
				errs <- err
				return
			}
			results <- assignment
		}(index)
	}
	groupWait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var winner sgsp.Owner
	for assignment := range results {
		if winner.ID == "" {
			winner = assignment.Owner
		}
		if assignment.Owner != winner {
			t.Fatalf("winner changed: %#v != %#v", assignment.Owner, winner)
		}
	}
	if winner.ID == "" {
		t.Fatal("no assignment winner")
	}
}

func TestAssignmentVersionAndClose(t *testing.T) {
	db := integrationDB(t)
	store, err := adapter.New(db)
	if err != nil {
		t.Fatal(err)
	}
	group := placement.GroupID{App: sgsp.AppIdentity{ID: "integration", Version: "1"}, Key: "close"}
	owner := sgsp.Owner{ID: "owner", Incarnation: sgsp.Incarnation{1}, Endpoint: sgsp.Endpoint{Address: "127.0.0.1:1", ServerName: "owner"}}
	if _, err := store.Assign(context.Background(), group, owner); err != nil {
		t.Fatal(err)
	}
	wrongVersion := group
	wrongVersion.App.Version = "2"
	if _, err := store.Assign(context.Background(), wrongVersion, owner); !errors.Is(err, sgsp.ErrUnsupportedVersion) {
		t.Fatalf("version conflict = %v", err)
	}
	wrongOwner := owner
	wrongOwner.Incarnation[0]++
	if err := store.Close(context.Background(), group, wrongOwner); !errors.Is(err, sgsp.ErrForbidden) {
		t.Fatalf("wrong owner close = %v", err)
	}
	if err := store.Close(context.Background(), group, owner); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(context.Background(), group, owner); err != nil {
		t.Fatalf("idempotent close = %v", err)
	}
	if _, err := store.Assign(context.Background(), group, owner); !errors.Is(err, sgsp.ErrGroupClosed) {
		t.Fatalf("closed assignment reuse = %v", err)
	}
}

func Example() {
	fmt.Println("go -C integration/postgres test -race -count=20 -v ./...")
	// Output: go -C integration/postgres test -race -count=20 -v ./...
}
