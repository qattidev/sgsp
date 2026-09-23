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
	"qattidev/sgsp/placement/memory"
	adapter "qattidev/sgsp/placement/postgres"
)

type integrationSigner struct{}

func (integrationSigner) Sign(context.Context, sgsp.Admission) (string, error) {
	return "integration-ticket", nil
}

func integrationOwner(id byte) sgsp.Owner {
	return sgsp.Owner{
		ID:          string(rune('a' + id)),
		Incarnation: sgsp.Incarnation{id},
		Endpoint: sgsp.Endpoint{
			Address:    fmt.Sprintf("127.0.0.1:%d", 4100+int(id)),
			ServerName: fmt.Sprintf("owner-%d", id),
		},
	}
}

func integrationBootstrap(t *testing.T, app sgsp.AppIdentity, store placement.AssignmentStore, registry placement.Registry) *placement.Bootstrap {
	t.Helper()
	bootstrap, err := placement.NewBootstrap(placement.BootstrapConfig{
		App:      app,
		Store:    store,
		Registry: registry,
		Signer:   integrationSigner{},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bootstrap.Close)
	return bootstrap
}

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
	adminConfig, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal("SGSP_TEST_DATABASE_URL is invalid")
	}
	admin := stdlib.OpenDB(*adminConfig)
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
		t.Fatal("SGSP_TEST_DATABASE_URL is invalid")
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
	assertSchemaSearchPath(t, ctx, db, schema)
	// The probe temporarily holds four connections. Restore the driver's normal
	// unbounded test-pool limit before concurrent assignment coverage begins.
	db.SetMaxOpenConns(0)
	if err := adapter.ApplyMigrations(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := adapter.ApplyMigrations(ctx, db); err != nil {
		t.Fatalf("migration reapplication = %v", err)
	}
	var migrationSchema string
	if err := db.QueryRowContext(ctx, `SELECT n.nspname
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE c.oid = 'sgsp_schema_migrations'::regclass`).Scan(&migrationSchema); err != nil {
		t.Fatal(err)
	}
	if migrationSchema != schema {
		t.Fatalf("migration schema = %q, want disposable schema %q", migrationSchema, schema)
	}
	return db
}

func assertSchemaSearchPath(t *testing.T, ctx context.Context, db *sql.DB, schema string) {
	t.Helper()
	db.SetMaxOpenConns(4)
	connections := make([]*sql.Conn, 0, 4)
	defer func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}()
	for range 4 {
		connection, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, connection)
	}
	for _, connection := range connections {
		var currentSchema string
		if err := connection.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&currentSchema); err != nil {
			t.Fatal(err)
		}
		if currentSchema != schema {
			t.Fatalf("connection search_path selected schema %q, want %q", currentSchema, schema)
		}
	}
}

func quoteIdentifier(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }

func TestQuoteIdentifier(t *testing.T) {
	for _, test := range []struct {
		value string
		want  string
	}{
		{value: "plain", want: `"plain"`},
		{value: `has"quote`, want: `"has""quote"`},
		{value: "", want: `""`},
	} {
		if got := quoteIdentifier(test.value); got != test.want {
			t.Errorf("quoteIdentifier(%q) = %q, want %q", test.value, got, test.want)
		}
	}
}

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

func TestConcurrentBootstrapResolve(t *testing.T) {
	db := integrationDB(t)
	firstStore, err := adapter.New(db)
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := adapter.New(db)
	if err != nil {
		t.Fatal(err)
	}
	app := sgsp.AppIdentity{ID: "integration", Version: "1"}
	registry := memory.NewRegistry()
	for id := byte(0); id < 3; id++ {
		if err := registry.Register(context.Background(), integrationOwner(id)); err != nil {
			t.Fatal(err)
		}
	}
	first := integrationBootstrap(t, app, firstStore, registry)
	second := integrationBootstrap(t, app, secondStore, registry)
	principal := sgsp.Principal{Issuer: "integration", Subject: "player", ExpiresAt: time.Now().Add(time.Minute)}

	const attempts = 100
	start := make(chan struct{})
	placements := make(chan placement.Placement, attempts)
	errs := make(chan error, attempts)
	var ready, workers sync.WaitGroup
	for index := 0; index < attempts; index++ {
		ready.Add(1)
		workers.Add(1)
		bootstrap := first
		if index%2 == 1 {
			bootstrap = second
		}
		go func(bootstrap *placement.Bootstrap) {
			defer workers.Done()
			ready.Done()
			<-start
			resolved, err := bootstrap.Resolve(context.Background(), principal, "shared-winner")
			if err != nil {
				errs <- err
				return
			}
			placements <- resolved
		}(bootstrap)
	}
	ready.Wait()
	close(start)
	workers.Wait()
	close(placements)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var winner sgsp.Owner
	for resolved := range placements {
		if resolved.AdmissionTicket == "" {
			t.Fatal("bootstrap returned an empty admission ticket")
		}
		if winner.ID == "" {
			winner = resolved.Owner
		}
		if resolved.Owner != winner {
			t.Fatalf("bootstrap winner changed: %#v != %#v", resolved.Owner, winner)
		}
	}
	if winner.ID == "" {
		t.Fatal("no bootstrap assignment winner")
	}
	stored, err := firstStore.Get(context.Background(), placement.GroupID{App: app, Key: "shared-winner"})
	if err != nil {
		t.Fatal(err)
	}
	if stored.Closed || stored.Owner != winner {
		t.Fatalf("stored bootstrap assignment = %#v, want open winner %#v", stored, winner)
	}
}

func TestBootstrapCloseRace(t *testing.T) {
	db := integrationDB(t)
	firstStore, err := adapter.New(db)
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := adapter.New(db)
	if err != nil {
		t.Fatal(err)
	}
	app := sgsp.AppIdentity{ID: "integration", Version: "1"}
	registry := memory.NewRegistry()
	for id := byte(0); id < 3; id++ {
		if err := registry.Register(context.Background(), integrationOwner(id)); err != nil {
			t.Fatal(err)
		}
	}
	first := integrationBootstrap(t, app, firstStore, registry)
	second := integrationBootstrap(t, app, secondStore, registry)
	principal := sgsp.Principal{Issuer: "integration", Subject: "player", ExpiresAt: time.Now().Add(time.Minute)}
	assigned, err := first.Resolve(context.Background(), principal, "close-race")
	if err != nil {
		t.Fatal(err)
	}

	const attempts = 100
	start := make(chan struct{})
	errs := make(chan error, attempts)
	var ready, workers sync.WaitGroup
	for index := 0; index < attempts; index++ {
		ready.Add(1)
		workers.Add(1)
		bootstrap := first
		if index%2 == 1 {
			bootstrap = second
		}
		go func(bootstrap *placement.Bootstrap) {
			defer workers.Done()
			ready.Done()
			<-start
			errs <- bootstrap.CloseGroup(context.Background(), "close-race", assigned.Owner)
		}(bootstrap)
	}
	ready.Wait()
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := second.Resolve(context.Background(), principal, "close-race"); !errors.Is(err, sgsp.ErrGroupClosed) {
		t.Fatalf("closed group bootstrap resolve = %v, want ErrGroupClosed", err)
	}
	stored, err := firstStore.Get(context.Background(), placement.GroupID{App: app, Key: "close-race"})
	if err != nil {
		t.Fatal(err)
	}
	if !stored.Closed || stored.Owner != assigned.Owner {
		t.Fatalf("stored closed assignment = %#v, want closed owner %#v", stored, assigned.Owner)
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

func TestMigrationReapplicationAndDrift(t *testing.T) {
	db := integrationDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// integrationDB already proves that applying the embedded migrations twice
	// is a no-op. Mutate only this disposable schema's recorded checksum to
	// prove the runner rejects source/history drift instead of guessing a repair.
	result, err := db.ExecContext(ctx,
		`UPDATE sgsp_schema_migrations SET checksum=$1 WHERE version=$2`,
		[]byte{0}, "001_group_assignments")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("updated migration records = %d, want 1", rows)
	}
	if err := adapter.ApplyMigrations(ctx, db); !errors.Is(err, adapter.ErrMigrationDrift) {
		t.Fatalf("ApplyMigrations after checksum drift = %v, want ErrMigrationDrift", err)
	}
}

func Example() {
	fmt.Println("go -C integration/postgres test -race -count=20 -v ./...")
	// Output: go -C integration/postgres test -race -count=20 -v ./...
}
