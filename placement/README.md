# Placement and external persistence

`placement` is the optional group-to-owner bootstrap layer above SGSP's public
API. It selects an owner, persists a group's assignment through an injected
store, checks the owner's current availability, and signs an admission ticket.
The protocol does not need to know which database stores assignments.

## Repository boundary

Keep `placement`, its interfaces and bootstrap behavior, and the development
`placement/memory` implementation in this repository. Implement durable stores
in a separate application or adapter repository that imports
`qattidev/sgsp/placement`. SQLite, PostgreSQL, and other backends should each own
their drivers, queries, migrations, connection settings, retry policy, and
database integration tests there. No backend switch or SQL abstraction needs
to be added to SGSP.

The dependency direction should be:

```text
application / bootstrap service
  -> external persistence adapter -> sgsp/placement -> sgsp
```

The application constructs the concrete adapter and passes it to
`BootstrapConfig.Store`. SGSP must not import the adapter. A nested Go module alone would isolate dependencies but still track database
implementation in this repository. The submodules below instead own their
source and history in separate Git repositories.

## Adapter submodules

Durable implementations are separate Git repositories and Go modules mounted
as submodules in this checkout:

| Backend | Checkout | Module |
| --- | --- | --- |
| PostgreSQL | [`adapters/postgres`](../adapters/postgres/README.md) | `qattidev/sgsp-postgres` |
| SQLite | [`adapters/sqlite`](../adapters/sqlite/README.md) | `qattidev/sgsp-sqlite` |

Both own their SQLC queries/generated code, embedded Goose migrations, database
configuration, and integration tests. SGSP's module has no adapter dependencies.
The memory adapter remains under `placement/memory` for development and tests.
The old `qattidev/sgsp/placement/postgres` import is removed; applications must
import `qattidev/sgsp-postgres` instead. Existing PostgreSQL schema adoption is
documented in that adapter's README.

See [`adapters/README.md`](../adapters/README.md) for local Git setup and commands.

## Store contract

An external adapter implements the existing interface from `placement.go`:

```go
type AssignmentStore interface {
    Assign(context.Context, GroupID, sgsp.Owner) (Assignment, error)
    Get(context.Context, GroupID) (Assignment, error)
    Close(context.Context, GroupID, sgsp.Owner) error
}
```

Preserve these semantics across every backend:

| Operation | Required behavior |
| --- | --- |
| Identity | Key assignments by `(GroupID.App.ID, GroupID.Key)`. Store the application version as an invariant, not another key component. |
| `Assign` | Atomically create an assignment if absent. Concurrent callers must return the same persisted winner, even when they propose different owners. Never overwrite an existing owner. |
| `Get` | Return the persisted assignment, including `Closed`. Return `placement.ErrAssignmentNotFound` only when the assignment is absent. |
| Version check | Return `sgsp.ErrUnsupportedVersion` when an existing assignment's version differs from the requested version. |
| Closed assignment | `Assign` returns `sgsp.ErrGroupClosed`; `Get` returns the closed record so bootstrap can reject it. Never reopen it implicitly. |
| `Close` | Atomically check the version and expected owner ID **and incarnation**, then persist closure. A different owner returns `sgsp.ErrForbidden`; a missing assignment returns `placement.ErrAssignmentNotFound`. Repeated closure by the same owner succeeds. |
| Validation | Reject empty application ID, version, group key, or required owner fields with `sgsp.ErrInvalidArgument`, consistently with the memory implementation. |
| Failure | Respect context cancellation and deadlines. Propagate storage failures; never disguise an outage as a missing record or fall back to a local memory store. |

Persist the full assignment: application identity/version, group key, owner ID,
16-byte incarnation, endpoint address and server name, and closed state. A
successful mutation must be committed before returning. Concurrency guarantees
must hold across independent adapter instances and processes, not just callers
sharing a Go mutex.

Closed records prevent group keys from silently being reused. Any retention,
deletion, or recovery policy belongs to the application and must preserve that
invariant. An unavailable owner does not authorize reassigning its group:
bootstrap returns an availability error. Owner restart and group recovery need
an explicit application policy beyond this interface.

## Application wiring

The host can accept interfaces without importing any database package:

```go
func newPlacement(
    app sgsp.AppIdentity,
    store placement.AssignmentStore,
    registry placement.Registry,
    signer placement.AdmissionSigner,
    authorize sgsp.GroupAuthorizer,
) (*placement.Bootstrap, error) {
    return placement.NewBootstrap(placement.BootstrapConfig{
        App:            app,
        Store:          store,
        Registry:       registry,
        Signer:         signer,
        AuthorizeGroup: authorize,
    })
}
```

In the external adapter package, add a compile-time assertion:

```go
var _ placement.AssignmentStore = (*Store)(nil)
```

The application opens storage, explicitly applies migrations, constructs the
adapter, and owns shutdown. `Bootstrap.Close()` stops its observation worker;
it does not close the store, registry, or signer. Register bootstrap requests
with `bootstrap.Register(router)`.

For owner-side durable group closure, wire `ServerConfig.CommitGroupClose`
to the store, preserving the callback's application identity:

```go
serverConfig.CommitGroupClose = func(
    ctx context.Context, app sgsp.AppIdentity, key string, owner sgsp.Owner,
) error {
    return store.Close(ctx, placement.GroupID{App: app, Key: key}, owner)
}
```

`bootstrap.CloseGroup(ctx, key, owner)` is also available when the application
identity is already fixed by that bootstrap instance. It needs a wrapper for
`CommitGroupClose`, whose signature also includes the application identity.

`Registry` is a separate extension point: it supplies current owner health,
draining, and capacity status. Persisting assignments does not implement owner
discovery or heartbeats. A host may implement registry storage externally as
well. Ungrouped resolution bypasses `AssignmentStore` entirely.

## Verification boundary

Run SGSP tests with `go test ./...`. Nested adapter modules are intentionally
excluded from that command. Run their tests separately:

```sh
go -C adapters/sqlite test -race ./...
SGSP_TEST_DATABASE_URL=... go -C adapters/postgres test -race ./...
```

Each adapter owns contract tests for competing assignments, version conflicts,
missing records, idempotent closure, stale owners, assignment/closure races,
cancellation, outages, persistence after reopening, and two bootstrap instances.
Database migrations and SQLC generation are checked in the adapter repositories.
Historical milestone evidence retains the original paths as history.

No SGSP wire-format change is required. New backends plug into the existing
store contract; only the host's construction and deployment configuration change.
