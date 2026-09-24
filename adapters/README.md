# Durable placement adapters

`postgres/` and `sqlite/` are separate Git repositories registered as submodules,
with independent Go modules, SQLC generation, Goose migrations, and tests.
SGSP imports neither adapter. Its built-in memory store remains available.

## Local, unpublished repositories

Both submodule commits exist locally and have not been pushed. `.gitmodules`
and each adapter's `origin` use these repositories:

- SQLite: `git@github.com:qattidev/sgsp-adapters-sqlite.git`
- PostgreSQL: `git@github.com:qattidev/sgsp-adapters-postgres.git`

Push the adapter commits before publishing SGSP's submodule pointers so fresh
recursive clones can retrieve them. The current checkout needs no fetch.
Keep this checkout (including `.git/modules/`) to retain unpublished commits.

```sh
git -C adapters/sqlite push -u origin main
git -C adapters/postgres push -u origin main
```

Inspect changes and commits separately:

```sh
git submodule status
git -C adapters/postgres log -1 --stat
git -C adapters/sqlite log -1 --stat
```

After publication, normal setup is `git submodule update --init --recursive`.
Changes inside an adapter must be committed in that adapter; SGSP records the
resulting commit as its gitlink. SGSP's own extraction changes are left for review.

## Development and verification

The adapters' `go.mod` files replace `qattidev/sgsp` with `../..` for local
submodule development. A standalone adapter checkout must point that replacement
at an SGSP checkout. Before release, select a published SGSP module version and
remove the local replacement. Root `go test ./...` intentionally excludes the
nested modules, and consumers need only their chosen adapter.

```sh
go test ./...
go -C adapters/sqlite test -race ./...
SGSP_TEST_DATABASE_URL=... go -C adapters/postgres test -race ./...
go -C adapters/postgres generate ./...
go -C adapters/sqlite generate ./...
```

SQLC v1.30.0 is pinned in each adapter's generation directive. Generated Go code
is checked in; SQLC is not linked into applications. SQLite tests use the CGO
mattn driver. PostgreSQL tests require a disposable database and fail if it is
missing. See each adapter README for connection settings and migration policy.
