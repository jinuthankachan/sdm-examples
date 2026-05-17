# SDM Demo

A runnable end-to-end example of [SDM](https://github.com/kapow-tech/sdm) —
the Go code generator that splits sensitive (PII) data from append-only
chain history from a single annotated `.proto` file. Two proto messages
(`User` and `Invoice`) exercise the full annotation surface: PII columns,
chain-stored fields, hashed sidecars, foreign keys, primitives, nested
messages, repeated fields, `google.protobuf.Timestamp`, `(sdm.json)`
strings, and auto-increment.

The demo also doubles as the **integration test suite** for the
generator, covering both feature modes:

| Mode | Config knob | Method surface |
|---|---|---|
| OFF (default for demo regen) | `chain-drafts: false` | `Save` / `SaveAll` / `Fetch(pk)` |
| ON | `chain-drafts: true` | `Save` / `Upsert` / `Update` / `DraftChain` / `CommitChain` / `DropChain` / `Fetch(pk, drafted)` |

A second knob `create-audit-tables: true|false` gates the per-PII audit
table + trigger + `AuditLog` method. The integration tests use Go build
tags to compile only the subset that matches the currently-generated
artifacts (`!noaudit && !chaindrafts`, `noaudit`, `chaindrafts`).

## Layout

```
demo/
├── proto/
│   ├── sdmprotos/annotations.proto    # exported by `sdm setup`
│   ├── user/user.proto                # User message + annotations
│   └── invoice/invoice.proto          # Invoice message + annotations
├── models/                             # generated — do not edit
│   ├── user/    user_sdm_model.go  user_sdm_repo.go  sdm_helpers.go  user.pb.go
│   ├── invoice/ invoice_sdm_model.go  invoice_sdm_repo.go  sdm_helpers.go  invoice.pb.go
│   └── sql/     user_sdm_schema.sql  invoice_sdm_schema.sql
├── integration/                        # build-tagged test suite
│   ├── setup_test.go                   # TestMain + container; runs in any mode
│   ├── setup_off_test.go               # !chaindrafts helpers (mustSaveUser via SaveAll)
│   ├── setup_chaindrafts_test.go       # chaindrafts helpers (mustSaveUser via Upsert+CommitChain)
│   ├── setup_audit_test.go             # !noaudit resetTables (truncates audit tables too)
│   ├── setup_noaudit_test.go           # noaudit resetTables (no audit tables to truncate)
│   ├── *_test.go                       # OFF-mode tests (tagged !chaindrafts)
│   ├── audit_test.go                   # audit tests (!noaudit && !chaindrafts)
│   └── chaindrafts_*_test.go           # ON-mode tests (chaindrafts)
├── main.go                             # quick-start application: insert + mutate + ChangeLog
├── migrate.go                          # applies models/sql/*.sql against a live DB
├── sdm.cfg.yaml                        # generator config — flip knobs here
├── docker-compose.yaml                 # local Postgres for main.go
└── go.mod
```

## Prerequisites

- Go ≥ 1.22
- Docker (only for `main.go` — the integration suite uses
  [testcontainers](https://golang.testcontainers.org/) to spin up its own
  Postgres on demand)
- The `sdm` CLI installed (`go install github.com/kapow-tech/sdm/cmd/sdm@latest`)

If you're developing against a local `sdm` checkout, install the live
binaries instead:

```bash
cd /path/to/sdm
go install ./cmd/sdm ./cmd/protoc-gen-sdm
```

## Quick start (main.go against a local Postgres)

```bash
# 1. Bring up Postgres
docker compose up -d

# 2. (Re-)generate from the .proto files. Re-running is a no-op if nothing changed.
sdm generate

# 3. Run the demo — applies SQL schemas, inserts users + an invoice, mutates,
#    and prints the chain ChangeLog for the invoice.amount field.
go run .

# 4. Tear down when done
docker compose down -v
```

`main.go` is written against the **chain-drafts ON** API (`Save` →
`CommitChain`) — flip `chain-drafts` to `false` in `sdm.cfg.yaml`,
regenerate, and you'd need to replace the `CommitChain` calls with
`SaveAll(..., true)`.

## Running the test suite

The suite uses [testcontainers-go](https://golang.testcontainers.org/)
internally — no manual Postgres setup needed. Each test gets a fresh
truncated DB via the mode-appropriate `resetTables`.

| Generator config | Test invocation | What runs |
|---|---|---|
| `chain-drafts: false`, `create-audit-tables: true` (default) | `go test ./integration/...` | 85 tests — full OFF-mode + audit suite |
| `chain-drafts: false`, `create-audit-tables: false` | `go test -tags noaudit ./integration/...` | OFF-mode minus the audit tests |
| `chain-drafts: true`, `create-audit-tables: true` | `go test -tags chaindrafts ./integration/...` | 57 tests — chain-drafts workflow + audit interaction |

**You must regenerate after flipping a knob** — the test files are
type-checked against whatever's in `models/`, so a mode mismatch
surfaces as compile errors.

```bash
# Example: regenerate in chain-drafts mode, then run the chaindrafts suite.
sed -i 's/chain-drafts: false/chain-drafts: true/' sdm.cfg.yaml
sdm generate
go test -tags chaindrafts ./integration/...
```

## What's exercised

### `user.proto`

Demonstrates a PII-backed message with:
- **`(sdm.primary_key)` + `(sdm.auto_increment)`** — BIGSERIAL `id`
- **`(sdm.chain_identifier_key)`** — stable string `user_id` used as the chain key
- **`(sdm.pii)`** — `user_id`, `email`, `name` stored in `pii_users`
- **`(sdm.hashed)`** — `hashed_email` chain sidecar with `sha256(email)`
- **`(sdm.unique)`** — generates `FetchByEmail` / `FetchByUserId` / `FetchByPan`

Chain-stored fields (no `pii`): `pan`, `country`.

### `invoice.proto`

Demonstrates the broader type surface:
- **String primary key** — `invoice_id` (no auto-increment)
- **`(sdm.references)`** — `seller_id` / `buyer_id` → `User.user_id` (FK)
- **Nested message in PII** — `Price` (`Money`) with `(sdm.pii)`
- **`(sdm.json)` string** — `metadata` stored as JSONB
- **Repeated scalar in chain** — `tags`
- **Repeated message in chain** — `items`
- **Repeated scalar in PII** — `pii_tags` (Postgres `text[]`)
- **Repeated message in PII** — `pii_items` (`jsonb` via `protojsonArray` serializer)
- **`google.protobuf.Timestamp`** — `transfer_date`

### Integration tests

Per-feature coverage spread across these areas:

| Area | OFF-mode files | ON-mode files |
|---|---|---|
| Basic CRUD + soft delete | `user_test.go`, `invoice_test.go` | `chaindrafts_pii_test.go`, `chaindrafts_invoice_test.go` |
| Chain mechanics, ChangeLog, AsBaseModel | `mutations_test.go` | `chaindrafts_view_test.go` |
| Actor attribution | `actor_test.go`, `actor_audit_test.go` | (covered inline in `chaindrafts_test.go` + `chaindrafts_pii_test.go`) |
| Audit trigger | `audit_test.go` | `chaindrafts_audit_test.go` |
| Version trigger | `version_test.go` | (shared mechanic, exercised indirectly) |
| Draft workflow | — | `chaindrafts_test.go` |

## Switching feature modes

```bash
# Toggle audit tables
sed -i 's/create-audit-tables: true/create-audit-tables: false/' sdm.cfg.yaml
sdm generate
go test -tags noaudit ./integration/...

# Toggle chain drafts
sed -i 's/chain-drafts: false/chain-drafts: true/' sdm.cfg.yaml
sdm generate
go test -tags chaindrafts ./integration/...

# Both off
sed -i 's/create-audit-tables: true/create-audit-tables: false/' sdm.cfg.yaml
sed -i 's/chain-drafts: true/chain-drafts: false/' sdm.cfg.yaml
sdm generate
go test -tags 'noaudit chaindrafts' ./integration/...   # both negation tags pass
```

(The `-tags` argument is a comma-separated *or* space-separated list of
build tags. Multiple negation tags are additive — each excludes its
matching files.)

## Regenerate from scratch

Generated artifacts (`models/`, `models/sql/`) are committed for review
convenience. To wipe and regenerate cleanly:

```bash
rm -rf models/
sdm generate
```

The generator emits the four `*_sdm_*` files per message, the per-package
`sdm_helpers.go`, the SQL schema files under `models/sql/`, and the
standard `.pb.go` Go protobuf code via `protoc-gen-go`.

## See also

- [SDM main README](https://github.com/kapow-tech/sdm) — annotations, type
  handling, feature reference, generator internals
- [`sdm.cfg.yaml`](sdm.cfg.yaml) — the full config schema with inline comments
- [`integration/setup_test.go`](integration/setup_test.go) — how
  testcontainers + schema application + `resetTables` fit together
