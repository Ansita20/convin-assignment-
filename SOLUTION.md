# Solution

## Bug 1: duplicate events/stats race

`Ingest` checked if an event existed, then separately inserted it, upserted the call, and
bumped account stats — four separate DB calls. `event_id` had a plain index, not a unique
constraint. So two retries of the same webhook could both pass the exists-check before
either had inserted, then both go ahead and both increment stats. Duplicate rows, inflated
counts.

It's a check-then-act race. Checking again doesn't fix it — there's always a gap between the
check and the act. The DB has to enforce it, not the app.

Fix:

- Added a unique constraint on `event_id`.
- Wrote one `IngestEvent` method that does the insert (`ON CONFLICT DO NOTHING`), checks
  if it actually inserted a row, and only then upserts the call and bumps stats — all in one
  transaction.
- Swapped the four calls in `Ingest` for this one.

Proof: ran 40 concurrent deliveries of the same event against the old code — got duplicate
rows and a data race the `-race` flag caught. Same test against the fix, run repeatedly:
always exactly one row, correct stats, no race. Also fired it at the live server and checked
the DB directly.

## Bug 2: stats.Cache.Record missing lock

`Cache.Record` touched the same `map[string]*AccountStats` and the same `*AccountStats`
struct that `Get` reads, but never took the mutex. `Get` was doing it right (`RLock`), which
made this easy to miss — the cache looked protected at a glance.

Two ways this breaks under concurrent webhook deliveries: for an account already in the map,
two goroutines can both read the same counter before either writes it back, so an increment
gets silently lost. For a brand new account, two goroutines can both see it's missing and
both try to insert into the map at the same instant — Go maps aren't safe for that, and the
runtime kills the process with `fatal error: concurrent map writes`.

Fix: take `c.mu.Lock()` / `defer c.mu.Unlock()` at the top of `Record`, same mutex `Get`
already uses.

Proof: wrote a test hammering `Record` from 50 goroutines, both on one shared account and on
a brand-new account. Against the old code it panicked with a nil pointer dereference from the
unsynchronized map write, caught immediately by `-race`. Against the fix, ran it 10 times
under `-race` — always the exact expected count, no races, no panics.

## Bug 3: recording never marked processed, nothing logged

`processRecording` ran in a background goroutine but got handed the request's `ctx`
(`r.Context()`). `net/http` cancels that the moment the handler returns, and the handler
returns almost immediately since the goroutine is fire-and-forget. The simulated 50ms of
recording work is much slower than that, so by the time `MarkRecordingProcessed` ran, the
context was basically always already cancelled — the query failed with `context.Canceled`.
And the error handling was `// TODO: handle`, so it just got dropped. Nothing in the DB, nothing
in the logs.

Fix: give the goroutine its own context — `context.WithTimeout(context.Background(),
recordingProcessTimeout)` — instead of the request's, so it isn't tied to when the handler
returns, but is still bounded (5s) in case a recording fetch hangs. And actually log the
error with the event/call IDs instead of the empty TODO branch.

Proof: sent a webhook with a `recording_url` against the old code — `recording_processed`
stayed `false` and nothing showed up in the logs, exactly as reported. Same request against
the fix, run 5 times: `recording_processed` flipped to `true` every single time.

## Bug 4: in-flight work lost on deploy

`srv.Shutdown()` only waits for HTTP handlers that are still executing — it has no idea the
handler spawned a goroutine and already returned. So on SIGTERM, the server stops accepting
connections, waits for the (already-finished) handlers, and exits, while a recording goroutine
from a request that just got acked 200 could still be mid-flight. Then `main()` returns, the
Postgres pool closes, and the goroutine just dies wherever it was. Work the provider was told
succeeded silently vanishes on every deploy.

Fix: added a `sync.WaitGroup` to `Service`, `wg.Add(1)`/`defer wg.Done()` around the recording
goroutine, and a `Shutdown(ctx)` method that waits on the group but bails out if `ctx` expires
first (`wg.Wait()` has no built-in timeout, so it runs in its own goroutine and races against
`ctx.Done()` in a select). Called from `main.go` right after `srv.Shutdown`, using the same
10s shutdown context — after `srv.Shutdown`, not before, since that's what stops new requests
from spawning more goroutines in the first place.

Proof: built the actual binary, fired a webhook with a `recording_url`, then sent SIGTERM
immediately after. On the old code the process exited almost instantly and
`recording_processed` stayed `false`. On the fix, same sequence: the process waited for the
goroutine and `recording_processed` was `true` by the time it exited. Also added unit tests
for `Shutdown` directly — it waits for real in-flight work to finish, and it returns the
context's error instead of hanging when the deadline is already gone.

## Why Postgres, not Redis, for dedup

Went with the `events.event_id` unique constraint + `INSERT ... ON CONFLICT DO NOTHING` from
bug 1, rather than a Redis `SETNX` lock or an app-level in-memory dedup set.

Redis `SETNX event_id` would work and would be faster, but it's a second source of truth that
can drift from Postgres — if the process dies after the `SETNX` succeeds but before the
Postgres insert commits, the event is marked "seen" in Redis forever but never actually
landed in the DB, and a real redelivery would be dropped silently. Postgres already has to be
the durable write for `events`/`calls`/`account_stats` either way, and it already gives me
atomicity for free with a transaction — adding Redis into the dedup path buys speed at the
cost of a second failure mode to reason about, for an endpoint that isn't latency-critical.
An in-memory set is worse: it doesn't survive a restart or work across more than one instance.

## At 10,000 webhooks/second

The single-row `UPDATE account_stats ... call_count + 1` becomes the bottleneck first — every
event for the same account serializes on that row. I'd move to sharded counters (or just let
Postgres batch commits and lean on `synchronous_commit = off` for that table) and reconcile
periodically, rather than updating the aggregate synchronously inside the request path.
Beyond that: the in-memory `stats.Cache` mutex is a single global lock per process, so I'd
shard it by account_id hash; and I'd add a connection pool ceiling / backpressure so a burst
doesn't exhaust Postgres connections — right now there's no limit on concurrent in-flight
`Ingest` calls beyond `DBMaxConns`. Redis would earn its place here as a write-behind buffer
in front of Postgres rather than as the dedup mechanism.
