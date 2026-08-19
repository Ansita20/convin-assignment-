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
