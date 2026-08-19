# Solution

## Bug

`Ingest` checked if an event existed, then separately inserted it, upserted the call, and
bumped account stats — four separate DB calls. `event_id` had a plain index, not a unique
constraint. So two retries of the same webhook could both pass the exists-check before
either had inserted, then both go ahead and both increment stats. Duplicate rows, inflated
counts.

## What made it click

It's a check-then-act race. Checking again doesn't fix it — there's always a gap between the
check and the act. The DB has to enforce it, not the app.

## Fix

- Added a unique constraint on `event_id`.
- Wrote one `IngestEvent` method that does the insert (`ON CONFLICT DO NOTHING`), checks
  if it actually inserted a row, and only then upserts the call and bumps stats — all in one
  transaction.
- Swapped the four calls in `Ingest` for this one.

## Proof

Ran 40 concurrent deliveries of the same event against the old code — got duplicate rows and
a data race the `-race` flag caught. Same test against the fix, run repeatedly: always exactly
one row, correct stats, no race. Also fired it at the live server and checked the DB directly.
