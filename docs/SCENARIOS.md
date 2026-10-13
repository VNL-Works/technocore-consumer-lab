# Scenarios

All five run against the real upstream app at commit
`0e47f770b13cc27e1e2e199d4cdf70a4778c97cc`, served on `127.0.0.1` from a fresh temp root in a
fresh process. Writes go over HTTP (`POST /r/<room>?format=json`). The few non-HTTP steps are
private `store` calls made in the server's own process, listed per scenario. The kit computes
the truth from store state it controls (`store.export_room`, `store.read_messages`), and checks
each precondition before running the next cycle. A precondition that does not hold is a setup
error (exit 2), never a verdict.

**What a PASS means.** The client behaved correctly against the pinned upstream under these 5
controlled cases. It does not mean it is correct against the live service, under concurrency,
or under future upstream changes. The private functions below are not an upstream contract;
the pin is what makes them mean what this file says.

A *cycle* is one run of the client command. Each scenario's cycles share one `TC_STATE_DIR`
and one `TC_EVENTS` file.

## Facts at the pin that the scenarios rely on

- `read_messages(since=N)` returns the **newest** `limit` records with `seq > N` (`limit`
  clamps to `MAX_LIMIT = 200`, default 50). It is not a forward cursor (#481).
- An empty window reports `last_seq = min(since, head)`: at this commit it never advances
  past the cursor.
- `GET /r/<room>/export` streams the retained ring, with `X-Room-Generation`. For `e-` rooms
  it starts after the expired prefix (`_export_start`).
- `_cutoff(room)` is `time.time() - EPHEMERAL_TTL_SECONDS` for `e-` rooms, else `None`, and
  reads the module global **on every call**. `_expired` parses each record's `ts` and expires
  it when `ts < cutoff`.
- `_compact(path, cutoff=None, keep=...)` keeps the newest records whose bytes (each line
  plus its newline) fit `keep`. It documents "caller holds the lock".
- `_reap_pass(root, now)` deletes rooms idle for more than `IDLE_SECONDS` at `now`, recording
  the room's last seq as a floor and keeping its generation. The next write recreates the
  room: the generation goes up by one and `seq` continues from the floor.

## S1 `lag-within-ring`

- **Setup.** In `lab-s1`: post 10, cycle, post 300, cycle. All 310 records stay retained.
- **Expected.** Deliveries 1..310, each once, in order. No loss event.
- **Why.** 300 is more than one `MAX_LIMIT` window. `since=10` returns 111..310 (or 261..310
  without `limit`); 11..110 are still in the ring, reachable only through `/export`. This is
  the #481 bug: a client that advances to `last_seq` loses retained records silently.
- **Reference clients.** naive: `FAIL:missed_retained` (it delivers 1..10 and 261..310).
  careful: PASS (sees `first_seq > since + 1`, exports, finds 11 at the start, delivers 11..310).

## S2 `lag-beyond-retention`

- **Setup.** In `lab-s2`: post 10, cycle, post 300. Then
  `store._compact(room_path, keep=<bytes of the newest 250 lines>)`, which leaves exactly
  61..310 in the ring (checked through `export_room`). Cycle.
- **Expected.** A loss event with `after_seq = 10`, written before the first delivery of any
  seq above 10. No delivery of 11..60. Deliveries of 61..310, once, in order.
- **Why.** The retained ring now starts above cursor + 1. Nothing can recover 11..60; the
  only correct behaviour is to say so before delivering past the gap. 250 retained records is
  more than one window, so the client also has to use `/export` to get them all.
- **Private call.** `_compact` is called without the room lock: the kit is the only writer and
  no cycle runs at that moment.
- **Reference clients.** naive: `FAIL:unreported_loss`. careful: PASS (export starts at 61,
  not 11, so it reports `after_seq=10`, then delivers 61..310).

## S3 `ephemeral-all-expired`

- **Setup.** In `e-lab-s3`: post 3, cycle, post 3 more (4..6). Freeze the cutoff one second
  after record 6's `ts`: every record is expired (checked: nothing readable after 3, export
  empty). Cycle. Post 1 record (7), freeze the cutoff halfway between the `ts` of 6 and 7:
  4..6 expired, 7 live (checked: only 7 readable after 3, export holds only 7). Cycle.
- **Expected.** A loss event with `after_seq = 3`, at or before the delivery of 7. No
  delivery of 4..6. Silent advance is a failure.
- **Why.** The middle cycle sees an empty window. At this pin `last_seq` stays at 3, so a
  client cannot tell yet. When 7 arrives, `first_seq = 7 > 4` is the evidence. With #800/#910's
  empty-window `last_seq`, the middle cycle would instead hand back `last_seq = 6`, and #545's
  loop would checkpoint 6 and never notice: that is the composition bug found on 2026-10-13
  (PR #545 comment 6136144371). The careful client checks "empty page with `last_seq > since`"
  for that case too.
- **Reference clients.** naive: `FAIL:unreported_loss`. careful: PASS.

## S4 `ephemeral-prefix-expired`

- **Setup.** Like S3 without the middle cycle: post 3, cycle, post 3, post 1 (7), freeze the
  cutoff between 6 and 7, cycle. The poll after the expiry already has a live record.
- **Expected.** Same as S3.
- **Why.** The window is non-empty (`[7]`), so the empty-window check never fires; only the
  `first_seq > since + 1` check (and an export that also starts at 7) reveals 4..6.
- **Reference clients.** naive: `FAIL:unreported_loss` (observed and asserted). careful: PASS.

### How the cutoff is frozen (S3, S4)

`_cutoff` computes `time.time() - EPHEMERAL_TTL_SECONDS` on every call, so a plain float TTL
lets the cutoff move forward with the wall clock while a client cycle runs: the verdict would
depend on how long the cycle took. The kit sets `store.EPHEMERAL_TTL_SECONDS` to a `float`
subclass whose reflected subtraction returns a fixed instant, so the cutoff is the same on
every call, and checks that `store._cutoff(room)` returns exactly that instant. The cutoff is
chosen from the records' own `ts` values (parsed as `_expired` parses them), never from the
kit's clock, and no sleep is involved. Delays in a cycle cannot change the outcome. What the
kit cannot survive is the system clock stepping backwards between two posts; the
`ts7 > ts6` precondition then fails and the run is a setup error (exit 2), not a verdict.

Side effects while the cutoff is frozen (S3 and S4 only, and only in the scenario's own
worker process):
- `GET /.well-known/agent.json` publishes `ephemeral_ttl_seconds` from the patched global, so
  it shows a meaningless nominal value (e.g. `-0.99`), while `GET /config` still says 900
  (it reads `config`, which is not patched).
- The patched value is a `float` subclass that `orjson.dumps` refuses (`TypeError: Type is not
  JSON serializable`). No code path the scenarios reach serializes it with orjson, but one
  added upstream later would turn into a 500.
- A client must therefore not derive the TTL from the manifest during S3/S4.

### Why S3 and S4 are a policy choice

Upstream treats expiry in `e-` rooms as a promise ("gone by now"), not as a fault, so a reader
that skips expired records silently is arguably within the service's contract. The kit takes
the stricter position: a consumer that mirrors, archives or scores a room should be able to say
which records it never saw, and the #545 + #800/#910 composition shows how easily that is lost.
A client author who accepts silent expiry can read S3/S4 failures in that light.

## S5 `room-recreated`

- **Setup.** In `lab-s5`: post 5, cycle. `store._reap_pass(root, time.time() + IDLE_SECONDS + 1)`
  reaps the room (checked: the room file is gone). Post 3: the room is recreated as
  generation 2 with seqs 6..8 (checked through `room_generation` and `export_room`). Cycle.
- **Expected.** A loss event whose reason contains `generation` or `recreated`, with
  `after_seq = 5`, before the first delivery from generation 2. No delivery of generation 2's
  records under generation 1. Delivering 6..8 as generation 2 after the loss is fine.
- **Why.** The floor makes the new seqs continue (6, 7, 8), so a `since`-only client reads
  them as if nothing happened. Only the generation says it is a different conversation.
- **Reference clients.** naive: `FAIL:unreported_loss`. careful: PASS.

## When a loss counts

A loss event satisfies a gap only if it is written **in or after the cycle in which the gap
first becomes observable** from the pinned upstream's responses, and before the first delivery
past the gap. The kit knows that cycle from its own steps: S2, S4 and S5 the second cycle, S3
the third (its middle cycle sees an empty window with `last_seq = 3`, exactly what it would see
had 4..6 never existed). A matching loss written earlier cannot be based on anything the client
saw; it is graded `FAIL:bad_events` as a false alarm. So is a rewrite of the event log: after
every cycle the kit checks that the bytes the log held before the cycle are still its prefix.
Known limit: lines a client reorders within the cycle that wrote them are not detected; the kit
trusts the client's ordering inside one cycle.

## Judging order

The first failing check is the verdict: a rewritten event log, then malformed events
(both `FAIL:bad_events`), a cycle over 30 s
(`FAIL:timeout`), a non-zero exit from a cycle that wrote no loss event (`FAIL:crash`), a
delivery of a record that was not retained or under the wrong generation
(`FAIL:delivered_lost`), a record delivered twice (`FAIL:duplicate`), deliveries out of store
order (`FAIL:out_of_order`), a loss written before its gap was observable (`FAIL:bad_events`),
a gap with no matching loss event before the first delivery past
it (`FAIL:unreported_loss`), a retained record never delivered (`FAIL:missed_retained`), and
finally a loss event where nothing was lost (`FAIL:bad_events`). Otherwise `PASS`.
