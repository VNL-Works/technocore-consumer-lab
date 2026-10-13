# Scenarios

Reader scenarios S1..S5 come first; the writer scenarios W0..W7 are in
[Writer scenarios](#writer-scenarios-w0w7) at the end. All five reader scenarios run against the real upstream app at commit
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
- **The false-alarm check.** S1 is the only scenario in which nothing is lost, so it is the
  only one that requires the client to report no loss at all. S2..S5 each contain a real gap,
  and a loss reported at that gap is accepted there. A PASS on S2..S5 alone, or on any single
  scenario, does not show that the client stays silent when nothing was lost: run
  `--scenario all`.

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
- **Why.** The middle cycle's room read is an empty window, and at this pin its `last_seq`
  stays at 3. The gap is already visible elsewhere, though: `GET /rooms?format=json` lists the
  room's on-disk head (6, behind a listing cache of a few seconds), and head 6 above cursor 3
  with nothing readable means records were dropped unread. So a loss reported in the middle
  cycle is valid. A client that only reads the room learns it when 7 arrives:
  `first_seq = 7 > 4` is the evidence. With #800/#910's
  empty-window `last_seq`, the middle cycle would instead hand back `last_seq = 6`, and #545's
  loop would checkpoint 6 and never notice: that is the composition bug found on 2026-10-13
  (PR #545 comment 6136144371). The careful client checks "empty page with `last_seq > since`"
  for that case too.
- **On the #800/#910 trees** (open PRs, heads `6ce0852` and `b72607e`; not the pin). These
  trees make an empty window report the room head, so the middle cycle's read is
  `messages = []` with `last_seq = 6` above the cursor 3. Observed on both: the careful client
  takes its empty-window branch there, writes `loss after_seq=3` ("empty window with last_seq 6
  > since 3") in the middle cycle, moves its cursor to 6, and delivers 7 in the last cycle as
  `first_seq = since + 1`: PASS. This is the only scenario where that branch ran
  (`tests/test_lab_variants.py`); at the pin it never runs. The naive client still fails with
  `FAIL:unreported_loss`, now because the empty page's `last_seq` moves it past 4..6.
- **Reference clients.** naive: `FAIL:unreported_loss`. careful: PASS.

## S4 `ephemeral-prefix-expired`

- **Setup.** Like S3 without the middle cycle: post 3, cycle, post 3, post 1 (7), freeze the
  cutoff between 6 and 7, cycle. The poll after the expiry already has a live record.
- **Expected.** Same as S3.
- **Why.** The window is non-empty (`[7]`), so the empty-window check never fires; only the
  `first_seq > since + 1` check (and an export that also starts at 7) reveals 4..6.
- **On the #800/#910 trees.** The `last_seq` change is to the empty window only, and S4 has
  none (#800 also adds `first_retained_seq`/`first_retained_ts` to every read, which neither
  reference client reads). Observed on both: careful PASS through the `first_seq` check, naive
  `FAIL:unreported_loss`, and no empty-window loss.
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
past the gap. The kit knows that cycle from its own steps: it is the first cycle after the kit
step that created the gap, which is the second cycle in all of S2 to S5 (in S3 the room read is
still empty then, but `/rooms` already shows the head at 6). A matching loss written earlier
cannot be based on anything the client saw; it is graded `FAIL:bad_events` as a false alarm. So is a rewrite of the event log: after
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

# Writer scenarios (W0..W7)

Each writer scenario runs in a fresh worker process like a reader scenario, with a fresh plain
room `lab-w0`..`lab-w7` (not `e-`), one message and **one** client cycle. Before the cycle the
kit posts one seed record (`consumer-lab seed`, from `lab`) straight to upstream, not through
the proxy, and checks that the room holds exactly that record: the room exists, so a read that
follows a redirect reads a real room. The client then runs with `TECHNOCORE_URL` pointing at
the kit's `FaultProxy` (`consumer_lab/proxy.py`), which relays to upstream on loopback and
applies the scenario's action to the **first** `POST /r/{room}`; every other request passes
through unless the scenario says otherwise. The proxy matches paths as
upstream routes them: any `#fragment` dropped (as httptools does), then **percent-decoded**, so
`/r/lab%2Dw2` and `/r/lab-w2#x` are the room `lab-w2` and get its fault too. The text the client must post is
`consumer-lab write <id> <16 random hex>`, unique in the room; the kit checks in-process that
upstream's `clean_text` keeps it unchanged, so `stored` counts what was posted.

**Truth.** After the cycle the kit waits for the proxy to go idle (no handler in flight, no
connection waiting in the listen backlog; at most 10 s), then counts the records in the room
whose `text` equals the write text, with `store.read_messages(root, room, limit=MAX_LIMIT)`
in-process, never through the proxy. Upstream trims the text, so a copy posted with extra
surrounding whitespace still counts; a retry that **alters** the text (a suffix, a counter) is a
different message and is not counted as a second copy. The seed never counts and must still be
there exactly once. After the proxy has stopped, the kit waits for it once more and requires
that no request arrived after its snapshot of the log. The proxy's log (method, path without query, action, forwarded, origin status; no
bodies, no texts) is the truth about forwarding, and the kit asserts each scenario's
precondition from it: in W1..W4 and W7 the first POST was forwarded and upstream answered 200
(and the write is stored); in W5 and W6 it was not forwarded. A broken precondition is a setup
error (exit 2), not a verdict. In W1..W7 the kit also fails closed when the message is
stored but no room POST in the log got the scenario's fault: a write that reached upstream
without the fault proves nothing. Only when no POST for the room reached the proxy and nothing
is stored (a client that crashed first, say) is there nothing to check, and the cycle is judged
as is. A client that deletes or empties its event log is judged on the empty log.

**Determinism.** Every "after write" action reads upstream's whole reply before it misbehaves,
so the write has committed before the client sees anything; no step waits on a timer except
the W3 stall itself, which is the fault. `CHAT_DUPE_FILTER_SECONDS=0` lets an exact repeat of
the text be stored again (probed at the pin: 7 identical posts in a row are all stored), so a
second copy is always visible in the truth.

| id | name | proxy action on the first POST | stored if the client posts once | naive | careful |
|---|---|---|---|---|---|
| W0 | clean | pass | 1 | PASS | PASS, confirmed |
| W1 | redirect-303-after-write | forward, read the reply, discard it, answer 303 | 1 | PASS | PASS, confirmed |
| W2 | redirect-307-after-write | as W1 with 307 | 1 | FAIL:duplicate_post | PASS, confirmed |
| W3 | timeout-after-write | forward, read the reply, send nothing for 10 s, close | 1 | FAIL:duplicate_post | PASS, confirmed |
| W4 | truncated-after-write | forward, relay status line and headers unchanged plus half the body, close | 1 | FAIL:duplicate_post | PASS, confirmed |
| W5 | reset-before-write | read the request, never forward, close with `SO_LINGER` 0 (TCP reset) | 0 | PASS | PASS, unconfirmed |
| W6 | redirect-without-write | never forward, answer 303 | 0 | FAIL:false_confirmed | PASS, unconfirmed |
| W7 | unreadable-after-write | as W4; then every later POST is reset, every GET of the room is a 503 | 1 | FAIL:false_not_written | PASS, unconfirmed |

Redirects carry `Location: /r/{room}?format=json` and `Content-Length: 0`.

## W0 `clean`

- **Expected.** `confirmed`, stored 1. Any other outcome is `FAIL:overcautious`; nothing stored
  is `FAIL:missing_write`.
- **Why.** The control. A writer that never says `confirmed` passes every other scenario by
  saying `unconfirmed`, and is useless; W0 is where that shows.

## W1 `redirect-303-after-write`

- **Expected.** Stored 1; `confirmed` (after a read-back, or after following the 303 to the
  room) or `unconfirmed`.
- **Why.** The case of open upstream PR #944. The write landed, but the answer is a redirect. Following a 303
  turns the request into a GET, which posts nothing, so the naive writer passes, by luck: it
  read a 200 from the room, not from its write. This PASS is kept on purpose: following a 303
  after a committed write does no harm.

## W2 `redirect-307-after-write`

- **Expected.** Stored 1; `confirmed` or `unconfirmed`.
- **Why.** A 307 keeps the method: a client that follows it, or (like urllib, which raises on
  a POST 307) retries the error, re-POSTs. That second POST passes through the proxy and lands:
  `FAIL:duplicate_post`.

## W3 `timeout-after-write`

- **Expected.** Stored 1; `confirmed` or `unconfirmed`.
- **Why.** Issue #982: the write landed and the answer never came. The 10 s silence exceeds
  `TC_WRITE_TIMEOUT` (3 s). A client that retries on timeout stores a second copy. The stalled
  handler ends at 10 s, or as soon as the client hangs up (nobody is left to be silent to; a
  client that posts and closes at once must not keep the proxy busy past the cycle), or when
  the kit stops the proxy; the kit waits for it before
  counting, so the cycle stays well under 30 s and nothing in flight is missed. A client that
  half-closes its socket (`shutdown(SHUT_WR)`) after sending the request reads as hung up: it
  gets an immediate close instead of a timeout, which is just as ambiguous.

## W4 `truncated-after-write`

- **Expected.** Stored 1; `confirmed` or `unconfirmed`.
- **Why.** The case of open upstream PR #667: a 200 whose body is cut off (`IncompleteRead`). The status says
  success, but a client that only trusts a *complete* reply, and then retries, posts twice.

## W5 `reset-before-write`

- **Expected.** Stored 0 unless the client retries (then 1); `unconfirmed`, or `confirmed`
  after a retry that succeeded; `not_written` passes while nothing is stored.
- **Why.** The control for retries: the request never reached upstream, so retrying it is
  harmless. The naive writer passes (its retry lands once); the careful writer reads back,
  does not find it, and says `unconfirmed`. A reset alone does not tell a client whether the
  request arrived, so `not_written` is a guess here; it is a right guess, and the kit cannot
  tell a right guess from knowledge.

## W6 `redirect-without-write`

- **Expected.** Stored 0; `unconfirmed` or `not_written`.
- **Why.** A redirect is not a success. A client that follows the 303 to a 200 and calls that
  `confirmed` claims a write that never happened: `FAIL:false_confirmed`.

## W7 `unreadable-after-write`

- **Proxy.** The first POST is truncated after the write. During the cycle every later
  `POST /r/{room}` is reset before the write, and every GET of `/r/{room}` or of a path under it
  (`/r/{room}/export` included), in any percent-encoding and with or without a `#fragment`,
  gets `503` with a short plain-text body
  and is not forwarded. These are the only upstream read paths that return the room's messages;
  `/rooms` still answers, with the room's head seq and size but no text.
- **Expected.** Stored 1; `unconfirmed`. No read path shows the text, so a client cannot see its
  write (one that infers it from the head seq in `/rooms` and says `confirmed` is right, and
  passes); `not_written` is false.
- **Why.** Honest reporting under total ambiguity. The naive writer retries, gets two resets,
  and concludes `not_written`: `FAIL:false_not_written`. Its retries never landed, but its
  first try did.

## Writer judging

`consumer_lab.writer_judge.judge_write`; the first rule that matches wins:

1. `FAIL:timeout`: the cycle ran over 30 s (the process group is killed).
2. `FAIL:crash`: a non-zero exit with an empty event log.
3. `FAIL:bad_events`: the log changed after the cycle ended (the kit judges the log as it
   stood when the client exited, and compares it again after the proxy went idle), malformed
   JSONL, no `write` event for `TC_WRITE_ID`, more than one, an unknown outcome, any other
   event kind or id, a non-string `detail` or a non-integer `attempts`.
4. `FAIL:duplicate_post`: stored 2 or more, whatever the client claims.
5. `FAIL:false_confirmed`: `confirmed`, stored 0.
6. `FAIL:false_not_written`: `not_written`, stored 1 or more.
7. W0 only: `FAIL:missing_write` (stored 0), then `FAIL:overcautious` (not `confirmed`).
8. `PASS`, with the outcome and the count, e.g. `W5: unconfirmed, stored 0`.

A non-zero exit with a valid write event is acceptable. **`unconfirmed` is never penalised
outside W0**: when a client cannot know, saying so is the correct answer, and a kit that
punished it would push writers towards guessing. **`not_written` must be backed by knowledge**
(a 4xx refusal, or a read-back the client has reason to trust), because it is the claim that
licenses a retry: a writer that says `not_written` about a write that landed will post it
again, which is the duplicate this whole family is about. A `not_written` that happens to be
true (nothing stored) passes; the kit cannot tell a guess from knowledge when the guess is
right.
