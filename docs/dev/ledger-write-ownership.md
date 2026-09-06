# Ledger write ownership

## Purpose and evidence

Independent conversations must be able to prepare concurrently while every visible effect still waits for its required durable write.
The handled-turn ledger currently holds one agent-wide lock across both in-memory publication and the awaited database operation.
A correlated Tuwunel startup trace at MindRoom `738ee46bc` and Nio `fa587be` identifies that lock as the critical serialization point.

| Roots | First-to-last initial reply | Startup from first input | Ledger held | Ledger waiting in writer queue | Ledger's own worker execution |
| --- | ---: | ---: | ---: | ---: | ---: |
| 50 | 4.335 s | 4.663 s | 4.573 s | 3.943 s | 0.228 s |
| 100 | 9.983 s | 10.418 s | 10.315 s | 8.908 s | 0.473 s |
| 200 | 23.736 s | 24.277 s | 24.102 s | 20.660 s | 1.698 s |

These are instrumented timings, not uninstrumented capacity claims.
The matched 200-root control had a 21.811-second initial reply spread and 39.712 seconds of full overlap.
Tracing increased the observed spread by 8.8%; one pair cannot separate overhead from ordinary run variation.
All 50/100/200 controls completed exactly one reply per root, settled all three fence principals and drained producer, application and outbox work.
The 50/100 controls passed every predicate; the ordinary 200-root control failed only the unchanged 45-second overlap requirement.

The last traced 200-root request was admitted 0.354 seconds after its Matrix timestamp, then waited 22.060 seconds for preparation.
Admission and settlement operations occupied 12.612 seconds of the ledger's writer-queue wait.
Nio transactions also blocked the event loop for 8.297 seconds during startup, with about 1.89 seconds of transaction thread CPU.
That blocking overlaps the ledger waits and must not be added to them.
Deferral probes used 0.244 seconds, which does not justify another ownership-index redesign.

A 25 Hz py-spy trace includes idle threads and two clock markers.
Its clock offsets differ by 24.5 ms; the analysis retains approximately 92 ms of alignment uncertainty.
The sampled startup spends 8.64 seconds under Nio durable processing and 6.13 seconds in event-loop idle stacks.
Samples identify blocking locations, not CPU-only cost.
Request traces record the synthetic model generator and first chunk explicitly: this workload does not use an HTTP model for its replies.
The retained `startup-trace` evidence includes metadata-only request correlation, worker operation IDs, source hashes, an exact-cohort analysis, and a Perfetto timeline.

A diagnostic that partitions the ledger lock by benchmark root reduces initial visibility spread from 23.736 to 12.684 seconds with the same tracing.
It completes all 200 replies, fences and drains, with 49.611 seconds of full overlap.
This establishes a worthwhile optimization hypothesis.
That shortcut is not a production implementation: benchmark root context does not express alias conflicts or concurrent cleanup.

## Chosen ownership

Keep the existing backend transaction and cancellation behavior.
SQLite retains its one writer; Postgres retains its backend-owned transactions.
Keep FULL durability, both pending-turn writes, the eight preparation slots, and every existing producer guarantee.
Only conflicting ledger mutations need to serialize their complete read/derive/publish/commit-or-rollback sequence.
There is no required global commit order between unrelated turns.

The shared ledger state owns a transient map from affected event identities to completion futures.
The existing asynchronous lock protects reservation and exclusive maintenance, rather than remaining held while every unrelated database operation runs.
An update waits for earlier mutations affecting its lookup identities before deriving a candidate.
It also checks every identity and anchor that the candidate or resolved record can affect.
Existing records' anchors matter because the SQL upsert can delete old sibling indexes while re-anchoring a record.
A conflict causes the update to wait for settlement and derive again from the resulting state.
Update callbacks are synchronous derivations from the supplied records and may be evaluated again after such a wait.

Reserve all affected identities and publish the provisional record together under the existing state lock.
Release the reservation lock before awaiting persistence.
Keep each identity reserved until the write's outcome is known and any rollback is complete.
A completion future signals settlement, not success; the writer's own caller receives its exception.
Cancelling a waiter must not cancel another update's persistence or completion notification.
Different ledger instances for the same agent and backend share these reservations.

Cleanup excludes new reservations and waits for all active updates before deriving its retained set or deleting rows.
Loading and legacy import retain exclusive ownership.
No new persistent format, background writer, write-behind acknowledgment, retry protocol, serializer dependency or scheduler is introduced.
The reservation map exists only while callers have writes in flight and releases entries on success, failure and cancellation.

## Alternatives and deliberate limits

Keeping the global lock preserves correctness but imposes the measured unnecessary serialization.
Changing writer priority would make unrelated database operations compete under a new scheduling policy without expressing which ledger mutations actually conflict.
Locks keyed only by a benchmark root, conversation, or one source ID miss alias and old-anchor interactions.
Identity reservations belong in the ledger that already derives these relationships.

Durability still means the caller waits for its own committed write before continuing.
Synchronous readers may see provisional claims while persistence is in flight, preserving duplicate suppression.
Definite write failure restores the prior claim before dependent updates resume.
The existing conservative treatment of an externally cancelled write with an unknown outcome remains unchanged.
This change does not promise exactly-once external effects, remove bounded recovery limits, or qualify 1,000 concurrent replies.

## Implementation and verification

- Reproduce the blocking of an unrelated durable ledger update with a controlled store and real SQLite persistence.
- Preserve ordering for shared source IDs, discovery aliases, old anchors and re-anchoring; verify the resulting records after reload.
- Cover a failed provisional write followed by a dependent update, repeated cancellation, cancelled waiters and cleanup racing an active write.
- Implement reservations only in `src/mindroom/handled_turns.py`; keep test helpers in the ledger tests.
- Run the focused ledger/store tests, the complete suite and repository hooks, then self-review the changed ownership and cancellation paths.
- Compare alternating unchanged-workload controls against the original source, checking exact replies, fence, drain, health, shutdown and source identity.
- Record actual production size and measured results here before pushing the completed change.

## Implementation review

The production implementation changes only ledger ownership in `src/mindroom/handled_turns.py` and its caller documentation in `src/mindroom/turn_store.py`.
Relative to `738ee46bc`, the ledger adds 98 lines and removes 51: 47 net lines.
Condensing the caller documentation removes another 19 net lines, making the total production-file increase 28 lines.
The source change introduces no database or producer changes.

Twenty new cases exercise both real SQLite and Postgres backends.
The unrelated-write regression fails against the original global lock on both backends.
The conflict cases cover source and discovery IDs, old-anchor deletion, definite failure, repeated cancellation, cancelled waiters and cleanup exclusion.
Candidate-only conflicts are tested even when lookup IDs are unrelated: a provisional completed owner cannot permanently reject a competing candidate before its commit or rollback.
An unsafe early-return mutation fails all four candidate-only cases; the restored implementation passes them.
Independent review found no blocking issue in reservation closure, rollback, cancellation or cleanup ownership.
The complete suite passes 15,542 tests with 22 skipped and 15 warnings in 92.31 seconds.
The earlier focused ledger, turn-store and journal group passes 843 tests; the four candidate-only cases were added afterward and pass separately.
All repository hooks pass, including types, dependency boundaries, module privacy and frontend checks.

The diagnostic prototype remains evidence only.
Production capacity acceptance awaits the alternating controls below.
