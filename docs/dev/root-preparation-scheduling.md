# Independent root preparation

## Decision and ownership

Completed new conversation batches in a threaded room may prepare concurrently, with at most eight such preparations per `CoalescingGate` instance.
The gate still owns receipt ordering, asynchronous readiness, attachment/caption grouping, debounce, and the exact claimed source events until dispatch hands them to the existing response lifecycle.
Only the preparation of independent, completed root batches changes; the original `CoalescingKey(room, None, requester)` remains the semantic context and reply-target input.
Explicit threads, active follow-up queues, and rooms configured as one conversation retain their existing serial dispatch.
The eight-slot bound covers tasks from registration through dispatch completion, including time waiting for durable handoff and the response lifecycle lock; it is not a limit on running model responses.

The existing gate waits for context preparation, planning, durable handoff, and lifecycle acquisition before preparing the next new root from the same requester.
The recorded 200-conversation controls spread first visible replies over approximately 20 seconds.
Concurrent preparation is a measured candidate for reducing that delay, not a promised latency bound.

## Alternatives

Keep serial preparation: smallest implementation, but an unrelated slow root continues to block following roots.
Move all preparation into the response runner: reuses its task registry, but transfers work before its existing durable adoption point and requires new metadata and retry ownership across components.
Retain preparation in the coalescing gate: selected because its claimed-batch cleanup and dispatch-failure callback already own these responsibilities.
Changing the semantic thread key to a root event ID is rejected because that also changes context lookup, reply targeting, and room-level media promotion.

## Lifecycle

Each detached preparation uses an existing `_GateEntry` holding its exact claimed admissions and a tracked task; queued events stay on the original gate.
Capacity is awaited before claiming events or creating another task, and the eligible front batch is recomputed after that await because ingress or media promotion may have changed it.
Task registration and source ownership transfer have no intervening await.
Source-ownership checks include detached entries so the durable journal worker cannot redispatch a still-owned source.
The existing `_dispatch_claim` owns dispatch, failure notification, and metadata cleanup.
Task completion removes the tracked entry and retrieves unexpected failures; cancellation before the task's first step must also release metadata.
An in-gate bypass barrier waits for older detached preparations with the same semantic key; commands routed outside the gate keep their existing behavior.

Graceful drains await both queued gates and detached preparations.
Bounded drains apply the existing shutdown budget and cancellation provenance to both, close metadata once, and report incomplete work instead of certifying a clean checkpoint.
Cancelling the queue's wait for capacity does not cancel independent preparations; shutdown remains their owner.
Detached tasks expose no independent cancellation API; bounded shutdown also delivers cooperative cancellation before returning without waiting indefinitely for cancellation-resistant callbacks.
No new persistent queue, database writer, retry protocol, configuration field, or dependency is introduced.
Full durability, device verification, shared workload deadlines, and the original capacity acceptance predicates remain unchanged.

## Implementation plan

Use the test-driven-development and verification-before-completion skills, with independent focused review before publication.

- [x] Inspect current preparation, source ownership, grouping, and shutdown paths; establish the existing coalescing test baseline.
- [x] Add a failing real-gate test that blocks root A's preparation and observes independent root B starting with its original semantic key and target.
- [x] Add bounded-concurrency, source-ownership, failure/retry, graceful drain, bounded cancellation, and metadata-cleanup regressions in `tests/test_root_preparation_scheduling.py`.
- [x] Update `src/mindroom/coalescing.py` to register bounded detached root claims and include them in existing ownership and drain traversal.
- [x] Preserve explicit-thread and room-mode ordering, media/caption grouping, readiness, and bypass barriers; run the owning-seam and integration tests.
- [ ] Compare serial and concurrent preparation with a controlled real-gate benchmark, then repeat the unchanged real Tuwunel and Synapse 200-conversation controls on frozen source.
- [x] Run the full scheduling test suite: 15,850 passed, 22 skipped (16 warnings), including all 12 new scheduling cases.
- [x] Update the Nio artifact pin to verified commit `398dae4d7e7079e3dd5e7a3f360b4f1ec2573c6e`; its installed files match the tested wheel. Complete repository hooks pass, including full-project types and frontend checks.
- [ ] Record actual latency, acceptance results, and production line growth here, self-review, commit, and push the existing PR.

## Results

Baseline: 53 existing coalescing tests pass before the change.
Both the real-gate and real-controller regression fail against the published baseline because root B cannot begin while root A awaits context preparation.
The new scheduling suite passes 12 cases, including a zero-budget drain with eight active roots and a ninth waiting for capacity.
The broader coalescing and live integration group passes 241 cases before that final added test; all repository hooks pass.
The production change adds 63 net lines in `coalescing.py`.

Two existing cancellation tests now use explicit-thread events and keys because they test cancellation of an active serial conversation queue.
Their former root fixtures assumed independent roots shared that queue; new root-specific tests separately verify the changed ownership and cancellation behavior.
Review found and fixed the drain snapshot boundary: a queue task can create root tasks after the drain's first snapshot, so completion also requires that no owned preparation tasks remain.

Three alternating local runs of 40 roots with controlled 10-millisecond asynchronous preparation measured median completion of 409.05 milliseconds with one slot and 56.39 milliseconds with eight.
Preparation start spread fell from 396.48 to 43.38 milliseconds, and peak active preparation was exactly one and eight respectively.
The probe also preserves configured room-mode ordering, explicit-thread ordering, and same-key bypass barriers.
This measures scheduler overlap under controlled asynchronous work, not application reply latency.
The real 200-conversation comparison uses frozen production source and unchanged health, overlap, deadline, exact-reply, fence, drain, and shutdown predicates.

One sequential frozen-source Tuwunel pair used published Nio `09d5a25` and MindRoom `b45966053`; the candidate differed only in `coalescing.py`.
All source hashes and loaded module paths matched their manifests throughout both runs.

| Measure | Serial | Eight preparations |
| --- | ---: | ---: |
| Exact completed replies | 200 | 200 |
| Preparation start spread | 21.076 s | 19.845 s |
| Initial visible reply spread | 21.147 s | 20.571 s |
| Input-to-completion median | 74.579 s | 74.677 s |
| Input-to-completion p95 | 86.211 s | 85.139 s |
| First input to last completion | 86.842 s | 85.708 s |
| Full visible overlap | 42.491 s | 43.247 s |

Both runs settled the post-terminal reaction for all three principals, retained zero journal/outbox debt, continued sync and shut down cleanly.
Both failed only the unchanged 45-second overlap requirement.
The workload includes approximately 60 seconds of synthetic generation.
This single pair establishes no large application speedup: median reply time was unchanged, and initial visibility spread improved by 0.576 seconds.
Retain the change for independently blocked root preparation and bounded ownership, rather than claiming it resolves the remaining throughput limit.

The previous serial dispatch diagnostic attributed 20.337 of 21.314 seconds to `record_pending_turn`: 8.297 seconds awaiting the per-agent ledger lock and 11.836 seconds awaiting the database call.
Only 0.607 seconds occurred inside its database transactions; 10.259 seconds preceded execution and 0.969 seconds followed it before the caller resumed.
These overlapping-stage timings explain why adding preparations does not remove the shared persistence bottleneck; they are prior diagnostic evidence, not a current-candidate breakdown or disk-only measurement.
The durable handoff and shared writer remain unchanged. A writer redesign or removing pending-turn persistence is outside this scheduling change.
That earlier diagnostic used Nio `5e9c79d` with the then-current companion source; it sampled the process at 25 Hz and must not be compared as an uninstrumented latency result.

The first full-suite run started with all 72 Nio source files matching the built key-share wheel, but a test subprocess restored the old Git dependency pin during that run.
Its passing result covers the scheduling suite but does not certify the final combined artifact.
A repeat with automatic resync disabled for the suite and its subprocesses passed 15,850 tests, with 22 skipped and 16 warnings, in 133.41 seconds.
All 72 installed Nio source files matched the wheel and reviewed source both before and after the suite.
The final Git pin `398dae4d7e7079e3dd5e7a3f360b4f1ec2573c6e` installs those same files; this equality check connects the built-artifact tests to the committed dependency.
