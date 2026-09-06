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
No new persistent queue, database writer, retry protocol, configuration field, or dependency is introduced.
Full durability, device verification, shared workload deadlines, and the original capacity acceptance predicates remain unchanged.

## Implementation plan

Use the test-driven-development and verification-before-completion skills, with independent focused review before publication.

- [x] Inspect current preparation, source ownership, grouping, and shutdown paths; establish the existing coalescing test baseline.
- [ ] Add a failing real-gate test that blocks root A's preparation and observes independent root B starting with its original semantic key and target.
- [ ] Add bounded-concurrency, source-ownership, failure/retry, graceful drain, bounded cancellation, and metadata-cleanup regressions in `tests/test_root_preparation_scheduling.py`.
- [ ] Update `src/mindroom/coalescing.py` to register bounded detached root claims and include them in existing ownership and drain traversal.
- [ ] Preserve explicit-thread and room-mode ordering, media/caption grouping, readiness, and bypass barriers; run the owning-seam and integration tests.
- [ ] Compare serial and concurrent preparation with a controlled real-gate benchmark, then repeat the unchanged real Tuwunel and Synapse 200-conversation controls on frozen source.
- [ ] Run the full test suite and repository hooks; update the Nio artifact pin only after its key-share fix is tested and published.
- [ ] Record actual latency, acceptance results, and production line growth here, self-review, commit, and push the existing PR.

## Results

Baseline: 53 existing coalescing tests pass before the change.
The prior application controls and their unchanged predicates remain recorded in the companion Nio recovery plan; this document will record measurements from the implemented candidate before publication.
