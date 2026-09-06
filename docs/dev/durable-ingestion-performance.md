# Durable ingestion performance evidence

This is the entry point for the September 2026 producer/consumer investigation.
Use it with the [ledger ownership and measured results](ledger-write-ownership.md) and the producer's `docs/design/durable-sync.md` and `docs/design/durable-sync-plan.md` in `mindroom-nio`.
Those documents define the retained guarantees and supersede the retired ingestion-engine plans.
The benchmark result is 200 concurrent conversations with one responder account, three syncing principals and a synthetic model; it does not qualify 200 independent agents or 1,000 concurrent replies.

## Decisions to retain

| Question | Result and decision | Evidence to read |
| --- | --- | --- |
| Was the old producer doing excessive work? | Yes; shared interpretation and durable batches replace the old engine. The final 200-event plain FULL capture/prepare/read/ack cycle is 23.2 ms versus 196.7 ms for the old NORMAL implementation. This is a local engine benchmark, not reply latency. | Producer plan, final engine table; `durable-sync-kernel/micro-final-20260906T091913Z`. |
| Do we need orjson or PR #57's implementation? | Retain stdlib JSON. Batching already reduces 200-event plain decoding from 1,854 to 3 and commits from 404 to 3; the old-engine patch is not used. | Producer plan, measured engine results. |
| Were the earlier failures just a slow server? | No; the old capacity evidence exposed real recovery/fence defects. The producer replacement and coordinated consumer fixes have separate correctness tests and final acceptance evidence. | Producer contract/plan; original `profiling-report.md` is historical evidence. |
| Did removing global ledger serialization help? | Yes; alternating Tuwunel controls reduce initial spread from 21.69–21.81 s to 12.03–12.07 s. Keep identity-based conflict ownership. | Ledger ownership document; `durable-sync-kernel/startup-trace`. |
| Should preparation concurrency increase? | The measured preparation wait is predominantly pending-turn persistence. Larger limits did not fix it. Keep eight slots. | Producer plan and ledger dependency-tracing sections. |
| Do smaller worker pools or a shorter GIL switch interval help? | Neither demonstrates a startup gain; retain existing settings. | `durable-sync-kernel/startup-waits`. |
| Does one fewer membership SELECT help? | No worthwhile end-to-end gain in two alternating comparisons. The nine-line production candidate was withdrawn; useful membership-transition tests remain. | Ledger membership-query section; `durable-sync-kernel/query-reuse`. |
| Why is the writer occupied without much CPU use? | Live SQLite execution contends for Python execution and then waits for completion propagation through the event loop. Same-SQL isolated replay is much faster, with important limits. | Ledger SQL/native-wait sections; `durable-sync-kernel/transaction-path`. |
| Should SQLite writes run directly on the event loop? | No useful gain in the diagnostic, and real database contention blocks the loop. Keep the existing writer. | Ledger direct-writer section; transaction-path lock probe and inline run. |
| What next? | Benchmark fewer complete delivery transactions at existing ownership boundaries, starting with the duplicate device binding after a proven fresh claim. No performance gain is established yet. | Ledger transaction map and required recovery/settlement ordering. |

The latest investigation changes no production code.
Its measurements explain costs; they do not establish a general speedup, justify weaker durability, or prove all possible failures impossible.
Reply completion includes roughly 60 seconds of synthetic generation, so an engine or startup speedup does not translate proportionally into total response time.

## Retained artifacts

All paths below are relative to the persistent capacity evidence workspace originally supplied with `profiling-report.md`.
The searchable archive is `reproducibility/20260906-writer-investigation.tar.gz`, with an extracted sibling directory of the same name.
It contains the campaign's available scripts/reports/result manifests, dependency locks, runtime versions, server-image identities observed at preservation time, and the latest four runs' metadata traces.
It includes exact historical native libraries, the earlier trace-child variant reconstructed and verified against its recorded SHA-256, and native symbol tables for portable re-analysis.
`MANIFEST.sha256` checks every packaged file; the companion archive checksum verifies the package itself.
The archive is retained in the capacity workspace, not hosted in this Git repository.
Raw application logs, synthetic database files and server state remain in their original evidence directories; they are not needed for the packaged metadata analysis.
The tracked [measurement summary](durable-ingestion-performance-results.json) preserves the key numeric results, source revisions and run mappings independently of the archive.

| Latest run | Driver directory under `durable-sync-kernel` | Separate evidence directory under `durable-sync-kernel` |
| --- | --- | --- |
| Valid SQL/native trace | `transaction-path/trace-200-20260906T153922Z` | `mindroom-live-matrix-fuzz-68e81282` |
| Valid trace counting short native waits | `transaction-path/trace-200-20260906T154420Z` | `mindroom-live-matrix-fuzz-bc2989a3` |
| Normal control | `startup-waits/control-200-20260906T154923Z` | `mindroom-live-matrix-fuzz-71dab4e7` |
| Inline diagnostic | `transaction-path/inline-200-20260906T155552Z` | `mindroom-live-matrix-fuzz-ecc9c42a` |
| External writer contention | `transaction-path/lock-contention-20260906T155542Z` | Same directory; `results.json` and probe databases. |

`trace-200-20260906T153559Z` failed postprocessing and `trace-200-20260906T154323Z` was stopped; neither is an accepted capacity control.
The 50-root trace is an instrumentation calibration, not 200-root qualification.
Older measurements belong to their recorded revisions; the original report's Nio CPU percentages do not describe the replacement producer.

## Re-analysis and fresh reproduction

The package README gives the complete commands and path configuration; use its checksums before analysis.
From the extracted package, run the following with a Python 3.13 environment:

```sh
sha256sum -c MANIFEST.sha256
uv run --no-project --python 3.13 python analyze-retained.py mindroom-live-matrix-fuzz-bc2989a3
```

The portable wrapper uses archived symbol tables, avoiding a dependency on this host's absolute interpreter/library paths.
It reproduces the SQL/native/future timing report from retained metadata.
SQL parameters and fetched results existed only in the diagnostic child's memory and were discarded at exit.
The old exact SQL replay therefore cannot be rerun from database snapshots alone; reproduce it by running the synthetic workload again with the SQL probe installed.

For a fresh run, restore MindRoom `e276982fdbd3f58b8d471376c417767654819da4` and Nio `ce18a1fed0a592b93b789de4480ac3e61e8c3145` in persistent checkouts, install the locked consumer environment and verify installed producer source against the Nio checkout.
Use the package's complete helper hierarchy and update the documented checkout/evidence path constants when relocating it.
Run the normal control first, then the diagnostic, sequentially on the same filesystem without competing tests or benchmarks.
The controls retain FULL durability, eight preparations, 200 roots, the shared 180-second deadline, 45-second overlap, two-second health timeout, three-principal fence, debt audit and clean shutdown.
Verify source hashes before and after each run; runtime probes must be labeled diagnostic even when the production source hashes match.
Never change runtime source or probe files during a run, compare profiled latency directly with normal latency as a speedup, or add overlapping task/native/CPU timings together.

The original launcher identity hashed the extracted control function rather than every helper, and did not record the native compiler invocation or immutable server image identity per run.
The package preserves the full available helper files, exact native binaries and a rebuild recipe, but these missing historical facts cannot be recreated as contemporaneous proof.
New runs should capture complete helper hashes and resolved image IDs at launch.
The exact native/source probe variants used by the two valid traces are mapped in the package README; the earlier trace child matches its recorded hash.
