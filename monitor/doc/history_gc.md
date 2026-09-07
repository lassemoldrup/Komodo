# Optional history-delta reclamation

History GC is disabled by default. Enable it at monitor startup with:

```sh
blackbox_run_monitor ... --history-gc on
blackbox_evidence_replay ... --history-gc on
```

Use `--history-gc off` for the original append-only behavior. The live `e2e.sh`
runner and saturated `run_blackbox_monitor_performance.sh` runner also accept
`BLACKBOX_MONITOR_HISTORY_GC=on` (default `off`). `e2e.sh` also appends this flag
to a custom `BLACKBOX_MONITOR_COMMAND`. Use a separate result
directory for each setting; the performance reporter rejects mixed settings.

The registered evaluation runner also supports a boolean `history_gc` per cell.
Set `alternate_cell_order: true` on a two-cell matrix to run off/on in odd
repetitions and on/off in even repetitions. Both cells use `mode: full_e2e` and
the same workload parameters. `run_registered_performance_cell.sh` accepts
`--history-gc on|off` directly. Results and GC comparison summaries include
`monitor_peak_rss_mib`, `monitor_cpu_seconds`, and `monitor_history_gc_seconds`;
the raw result also records checking cost including GC. These process memory
and CPU measurements cover startup/bootstrap through final reconciliation.

Optional workload RSS sampling is independent of history GC and defaults to
off. Enable it with `blackbox_run_monitor ... --workload-rss-sampling on
--metrics-output metrics.json`, or pass `--workload-rss-sampling on` to
`run_registered_performance_cell.sh`. For `e2e.sh`, set
`BLACKBOX_MONITOR_WORKLOAD_RSS_SAMPLING=on` and
`BLACKBOX_MONITOR_METRICS_PATH`; registered matrix cells accept the boolean
`workload_rss_sampling`. Use the same setting in both GC comparison cells.

The joiner's temporary bootstrap map is released after successful bootstrap
approval when entering workload, regardless of either toggle. Validation
flags, prefix, counters, and bootstrap high-water metrics remain available;
current `bootstrap_state_rows/bytes` become zero. Engine state is preserved.

The monitor samples `Rss:` from Linux `/proc/self/smaps_rollup` immediately
before publishing `LOAD_FINISHED_SEEN`, then on a background thread with a
100 ms wait between readings, and once at termination before destroying the
checker. The window includes warmup, workload and final drain. It excludes
bootstrap processing but includes any resident allocator pages still retained
after clearing the map. Sampling and its overhead are disabled when off.

`process.max_rss_kib` remains the lifetime high-water value from `getrusage`.
The separate `process.workload_sampled_peak_rss_kib` is reported only if the
sampling window started and all readings succeeded; otherwise it is null.
`workload_rss_sampling_enabled/started/valid`, `rss_sample_interval_ms`,
`workload_rss_samples`, and `workload_rss_sample_errors` record its provenance.
The registered reporter emits `monitor_workload_sampled_peak_rss_mib` and
rejects requested sampling if it failed or never started. Matrix resume
rejects mismatched sampling settings. Short spikes between samples can be
missed; these measurements must not be labeled an exact interval RSS peak.

The input and consistency semantics are the same in both modes. The existing
fixed manifest roster and post-bootstrap phase anchors supply session starts;
there are no new database transactions or wire records.

## Algorithm

After every workload transaction, `CheckerAdapter` records its commit sequence
for its active session. Once **all** phase anchors have been consumed, it
computes the minimum of those sequences and calls
`SnapshotSOWWEngine::pruneHistoryThrough(frontier)`. Bootstrap's one-shot
synthetic sessions are excluded from the minimum.

The engine deletes write-history entries with `commit_seq <= frontier`. A delta
at F reconstructs state *before* F; retaining state *after* F as the oldest
candidate needs only deltas after F. Current snapshot values and latest-writer
positions are preserved. Read-only transactions advance session progress even
though they have no history entry. Committed writes still apply after a
consistency violation.

`RunController` forwards only a successfully validated `SessionDone` to the
joiner. The joiner retires that session only after all of its accepted client
transactions have been consumed in CDC order. Completion can arrive before CDC
or even before the anchors; retirement waits for both conditions. It uses a
pending-successful-transaction count, not `final_session_seq`, which may include
aborted transactions. Retirement consumes no commit sequence. With no active
sessions, the engine can discard all history through its last consumed sequence.

The core pruning API trusts the caller's proof of the frontier. It rejects a
regressing or unconsumed frontier, and subsequent consumes fail with
`HistoryUnavailable` if their SO/WW lower bound precedes the retained floor.
Errors are sticky and never become a consistency verdict. The generic online
runtime does not call pruning; this prototype enables it only at the black-box
boundary with the fixed, anchored session roster.

## Measurements and scope

Monitor metrics record `limits.checker.history_gc`, the engine's
`retained_floor`, `reclaimed_history_entries`, `reclaimed_history_deltas`, and
`reclaimed_history_bytes`, alongside the existing history-size and current/peak
retained-state metrics. `checker.history_gc_total_nanoseconds` records frontier
calculation and pruning separately from the existing engine-consumption timers;
end-to-end monitor wall/CPU measurements include all of this work.

Pruning releases history entries, their delta arrays, and value references.
When no current snapshot entry or retained delta owns a value, its payload is
freed and its ID slot recycled. Shared values remain alive until their last
reference is released. Key interning, current state, and evidence identity
tables are unchanged. An idle session can pin history until it advances or
completes; no fixed bound on total memory is claimed. The existing resource
limits still apply, including during bootstrap and while processing a
transaction before its subsequent GC step.

The engine reports current `interned_value_count/bytes` and cumulative
`reclaimed_value_count/bytes`. Repeated reclamation of a recycled ID counts
again. GC-off preserves history references and does not trigger value reclamation.

Set `BLACKBOX_MONITOR_MEMORY_PROFILE=on` to collect approximately 30-second
RSS, allocator, payload and identity-accounting samples. Final metrics also
include a unique-value reference census and container capacities under
`checker.engine.memory_profile`. The census runs after workload RSS sampling
stops; its temporary allocations do not contribute to the sampled peak. The
three value categories (snapshot-referenced, history-only, unreferenced) are
disjoint. Capacity/accounting bytes are not a decomposition of resident memory.
Profiling is off by default and reports its own sampling/census overhead.
