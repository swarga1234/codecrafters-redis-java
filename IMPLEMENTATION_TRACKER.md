# Redis 6.x I/O Architecture Implementation Tracker

**Implementation status reviewed:** 2026-10-06 01:56:11 IST (+05:30)

**Target contract:** The source-derived Redis 6.x architecture in `REDIS_ARCHITECTURE_VALIDATION.md` and the user's verified source notes. Java-specific synchronization and implementation choices must remain clearly distinguished from Redis internals.

**Working rule:** Guide-only unless explicitly asked to edit code. Preserve existing abstractions when they satisfy the contract; change behavior/ownership, not names for their own sake.

## Current Baseline
- [ ] Main-thread `Selector` handles accept/read/write readiness and owns current `SelectionKey` operations.
	- **Implementation note (2026-10-04 03:03:44 IST):** `EventLoop` handles readiness and most interest-op changes, but `ParseTask` cancels its `SelectionKey` on EOF and read errors. Main-thread-only ownership is not yet satisfied; this is unfinished alignment work, not an intentional deviation.
- [x] Later writable readiness is routed to `ResponseWriter.handleWrite()` from the main event loop.
	- **Current write-test status (2026-10-06):** `EventLoopFreshWritesTest` verifies later writable readiness: a constrained loopback send buffer causes `OP_WRITE`, the exact 1 MiB ECHO response is drained, and `OP_WRITE` clears.
	- **Historical note (2026-10-04):** A dedicated automated selector-interest test had not yet been added; see the current write-test status above.
- [ ] Read path matches Redis batches: current `EventLoop` submits one `ParseTask` per readable event to an executor; tasks parse and publish to a global queue.
	- **Implementation note:** This per-event task/global-queue design remains current. Persistent helper threads receiving temporary round-robin batches are not implemented; no intentional deviation has been recorded.
- [ ] Historical baseline wording (superseded by the current write-status note): Fresh replies match Redis pending-write batches: current `CommandDispatcher` calls `tryWriteAndQueue()` immediately and enables `OP_WRITE` when needed.
	- **Current write status (2026-10-06):** The baseline wording is historical. `CommandDispatcher` enqueues, flags, and adds successfully queued clients to an event-loop-owned `LinkedHashSet`; the loop flushes and clears it before the next select. This completes the first-slice separation from later `OP_WRITE`, but not the full Redis temporary I/O-helper batch architecture. Persistent helpers and temporary batches remain unimplemented.
	- **Implementation note:** The baseline wording above is historical. Current `CommandDispatcher` enqueues the reply and sets a per-client fresh-write flag; `EventLoop` scans flagged clients and flushes them. This is not a temporary Redis-style batch shared with persistent helpers. No intentional reason for this difference has been recorded.
- [ ] Persistent helper threads receive temporary read/write batches; current executor tasks are event-driven per-client submissions, not Redis-style batches.
	- **Implementation note:** Persistent helpers, temporary batches, and their completion/error/shutdown coordination are not implemented; this is outstanding work, not a selected Java-specific alternative.
- [ ] Threaded reads are optional and conditional on I/O threads being active.
	- **Implementation note:** Reads currently run as executor-submitted `ParseTask`s. Conditional activation is not implemented; no default/configuration decision has been made.
- [ ] Current incremental parser/input limits and output accounting have been validated against the Redis 6.x target.
	- **Implementation note:** The normal write loop stops after total bytes written exceed approximately 64 KiB; a single write may carry it past the threshold. Redis 6.0 input/output defaults and fairness exceptions are recorded in the source-validated section below; Java enforcement/configuration is not yet validated, and the output-byte enqueue cap remains commented out.
- [ ] Automated tests: no `src/test` files were found during baseline inspection.
	- **Current test status (2026-10-06):** The checkbox text is historical. `WriteQueueManagerTest` and `EventLoopFreshWritesTest` provide focused write coverage; `ParseTaskReadPathTest` covers fragmented input, pipelining, buffer growth, and zero-byte reads. The focused write command passed 3 tests. Its capacity-one saturation case is deliberately constrained and currently fails its blocking expectation; this does not establish a defect at the production 10,000-command threshold. Production-threshold or near-threshold validation remains open.
	- **Implementation note:** This remains a historical baseline observation. `WriteQueueManagerTest` now covers a scripted partial write, zero write, and resume, including buffer position, outstanding-byte accounting, queue state, and output. Focused and full `mvn test` passed (1 test, 0 failures/errors). The scripted `WritableByteChannel` is a testability seam; production `SocketChannel` use remains compatible.

## First Slice

**Separate fresh replies from later writable readiness.**

**Historical first-slice requirement (wording retained):** Today `CommandDispatcher.sendResponse()` attempts the socket write directly. First guide the change so command execution appends the reply to per-client output state and records that client in a main-thread-owned pending-write collection. A before-sleep-style batch can then attempt writes. Keep `ResponseWriter` for later `OP_WRITE` readiness; do not route those readiness callbacks back into the worker batch.

**Historical implementation note (2026-10-04 03:03:44 IST):** The paragraph above is preserved as the original first-slice requirement. The implementation status recorded at that time was superseded by the 2026-10-06 first-slice review below.

**Superseding implementation status (2026-10-06 01:32:06 IST):** The first-slice behavior is implemented: `sendResponse()` enqueues and registers the client in the main-thread-owned fresh-write set; the loop attempts the write before the next select; a partial write enables `OP_WRITE`; later readiness resumes through `ResponseWriter`. `EventLoopFreshWritesTest` covers deduplication, batch clearing, and this partial-write lifecycle. The older note above is retained as history; persistent helper batches remain future work.

Before implementation, define a narrow observable check for both paths:

1. A freshly generated PING/ECHO reply is included in the pending-write batch and is flushed without waiting for a later readiness notification when the socket accepts data.
2. When the socket accepts only part of a reply, the remainder and buffer position survive; main enables `OP_WRITE`.
3. A later writable event resumes through the direct `ResponseWriter` callback and removes `OP_WRITE` when drained.

This is the best first slice because it addresses a concrete mismatch visible in the current code and protects the important distinction between the two Redis write paths. Do not build the whole worker framework before validating this boundary.

## Work Items and Provisional Estimates

Estimates are focused engineering time, not elapsed calendar time. They assume the existing parser and managers can be retained. Re-estimate after the first slice and after worker synchronization design is chosen.

| ID | Work item | Status | Estimate |
|---|---|---|---:|
| 0 | Add/choose behavior checks for current PING/ECHO, fragmented input, partial output; record baseline | In progress | 2-4 h |
| 1 | Split fresh-reply pending-write batching from direct later `OP_WRITE` callback | Complete | 2-4 h |
| 2 | Add persistent helper activation, temporary round-robin batches, completion/error/shutdown coordination | Not started | 4-8 h |
| 3 | Route pending-write batches through main + helpers; preserve output queues and partial writes | Not started | 2-5 h |
| 4 | Convert readable events to pending-read batches; make threaded reads optional/conditional; workers prepare the first pending command while main-thread execution may continue through buffered commands | Not started | 3-6 h |
| 5 | Verify per-client query-buffer ceiling, large-argument copy behavior, output accounting/limits, and normal-client defaults | Not started | 2-5 h |
| 6 | Stress and regression checks: pipelining, concurrent clients, slow readers, large input/output, disconnects, fairness | Not started | 3-6 h |
| 7 | Reconcile `ARCHITECTURE_COMPARISON.md` with the Redis 6.x contract and mark unsupported alignment/performance claims | Not started | 1-2 h |

**Historical progress note (2026-10-04 03:03:44 IST):** At that point item 0 was in progress, fragmented-input coverage and a complete baseline record remained open, and items 1-7 were not started. See the current progress snapshot below for superseding status.

**Current progress (2026-10-06 01:56:11 IST):** Item 0 is in progress; item 1 is complete for the first-slice lifecycle; items 2-7 remain not started. Existing focused tests cover fragmented input, pipelining, and partial writes. The capacity-one test is a constrained stress case; production-threshold or near-threshold saturation coverage remains open.

**Provisional total:** 17-40 hours, including adding validation where no test suite currently exists. This range is deliberately broad; the worker-coordination and test-harness choices dominate uncertainty. The existing `ClientConnection`, `WriteQueueManager`, parser, and `ResponseWriter` are not presumed to need replacement.

## Validation Gates

- [x] Fresh pending-write batch and later writable callback are distinct and both work.
	- **Status (2026-10-06):** `EventLoopFreshWritesTest` verifies one set entry per client, fresh-batch clearing, `OP_WRITE` after partial output, and later `ResponseWriter` drain/interest-op clearing. This closes the first-slice gate only; Redis helper-thread batches remain open.
- [ ] Commands are executed sequentially on the main thread only.
	- **Status:** Checked in current code path: `EventLoop` drains parsed commands and dispatches them sequentially. Parsing/socket reads remain executor tasks.
- [ ] Persistent helpers do I/O only and receive temporary batches; no worker-owned Selector.
	- **Status:** Open. Persistent helpers/batches do not exist, and `ParseTask` currently receives a `Selector` and cancels its key on EOF/read errors.
- [ ] Main thread alone changes selector registrations and interest operations.
	- **Status:** Open for the same worker-side key-cancellation reason noted above.
- [ ] Partial RESP input, pipelined commands, and partial writes preserve state correctly.
	- **Status (2026-10-06):** Fragmented RESP input, pipelined commands, and partial writes have focused tests. The capacity-one saturation case is deliberately constrained and does not validate the production 10,000-command threshold; production-threshold or near-threshold coverage is still needed, so this combined gate remains open.
- [ ] Write turns follow Redis's normal ~64 KiB fairness threshold, with Redis 6 exceptions documented where applicable.
	- **Status:** The normal loop stops after `totwritten > NET_MAX_WRITES_PER_EVENT`, so a single write can take the total somewhat past ~64 KiB; this is not a hard cap at exactly 64 KiB. Redis bypasses the fairness stop when over `maxmemory` or for replica/monitor-style clients. Java behavior and these exceptions still need validation/documentation, so this gate remains open.
- [ ] Query and output limits match the chosen Redis-compatible configuration; Java-specific differences are explicit.
	- **Status:** Open; the enqueue byte cap is commented out and the compatibility review is incomplete.
- [ ] No deadlock, task-loss, or busy-spin under empty batches, errors, shutdown, or slow clients.
	- **Status:** Slow-reader behavior was exercised end-to-end, but the broader empty-batch/error/shutdown/fairness stress gate remains open.

## Architectural Doubts / Source-Validated Answers

This section records the user's source-validated Redis 6.0 findings from `src/networking.c`, `src/server.c`, `src/server.h`, and `src/config.c`. Treat these answers as the authoritative architectural constraints for subsequent work; distinguish Java synchronization choices from Redis mechanisms.

1. **Is the write path fully aligned with Redis 6.0?**
	- No. The first-slice lifecycle is aligned: command execution queues output; the main thread owns the fresh-write collection and attempts an initial flush before the next select; partial output enables `OP_WRITE`; later writable readiness drains directly through `ResponseWriter`.
	- Full alignment still requires temporary `clients_pending_write`-style batches shared across the main thread (thread 0) and persistent I/O helpers. Describe the current state as **write lifecycle aligned, threaded write batching not yet aligned**.
2. **Should later `OP_WRITE` readiness be put back into the pending-write batch?**
	- No. Fresh replies get an initial batch flush; if bytes remain, install a writable handler. Later writable readiness drains directly through the response writer and is not re-enqueued into the fresh batch.
3. **Should helper threads own a Selector or event loop?**
	- No. The main event loop owns readiness polling. Persistent helpers process temporary batches assigned by the main thread; per-worker selectors would be a different architecture.
4. **Are clients permanently assigned to workers?**
	- No. Redis distributes each pending batch round-robin (`target_id = item_id % io_threads_num`). Assignment is temporary; thread 0 is the main thread and processes its own slice.
5. **Should the executor-per-readable-event model remain?**
	- Not if Redis 6.0 parity is the target. It is a valid Java design, but Redis accumulates pending reads and distributes temporary batches to persistent helpers. Replace per-event `ParseTask` submission only after helper/batch coordination is stable.
6. **Do threaded reads always happen when enabled?**
	- No. Redis 6.0 postpones a normal client's read only while I/O threads are active, threaded reads are enabled, clients are not paused, the server is not in the special blocked-event-processing path, and the client is not a master/replica/already-pending-read client. Otherwise reads may be handled synchronously on the main thread.
7. **Should a worker parse or execute multiple commands from one client?**
	- Workers never execute commands. Threaded reads accumulate socket data and prepare the first pending command in client state; after read helpers finish, the main thread executes it and may continue through buffered input. Preserve the worker/main execution boundary; do not impose an artificial parser API solely to return one command.
8. **Who may mutate `SelectionKey` or registration state?**
	- Main thread only for this Redis-like ownership model. `ParseTask` cancellation on EOF/read error remains an unfinished mismatch. Workers should report EOF/error/close intent; the main thread should cancel keys and change interests or registrations.
9. **Is Phaser or CountDownLatch part of Redis architecture?**
	- No. Redis uses its own pending counters/atomics and thread activation/waiting synchronization. Java primitives such as `Phaser` or `CountDownLatch` may implement equivalent batch-completion coordination, but must not be described as Redis mechanisms.
10. **Should I/O helpers always be active?**
	 - No. Redis 6.0 can stop threaded I/O for small workloads. `stopThreadedIOIfNeeded()` uses pending writes as the workload signal and falls back to main-thread handling when `pending < io_threads_num * 2`. Implement helper correctness and batching first; add adaptive activation afterwards.
11. **Is the 64 KiB write-turn limit absolute?**
	 - No. `NET_MAX_WRITES_PER_EVENT` is approximately 64 KiB. Redis checks `totwritten > NET_MAX_WRITES_PER_EVENT`, so the loop normally stops after exceeding the threshold, not at an exact hard cap; a single write can take it somewhat farther. Redis bypasses the fairness stop when over `maxmemory` or for replica/monitor-style clients. Keep the threshold and exceptions distinct in Java behavior and documentation.
12. **Should a normal-client output-byte cap be invented?**
	 - No. Redis 6.0's default output-buffer limits are:
	   - Normal clients: hard `0`, soft `0`, duration `0` (limits disabled).
	   - Replicas: hard `256 MiB`, soft `64 MiB` for `60 s`.
	   - Pub/Sub clients: hard `32 MiB`, soft `8 MiB` for `60 s`.
	 - A hard limit closes immediately; a soft limit closes only after remaining above its threshold for the configured duration. Preserve these defaults when claiming Redis-compatible behavior; Java-specific configured deviations should be explicit.
13. **Which Redis 6.0 input constants are source-validated?**
	 - `PROTO_MAX_QUERYBUF_LEN = 1 GiB`, `PROTO_IOBUF_LEN = 16 KiB`, `PROTO_INLINE_MAX_SIZE = 64 KiB`, and `PROTO_MBULK_BIG_ARG = 32 KiB`. Large bulk values are accumulated before datastore mutation; Redis can reuse query-buffer storage for large arguments to avoid an extra userspace copy.
14. **Does the capacity-one read-path test represent production behavior?**
	 - No. It is a deliberately constrained stress case, not evidence that the production 10,000-command threshold is incorrect. Keep it labeled as constrained and add a separate production-threshold or near-threshold test before treating saturation as an architectural blocker.
15. **What should future refactors optimize for?**
	 - Preserve existing working abstractions (`ClientConnection`, the segmented output queue, `ResponseWriter`, and parser) when they satisfy the invariants. Change ownership and scheduling first; do not rename or rebuild components merely to mimic Redis C names.

### Source-Validated Implementation Order

1. Keep the completed fresh-write versus later-`OP_WRITE` split.
2. Enforce main-thread-only `Selector` and `SelectionKey` mutation, including moving worker close/error handling to the main thread.
3. Add persistent helper-thread infrastructure and Java batch-completion coordination.
4. Route fresh pending-write batches round-robin across main/thread 0 and helpers.
5. Replace per-event read tasks with optional/conditional pending-read batches.
6. Add Redis-like adaptive I/O-thread activation/fallback.
7. Validate input/output limits, fairness exceptions, and slow-client behavior.
8. Continue higher CodeCrafters modules on top of this base.

## Decisions to Record

- Java batch-completion primitive and why it is appropriate (not described as Redis's primitive).
	- **Status:** Not selected; batch/helper implementation is not started.
- Whether threaded reads are enabled by default or configurable.
	- **Status:** Not decided; conditional threaded reads are not implemented.
- Test strategy for deterministic partial reads/writes and large buffers.
	- **Status:** Partial-write behavior uses a scripted `WritableByteChannel` test; `ParseTaskReadPathTest` covers fragmented input, pipelining, buffer growth, and zero-byte reads. Production-threshold queue-saturation coverage remains open. Recorded CLI checks include 1 MiB binary ECHO, 8 MiB slow-reader ECHO with matching digest/trailer, and `redis-cli --pipe` through 700 commands. Exact CLI command lines/output were not retained in the previous log entry; future runs should record both.
- Any intentional Java-specific deviation from Redis 6.x behavior.
	- **Status:** No intentional deviations have been approved. The differences above are incomplete implementation unless a later dated decision says otherwise.

