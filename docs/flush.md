# Flush

How a flush gets data onto staging, and what the front end is allowed to
do while that happens.

For how concurrent callers are serialized in general, see
[concurrency.md](concurrency.md). For what happens when two writers flush
the same file, see [`FlushConflictPolicy`](concurrency.md#flushconflictpolicy)
and the [OCC](concurrency.md#s3-optimistic-concurrency-control-occ)
section there. For the WAL itself — layout, recovery, cleanup — see
[wal.md](wal.md).

## What a flush does

A flush collects everything dirty, turns it into one segment, and writes
that segment out — which is what happens unless the container has already
written part of the dirty set out under memory pressure, for which see
[partial segments](#partial-segments-writing-out-without-a-consistency-point):

1. Collect the dirty data blocks and the dirty meta (bmap) nodes.
2. Serialize them into one contiguous segment buffer, allocating the next
   segid. The segment holds *copies* of the blocks, not views of them.
3. Repoint the bmap at the new segment, so the blocks it covers now
   resolve to locations inside it.
4. Write the segment out, then the inode. The inode write is what makes
   the checkpoint visible.
5. Clear the dirty lists and mark the checkpoint as on disk.

Hyperfile never overwrites a persisted segment, so this is append-only:
each flush produces a new checkpoint and the previous ones stay readable.

## Without WAL: the world stops

Steps 2 to 5 rewrite the bmap and drain the dirty cache underneath any
reader. There is nowhere else to get the data from while that happens —
the blocks are leaving the cache and are not on staging yet — so the
front end waits.

`state.is_flushing()` is set for the duration, and `spawn_read`, the
block read path, `spawn_write` and `spawn_write_zero` all defer while it
is set, putting their request back on the high-priority queue to retry
afterwards. A non-WAL flush also runs inline in the handler arm, so
nothing else runs on that task until it finishes.

## With WAL: the segment is pinned and the front end carries on

With WAL the data is already durable before the flush starts. That
changes what a flush *means*: its completion is a given, because a
failure is recovered by replaying the WAL rather than by unwinding what
the flush published. So the segment can be treated as though it were
already persisted while it is still being written out, and reads and
writes can carry on against it instead of waiting.

This is a large part of what writing the WAL first buys. Without it, the
WAL is pure cost on the write path with no benefit to anything else.

### The window

A WAL-protected flush does this, all on the handler task:

1. `flush_process_build_segment` serializes the dirty blocks into one
   buffer and repoints the bmap at the new segid.
2. `wal_set_mem_segment` registers a `Weak` to that buffer in
   `flushing_segments`, which is what keeps it reachable.
3. The segment write and the inode write are handed to a spawned task.
4. `set_last_cno(segid)`, then the dirty cache is cleared.
5. The arm returns. The handler is free.

and later, when the write-out finishes:

6. `wal_flush_done` advances `last_ondisk_cno` to the new segid and
   `wal_clear_mem_segment` drops the pinned buffer.

Steps 2 to 6 are the window. Inside it the newest data lives only in that
pinned buffer: the bmap points at a segment that is not on staging yet.

That the segment is a copy rather than a view is what makes the rest of
this safe — a write that modifies a block afterwards cannot alter the
bytes being written out.

### What the front end does inside it

**Reads** are planned as `ReadOp::Inmem` and copy straight out of the
pinned buffer. No object request, and no waiting for the flush. The
planner decides this on `segid > last_ondisk_cno`.

**Writes** work the same way for the block they need to modify: a partial
write is a read-modify-write, and `spawn_load_data_block_write_path` has a
branch that copies the block out of the pinned segment rather than reading
staging.

`last_ondisk_cno` is a sound test for "still pinned" because
`wal_flush_done` advances it and drops the buffer in the same arm, back to
back, so nothing on the handler task can observe one without the other.

A read planned as `Inmem` can still find the segment gone by the time it
runs, if the write-out finished first. That is not a failure: the same
bytes are on staging at the same offset, so the read reads them from
there. Both outcomes occur in a single run of the test that covers this.

### Why a write needs more care than a read

A WAL write is a multi-hop pipeline — the WAL write sits between the hop
that decides which blocks to fetch and the hop that applies the data — and
the handler task is free in between. So a write's two halves can straddle
a flush, and the flush can take a block the second half was about to
modify. Three things keep that correct:

- Blocks the write fetched travel on in the request and are installed in
  `absorb_write_bh` immediately before the data is applied, in the same
  arm. A block that is not in the cache is not a block a flush can take.
- A block that needed no fetch, because it was already dirty, can still be
  taken. `absorb_write_bh` notices it is no longer resident and refills it
  in place from the pinned segment — again no object request.
- If that is not possible, because the write-out finished first or the
  block was evicted rather than flushed, the request goes back for a
  staging read off the handler task. Awaiting an object request inline
  would stall every other request behind it.

Getting this wrong is quiet. `update_cache` fabricates a zero-filled block
when nothing is resident, which is right for a write covering a whole
block and silent data loss for a partial one: the bytes it covers survive
and the rest of the block reads back as zeroes.

### Failure handling inside the window

A staging read that fails is reported, not absorbed. The spawned loads
carry a `Result`, and a write whose block could not be read fails instead
of applying itself over a block that was never filled.

## Waiting for in-flight operations to drain

Under `range-lock`, a flush waits for operations already in flight before
it starts. The handler checks `range_lock.is_locked()` first, which is
"any range is held" rather than "any range conflicts with mine" — the
right test, since a flush covers the whole file and so conflicts with
everything. Note that reads take range locks too, so a flush waits behind
in-flight reads as well as writes.

That check is a drain, and it needs admission control to terminate.
`state.is_flushing()` stops *new* operations once a flush has begun, but it
cannot recall the ones already in flight. Deferring alone is not enough:
new reads and writes keep being admitted while the flush waits, so under a
steady stream of them the range map is never observed empty and the flush
is starved for as long as the traffic lasts. Six readers looping on one
handle were enough to starve a flush indefinitely.

So the wait is announced. On deferring, the flush sets a flush-pending
flag, and `spawn_read` / `spawn_write` / `spawn_write_zero` defer instead
of taking a fresh range lock while it is set. In-flight ranges drain, the
flush runs, the flag clears. Flush progress becomes guaranteed rather than
dependent on a gap in the arrival pattern, at the cost of a short stall for
operations arriving during the drain — bounded, because a flush completes.

The flag must be cleared on every path that stops waiting, including a
failed requeue; leaving it set would stall every subsequent operation.
That is also why nothing releases a range lock by panicking out of a
spawned task.

## What it costs

Reads gain a great deal from not waiting. In a test with one task writing
and flushing in a loop and another reading the same blocks, the reader
completes roughly 10,000 reads served from the pinned segment where
deferring managed 160 — a deferred read spends the window being requeued
and retried rather than answering.

Letting *writes* overlap has a cost to a concurrent reader, and it depends
on what a write holds while it overlaps:

| | reader on the writer's blocks | reader on other blocks |
|---|---|---|
| `wal` | much slower | **much slower** |
| `wal` + `range-lock` | much slower | **unaffected** |

Without `range-lock` the per-file semaphore has a single permit for the
*whole file*, and a write holds it across its WAL write, so block
disjointness cannot help: a reader on unrelated blocks dropped from 720
reads to 152 once writes stopped waiting for the flush. With `range-lock`
the permit is unbounded and a range is what excludes, so a reader
elsewhere is unaffected — 848 against 712, inside the run-to-run spread —
while a reader on the blocks being written still pays, as it should,
because it conflicts.

So a workload that reads and writes heavily through one handle wants
`range-lock`, and wants its readers and writers not to chase the same
blocks. These numbers come from a deliberately harsh shape — a writer
looping on eight blocks with the data cache off — and are worth taking as
the direction of the effect rather than its magnitude.

## Partial segments: writing out without a consistency point

A dirty-data threshold has always published. That ties two things that have
nothing to do with each other: how much a writer is holding, and where the
checkpoint history has a point in it. The caller gets a checkpoint it never
asked for, at a moment of its own work that it did not choose.

Where nothing may publish, the coupling is worse than untidy. Inside a
transaction, and throughout `WalRecoveryMode::Barrier`, a crossing is refused
with `OutOfMemory` — a checkpoint there would put a state nobody declared
beneath the floor recovery lands on. So a unit of work larger than the
threshold could not be written at all.

A **partial segment** is somewhere to put the data that is not a checkpoint.

### When one is written

Only when all of these hold:

- the container's format is `BlockPtrFormat::PartedSegment`, which is fixed
  when it is created — a partial is addressable only under that format
- `parted_segment_enabled`, which is on by default and exists to take the
  behaviour back on a container whose format allows it
- the dirty set is at least `data_cache_dirty_min_bytes_to_part`, because a
  partial smaller than the amount that triggers a write buys little memory
  for a lasting cost in fragmented reads
- one of the four non-consistency triggers fires: either dirty-data threshold,
  the flush interval, or the point that used to be an `OutOfMemory` refusal

Miss any of them and the trigger behaves exactly as it always did.

### What is written, and what is not

The object has the shape of any other segment — its own summary, its own
metadata blocks, its own data blocks, and the inode inline — so anything that
can parse a segment can parse one. A partial writes *every* dirty node, so the
bmap root in that inode reaches every block the checkpoint holds so far: a
partial is a whole state, not a fragment. Its header carries
`SEGMENT_FLAG_PARTIAL`, which is what tells a reader arriving with an object
name rather than a checkpoint number.

What is **not** written is the inode object. That write is the consistency
point, and without it a reader that opens the container normally still sees the
previous checkpoint.

**A partial is not a durability event.** A crash leaves its objects referenced
by nothing, and without a WAL the data in them is gone — no flush returned, so
no promise is broken, but "written out" must not be read as "durable".
Recovering to a partial means opening it by name, which belongs to a repair
tool: the caller's flush is the consistency point, and reading past it would
take that away. With a WAL nothing changes, because the writes were durable
before any of this.

### Completing the checkpoint

An explicit `flush`, `fdatasync`, `release` or `commit_txn` writes the
consistency point: the remaining data, the remaining metadata, the summary, the
inode inline, and then the inode object. It goes into part 0 of the same
checkpoint — a checkpoint is one number however many objects it took, so the
completion reuses the number the first partial took rather than allocating a
fresh one.

With no partial outstanding, that writes exactly the single segment it always
did. This is the shape on purpose: partials appear only where memory pressure
put them, and the ordinary case is one object per checkpoint.

### The one ordering that matters

Metadata pointers are assigned before the nodes are serialized, because a
parent has to contain its children's pointers. So the data in the same object
cannot be uploaded before its pointers are assigned either.

What protects a reader instead is that **a block does not leave the dirty tier
until its upload has landed**. Until then a read finds it in the cache and
never resolves the pointer; afterwards the pointer resolves to bytes that are
there. Moving that step earlier would open exactly the window a single-object
WAL flush keeps a pinned buffer for.

### What partials cost

Writing the metadata early means writing some of it twice: a node written into a
partial and then dirtied again is written again, and the earlier copy is
unreferenced. For a write that moves forward through the file — a large repair
pass, the case this exists for — leaf nodes finalize as the write passes them and
the duplication is close to nothing. For scattered writes the same nodes keep
being dirtied and the duplication grows with the number of partials.

On the read side, each partial is a place a run of blocks breaks; see
[placement.md](placement.md#why-the-two-are-not-the-same-question).

### Interactions

| | |
|---|---|
| a failed flush's rollback, or `abort_txn` | discards the accumulated checkpoint; its partials become unreferenced |
| `publish_every` | does not defer while a checkpoint is pending — deferring says no checkpoint is needed, and partials on storage say one was started |
| the part index | 14 bits, so 16384 partials to one checkpoint; reaching it is refused, not wrapped |
| a threshold crossing during recovery | skipped, because recovery replays through the write path while holding the flush lock |

## Related tests

- `reactor_wal_read_block_in_segment_still_uploading` — a reader against a
  task writing and flushing in a loop, data cache off so reads have to go
  through the planner. Asserts on the `inflight_reads` counter rather than
  on throughput, because correctness cannot tell a read served from the
  pinned segment from one that waited for the flush and then read staging.
- `reactor_wal_write_during_flush_keeps_the_untouched_bytes` — a partial
  write landing while a flush is in flight. Checks the bytes the write did
  not cover, which is where a block rebuilt from nothing shows up.
- `reactor_write_reports_a_failed_read_modify_write` — a write whose block
  cannot be read must fail rather than persist zeroes, and must leave the
  handle usable.
- `integration_reactor_s3_contention` — the drain and its admission
  control, among other things. Run it under `range-lock` as well as
  default features; the two exercise different mechanisms.
- `integration_s3_parted_segment` — the only suite that produces a partial
  segment, since it needs a format and a switch no other container uses. A
  threshold crossing writes one and publishes nothing; the flush after several
  of them reads back every byte; a partial is marked and an abandoned checkpoint
  is not openable; and with the switch off the same workload publishes as
  before.
- `reactor_wal_partial_segments_still_recover_from_the_log` — the two mechanisms
  composed. A partial relieves memory and is not durable; the log is what makes
  the writes durable. What a crash after partials has to produce is every
  acknowledged write, reached through the log.
