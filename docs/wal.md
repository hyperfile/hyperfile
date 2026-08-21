# Write-Ahead Log (WAL)

This doc describes Hyperfile's optional write-ahead log and what
durability / consistency guarantees it offers. It complements the
`range-lock` / conflict-policy discussion in `concurrency.md` by
covering the crash path.

WAL is gated behind the `wal` Cargo feature and configured via
`HyperFileWalConfig`.

## Why WAL

Without WAL, a write goes to an in-memory cache. It is **not**
durable until a flush successfully persists the inode to S3. If
the process crashes between the write and the flush, the write is
lost.

With WAL enabled, every write is persisted to a separate S3 prefix
before its ack. On crash-and-reopen, the recovery path replays
those entries into the file. The caller observes a simple
"`fh_write` returned `Ok` = durable" contract.

## Durability contract

With `wal` enabled:

- `fh_write` / `fh_write_zero` / `HyperFileTokio::write` return
  `Ok` only after the corresponding WAL object PUT to S3 has
  completed.
- A crash after `Ok` keeps the write on S3 as a WAL object and
  recoverable on next open.
- A crash before `Ok` may or may not have landed on S3 — the caller
  should treat it the same as any other pending `Result`.

This is stronger than the no-WAL baseline, where "`fh_write`
returned `Ok`" only means the data is in the in-memory cache and a
crash erases it.

## On-disk layout

```text
<wal_root>/
    0000000000/
        0_0_4096          # seq 0, offset 0, length 4096
        1_4096_4096       # seq 1, offset 4096, length 4096
        2_8192_512        # seq 2, offset 8192, length 512
    0000000001/
        0_0_4096
        1_16384_4096
```

- **Top-level directory**: zero-padded segment id (the inode's
  `last_seq` at the time of the WAL write, **not** the next flush's
  segid — see "Indexing" below).
- **Object name**: `<seq>_<offset>_<len>`. `seq` is monotonic per
  segid, reset to zero whenever the segid changes.
- **Body**: the raw bytes of the write. A zero-length object
  represents `fh_write_zero` (hole punch).

`seq` is tracked in memory (`S3Wal::seq`, an `AtomicU64`). It is
not recovered from S3 — the invariant is that the writer owning
the S3Wal is the only process writing to this prefix, so the
in-memory counter is authoritative for the lifetime of that
handle.

### Indexing nuance

WAL chunks for flush N are indexed under **segid N - 1** (the
inode's `last_seq` at the time of the write). On flush, the code
calls `get_next_seq()` which bumps `last_seq` to allocate the new
segment's segid = N; the WAL chunks that produced segment N
remain under segid N - 1. This is the reason
`HyperFile::wal_flush_done` deletes `segid.saturating_sub(1)`.

If you grep for `delete_segment`, this is the subtle detail to
keep in mind.

## Recovery

`HyperFile::do_open` checks the WAL on every open:

```text
let wal_max_segid = wal.list_segments().iter().max();
if wal_max_segid >= inode.last_seq {
    trigger wal_flush_recovery
}
```

`wal_flush_recovery` walks all segids `>= inode.last_ondisk_cno`
(anything whose data has not yet been persisted in the inode),
calls `wal_replay_chunks` for each in ascending order. Replay
reads each chunk's bytes from the WAL, calls the equivalent of
`fs_write` / `fs_write_zero` on the reopening `HyperFile`, then
does a force flush to produce a new segment. After the force flush
succeeds, the replayed WAL segment is scheduled for deletion (as
a fire-and-forget `tokio::spawn`).

Recovery is idempotent: if it runs and is interrupted partway
through, the next open starts from scratch and replays the same
set of segids again. The replay is deterministic because `seq`
gives a total order of writes within a segid.

## Flushing without stopping the front end

Because the WAL makes a flush's completion a given, the flush does not
have to block the front end: the segment stays pinned in memory while it
is written out, and reads and writes are served from it rather than
waiting. That is the main thing the WAL buys beyond durability, and it is
described in [flush.md](flush.md#with-wal-the-segment-is-pinned-and-the-front-end-carries-on)
together with what a write has to do differently and what it costs a
concurrent reader.

## Cleanup (WAL object delete)

After a successful flush (in the reactor WAL path,
`wal_flush_done`), the WAL chunks for the flushed segid are no
longer needed and are deleted from S3 asynchronously via
`tokio::spawn`. Three properties follow:

- **Non-blocking**: the caller's `fh_flush` ack does not wait for
  the delete to complete.
- **Tolerant**: a dropped or failed delete does not affect
  correctness. Recovery filters by `last_ondisk_cno`, so stale
  WAL chunks are skipped rather than replayed twice.
- **Best-effort**: if a lot of flushes happen and one spawned
  delete task lags, storage usage temporarily grows. It is
  eventually consistent with "one WAL prefix per unflushed
  segment".

## Create-time invariant

`Hyper::create` with `wal` enabled returns
`ErrorKind::ResourceBusy` if the WAL prefix is not empty. The
rationale: creating a fresh file while the WAL holds stale entries
would cause `open` to replay old data into the new file, which is
almost certainly not what the caller wants.

Either unlink or explicitly empty the WAL prefix first. Typical
application code uses `Hyper::fs_unlink(uri)` which removes the
whole `<uri>/...` tree including WAL.

## Performance impact

- **Write**: extra S3 PUT per write. On S3 Express One Zone this
  adds roughly 5-10 ms per write depending on payload size.
  Large writes may be more costly (the WAL PUT is one S3 object
  per write, regardless of payload).
- **Flush**: no direct overhead. The delete that happens after
  flush runs off the critical path, and the segment upload does
  too — reads and writes carry on against the pinned segment while
  it happens, see [flush.md](flush.md).
- **Open**: adds a `list_segments` call. On a recently-flushed
  file the list is empty. On crash recovery, the open pays the
  replay cost proportional to the number of unflushed writes.

If your workload is dominated by many small writes with rare
flushes, WAL amortizes the flush overhead in exchange for the
per-write PUT. If your workload is dominated by large bulk writes
with synchronous flushes, the extra PUT may be pure cost —
measure.

## Current limitations

- **Single-writer**: Two `Hyper` instances open on the same URI
  with WAL both write into the same `<wal_root>` prefix and
  allocate `seq` independently. Key collisions are possible. Use
  WAL from a single writer at a time.

## Related tests

- `reactor_wal_smoke_write_flush_reopen` — happy path round trip.
- `reactor_wal_flush_and_reopen_is_idempotent` — reopen after
  flush does not pick up stale WAL.
- `reactor_wal_delete_after_flush` — WAL objects are actually
  removed from S3 after a reactor flush.
- `direct_api_wal_delete_after_flush` — same, but via
  `Hyper::fs_*` direct API.
- `reactor_wal_crash_recovery_replays_unflushed_write` — drop
  handler without flush, reopen, verify the write is restored.
- `reactor_wal_crash_recovery_multiple_disjoint_writes` — two
  sequential writes survive crash.
- `reactor_wal_crash_recovery_multiple_handles` — multiple
  handler clones with concurrent writes via `tokio::join!`,
  survive crash.
- Tests covering reads and writes during a flush are listed in
  [flush.md](flush.md#related-tests).
- S3Wal unit tests in `src/wal/s3.rs` cover `next_seq`,
  `reset_seq`, `encode`, and `decode`.
