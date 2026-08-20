# Block API

Hyperfile's byte API (`fs_read` / `fs_write`) copies in both
directions: a read copies out of the cached block into your buffer, a
write copies your buffer into the cached block. That is the right shape
for a file consumer.

It is the wrong shape for a **block-storage** consumer — one that never
thinks in terms of "file content" at all, only "read block N, modify
block N, write block N back". Such a caller pays for a buffer it does
not want, and in practice ends up keeping a shadow copy of every block
it intends to modify so that it can write those copies out at commit
time. That staging layer is a recurring source of bugs: the copy
diverges from the original, or is written back after being released.

The block API removes the reason to have one. You borrow the block
hyperfile is going to flush, and modify it in place.

## Surface

On `Hyper` (direct API):

```rust
async fn fs_block(&mut self, idx: BlockIndex) -> Result<Option<BlockRef<'_>>>;
async fn fs_block_mut(&mut self, idx: BlockIndex, create: bool) -> Result<Option<BlockMut<'_>>>;
async fn fs_block_state(&mut self, idx: BlockIndex) -> Result<BlockState>;
```

On `HyperFileHandler` (reactor API) — closure-scoped, see
[below](#why-the-reactor-surface-takes-a-closure):

```rust
async fn fh_with_block<R, F>(&mut self, idx: BlockIndex, f: F) -> Result<Option<R>>
where F: FnOnce(&[u8]) -> R + Send, R: Send;

async fn fh_with_block_mut<R, F>(&mut self, idx: BlockIndex, create: bool, f: F) -> Result<Option<R>>
where F: FnOnce(&mut [u8]) -> R + Send, R: Send;
```

`f` may borrow the caller's locals — it is not `'static`. That is what
lets a caller copy straight out of a block into a buffer it already
owns, instead of returning an owned buffer and copying again. `Send` is
still required, because `f` runs on the reactor's thread.

Both forms address blocks by index, and the slice is always exactly
`data_block_size` bytes.

```rust
// Direct: insert a record into a directory leaf, in the very buffer
// the next flush will write out.
let mut blk = file.fs_block_mut(leaf_idx, false).await?.ok_or(EIO)?;
leaf_insert(blk.as_mut_slice(), hash, ino, name)?;
// dropping the guard is all; no write-back call

// Reactor: same thing, with the action sent to the block.
let name = name.to_owned();
file.fh_with_block_mut(leaf_idx, false, move |buf| {
    leaf_insert(buf, hash, ino, &name)
}).await?.ok_or(EIO)??;
```

## Dirty from acquisition, not from drop

`BlockMut` marks the block dirty when it is acquired. The next `flush`
persists it; there is no commit or write-back call. An early return or
a panic between acquire and drop therefore cannot lose a modification.

The flip side is that acquiring a `BlockMut` and not modifying anything
still produces a new version at the next flush, exactly as writing
identical bytes through `fs_write` would. Use `fs_block` when a
read-only borrow will do.

Borrowing the same block any number of times inside one flush window
produces exactly **one** new version, matching `fs_write`.

## Holes: three states, not two

Through the byte API, a hole, an explicit zero block and real data that
happens to be all zeros are indistinguishable — all three read back as
zeros. A caller that keeps its own index cannot then tell "this block
was never written" from "this block holds data that happens to be
zeros", which is the difference between a consistent index and a lost
block.

`fs_block_state` names the three:

| `BlockState` | meaning | reads as |
|---|---|---|
| `Unmapped` | no bmap entry; never written | zeros |
| `Zero` | mapped to an explicit zero block, from `write_zero`, a truncate, or an aligned zero batch. Occupies no staging space | zeros |
| `Mapped` | mapped to real data, dirty in cache or persisted in staging | its data |

`fs_block` returns `Ok(None)` for both hole flavors — there is no data
to borrow — and `fs_block_state` distinguishes them when it matters.

`fs_block_mut`'s `create` decides what happens for a block with no
data: `false` reports `Ok(None)`, `true` materializes a zero-filled
block. A created block is guaranteed zeroed on every cache tier.

## `i_size` is not moved

Neither guard changes `i_size`. Callers of this API manage their own
address-space layout and do not want a block write to shift EOF.

The consequence is worth understanding: a block dirtied above `i_size`
**is** flushed durably, because the dirty set drives segment build, but
`fs_read` will not return it, because `read` stops at `i_size`. Such a
block is reachable only through this API. `i_blocks` *is* updated, so
`st_blocks` accounts for the storage either way.

If you want EOF to move, call `fs_truncate`.

## Guards and flush

A guard borrows the file, so the borrow checker alone prevents any
other operation — including flush and eviction — for as long as the
guard is alive. There is no runtime pinning involved, and no lock to
forget to release.

The cost is that a guard cannot be held across an `await` on the same
file. Acquire it, use it, drop it.

`fs_block_mut` may flush on entry, if the dirty set is already over
threshold. It cannot flush on drop, because `Drop` cannot await. A
caller that dirties blocks only through this API and never calls
`fs_flush` will grow the dirty set without bound.

## Why the reactor surface takes a closure

`HyperFileHandler` is a channel handle: the `Hyper` lives inside a
reactor task, and requests cross an mpsc channel. A borrow guard cannot
cross with them.

The guard would point into a `DataBlock` owned by the reactor task's
cache, and the caller would receive it only *after* the response
arrives. From that moment the reactor is free to serve the next request
— from this handle or from any of its clones, since the handle is
`Clone`. A write, flush, truncate, or even another block load that
triggers eviction can then invalidate the borrow; blocks in the
local-disk cache additionally have their backing space punched when
evicted. There is no lifetime that expresses "valid until the next
message on this channel".

This is why `fh_read` works the way it does: it launders a raw pointer
across the channel, which is sound only because the caller stays parked
on the response for the entire time the reactor holds that pointer. A
guard has no such bound.

So the reactor surface inverts the direction: the action travels to the
block. `f` runs inside the reactor task while it holds the real borrow,
and only owned values cross the channel — hence `Send + 'static`. Move
what the closure needs into it, and return what the caller needs out.

`fh_with_block*` returns `Ok(None)` without running the closure when
the block has no data.

### Cancellation

Because `f` may borrow the caller's frame, it must not run once the
caller is gone. If the future is dropped before the reactor has run
`f`, `f` is skipped; if it is dropped while `f` is running, `Drop`
waits for `f` to finish. The wait is one closure body — the block is
already in hand by then and nothing awaits — so it is bounded and
cannot deadlock against the reactor's I/O.

The byte entry points on this surface need more care, because the
reactor touches the caller's buffer from inside the object-store
request rather than in one step afterwards:

| | cancel-safe | buffer |
|---|---|---|
| `fh_read` | **no** | caller's, borrowed for the whole request |
| `fh_write` | **no** | caller's; with `wal`, borrowed across a PUT |
| `fh_read_owned` | yes | allocated by the reactor, returned as `Bytes` |
| `fh_write_owned` | yes | owned by the request, a `Bytes` the caller hands over |

`fh_read` and `fh_write` are the zero-copy path and stay that way; use
them when the read or write will be allowed to finish. Use the owned
variants behind a `select!`, a `timeout`, or in a task that may be
aborted. `Bytes` is reference-counted, so handing one over or getting
one back costs nothing.

## Access mode

Both borrows honor the open access mode, like the byte API:

| | `O_RDONLY` | `O_WRONLY` | `O_RDWR` |
|---|---|---|---|
| `fs_block` / `fh_with_block` | yes | `EBADF` | yes |
| `fs_block_mut` / `fh_with_block_mut` | `EBADF` | yes | yes |
| `fs_block_state` | yes | `EBADF` | yes |

`EBADF` is not representable in `std::io::ErrorKind`; test for it with
`e.raw_os_error() == Some(libc::EBADF)`. See
[POSIX semantics](posix.md#access-mode-enforcement).

## Interaction with the data cache

The block API populates the data cache; the byte API does not. Both
read from it.

| entry point | reads the cache | populates the cache |
|---|---|---|
| `fs_read` / `fh_read` | yes | **no** |
| `fs_block` / `fh_with_block` | yes | **yes** |
| `fs_block_mut` / `fh_with_block_mut` | yes | **yes** |
| `fs_write` / `fh_write` | yes | while dirty, and kept after the flush only for a partially-written block |

So reading the same bytes twice through `fs_read` fetches them twice,
while borrowing the same block twice fetches it once. A byte read after
a block borrow of the same data is served from memory; a block borrow
after a byte read is not.

`read_timing` reports this directly: `data_gets` counts fetches and
`cache_hits` counts reads served from the cache. On the reactor surface
use `fh_read_timing`, which returns an owned snapshot because a
reference to the live counters cannot leave the reactor task.

Two configurations behave differently:

* **Data cache disabled** (`data_cache_blocks = 0`, which `O_DIRECT`
  without the `wal` feature forces): nothing is retained by anything.
  There is no cached buffer to borrow, so `fs_block` owns the block it
  loaded for the life of the guard, and the bytes are not kept for a
  subsequent call.
* **Local-disk data cache**: blocks are views into a memory-mapped
  file. Nothing about the block API differs, but this tier keeps a
  fixed pool of block slots, so a working set larger than the pool
  falls back to heap allocations.

## Relationship to the byte API

The two are interchangeable on the same file. A block dirtied through
`fs_block_mut` is an ordinary dirty block: it participates in the same
flush, the same segment, the same rollback, and the same checkpoint
semantics. Nothing about segment layout, WAL, or the cleaner changes.

The one asymmetry is `i_size`, described above.
