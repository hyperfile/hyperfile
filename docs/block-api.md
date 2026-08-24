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

### Concurrency

The reactor's handler task takes `&mut self` and runs one request at a
time, so anything it awaits serializes every other request. Whether an
entry point fetches on that task or off it therefore decides whether
concurrent callers overlap:

| | fetch runs | concurrent callers overlap |
|---|---|---|
| `fh_read` / `fh_read_owned` | off the handler task | yes |
| `fh_with_block` | off it, when the block has to be fetched | yes |
| `fh_with_block_mut` | on it | no |

A `fh_with_block` that hits the cache is served on the handler task,
which costs nothing to overlap because it awaits nothing. A miss is an
object-store round trip, and moves off: the block it fills is owned
rather than borrowed from the file, so the load, the closure and the
gate travel into a spawned task together. The filled block is handed
back afterwards to be cached, which is what keeps repeated block access
at one request rather than one per access.

`fh_with_block_mut` stays on the handler task deliberately. It dirties
the block, installs a block-map placeholder and joins the next flush;
those are serialized against everything else, and overlapping them is a
much larger question than overlapping a read.

What a reactor round trip costs, and when that cost matters, is in
[Concurrency semantics](concurrency.md#what-the-reactor-costs). The
short version: a cache hit through `fh_*` costs far more in channel
latency than the access itself, and the way out is to keep requests in
flight rather than to issue them one at a time.

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

The block API populates the data cache; the byte API does not, unless
asked to warm a range explicitly. Everything reads from it.

| entry point | reads the cache | populates the cache |
|---|---|---|
| `fs_read` / `fh_read` | yes | **no** |
| `fs_read_ahead` / `fh_read_ahead` | yes | **yes** |
| `fs_block` / `fh_with_block` | yes | **yes** |
| `fs_block_mut` / `fh_with_block_mut` | yes | **yes** |
| `fh_with_blocks` | yes | no — reports what is not cached |
| `fh_read_many` | yes | **yes** — fetches what is not cached |
| `fs_write` / `fh_write` | yes | while dirty, and kept after the flush only for a partially-written block |

So reading the same bytes twice through `fs_read` fetches them twice,
while borrowing the same block twice fetches it once. A byte read after
a block borrow of the same data is served from memory; a block borrow
after a byte read is not.

`fs_read_ahead` is the byte path's way into the cache, for a caller that
knows what comes next. It is worth having as its own entry point rather
than warming through the block API: a block borrow costs one channel
crossing each on the reactor surface, so warming a megabyte that way is
hundreds of crossings, while `fh_read_ahead` takes one for the whole
range and coalesces the requests underneath it. Nothing is returned —
the read that wants the bytes asks normally and finds them.

`read_timing` reports this directly: `data_gets` counts fetches and
`cache_hits` counts reads served from the cache. On the reactor surface
use `fh_read_timing`, which returns an owned snapshot because a
reference to the live counters cannot leave the reactor task.

Read-ahead's own requests are counted twice over: in `data_gets` with
everything else, and again in `read_ahead_gets`, which is a subset rather
than a separate total. A read's own requests are therefore
`data_gets - read_ahead_gets`, and the same holds for the byte counters.

That subtraction is the only way to tell the two apart. The counter is
incremented where the request is made, and staging sees a ranged load with
nothing about its purpose — so a measurement that leaves read-ahead on
cannot otherwise attribute what it cost, and turning read-ahead off measures
a different system. A consumer comparing two files of identical layout found
one costing twice the requests of the other, and could not establish whether
they were measuring the layout or how much read-ahead each file attracted.

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

## Visiting many blocks at once

On the reactor surface the cost of block access is the channel crossing,
not the copy. A crossing is around 16 µs when the queue is empty; copying
a 4 KiB block is a fraction of one. So a caller touching hundreds of
blocks — a directory listing reading inode records, say — spends nearly
all its time in the channel, and zero-copy would not help it.

`fh_with_blocks(&[BlockIndex], f)` takes one crossing for the whole batch,
calling `f(blk_idx, Some(bytes))` for each resident block and
`f(blk_idx, None)` for one that is not. Measured on 300 warm blocks:

| | time | per block |
|---|---|---|
| `fh_with_block` in a loop | 4.87 ms | 16.2 µs |
| `fh_with_blocks` once | **23.9 µs** | 0.08 µs |

Nothing in a batch reaches staging, which is what keeps it cheap: an index
that is not cached is reported absent rather than fetched, so the handler
task answers the whole batch without awaiting anything. The pairing is
`fh_read_ahead` first, which warms a range with its object requests
coalesced, and then `fh_with_blocks` — two crossings for a region however
many blocks it holds.

`fh_with_blocks` never reaches staging, so an index that is not cached is
reported absent. `fh_read_many` takes the same shape of list and fetches
those instead. Use the first to look at what is in hand, the second to read
a list whatever its state.

Both cost one crossing. Where `fh_read_many` runs its closure depends on
whether anything has to be fetched, because the two cases want opposite
things:

- **Nothing missing.** The closure runs on the handler task, borrowing
  straight out of the cache, and the answer goes back from there. No copies.
- **Something missing.** The fetch has to leave the handler task, so the
  closure follows it and runs where the fetched bytes are. Blocks that
  *were* resident are copied, since a spawned task cannot borrow the cache
  — noise next to the object request being waited on.

The second case also keeps an expensive closure from stalling everything
else. The first does not: it still occupies the handler, so a batch closure
should stay cheap and carry its work out rather than doing it inside.

Doing this as two steps instead — `fh_read_ahead` then `fh_with_blocks` — is
two round trips where the second waits on the first, and it has to name a
range when a directory walk's list has gaps.

Whether a batch beats reading blocks one at a time depends on the shape, and
not always the way intuition suggests. Measured over 300 cold blocks asked
for as 100 lists of 3 — a listing's shape, where the kernel asks for a
window at a time:

| | time | object requests |
|---|---|---|
| one block at a time, concurrently | 488 ms | 300 |
| `fh_read_many` per list | **445 ms** | **100** |

The wall-clock gain is small because a caller that dispatches concurrently
has already overlapped the latency; what a batch saves there is requests.
A batch that costs *more* crossings than the list has blocks loses, which an
earlier two-crossing version of this did on exactly this shape.

A list with gaps raises a question worth answering explicitly: fetch the
gaps, or split into one request per run? On an object store the request
costs far more than the bytes a gap spans, so runs are merged while the gap
stays under what one request may cover (`read_get_max_bytes`), and
`plan_read` then decides how the merged range breaks up. Measured over 38
blocks scattered every eighth across a cold file: one request per run took
32 ms, a single merged request 28 ms, and one block at a time 197 ms.

There is no direct-API equivalent of either, and there is no point in one:
`fs_block` costs 0.02 µs, so a loop over it is already what a batch would
be.

## Relationship to the byte API

The two are interchangeable on the same file. A block dirtied through
`fs_block_mut` is an ordinary dirty block: it participates in the same
flush, the same segment, the same rollback, and the same checkpoint
semantics. Nothing about segment layout, WAL, or the cleaner changes.

The one asymmetry is `i_size`, described above.
