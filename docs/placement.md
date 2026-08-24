# Seeing where blocks are

A read is served by one object request per stretch of blocks that adjoins
within a single segment. So where a file's blocks landed decides what reading
it costs — and a caller cannot see that decision, because it is made here.

Two read-only queries expose it. Neither influences placement, and neither
issues a data request.

| | answers | depends on the cache |
|---|---|---|
| `read_plan(off, len)` | what reading this range would cost | **yes** |
| `block_placement(start, n)` | where each block is | no |

Batch forms take several ranges in one crossing: `read_plan_many`,
`block_placement_many`. On the reactor surface all four are `fh_*`.

## The plan

```rust
let plan = fh.fh_read_plan(0, len).await?;
let requests = plan.iter().filter(|e| e.is_get()).count();
```

Entries account for the whole range asked about, in the order the requests
would be made, so a stretch needing no request still appears — as
`PlannedRead::Local`, which covers a hole, a block in the data cache, and
(with `wal`) a block in a segment still pinned in memory. A `Get` carries the
file range it serves along with the segment and offset it reads, so the
entries can be walked without reconstructing anything.

This is the planner the read path itself uses. That is the point of exposing
it rather than leaving callers to work the merging rule out from raw
placement: a second implementation of the rule can disagree with the first
without saying so, and it will disagree in the direction of reporting a
layout as fine when it is not. `plan_agrees_with_what_the_read_does` in
`tests/integration_reactor_s3_placement.rs` asserts the predicted request
count against what a real read issues, on layouts built to make the two easy
to disagree about.

### It follows the cache

A resident block needs no request, so it is reported as `Local`. The plan
therefore says what a read *now* would cost, which is the question it is
asked — but it means a warm file looks free. To judge a layout, ask on a cold
handle, or use `block_placement`, which does not consult the cache.

## The placement

```rust
let places = fh.fh_block_placement(0, n).await?;   // Vec<Option<(SegmentId, u64)>>
```

`None` for a block in no segment: a hole, or one written and not yet flushed.
The segment id is meaningful only for equality and ordering.

The plan says a read costs more than it should; this says what the cost is
made of — how many distinct segments a file touches, whether its blocks are
in file order within them, how far apart they are.

## Why the two are not the same question

A flush writes its blocks sorted by index and packed, so two file-consecutive
blocks that are both in one segment are always adjacent within it. Two
consequences follow, and they are worth knowing before reading a plan:

- Requests break where the **segment** changes, which is a property of when
  each block was last flushed, not of the file's own order. Overwriting one
  block in the middle of a flushed run splits a read of that run into three
  requests: the blocks before it, the new block in its new segment, the
  blocks after it.
- Two *consecutive* requests in the **same** segment can therefore only come
  from the request-size cap, `read_get_max_bytes`. A 20 MiB contiguous run
  reads as two requests under the 16 MiB default.

## What asking costs

Metadata only. Where the map is resident, CPU; where it is not, the index
reads land in `meta_gets` and `meta_bytes`, so a measurement using these
queries can subtract what asking cost from what it is measuring. No data
request is issued — asserted, not assumed.

The batch forms exist because these queries are metadata-only: with no object
request to wait on, reaching the file is most of the cost, so a tool asking
about thousands of files pays for the asking rather than for the answers.
That cuts the other way too — the walk needs the map, which does not leave
the file's task, so a long batch occupies the file for its whole duration.
These are for a tool, not for a latency-sensitive path.

## What they do not do

No influence over placement: not a hint, not a grouping, not an ordering.
Nothing about segment contents — no way to read one, enumerate one, or learn
its size. And no guarantee about what the answers will be; they show the
placement, they do not promise it.

## Related

- [Block API](block-api.md) — reading and modifying cached blocks.
- [Concurrency](concurrency.md#what-coalescing-does-and-how-much-it-depends-on-the-ask)
  — the coalescing rule these queries expose, and why the same file measures
  as unfragmented or badly fragmented depending on how much is asked for at
  once.
