use std::fmt;
use std::collections::{HashMap, BTreeMap, BTreeSet};
use std::sync::Arc;
#[cfg(feature = "wal")]
use std::sync::Weak;
#[cfg(feature = "wal")]
use std::pin::Pin;
use std::time::{Instant, Duration};
use std::io::{Error, ErrorKind, Result};
use log::{debug, warn};
#[cfg(all(feature = "wal", feature = "reactor"))]
use crate::file::handler::ChannelGroup;
use btree_ondisk::{bmap::BMap, BlockLoader, NodeCache};
use btree_ondisk::btree::BtreeNodeDirty;
use btree_ondisk::DEFAULT_CACHE_UNLIMITED;
#[cfg(feature = "wal")]
use tokio::sync::RwLock;
use tokio::sync::{
    Semaphore, OwnedSemaphorePermit,
    Mutex, OwnedMutexGuard,
};
use crate::{BlockIndex, BlockPtr, BlockIndexIter, SegmentId, SegmentOffset, BMapUserData};
use crate::meta_format::BlockPtrFormat;
use crate::buffer::{DataBlock, AlignedDataBlockWrapper, BatchDataBlockWrapper};
use crate::staging::{StagingIntercept, Staging, config::StagingConfig};
use crate::segment::SegmentReadWrite;
use crate::ondisk::{InodeRaw, BMapRawType};
use crate::inode::{Inode, FlushInodeFlag};
use crate::config::{HyperFileConfig, HyperFileMetaConfig};
#[cfg(all(feature = "wal", feature = "reactor"))]
use crate::file::handler::FileContext;
#[cfg(feature = "wal")]
use crate::wal::{WalReadWrite, WalChunkDesc};
#[cfg(all(feature = "wal", feature = "reactor"))]
use crate::inode::OnDiskState;
use crate::data_cache::Cache;
use super::block::{BlockRef, BlockMut, BlockState};
use super::flags::HyperFileFlags;
use super::mode::HyperFileMode;
use super::{HyperTrait, DirtyDataBlocks, FlushTiming, PlannedRead};
#[cfg(feature = "wal")]
use crate::{DEFAULT_FLUSH_RETRIES, DEFAULT_FLUSH_BACKOFF_SECS};
#[cfg(feature = "wal")]
use crate::wal::config::WalRecoveryMode;
#[cfg(feature = "wal")]
use crate::wal::WalRecoveryReport;
#[cfg(feature = "range-lock")]
use super::lock::RangeLock;
use super::state::State;

/// One step of a coalesced read plan. Each variant carries a
/// `dst_len` — the number of bytes it consumes from the user's
/// destination buffer in iteration order. The executor walks the
/// ops, repeatedly `split_at_mut(dst_len)` on the user buffer,
/// and dispatches the slice to the right source.
///
/// `Range` is the coalesced variant: it covers a contiguous
/// byte range of one segment, possibly spanning multiple data
/// blocks. The Level-A planner merges adjacent
/// `is_on_staging` blocks whose decoded `(segid, staging_off)`
/// are contiguous, capped by `runtime.read_get_max_bytes`.
#[derive(Debug)]
pub(crate) enum ReadOp {
    /// Block is in the in-memory data cache (dirty or clean).
    /// Copy bytes out of the cached block.
    Cache {
        blk_idx: BlockIndex,
        src_off_in_block: usize,
        dst_len: usize,
    },
    /// Block has no backing storage (zero-block ptr or sparse
    /// hole). Fill destination with zero.
    Zero { dst_len: usize },
    /// Block lives in an in-flight (not-yet-flushed-to-S3) WAL
    /// segment whose contents are still pinned in memory. Copy
    /// from the in-memory segment buffer.
    #[cfg(feature = "wal")]
    Inmem {
        segid: SegmentId,
        s3_off: usize,
        dst_len: usize,
    },
    /// Coalesced ranged GET against the staging segment. May
    /// cover multiple consecutive blocks.
    Range {
        segid: SegmentId,
        s3_off: usize,
        dst_len: usize,
    },
}

impl ReadOp {
    pub(crate) fn dst_len(&self) -> usize {
        match self {
            Self::Cache { dst_len, .. } => *dst_len,
            Self::Zero { dst_len } => *dst_len,
            #[cfg(feature = "wal")]
            Self::Inmem { dst_len, .. } => *dst_len,
            Self::Range { dst_len, .. } => *dst_len,
        }
    }
}

/// Whether a checkpoint's log group may be replayed.
#[cfg(feature = "wal")]
#[derive(Debug, Clone, PartialEq, Eq)]
enum WalGroupState {
    /// Sealed, and every record the barrier names is stored.
    Complete,
    /// No barrier: the flush that would have written it did not get that far.
    Unsealed,
    /// Sealed, but records the barrier names are absent or the wrong shape.
    Incomplete { missing: usize },
    /// No barrier, and a transaction was left open from `from_seq`.
    ///
    /// Records before that seq are ordinary and are applied; from it on they are
    /// half of a unit the caller declared, and applying them is what the
    /// interval exists to prevent. Unlike the other incomplete states this is
    /// honoured in **both** recovery modes, because the caller asked for it
    /// explicitly rather than it being a property of where the crash fell.
    TxnOpen { from_seq: usize },
}

pub struct HyperFile<'a, T: Send + Clone, L: BlockLoader<BlockPtr>, C: NodeCache<BlockPtr>> {
    pub(crate) staging: T,
    pub(crate) bmap: BMap<'a, BlockIndex, BlockPtr, BlockPtr, L, C>,
    pub(crate) bmap_ud: BMapUserData,
    pub(crate) cache: Box<dyn Cache + Send>,
    pub(crate) inode: Inode,
    pub(crate) config: HyperFileConfig,
    pub(crate) max_dirty_blocks: usize,
    pub(crate) flags: HyperFileFlags,
    pub(crate) state: State,
    pub(crate) sema: Arc<Semaphore>,
    pub(crate) flush_lock: Arc<Mutex<()>>,
    pub(crate) flush_timing: FlushTiming,
    #[cfg(feature = "range-lock")]
    pub(crate) range_lock: RangeLock,
    #[cfg(feature = "reactor")]
    pub(crate) rt: Option<tokio::runtime::Runtime>,
    #[cfg(feature = "wal")]
    pub(crate) wal: Option<Box<dyn WalReadWrite + Send>>,
    /// Segments that have been built and published but not yet written
    /// out, kept reachable so the front end can read them meanwhile.
    ///
    /// A WAL-protected flush repoints the bmap at its new segment, hands
    /// the upload to a spawned task, and registers the segment's buffer
    /// here. Until that upload finishes the newest data lives only in
    /// this buffer, and reads and writes are served from it rather than
    /// waiting for the flush — which is safe because the WAL already
    /// holds the data, so the flush completing is a given and a failure
    /// replays the WAL instead of unwinding what was published.
    ///
    /// `wal_set_mem_segment` puts an entry here and `wal_flush_done`
    /// removes it, the latter in the same arm as it advances
    /// `last_ondisk_cno`. That pairing is the invariant everything else
    /// relies on: `segid > last_ondisk_cno` means "in here", and nothing
    /// on the handler task can observe one without the other. The entry
    /// is a `Weak`, so a caller that finds it gone has to fall back to
    /// reading staging.
    ///
    /// See [`docs/flush.md`](../../docs/flush.md) for the lifecycle and
    /// what it costs.
    #[cfg(feature = "wal")]
    pub(crate) flushing_segments: Arc<RwLock<HashMap<SegmentId, Weak<Pin<Box<Vec<u8>>>>>>>,
    /// What recovery did when this file was opened. See
    /// [`WalRecoveryReport`](crate::wal::WalRecoveryReport).
    #[cfg(feature = "wal")]
    pub(crate) wal_recovery_report: WalRecoveryReport,
    /// Flushes satisfied by the log alone since the last publish.
    ///
    /// Counts toward `HyperFileWalConfig::publish_every`, and resets when a
    /// segment is published. Also tells the publish how many log prefixes it
    /// supersedes, which it has to delete — one per deferred flush plus its
    /// own, since each of them sealed its own group.
    #[cfg(feature = "wal")]
    pub(crate) wal_deferred_barriers: usize,
    /// Set when the next flush must publish regardless of `publish_every`.
    ///
    /// Used on the way out: deferring is a bet that another flush is coming, and
    /// on close there is not one.
    #[cfg(feature = "wal")]
    pub(crate) wal_force_publish: bool,
    /// Set while the caller has declared its writes to be one unit.
    ///
    /// Suppresses publishing, so that nothing half-done can become the
    /// container's newest checkpoint. See [`Self::begin_txn`].
    #[cfg(feature = "wal")]
    pub(crate) txn_open: bool,
}

impl<T: Send + Clone, L: BlockLoader<BlockPtr>, C: NodeCache<BlockPtr>> fmt::Display for HyperFile<'_, T, L, C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "==== dump HyperFile ====")?;
        writeln!(f, "  {:?}", self.config)?;
        writeln!(f, "  max dirty blocks: {}", self.max_dirty_blocks)?;
        writeln!(f, "  {}", self.cache)?;
        writeln!(f, "  {}", self.inode)
    }
}

impl<T: Send + Clone, L: BlockLoader<BlockPtr>, C: NodeCache<BlockPtr>> Drop for HyperFile<'_, T, L, C> {
    fn drop(&mut self) {
        #[cfg(feature = "reactor")]
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

impl<'a, T, L, C> HyperFile<'a, T, L, C>
    where
        'a: 'static,
        T: Staging<L> + SegmentReadWrite + Send + Clone + 'static,
        L: BlockLoader<BlockPtr> + Clone + 'static,
        C: NodeCache<BlockPtr> + Clone,
{
    pub async fn new(staging: T, meta_block_loader: L, node_cache: C, config: HyperFileConfig, flags: HyperFileFlags, mode: HyperFileMode) -> Result<Self>
    {
        let meta_config = config.meta.clone();

        let bmap = BMap::<BlockIndex, BlockPtr, BlockPtr, L, C>::new(meta_config.root_size, meta_config.meta_block_size, meta_block_loader, node_cache)?;
        let bmap_ud = BMapUserData::new(meta_config.block_ptr_format);
        bmap.set_userdata(bmap_ud.as_u32());
        bmap.set_cache_limit(config.runtime.node_cache_blocks);

        let inode = Inode::default_file()
            .with_mode(&mode)
            .with_meta_config(&meta_config);
        let max_dirty_blocks = Self::calc_max_dirty_blocks(meta_config.data_block_size,
            config.runtime.data_cache_dirty_max_bytes_threshold,
            config.runtime.data_cache_dirty_max_blocks_threshold);

        let permits = if flags.is_rdonly() {
            Semaphore::MAX_PERMITS
        } else {
            #[cfg(feature = "range-lock")]
            { Semaphore::MAX_PERMITS }
            #[cfg(not(feature = "range-lock"))]
            { 1 }
        };

        let data_cache_blocks = if flags.is_direct() {
            #[cfg(not(feature = "wal"))]
            { 0 }
            // reset to data_cache_blocks if we are in wal mode
            #[cfg(feature = "wal")]
            { config.runtime.data_cache_blocks }
        } else {
            config.runtime.data_cache_blocks
        };

        #[cfg(feature = "wal")]
        let wal = config.wal.to_wal(config.meta.data_block_size, inode.get_last_seq())?;

        #[cfg(feature = "wal")]
        if let Some(ref wal) = wal {
            let v = wal.list_segments().await?;
            if v.len() > 0 {
                warn!("wal {} is not empty, please clear before create new file", config.wal.root_uri);
                return Err(Error::new(ErrorKind::ResourceBusy, "wal directory is not empty"));
            }
        }

        #[cfg(feature = "range-lock")]
        let range_lock = RangeLock::new(config.meta.data_block_size as u64);

        let mut file = Self {
            staging: staging,
            bmap: bmap,
            bmap_ud: bmap_ud,
            cache: crate::data_cache::cache_from_config(
                &config.data_cache,
                max_dirty_blocks,
                data_cache_blocks,
                config.meta.data_block_size,
            )?,
            inode: inode,
            config: config,
            max_dirty_blocks: max_dirty_blocks,
            flags: flags,
            state: State::default(),
            sema: Arc::new(Semaphore::new(permits)),
            flush_lock: Arc::new(Mutex::new(())),
            flush_timing: FlushTiming::default(),
            #[cfg(feature = "reactor")]
            rt: Some(tokio::runtime::Runtime::new().unwrap()),
            #[cfg(feature = "wal")]
            wal: wal,
            #[cfg(feature = "wal")]
            flushing_segments: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "wal")]
            wal_recovery_report: WalRecoveryReport::default(),
            #[cfg(feature = "wal")]
            txn_open: false,
            #[cfg(feature = "wal")]
            wal_deferred_barriers: 0,
            #[cfg(feature = "wal")]
            wal_force_publish: false,
            #[cfg(feature = "range-lock")]
            range_lock: range_lock,
        };
        // flush inode for hyper file new created
        let _ = file.flush_inode(FlushInodeFlag::Create).await?;
        Ok(file)
    }

    /// open a hyper file
    /// open by loading inode from staging,
    /// if inode is not found in staging, create hyper file from scratch
    pub async fn open(staging: T, meta_block_loader: L, node_cache: C, config: HyperFileConfig, flags: HyperFileFlags) -> Result<Self>
    {
        Self::do_open(staging, meta_block_loader, node_cache, config, flags, 0).await
    }

    /// open a hyper file with cno for read-only
    pub async fn open_cno(staging: T, meta_block_loader: L, node_cache: C, config: HyperFileConfig, flags: HyperFileFlags, cno: u64) -> Result<Self>
    {
        if !flags.is_rdonly() {
            return Err(Error::new(ErrorKind::ReadOnlyFilesystem, "write access is not allowed for open specific cno"));
        }
        Self::do_open(staging, meta_block_loader, node_cache, config, flags, cno).await
    }

    async fn do_open(staging: T, meta_block_loader: L, node_cache: C, mut config: HyperFileConfig, flags: HyperFileFlags, cno: u64) -> Result<Self>
    {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let inode_state;
        let res_inode = if cno == 0 {
            staging.load_inode(&mut raw_inode.as_mut_u8_slice()).await
        } else {
            staging.load_inode_from_segment(&mut raw_inode.as_mut_u8_slice(), cno as SegmentId).await
        };
        match res_inode {
            Ok(od_state) => {
                /* if we load inode without error, we use inode as truth of metadata */
                inode_state = od_state;
            },
            Err(e) => {
                return Err(e);
            },
        }
        // Get the meta config back from the inode — and refuse the open if it
        // is not one this build can represent. Everything below this line
        // trusts the inode, including `i_size` and the block size that cuts
        // every read up, and the container's config overwrites the caller's
        // further down. A container written before this field was populated
        // carries zero, which used to decode to 1-byte blocks and was read
        // without complaint.
        let meta_config = HyperFileMetaConfig::try_from_u32(raw_inode.i_meta_config)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!(
                "unrecognised container format: inode meta config {:#010x} — {}. \
                 Refusing to open rather than reading it with a format this build \
                 does not understand",
                raw_inode.i_meta_config, e)))?;
        // Borrowed, not copied: see the note in `HyperTrait::refresh_bmap`.
        let bmap = BMap::<BlockIndex, BlockPtr, BlockPtr, L, C>::read(&raw_inode.i_bmap, meta_config.meta_block_size, meta_block_loader, node_cache)?;
        let bmap_ud = BMapUserData::from_u32(bmap.get_userdata());
        bmap.set_cache_limit(config.runtime.node_cache_blocks);

        // if inode exists, we trust it

        let max_dirty_blocks = Self::calc_max_dirty_blocks(meta_config.data_block_size,
            config.runtime.data_cache_dirty_max_bytes_threshold,
            config.runtime.data_cache_dirty_max_blocks_threshold);

        let permits = if flags.is_rdonly() {
            Semaphore::MAX_PERMITS
        } else {
            #[cfg(feature = "range-lock")]
            { Semaphore::MAX_PERMITS }
            #[cfg(not(feature = "range-lock"))]
            { 1 }
        };

        let data_cache_blocks = if flags.is_direct() {
            #[cfg(not(feature = "wal"))]
            { 0 }
            // reset to data_cache_blocks if we are in wal mode
            #[cfg(feature = "wal")]
            { config.runtime.data_cache_blocks }
        } else {
            config.runtime.data_cache_blocks
        };

        // overwrite the default meta config with the one we get from inode
        config.meta = meta_config;

        let inode = Inode::from_raw(&raw_inode, inode_state);
        #[cfg(feature = "wal")]
        let wal = config.wal.to_wal(config.meta.data_block_size, inode.get_last_seq())?;

        #[cfg(feature = "range-lock")]
        let range_lock = RangeLock::new(config.meta.data_block_size as u64);

        let mut file = Self {
            staging: staging,
            bmap: bmap,
            bmap_ud: bmap_ud,
            cache: crate::data_cache::cache_from_config(
                &config.data_cache,
                max_dirty_blocks,
                data_cache_blocks,
                config.meta.data_block_size,
            )?,
            inode: inode,
            config: config,
            max_dirty_blocks: max_dirty_blocks,
            flags: flags,
            state: State::default(),
            sema: Arc::new(Semaphore::new(permits)),
            flush_lock: Arc::new(Mutex::new(())),
            flush_timing: FlushTiming::default(),
            #[cfg(feature = "reactor")]
            rt: Some(tokio::runtime::Runtime::new().unwrap()),
            #[cfg(feature = "wal")]
            wal: wal,
            #[cfg(feature = "wal")]
            flushing_segments: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "wal")]
            wal_recovery_report: WalRecoveryReport::default(),
            #[cfg(feature = "wal")]
            txn_open: false,
            #[cfg(feature = "wal")]
            wal_deferred_barriers: 0,
            #[cfg(feature = "wal")]
            wal_force_publish: false,
            #[cfg(feature = "range-lock")]
            range_lock: range_lock,
        };
        // refresh bmap if need to do recovery
        let _ = file.refresh_bmap().await?;

        #[cfg(feature = "wal")]
        if let Some(ref wal) = file.wal {
            let v = wal.list_segments().await?;
            if let Some(wal_max_segid) = v.iter().max() {
                let wal_max_segid = *wal_max_segid;
                // Decided against `last_ondisk_cno`, which is what the replay
                // itself filters by. Using two different bases for one question
                // is how a group becomes unreachable: the trigger says there is
                // nothing to do while the filter would have found something.
                //
                // They agree as long as every publish moves all three of
                // `last_seq`, `last_cno` and `last_ondisk_cno` together, which
                // was true until a session could advance `last_seq` on its own —
                // it does that now, to avoid writing into a checkpoint whose
                // records recovery declined. After that, an inode-only publish
                // (an attribute change with no dirty data, which a filesystem
                // above does constantly) writes the advanced `last_seq` out, and
                // a trigger reading it concludes there is nothing to recover
                // while the records are still sitting there.
                let last_ondisk = file.inode().get_last_ondisk_cno();
                if wal_max_segid >= last_ondisk {
                    warn!("wal holds checkpoints up to {} at or beyond the last one \
                          on disk ({}), recovering", wal_max_segid, last_ondisk);
                    let lock = file.flush_lock().await;
                    let _ = file.wal_flush_recovery(lock).await;
                }

                // Whatever recovery declined to apply is still in the log, and
                // this session must not write into its checkpoint.
                //
                // Two reasons, and the second is the worse one. Record keys are
                // `<seq>_<offset>_<len>` and the counter restarts at zero for a
                // new session, so the first write lands on the key an unapplied
                // record already occupies — a create-only PUT, so the write
                // fails with 412. Replaying a similar workload makes that
                // likely rather than rare: a consumer saw a run get partway
                // through and then fail, and reported it as data appearing
                // "partially".
                //
                // Worse, the two sets would end up in one group. A later
                // barrier for that checkpoint would list this session's records
                // *and* the ones recovery refused, and call the result
                // complete — a seal vouching for work that was deliberately
                // set aside.
                //
                // So move past everything the log holds. The records left
                // behind stay findable: recovery filters by `last_ondisk_cno`,
                // which a session that published nothing does not advance.
                let last_seq = file.inode().get_last_seq();
                if wal_max_segid >= last_seq {
                    let next = wal_max_segid + 1;
                    warn!("wal holds checkpoints up to {} that were not applied; \
                          starting this session at {} so its records do not join \
                          them", wal_max_segid, next);
                    file.inode_mut().set_last_seq(next);
                }
            }
        }
        Ok(file)
    }

    pub async fn release(&mut self) -> Result<SegmentId> {
        #[cfg(feature = "reactor")]
        if self.state.is_flushing() {
            return Err(Error::new(ErrorKind::ResourceBusy, "flush is in-progress"));
        };
        #[cfg(feature = "reactor")]
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
        // A read-only handle has nothing to publish, and must not try. It
        // matters most for a handle opened at a checkpoint: that one holds a
        // historical inode, so publishing would move the container back to it.
        // The attempt is caught by the on-disk state check and fails, which is
        // safe but leaves no way to close such a handle cleanly — and rests on
        // a conflict being detected rather than on not writing.
        let segid = if self.flags.is_rdonly() {
            self.inode().get_last_cno()
        } else {
            // Closing publishes, whatever `publish_every` says. Deferring is a
            // bet that another flush is coming; on the way out there is not
            // one, and leaving the groups unpublished would put their cost on
            // the next open instead.
            #[cfg(feature = "wal")]
            { self.wal_force_publish = true; }
            self.flush().await?
        };
        self.cache.shutdown();
        self.bmap.get_node_cache().shutdown();
        Ok(segid)
    }

    /// Refuse a write that would take an open transaction past the memory bound.
    ///
    /// Inside a transaction nothing publishes, so the dirty set is the only place
    /// the data can be — and the threshold that normally relieves it is
    /// suppressed. Reporting the overflow is the honest outcome: publishing
    /// would break what the interval promised, and growing without limit would
    /// trade the memory bound away without saying so.
    /// Whether publishing happens only when the caller asks.
    ///
    /// True inside a transaction, and true throughout `Barrier` recovery mode.
    /// They are the same requirement at different scopes: a transaction says
    /// "this stretch of writes is one unit, do not publish inside it", and
    /// `Barrier` says "every stretch between my flushes is one unit".
    ///
    /// `Barrier` has no choice about it. What that mode sells is that the state
    /// recovery lands on is one the caller declared consistent, and recovery
    /// cannot land earlier than the newest published checkpoint — so anything
    /// else that publishes puts a checkpoint the caller never declared beneath
    /// the floor, and the guarantee is gone. Sealing it or not makes no
    /// difference; publishing at all is what does it.
    #[cfg(feature = "wal")]
    pub(crate) fn publishes_on_request_only(&self) -> bool {
        self.txn_open || self.wal_recovery_mode() == WalRecoveryMode::Barrier
    }

    #[cfg(feature = "wal")]
    pub(crate) fn check_txn_room(&self) -> Result<()> {
        if !self.publishes_on_request_only() {
            return Ok(());
        }
        let dirty = self.cache.dirty_count();
        let bytes = dirty * self.config.meta.data_block_size;
        if bytes > self.config.runtime.data_cache_dirty_max_bytes_threshold
            || dirty > self.config.runtime.data_cache_dirty_max_blocks_threshold
        {
            return Err(Error::new(ErrorKind::OutOfMemory, format!(
                "{} dirty bytes in {} blocks, past the configured limits, and nothing \
                 may publish without the caller asking — flush, or commit the \
                 transaction, to make room", bytes, dirty)));
        }
        Ok(())
    }

    /// Begin a transaction: the writes from here until [`Self::commit_txn`] are
    /// one unit, and nothing is published in between.
    ///
    /// For work whose midpoints are not states anyone should come up in — a
    /// repair pass over the container's own contents is the case this exists
    /// for. Such a pass can write far more than the dirty-data thresholds
    /// allow, and a threshold crossing partway through would make a half-fixed
    /// container the newest checkpoint, which the next open would take as its
    /// baseline.
    ///
    /// # What it gives
    ///
    /// * **Atomic publication.** Nothing publishes until [`Self::commit_txn`],
    ///   including the dirty-data thresholds.
    /// * **Atomic recovery.** A marker in the log lets recovery tell an
    ///   unfinished transaction from ordinary writes. If one is still open when
    ///   the container is opened again its writes are **not** applied, in
    ///   either recovery mode — half a unit is what this exists to keep out.
    ///   Writes made before the transaction began are ordinary and unaffected,
    ///   so an interrupted transaction costs redoing it rather than repairing
    ///   the result of half of it.
    ///
    /// # What it does not give
    ///
    /// * **No isolation.** Writes inside the transaction are visible to readers
    ///   of this container immediately, exactly as they would be outside one.
    ///   The transaction governs what gets published and what survives a crash,
    ///   not who can see what.
    /// * **No implicit undo.** Dropping the file or never committing leaves the
    ///   writes in memory and in the log; it is the *next open* that discards
    ///   them. To undo within the same handle, call [`Self::abort_txn`].
    ///
    /// While one is open, a write that would take the dirty set past
    /// `data_cache_dirty_max_bytes_threshold` fails rather than publishing, and
    /// an explicit flush fails — asking to publish contradicts having asked not
    /// to.
    ///
    /// Requires a log: without one there is nowhere to record that the unit was
    /// left unfinished, and suppressing publishes would only mean losing the
    /// writes on a crash.
    #[cfg(feature = "wal")]
    pub async fn begin_txn(&mut self) -> Result<()> {
        if self.wal.is_none() {
            return Err(Error::new(ErrorKind::Unsupported,
                "a transaction needs a wal: without one there is nowhere to \
                 record that the unit was left unfinished"));
        }
        if self.txn_open {
            return Err(Error::new(ErrorKind::AlreadyExists, "a transaction is already open"));
        }
        let segid = self.inode().get_last_seq();
        let from_seq = self.wal.as_ref().expect("checked above").next_seq_peek(segid);
        let fut = match self.wal.as_mut() {
            Some(wal) => wal.write_txn_marker(segid, from_seq),
            None => unreachable!("checked above"),
        };
        fut.await?;
        self.txn_open = true;
        Ok(())
    }

    /// Close the interval opened by [`Self::begin_txn`] and publish it as
    /// one checkpoint.
    ///
    /// The publish seals the whole interval, which is what makes it applicable
    /// on a later open. The marker is then removed; that removal is cleanup
    /// rather than a correctness step, since a barrier for the same checkpoint
    /// already says the unit completed and takes precedence over a marker left
    /// behind.
    ///
    /// On failure the interval stays open, so the caller can retry or abandon
    /// it. Abandoning is safe: an unfinished transaction is not applied.
    #[cfg(feature = "wal")]
    pub async fn commit_txn(&mut self) -> Result<SegmentId> {
        if !self.txn_open {
            return Err(Error::new(ErrorKind::NotFound, "no transaction is open"));
        }
        let segid = self.inode().get_last_seq();
        // Cleared first so the flush is allowed to publish; restored if it does
        // not, so a failed close leaves the interval as it was.
        self.txn_open = false;
        let cno = match self.flush().await {
            Ok(cno) => cno,
            Err(e) => {
                self.txn_open = true;
                return Err(e);
            },
        };
        let fut = self.wal.as_mut().map(|wal| wal.delete_txn_marker(segid));
        if let Some(fut) = fut {
            // A marker left behind is harmless: the barrier written by the
            // flush above outranks it.
            if let Err(e) = fut.await {
                warn!("commit_txn - could not remove the transaction marker for {}: {}", segid, e);
            }
        }
        Ok(cno)
    }

    /// Whether an transaction is open.
    /// Abandon the open transaction, discarding its writes.
    ///
    /// Rolls the file back to what is persisted, so the writes made inside the
    /// transaction are gone from this handle as well as from any later open.
    /// That is the difference between this and simply never committing: the
    /// latter leaves them in memory until the file is closed, and only a
    /// reopen discards them.
    ///
    /// Writes made *before* the transaction began are not affected by the
    /// transaction, but they are affected by the rollback: it returns the file
    /// to its last published state. Publish before beginning a transaction if
    /// there is unflushed work worth keeping.
    #[cfg(feature = "wal")]
    pub async fn abort_txn(&mut self) -> Result<()> {
        if !self.txn_open {
            return Err(Error::new(ErrorKind::NotFound, "no transaction is open"));
        }
        let segid = self.inode().get_last_seq();
        self.txn_open = false;
        self.rollback_from_persisted().await?;

        // Rolling back memory is only half of it. The writes are also in the
        // log, and nothing about an abort seals them — so the next open finds
        // unsealed records and `Latest` applies them, which puts back exactly
        // what the caller asked to discard. The promise is that they are gone
        // from later opens too, so the records have to go.
        //
        // The whole group, and awaited rather than spawned. The rollback returns
        // the file to its last published state, which discards any writes made
        // before the interval opened as well — they are in this group too, and
        // leaving them would replay a state the file is no longer in. Awaited
        // because a lost delete here does not cost storage, it undoes the abort,
        // and the caller is the only one who can decide what to do about that.
        if let Some(ref mut wal) = self.wal {
            wal.discard_pending();
        }
        let fut = self.wal.as_ref().map(|wal| wal.delete_segment(segid));
        if let Some(fut) = fut {
            fut.await?;
        }

        let fut = self.wal.as_mut().map(|wal| wal.delete_txn_marker(segid));
        if let Some(fut) = fut {
            // Ordered after the records, and best-effort. A marker left behind
            // names a group with nothing in it, which recovery reads as an
            // unfinished unit of no records — the same outcome, reached the long
            // way.
            if let Err(e) = fut.await {
                warn!("abort_txn - could not remove the marker for {}: {}", segid, e);
            }
        }
        Ok(())
    }

    #[cfg(feature = "wal")]
    pub fn in_txn(&self) -> bool {
        self.txn_open
    }

    /// What recovery did when this container was opened.
    ///
    /// Ask once after opening. The field a caller usually wants is
    /// [`landed_on_barrier`](crate::wal::WalRecoveryReport::landed_on_barrier):
    /// when it is true the contents are a state that was declared consistent by
    /// whoever wrote them, so work proportional to the whole container — a
    /// repair pass, a full verification — can be skipped. That is the reason
    /// this is reported at all.
    #[cfg(feature = "wal")]
    pub fn wal_recovery_report(&self) -> WalRecoveryReport {
        self.wal_recovery_report
    }

    /// True when a write that has returned `Ok` is already recoverable with no
    /// further flush: its bytes are in the write-ahead log, and opening this
    /// container again replays them.
    ///
    /// Named for the guarantee rather than for what currently provides it. A
    /// caller deciding whether it may skip a flush is relying on the
    /// guarantee, so if this crate ever keeps a log while not offering the
    /// guarantee — batching log writes so a write can return before its bytes
    /// are down, say — this must start answering `false` and the caller
    /// degrades instead of silently losing data.
    ///
    /// What it covers, and does not:
    ///
    /// * **Scope** — the bytes each `Ok` return of `write` or `write_zero`
    ///   reports as written. Nothing about writes that returned an error, and
    ///   no ordering between separate writes.
    /// * **When** — from the moment the call returns.
    /// * **Independent of publishing** — holds regardless of
    ///   `commit_bytes` and `commit_interval_ms`, and does not require any
    ///   flush or segment publish to have happened.
    /// * **Recovery** — reopening the same container with the same log
    ///   configuration replays it. No manual step.
    ///
    /// It says nothing about a checkpoint existing for those bytes. Reading
    /// them back needs the container reopened, not a checkpoint published.
    pub fn writes_durable_on_ack(&self) -> bool {
        #[cfg(feature = "wal")]
        {
            // `Barrier` recovery discards everything after the last seal, and a
            // write with no flush behind it is in an unsealed group — so in
            // that mode an acknowledged write is not recoverable on its own and
            // this must say so. Reporting the guarantee rather than the
            // mechanism is what lets it.
            self.wal.is_some() && self.wal_recovery_mode() == WalRecoveryMode::Latest
        }
        #[cfg(not(feature = "wal"))]
        {
            false
        }
    }

    pub fn stat(&self) -> libc::stat {
        // TODO: set dev and rdev here
        let dev = 0;
        let rdev = 0;
        self.inode.to_stat(dev, rdev)
    }

    // fast stat by read inode without open file
    pub async fn stat_fast(staging: T) -> Result<libc::stat> {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        staging.load_inode(&mut raw_inode.as_mut_u8_slice()).await?;
        let inode = Inode::from_raw(&raw_inode, None);
        Ok(inode.to_stat(0, 0))
    }

    // fast update stat by load inode and flush inode
    pub async fn update_stat_fast(staging: T, stat: &libc::stat) -> Result<libc::stat> {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let od_state = staging.load_inode(&mut raw_inode.as_mut_u8_slice()).await?;
        let mut inode = Inode::from_raw(&raw_inode, od_state);
        inode.update_stat(stat);
        let raw = inode.to_raw(raw_inode.i_bmap);
        let od_state = inode.get_ondisk_state();
        let _ = staging.flush_inode(raw.as_u8_slice(), od_state, FlushInodeFlag::Update).await?;
        Ok(inode.to_stat(stat.st_dev, stat.st_rdev))
    }

    pub async fn update_stat(&mut self, stat: &libc::stat) -> Result<libc::stat> {
        // The flush lock may be travelling with a publish that is already in
        // flight. Under the reactor its guard is handed to a spawned task and
        // comes back as a callback to the handler task — the same task that runs
        // this. Waiting for it here means the callback never gets processed, the
        // guard never returns, and the handler is wedged for good. So report the
        // conflict and let the caller's arm re-queue the request, which is what
        // `release` has always done.
        #[cfg(feature = "reactor")]
        if self.state.is_flushing() {
            return Err(Error::new(ErrorKind::ResourceBusy, "flush is in-progress"));
        };
        let stat = self.inode.update_stat(stat);
        let _ = self.flush().await?;
        Ok(stat)
    }

    /// Opt this file in/out of cross-mount open-but-unlinked
    /// ([`InodeRaw::FLAG_KEEP_OPEN`]) and persist the change.
    pub async fn set_keep_open(&mut self, on: bool) -> Result<()> {
        self.inode.set_keep_open(on);
        let _ = self.flush().await?;
        Ok(())
    }

    /// True if this file opts in to cross-mount open-but-unlinked.
    pub fn is_keep_open(&self) -> bool { self.inode.is_keep_open() }

    /// Sorted set of dirty (cache-only, not-yet-flushed) data block
    /// indices that are `>= from`. These are unflushed writes that
    /// live only in the data cache and are not yet in the bmap, but
    /// the read path serves them, so for SEEK_DATA / SEEK_HOLE they
    /// count as data. The dirty set is bounded by the flush
    /// threshold, so collecting it is cheap.
    fn dirty_blocks_from(&self, from: BlockIndex) -> BTreeSet<BlockIndex> {
        self.cache.get_dirty().data().into_keys().filter(|k| *k >= from).collect()
    }

    /// `SEEK_DATA`: the smallest offset >= `off` that holds data, or
    /// `None` (the caller maps to `ENXIO`) if there is no data
    /// between `off` and EOF.
    ///
    /// Data lives in two places: unflushed writes in the dirty cache
    /// and flushed blocks in the bmap. We take the minimum of the
    /// first data block from each source. The bmap scan uses
    /// `seek_key` to skip runs of absent (hole) keys in O(log n) per
    /// jump, so a fully-sparse flushed file is cheap; only runs of
    /// zero-block keys are stepped through one block at a time.
    pub async fn seek_data(&self, off: usize) -> Result<Option<usize>> {
        let size = self.inode.size();
        if off >= size {
            return Ok(None);
        }
        let bsize = self.config.meta.data_block_size;
        let last = ((size - 1) / bsize) as BlockIndex;
        let start = (off / bsize) as BlockIndex;

        // Source 1: unflushed writes (always data).
        let dirty_next = self.dirty_blocks_from(start).into_iter().next();

        // Source 2: flushed, non-zero blocks in the bmap.
        let mut bmap_next = None;
        let mut blk = start;
        loop {
            match self.bmap.seek_key(&blk).await {
                Ok(k) => {
                    if k > last {
                        break; // no present key within the file
                    }
                    if !BlockPtrFormat::is_zero_block(&self.bmap.lookup(&k).await?) {
                        bmap_next = Some(k);
                        break;
                    }
                    if k == last {
                        break; // trailing zero block: no data to EOF
                    }
                    blk = k + 1; // skip this zero block
                }
                Err(e) if e.kind() == ErrorKind::NotFound => break,
                Err(e) => return Err(e),
            }
        }

        let cand = match (dirty_next, bmap_next) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        // Within the starting block, data begins at `off` itself.
        Ok(cand.map(|b| ((b as usize) * bsize).max(off)))
    }

    /// `SEEK_HOLE`: the smallest offset >= `off` that is in a hole.
    /// EOF is an implicit hole, so a file with no internal hole
    /// returns its size. `off == size` returns `size`; `off > size`
    /// returns `None` (the caller maps to `ENXIO`).
    ///
    /// Complexity note: runs of holes are skipped in O(log n) via
    /// `seek_key`, but a run of data blocks must be checked one block
    /// at a time (zero blocks are present bmap keys and are
    /// indistinguishable from data without a per-key lookup), so
    /// SEEK_HOLE over a large fully-dense region is O(n).
    pub async fn seek_hole(&self, off: usize) -> Result<Option<usize>> {
        let size = self.inode.size();
        if off > size {
            return Ok(None);
        }
        if off == size {
            return Ok(Some(size)); // EOF is an implicit hole
        }
        let bsize = self.config.meta.data_block_size;
        let last = ((size - 1) / bsize) as BlockIndex;
        let start = (off / bsize) as BlockIndex;

        let dirty = self.dirty_blocks_from(start);

        let mut blk = start;
        loop {
            if blk > last {
                return Ok(Some(size)); // data through EOF: hole is at EOF
            }
            if dirty.contains(&blk) {
                blk += 1; // unflushed write: data
                continue;
            }
            match self.bmap.seek_key(&blk).await {
                // No present key >= blk, and blk is not dirty:
                // blk and everything after it is a hole.
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    return Ok(Some(((blk as usize) * bsize).max(off)));
                }
                Err(e) => return Err(e),
                // blk is absent in the bmap (next present key is
                // beyond it) and not dirty: blk is a hole.
                Ok(k) if k > blk => {
                    return Ok(Some(((blk as usize) * bsize).max(off)));
                }
                // blk is present; a zero block is a hole.
                Ok(_) => {
                    if BlockPtrFormat::is_zero_block(&self.bmap.lookup(&blk).await?) {
                        return Ok(Some(((blk as usize) * bsize).max(off)));
                    }
                    blk += 1; // data, keep scanning
                }
            }
        }
    }

    /// Fetch a range into the data block cache without returning it.
    ///
    /// For a caller that knows what will be asked for next and would
    /// rather it were already here. Nothing comes back: a later `read` of
    /// those bytes asks the ordinary way and finds them.
    ///
    /// This exists because a byte read queries the data cache but does not
    /// fill it, so bytes fetched speculatively through `read` have nowhere
    /// to live and the next read fetches them again. Only blocks brought
    /// in through here are cached, which keeps a plain read from evicting
    /// the blocks the write path is holding.
    ///
    /// The range is widened to whole blocks, since a block is what the
    /// cache stores, and clamped to the end of the file. Blocks already
    /// resident are left alone, and holes are skipped: they read as zeroes
    /// without an object request, so caching them would buy nothing.
    ///
    /// Requests are coalesced exactly as a read of the same range would
    /// be, because the planning is shared. A wide read-ahead therefore
    /// costs a few requests rather than one per block.
    ///
    /// Returns how many blocks were installed.
    pub async fn read_ahead(&mut self, off: usize, len: usize) -> Result<usize> {
        if len == 0 {
            return Ok(0);
        }
        let i_size = self.inode().size() as usize;
        if off >= i_size {
            return Ok(0);
        }
        let bs = self.config.meta.data_block_size;
        // Whole blocks only: a partial block cannot be cached, and a caller
        // asking for part of one still wants the block it sits in.
        let start = off / bs * bs;
        let end = ((off + len).min(i_size) + bs - 1) / bs * bs;
        let span = end - start;
        if span == 0 {
            return Ok(0);
        }

        let plan = self.plan_read_with(start, span, false).await?;
        debug!("READ AHEAD - planned {} ops for {} bytes at {}", plan.len(), span, start);

        // One buffer for the whole span: a coalesced request needs a
        // contiguous destination and separate blocks are not contiguous, so
        // bytes land here and are copied into blocks after.
        let mut buf = vec![0u8; span];
        let mut consumed = 0usize;
        // Windows that came from staging. Only those become cached blocks —
        // one already in the cache, or a hole, is not worth installing.
        let mut fetched: Vec<(usize, usize)> = Vec::new();

        for op in plan {
            let dst_len = op.dst_len();
            match op {
                ReadOp::Cache { .. } | ReadOp::Zero { .. } => {},
                #[cfg(feature = "wal")]
                ReadOp::Inmem { segid, s3_off, dst_len: _ } => {
                    let copied = {
                        let lock = self.flushing_segments.read().await;
                        match lock.get(&segid).and_then(|weak| weak.upgrade()) {
                            Some(data) => {
                                let e = s3_off + dst_len;
                                buf[consumed..consumed + dst_len].copy_from_slice(&data[s3_off..e]);
                                true
                            },
                            None => false,
                        }
                    };
                    if !copied {
                        // The flush landed, so the same bytes are on staging.
                        self.staging.load_range(segid, s3_off, &mut buf[consumed..consumed + dst_len]).await?;
                        self.staging.read_timing().add_read_ahead_get(dst_len);
                    }
                    fetched.push((consumed, dst_len));
                },
                ReadOp::Range { segid, s3_off, dst_len: _ } => {
                    self.staging.load_range(segid, s3_off, &mut buf[consumed..consumed + dst_len]).await?;
                    self.staging.read_timing().add_read_ahead_get(dst_len);
                    fetched.push((consumed, dst_len));
                },
            }
            consumed += dst_len;
        }

        // Install what was fetched, a block at a time.
        let mut cached = 0usize;
        for (win_off, win_len) in fetched {
            let mut pos = win_off;
            while pos + bs <= win_off + win_len {
                let blk_idx = ((start + pos) / bs) as BlockIndex;
                let block = self.cache.new_block(blk_idx);
                block.set_should_cache();
                block.as_mut_slice().copy_from_slice(&buf[pos..pos + bs]);
                self.absorb_block(blk_idx, block);
                cached += 1;
                pos += bs;
            }
        }

        if !self.flags.is_noatime() {
            self.inode.update_atime();
        }
        debug!("READ AHEAD - cached {} blocks", cached);
        Ok(cached)
    }

    pub async fn read(&mut self, off: usize, mut buf: &mut [u8]) -> Result<usize> {
        // POSIX read(): "[EBADF] The fildes argument is not a valid
        // file descriptor open for reading." Unlike the write path
        // this needs no `_inner` escape hatch — nothing inside the
        // crate calls `read`. Internal reads (partial-write
        // read-modify-write, truncate tail zeroing, WAL replay) go
        // through the lower-level `load_data_block_*` helpers, which
        // are unaffected.
        if !self.flags.is_readable() {
            return Err(Self::ebadf_bad_access_mode());
        }
        let _permit = self.sema.clone().acquire_owned().await.unwrap();
        let fn_start = Instant::now();
        debug!("READ - off: {}, buf len: {}", off, buf.len());
        if off >= self.inode.size() {
            return Ok(0);
        }
        // if requested buffer exceed file size, cut off tailing buffer
        if off + buf.len() > self.inode.size() {
            let exceeded_len = off + buf.len() - self.inode.size();
            let mid = buf.len() - exceeded_len;
            debug!("READ - buf len shrink to: {}, due to file size {}", mid, self.inode.size());
            (buf, _) = buf.split_at_mut(mid);
        }
        if buf.is_empty() {
            let _ = fn_start;
            if !self.flags.is_noatime() {
                self.inode.update_atime();
            }
            return Ok(0);
        }

        let buf_len = buf.len();

        // Stage 1: walk blocks and build a coalesced plan.
        let plan = self.plan_read(off, buf_len).await?;
        debug!("READ - planned {} ops for {} bytes", plan.len(), buf_len);

        // Stage 2: execute. Walk ops, splitting `buf` as we go.
        let mut bytes_read = 0;
        let mut remaining = buf;
        for op in plan {
            let dst_len = op.dst_len();
            let (this, next) = remaining.split_at_mut(dst_len);
            match op {
                ReadOp::Cache { blk_idx, src_off_in_block, dst_len: _ } => {
                    let block = self.cache.get(&blk_idx)
                        .expect("planner classified as cache hit but block is gone");
                    block.copy_out(src_off_in_block, this);
                    block.unlock();
                }
                ReadOp::Zero { dst_len: _ } => {
                    this.fill(0);
                }
                #[cfg(feature = "wal")]
                ReadOp::Inmem { segid, s3_off, dst_len: _ } => {
                    // The segment can reach staging between the planner
                    // classifying it as in flight and this copy, at which
                    // point the entry is gone and the pinned buffer with
                    // it. Read it the ordinary way instead: a flush
                    // finishing is not a failure.
                    let copied = {
                        let lock = self.flushing_segments.read().await;
                        match lock.get(&segid).and_then(|weak| weak.upgrade()) {
                            Some(data) => {
                                let end = s3_off + this.len();
                                this.copy_from_slice(&data[s3_off..end]);
                                true
                            },
                            None => false,
                        }
                    };
                    if !copied {
                        debug!("read - segid {} no longer held in memory, reading it from staging", segid);
                        self.staging.load_range(segid, s3_off, this).await?;
                    }
                }
                ReadOp::Range { segid, s3_off, dst_len: _ } => {
                    self.staging.load_range(segid, s3_off, this).await?;
                }
            }
            bytes_read += dst_len;
            remaining = next;
        }

        let _ = fn_start;

        if !self.flags.is_noatime() {
            self.inode.update_atime();
        }
        Ok(bytes_read)
    }

    /// Walk the read range and produce a coalesced op list. See
    /// `ReadOp` for the coalescing rules; in short, contiguous
    /// blocks that all map to one segment with consecutive
    /// staging offsets get merged into one `ReadOp::Range`,
    /// capped at `runtime.read_get_max_bytes`. Cache hits, zero
    /// blocks, and in-flight WAL blocks each break the run and
    /// produce their own per-block op.
    /// Plan a read, counting the blocks it finds resident.
    /// What reading `[off, off + len)` would cost, without reading it.
    ///
    /// One entry per object request, in the order they would be made, plus
    /// an entry for every part of the range that needs no request, so the
    /// entries account for the whole range. See [`PlannedRead`].
    ///
    /// This is the planner the read path itself uses, which is the point of
    /// exposing it: a caller working the merging rule out from raw
    /// placement would be writing a second implementation of it, and a
    /// second implementation can disagree with the first without saying so.
    ///
    /// **The answer depends on the data cache, not only on placement.** A
    /// resident block needs no request and is reported as
    /// [`PlannedRead::Local`], so this says what a read *now* would cost —
    /// which is the question, but it means a warm file looks cheap. To
    /// judge a layout, ask on a cold handle, or use
    /// [`Self::block_placement`], which does not consult the cache.
    ///
    /// Metadata only: no data request is issued. Index nodes the walk has
    /// to fetch are counted in `meta_gets`, so a measurement using this can
    /// subtract what asking cost from what it is measuring.
    pub async fn read_plan(&mut self, off: u64, len: u64) -> Result<Vec<PlannedRead>> {
        let plan = self.plan_read_with(off as usize, len as usize, false).await?;
        let mut out: Vec<PlannedRead> = Vec::with_capacity(plan.len());
        let mut at_file = off;
        for op in plan {
            let dst_len = op.dst_len() as u64;
            match op {
                ReadOp::Cache { .. } | ReadOp::Zero { .. } => {
                    // Merge with the previous local run: whether two
                    // adjacent stretches need no request for the same
                    // reason is not something a caller asked about.
                    match out.last_mut() {
                        Some(PlannedRead::Local { len, .. }) => *len += dst_len,
                        _ => out.push(PlannedRead::Local { off: at_file, len: dst_len }),
                    }
                },
                #[cfg(feature = "wal")]
                ReadOp::Inmem { .. } => match out.last_mut() {
                    Some(PlannedRead::Local { len, .. }) => *len += dst_len,
                    _ => out.push(PlannedRead::Local { off: at_file, len: dst_len }),
                },
                ReadOp::Range { segid, s3_off, .. } => out.push(PlannedRead::Get {
                    off: at_file,
                    len: dst_len,
                    segid,
                    at: s3_off as u64,
                }),
            }
            at_file += dst_len;
        }
        Ok(out)
    }

    /// [`Self::read_plan`] for several ranges in one go, answered in the
    /// order the ranges were given.
    ///
    /// Worth batching because these queries are metadata-only: with no
    /// object request to wait on, reaching the file at all is most of the
    /// cost, so a tool asking about thousands of files pays for the asking.
    /// The other side of that is a long batch occupies the file for its
    /// whole walk — this is for a tool, not for a latency-sensitive path.
    pub async fn read_plan_many(&mut self, ranges: &[(u64, u64)]) -> Result<Vec<Vec<PlannedRead>>> {
        let mut out = Vec::with_capacity(ranges.len());
        for (off, len) in ranges.iter().copied() {
            out.push(self.read_plan(off, len).await?);
        }
        Ok(out)
    }

    /// Where each of `n` blocks starting at `start` currently lives.
    ///
    /// `None` for a block that is in no segment: a hole, or one written and
    /// not yet flushed. The offset is where in the segment the block
    /// starts.
    ///
    /// Unlike [`Self::read_plan`] this does not consult the data cache, so
    /// it answers about placement alone and a warm file does not look
    /// different from a cold one. It says nothing about what a read would
    /// cost — that is the plan's job — but it says what a cost is made of,
    /// in terms a caller can aggregate: how many distinct segments a file
    /// touches, whether its blocks are in file order within them, how far
    /// apart they are.
    ///
    /// The segment id is meaningful only for equality and ordering.
    pub async fn block_placement(&mut self, start: BlockIndex, n: usize)
        -> Result<Vec<Option<(SegmentId, u64)>>>
    {
        let bs = self.config.meta.data_block_size;
        let mut out = Vec::with_capacity(n);
        for blk_idx in start..start + n as BlockIndex {
            let blk_ptr = match self.bmap.lookup(&blk_idx).await {
                Ok(p) => p,
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    out.push(None);
                    continue;
                },
                Err(e) => {
                    warn!("block placement - lookup bmap for block index {blk_idx} error: {:?}", e);
                    return Err(e);
                },
            };
            // Same order of tests as the planner, so the two cannot
            // disagree about what a pointer means.
            if BlockPtrFormat::is_zero_block(&blk_ptr) {
                out.push(None);
            } else if BlockPtrFormat::is_on_staging(&blk_ptr) {
                let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
                out.push(Some((segid, staging_off as u64)));
            } else {
                // A dummy pointer: dirty and not yet flushed, so it has no
                // place on staging to report.
                let _ = bs;
                out.push(None);
            }
        }
        Ok(out)
    }

    /// [`Self::block_placement`] for several block ranges in one go,
    /// answered in the order the ranges were given. Batched for the reason
    /// given on [`Self::read_plan_many`].
    pub async fn block_placement_many(&mut self, ranges: &[(BlockIndex, usize)])
        -> Result<Vec<Vec<Option<(SegmentId, u64)>>>>
    {
        let mut out = Vec::with_capacity(ranges.len());
        for (start, n) in ranges.iter().copied() {
            out.push(self.block_placement(start, n).await?);
        }
        Ok(out)
    }

    pub(crate) async fn plan_read(&mut self, off: usize, buf_len: usize) -> Result<Vec<ReadOp>> {
        self.plan_read_with(off, buf_len, true).await
    }

    /// Plan a read of `[off, off + buf_len)`.
    ///
    /// `count_hits` decides whether a resident block is recorded in
    /// `cache_hits`. A read counts them, because a hit is a read this layer
    /// served. A read-ahead does not: it is not serving anybody, and
    /// counting its planning made the number useless for the thing it is
    /// there for — a wide read-ahead over already-warm blocks reported
    /// hits by the thousand and drowned out the reads.
    pub(crate) async fn plan_read_with(&mut self, off: usize, buf_len: usize, count_hits: bool) -> Result<Vec<ReadOp>> {
        let data_block_size = self.config.meta.data_block_size;
        let max_get = self.config.runtime.read_get_max_bytes;

        let mut ops: Vec<ReadOp> = Vec::new();
        // (segid, s3_off_start, accumulated_len)
        let mut current_range: Option<(SegmentId, usize, usize)> = None;
        let flush_range = |ops: &mut Vec<ReadOp>,
                           current_range: &mut Option<(SegmentId, usize, usize)>| {
            if let Some((seg, off, len)) = current_range.take() {
                ops.push(ReadOp::Range { segid: seg, s3_off: off, dst_len: len });
            }
        };

        let mut consumed = 0usize;
        let mut blk_idx = (off / data_block_size) as BlockIndex;
        let mut block_off = off % data_block_size;

        while consumed < buf_len {
            let block_remaining = data_block_size - block_off;
            let dst_len = block_remaining.min(buf_len - consumed);

            // Cache check first. This must be a side-effect-free
            // probe: `get` would hand out the block, which on the
            // local-disk tier mlocks it and asserts on the next `get`
            // that it was not already locked — so planning with `get`
            // and then executing with `get` panicked on any clean-tier
            // hit. `contains` is also unusable here, since it promotes
            // a clean block into the dirty tier.
            let cache_hit = self.cache.has(&blk_idx);
            if cache_hit {
                if count_hits {
                    self.staging.read_timing().add_cache_hit();
                }
                flush_range(&mut ops, &mut current_range);
                ops.push(ReadOp::Cache {
                    blk_idx,
                    src_off_in_block: block_off,
                    dst_len,
                });
                consumed += dst_len;
                blk_idx += 1;
                block_off = 0;
                continue;
            }

            // bmap lookup; NotFound → treat as zero block.
            let blk_ptr = match self.bmap.lookup(&blk_idx).await {
                Ok(p) => p,
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    BlockPtrFormat::new_zero_block()
                }
                Err(e) => {
                    warn!("plan_read - lookup bmap for block index {blk_idx} error: {}", e);
                    return Err(e);
                }
            };

            if BlockPtrFormat::is_zero_block(&blk_ptr) {
                flush_range(&mut ops, &mut current_range);
                ops.push(ReadOp::Zero { dst_len });
            } else if BlockPtrFormat::is_on_staging(&blk_ptr) {
                let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);

                // WAL feature: a staging-pointer for a segid
                // greater than the on-disk last_cno means the
                // segment is in flight (memory-pinned, not yet
                // on S3). Read from memory, no S3 GET, no
                // coalescing.
                #[cfg(feature = "wal")]
                let is_inflight = self.wal.is_some()
                    && self.inode().get_last_cno() > self.inode().get_last_ondisk_cno()
                    && segid > self.inode().get_last_ondisk_cno();
                #[cfg(not(feature = "wal"))]
                let is_inflight = false;

                if is_inflight {
                    flush_range(&mut ops, &mut current_range);
                    #[cfg(feature = "wal")]
                    ops.push(ReadOp::Inmem {
                        segid,
                        s3_off: staging_off + block_off,
                        dst_len,
                    });
                    #[cfg(not(feature = "wal"))]
                    {
                        let _ = staging_off;
                        unreachable!()
                    }
                } else {
                    let s3_off = staging_off + block_off;
                    let extended = match &current_range {
                        Some((cur_seg, cur_off, cur_len)) => {
                            *cur_seg == segid
                                && cur_off + cur_len == s3_off
                                && cur_len + dst_len <= max_get
                        }
                        None => false,
                    };
                    if extended {
                        let (seg, off, len) = current_range.take().unwrap();
                        current_range = Some((seg, off, len + dst_len));
                    } else {
                        flush_range(&mut ops, &mut current_range);
                        current_range = Some((segid, s3_off, dst_len));
                    }
                }
            } else if BlockPtrFormat::is_dummy_value(&blk_ptr) {
                panic!(
                    "plan_read - dummy block ptr at blk_idx {} (write path leaked into read?)",
                    blk_idx,
                );
            } else {
                panic!(
                    "plan_read - unknown block ptr {} at blk_idx {}",
                    self.blk_ptr_decode_display(&blk_ptr),
                    blk_idx,
                );
            }

            consumed += dst_len;
            blk_idx += 1;
            block_off = 0;
        }

        flush_range(&mut ops, &mut current_range);
        Ok(ops)
    }

    /// The error an operation must return when the handle was not
    /// opened for the required access mode.
    ///
    /// POSIX lists `[EBADF] The fildes argument is not a valid file
    /// descriptor open for writing` (`write()`) and `... open for
    /// reading` (`read()`) as mandatory ("shall fail") errors.
    /// `ftruncate()` permits `[EBADF] or [EINVAL]` for the same
    /// condition; we use `EBADF` there too so every access-mode
    /// violation reports one errno.
    ///
    /// Built with `from_raw_os_error` because `std::io::ErrorKind`
    /// has no `EBADF` variant: `PermissionDenied` would surface as
    /// `EACCES` (which POSIX reserves for permission-bit failures at
    /// `open` time) and `InvalidInput` would surface as `EINVAL`
    /// (conformant for `ftruncate` but not for `read` / `write`).
    /// Callers get the exact errno via `Error::raw_os_error()`; note
    /// that `Error::kind()` is `Uncategorized` for EBADF and so
    /// cannot be matched on. This is also why the error carries no
    /// custom message: an `io::Error` can have a raw errno or a
    /// custom message, not both.
    #[inline]
    pub(crate) fn ebadf_bad_access_mode() -> Error {
        Error::from_raw_os_error(libc::EBADF)
    }

    pub async fn write(&mut self, off: usize, buf: &[u8]) -> Result<usize> {
        self.check_writable()?;
        #[cfg(feature = "wal")]
        self.check_txn_room()?;
        if !self.flags.is_writable() {
            return Err(Self::ebadf_bad_access_mode());
        }
        self.write_inner(off, buf).await
    }

    /// `write` without the write-access check.
    ///
    /// Used by WAL crash recovery, which replays previously
    /// acknowledged writes while opening the file and must therefore
    /// run regardless of the access mode the caller opened with —
    /// otherwise a read-only open of a file that crashed mid-flush
    /// could not be served correctly.
    async fn write_inner(&mut self, off: usize, buf: &[u8]) -> Result<usize> {
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        let fn_start = Instant::now();
        let len = buf.len();
        // O_APPEND: write at end-of-file, ignoring the caller-
        // supplied offset. Direct-API writes take &mut self, so
        // i_size cannot change between this read and the size update
        // at the end of the function — the borrow checker provides
        // the atomic-append serialization that POSIX requires.
        let off = if self.flags.is_append() {
            self.inode.size()
        } else {
            off
        };
        debug!("WRITE - off: {}, buf len: {}", off, len);

        let v = self.write_prepare(off, len);
        let fetched = self.write_retrieve(v).await?;
        for block in fetched.into_iter() {
            let blk_idx = block.index();
            let None = self.cache.insert(blk_idx, block) else {
                panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
            };
        }

        #[cfg(feature = "wal")]
        if let Some(wal) = &mut self.wal {
            let _ = wal.write(self.inode.get_last_seq(), off, buf).await?;
        }

        let mut bytes_write = 0;

        let data_block_size = self.config.meta.data_block_size;
        let blk_iter = BlockIndexIter::new(off, len, data_block_size);
        let mut next_slice = buf;
        for (blk_idx, off, len) in blk_iter {
            let (this, next) = next_slice.split_at(len);
            debug!("      - update cache block index {}, offset {}, len {}", blk_idx, off, len);
            self.update_cache(blk_idx, off, this);
            bytes_write += this.len();
            next_slice = next;
        }

        // bulk update bmap
        let blk_iter = BlockIndexIter::new(off, len, data_block_size);
        let mut new_blocks: usize = 0;
        for (blk_idx, _, _) in blk_iter {
            // force bmap update for dirty blocks
            let prev = self.bmap.insert(blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
            if prev.is_none() {
                new_blocks += 1;
            }
        }

        let oldsize = self.inode.size();
        if off + len > oldsize {
            self.inode.set_size(off + len);
            self.cache.set_size(off + len);
        }
        if new_blocks > 0 {
            // Each newly-allocated bmap entry backs `data_block_size`
            // bytes; convert to the 512-byte units that st_blocks
            // reports.
            self.inode.update_blocks((new_blocks * data_block_size) as isize);
        }
        self.inode.update_mtime();
        drop(permit);

        if let Err(e) = self.try_flush().await {
            let _ = self.rollback_from_persisted().await;
            return Err(e);
        }
        let _ = fn_start;
        Ok(bytes_write)
    }

    pub async fn write_zero(&mut self, off: usize, len: usize) -> Result<usize> {
        self.check_writable()?;
        #[cfg(feature = "wal")]
        self.check_txn_room()?;
        if !self.flags.is_writable() {
            return Err(Self::ebadf_bad_access_mode());
        }
        self.write_zero_inner(off, len).await
    }

    /// `write_zero` without the write-access check. See
    /// [`Self::write_inner`] for why WAL recovery needs this.
    async fn write_zero_inner(&mut self, off: usize, len: usize) -> Result<usize> {
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        let fn_start = Instant::now();
        // O_APPEND: same rule as write(). See the corresponding
        // comment there.
        let off = if self.flags.is_append() {
            self.inode.size()
        } else {
            off
        };
        debug!("WRITE ZERO - off: {}, len: {}", off, len);

        let v = self.write_prepare(off, len);
        let fetched = self.write_retrieve(v).await?;
        for block in fetched.into_iter() {
            let blk_idx = block.index();
            let None = self.cache.insert(blk_idx, block) else {
                panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
            };
        }

        #[cfg(feature = "wal")]
        if let Some(wal) = &mut self.wal {
            let _ = wal.write_zero(self.inode.get_last_seq(), off, len).await?;
        }

        let mut bytes_write = 0;
        let mut new_blocks: usize = 0;

        let data_block_size = self.config.meta.data_block_size;
        let oldsize = self.inode.size();
        let blk_iter = BlockIndexIter::new(off, len, data_block_size);
        for (blk_idx, start_off, data_len) in blk_iter {
            // for a complete block,
            // no need to update data in cache, because is's already all zero
            // and insert zero block into block map
            if start_off == 0 && data_len == data_block_size {
                // insert or update
                let prev = self.bmap.insert(blk_idx, BlockPtrFormat::new_zero_block()).await.expect("failed to insert new zero to bmap");
                if prev.is_none() {
                    new_blocks += 1;
                }
                bytes_write += data_len;
                let _ = self.cache.remove(&blk_idx);
                continue;
            }
            // for a incomplete block
            // last block execption which start off from block start and len exceed current file
            // TODO: merge this with new cache impl
            if start_off == 0 && (blk_idx as usize * data_block_size) + start_off + data_len > oldsize {
                // insert or update
                let prev = self.bmap.insert(blk_idx, BlockPtrFormat::new_zero_block()).await.expect("failed to insert new zero to bmap");
                if prev.is_none() {
                    new_blocks += 1;
                }
                bytes_write += data_len;
                let _ = self.cache.remove(&blk_idx);
                continue;
            }
            // update cache data with zero
            debug!("      - update cache block index {}, offset {}, len {}", blk_idx, start_off, data_len);
            let mut zero = Vec::with_capacity(data_len);
            zero.resize(data_len, 0);
            self.update_cache(blk_idx, start_off, &zero);
            // force bmap update for dirty blocks
            let prev = self.bmap.insert(blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
            if prev.is_none() {
                new_blocks += 1;
            }
            bytes_write += data_len;
        }

        let oldsize = self.inode.size();
        if off + len > oldsize {
            self.inode.set_size(off + len);
            self.cache.set_size(off + len);
        }
        if new_blocks > 0 {
            self.inode.update_blocks((new_blocks * data_block_size) as isize);
        }
        self.inode.update_mtime();
        drop(permit);

        if let Err(e) = self.try_flush().await {
            let _ = self.rollback_from_persisted().await;
            return Err(e);
        }
        let _ = fn_start;
        Ok(bytes_write)
    }

    // write in batch style, all blocks in input vec should be full block
    /// Blocking-acquire wrapper. The reactor takes the permit itself
    /// with `try_lock`, so it can put the request back instead of
    /// waiting on the handler task; see `HyperFile::try_lock`.
    pub(crate) async fn write_aligned_batch(&mut self, blocks: Vec<AlignedDataBlockWrapper>) -> Result<usize> {
        self.check_writable()?;
        #[cfg(feature = "wal")]
        self.check_txn_room()?;
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        self.write_aligned_batch_locked(blocks, permit).await
    }

    /// Write log records covering a batch that may hold partial blocks.
    ///
    /// Full blocks are coalesced into runs as in
    /// [`Self::wal_log_aligned_batch`]. A partial block gets its own record at
    /// its own offset and length, since nothing adjacent can be joined to a
    /// range that does not reach a block boundary.
    #[cfg(feature = "wal")]
    async fn wal_log_batch(
        &mut self,
        merged: &BTreeMap<BlockIndex, (bool, Vec<BatchDataBlockWrapper>)>,
        bs: usize,
    ) -> Result<()> {
        let seq = self.inode.get_last_seq();
        let mut bufs: Vec<Vec<u8>> = Vec::new();
        let mut pending = Vec::new();

        // (start_index, run_len) of consecutive full blocks.
        let mut run: Option<(BlockIndex, Vec<u8>)> = None;
        let mut run_end: BlockIndex = 0;

        let flush_run = |run: &mut Option<(BlockIndex, Vec<u8>)>,
                             bufs: &mut Vec<Vec<u8>>| -> Option<(usize, usize)> {
            run.take().map(|(start, buf)| {
                let off = start as usize * bs;
                let len = buf.len();
                bufs.push(buf);
                (off, len)
            })
        };

        for (blk_idx, (is_full, v)) in merged.iter() {
            if *is_full {
                let block = v.first().expect("a full block group holds one block");
                match run.as_mut() {
                    Some((_, buf)) if *blk_idx == run_end => {
                        buf.extend_from_slice(block.as_slice());
                        run_end = blk_idx + 1;
                        continue;
                    },
                    _ => {},
                }
                if let Some((off, len)) = flush_run(&mut run, &mut bufs) {
                    let buf_ref = &bufs[bufs.len() - 1][..len];
                    let wal = self.wal.as_mut().expect("checked by the caller");
                    pending.push(wal.write(seq, off, buf_ref));
                }
                run = Some((*blk_idx, block.as_slice().to_vec()));
                run_end = blk_idx + 1;
            } else {
                if let Some((off, len)) = flush_run(&mut run, &mut bufs) {
                    let buf_ref = &bufs[bufs.len() - 1][..len];
                    let wal = self.wal.as_mut().expect("checked by the caller");
                    pending.push(wal.write(seq, off, buf_ref));
                }
                for block in v.iter() {
                    let off = *blk_idx as usize * bs + block.offset();
                    let buf = block.as_slice()[..block.len()].to_vec();
                    bufs.push(buf);
                    let buf_ref = bufs.last().expect("just pushed").as_slice();
                    let wal = self.wal.as_mut().expect("checked by the caller");
                    pending.push(wal.write(seq, off, buf_ref));
                }
            }
        }
        if let Some((off, len)) = flush_run(&mut run, &mut bufs) {
            let buf_ref = &bufs[bufs.len() - 1][..len];
            let wal = self.wal.as_mut().expect("checked by the caller");
            pending.push(wal.write(seq, off, buf_ref));
        }

        for res in futures::future::join_all(pending).await {
            res?;
        }
        drop(bufs);
        Ok(())
    }

    /// Write log records covering an aligned batch, coalescing adjacent
    /// blocks of the same kind into one record each.
    ///
    /// A record's key encodes the range it covers, so one object can stand for
    /// many consecutive blocks exactly as a multi-block `write_inner` does.
    /// Without the coalescing a 24 MiB batch would be six thousand objects,
    /// and the batch API exists because that cost was measured and rejected.
    ///
    /// The records go out concurrently. `WalReadWrite::write` hands back a
    /// future that does not borrow the log, so they can be started and then
    /// awaited together — but it aliases the buffer it was given without
    /// owning it, so the buffers built here must outlive the join.
    #[cfg(feature = "wal")]
    async fn wal_log_aligned_batch(&mut self, blocks: &[AlignedDataBlockWrapper], bs: usize) -> Result<()> {
        // (start_index, run_len, is_zero)
        let mut runs: Vec<(BlockIndex, usize, bool)> = Vec::new();
        for b in blocks.iter() {
            match runs.last_mut() {
                Some((start, n, zero))
                    if *zero == b.is_zero() && b.index() == *start + *n as BlockIndex =>
                {
                    *n += 1;
                },
                _ => runs.push((b.index(), 1, b.is_zero())),
            }
        }

        let seq = self.inode.get_last_seq();
        let mut bufs: Vec<Vec<u8>> = Vec::new();
        let mut pending = Vec::new();
        let mut at = 0usize;
        for (start, n, is_zero) in runs {
            let off = start as usize * bs;
            if is_zero {
                let wal = self.wal.as_mut().expect("checked by the caller");
                pending.push(wal.write_zero(seq, off, n * bs));
            } else {
                let mut buf = Vec::with_capacity(n * bs);
                for b in &blocks[at..at + n] {
                    buf.extend_from_slice(b.as_slice());
                }
                bufs.push(buf);
                let buf_ref = bufs.last().expect("just pushed");
                let wal = self.wal.as_mut().expect("checked by the caller");
                pending.push(wal.write(seq, off, buf_ref));
            }
            at += n;
        }

        for res in futures::future::join_all(pending).await {
            res?;
        }
        // Held until every record is down, since the futures alias them.
        drop(bufs);
        Ok(())
    }

    pub(crate) async fn write_aligned_batch_locked(&mut self, mut blocks: Vec<AlignedDataBlockWrapper>, permit: OwnedSemaphorePermit) -> Result<usize> {
        // Checked here rather than only in the outer wrapper: the reactor's
        // batch arms call this directly, so a guard above it covers one surface
        // and not the other.
        #[cfg(feature = "wal")]
        self.check_txn_room()?;
        if !self.flags.is_writable() {
            return Err(Self::ebadf_bad_access_mode());
        }
        if blocks.len() == 0 {
            return Ok(0);
        }

        // sort and dedup
        blocks.sort_by_key(|b| b.index());
        blocks.reverse();
        blocks.dedup_by_key(|b| b.index());
        blocks.reverse();

        let data_block_size = self.config.meta.data_block_size;

        // Log before touching any state, for the same reason `write_inner`
        // does: the caller is told this write is durable when the call
        // returns, and the flush path is allowed to answer before its upload
        // lands *because* the log holds the data. A write path the log never
        // saw breaks that argument rather than merely missing a feature — the
        // flush is acknowledged while the bytes live only in memory.
        //
        // Runs of adjacent blocks are coalesced into one record. One record
        // per block would be one object per block, which is the cost the
        // batch API exists to avoid.
        #[cfg(feature = "wal")]
        if self.wal.is_some() {
            self.wal_log_aligned_batch(&blocks, data_block_size).await?;
        }

        let mut bytes_write = 0;
        let mut new_blocks: usize = 0;
        for block_wrapper in blocks.iter() {
            let blk_idx = block_wrapper.index();
            let blk_sz = block_wrapper.size();
            assert!(blk_sz == data_block_size);
            if block_wrapper.is_zero() {
                let _ = self.cache.remove(&blk_idx);
                bytes_write += blk_sz;
                let prev = self.bmap.insert(blk_idx, BlockPtrFormat::new_zero_block()).await.expect("failed to insert new zero to bmap");
                if prev.is_none() {
                    new_blocks += 1;
                }
                continue;
            }
            self.update_cache(blk_idx, 0, block_wrapper.as_slice());
            bytes_write += blk_sz;
            // force bmap update for dirty blocks
            let prev = self.bmap.insert(blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
            if prev.is_none() {
                new_blocks += 1;
            }
        }
        // try update file size by offset and len from last block
        let last_block_wrapper = blocks.last().expect("unable to get last block, input blocks is empty");
        let oldsize = self.inode.size();
        let off = (last_block_wrapper.index() as usize) * data_block_size;
        let len = last_block_wrapper.size();
        if off + len > oldsize {
            self.inode.set_size(off + len);
            self.cache.set_size(off + len);
        }
        if new_blocks > 0 {
            self.inode.update_blocks((new_blocks * data_block_size) as isize);
        }
        self.inode.update_mtime();
        drop(permit);
        if let Err(e) = self.try_flush().await {
            let _ = self.rollback_from_persisted().await;
            return Err(e);
        }
        Ok(bytes_write)
    }

    /// How block `blk_idx` is mapped. See [`BlockState`].
    ///
    /// Cheap: one bmap lookup, no data transfer. A block that is
    /// dirty in cache reports [`BlockState::Mapped`], because the
    /// write paths install a bmap entry when they dirty a block.
    pub async fn block_state(&mut self, blk_idx: BlockIndex) -> Result<BlockState> {
        if !self.flags.is_readable() {
            return Err(Self::ebadf_bad_access_mode());
        }
        self.block_state_inner(blk_idx).await
    }

    /// `block_state` without the access-mode check, for in-crate
    /// callers that have already established their own.
    async fn block_state_inner(&mut self, blk_idx: BlockIndex) -> Result<BlockState> {
        match self.bmap.lookup(&blk_idx).await {
            Ok(blk_ptr) => {
                if BlockPtrFormat::is_zero_block(&blk_ptr) {
                    Ok(BlockState::Zero)
                } else {
                    Ok(BlockState::Mapped)
                }
            },
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(BlockState::Unmapped),
            Err(e) => Err(e),
        }
    }

    /// Borrow block `blk_idx` for reading, loading it from staging
    /// if it is not already cached.
    ///
    /// Returns `Ok(None)` when the block is not backed by data —
    /// either no bmap entry at all or an explicit zero block. That
    /// is deliberately *not* the same as "reads as zeros", which is
    /// all the byte API can tell you; use [`Self::block_state`] to
    /// tell the two hole flavors apart.
    ///
    /// The returned slice is the cache's buffer, not a copy, and is
    /// exactly `data_block_size` bytes. The guard borrows `self`,
    /// so no flush, eviction or other file operation can run while
    /// it is alive.
    ///
    /// Unlike `read`, this does not stop at `i_size`: a block above
    /// EOF that holds data (which only this API can produce, see
    /// [`Self::block_mut`]) is returned. Also unlike `read`, this
    /// does not update `atime`.
    pub async fn block(&mut self, blk_idx: BlockIndex) -> Result<Option<BlockRef<'_>>> {
        if !self.flags.is_readable() {
            return Err(Self::ebadf_bad_access_mode());
        }

        // Probe before borrowing. `Cache::has` is the only
        // side-effect-free test: on the local-disk tier `get` mlocks
        // the block it returns and asserts it was not already
        // locked, so it cannot be called twice for one clean block,
        // and `contains` would promote the block into the dirty
        // tier. Checked ahead of the bmap so that a dirty block
        // whose bmap entry is still a placeholder is served from
        // cache.
        if self.cache.has(&blk_idx) {
            self.staging.read_timing().add_cache_hit();
            let block = self.cache.get(&blk_idx)
                .expect("cache lost a block between has() and get() under &mut self");
            return Ok(Some(BlockRef::cached(block)));
        }

        // Not cached: consult the bmap.
        let blk_ptr = match self.bmap.lookup(&blk_idx).await {
            Ok(p) => p,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if BlockPtrFormat::is_zero_block(&blk_ptr) {
            return Ok(None);
        }

        // Backed by real data in staging. Load a whole block into a
        // fresh buffer, the same shape the byte read path uses for a
        // cache miss.
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        let block = self.cache.new_block(blk_idx);
        let buf = block.as_mut_slice();
        let res = self.load_data_block_read_path(blk_idx, blk_ptr, 0, buf).await;
        drop(permit);
        res?;

        // Keep it for the next reader if the cache will take it.
        // With the data cache disabled it comes straight back and the
        // guard owns it.
        match self.cache.insert_clean(blk_idx, block) {
            None => {
                let block = self.cache.get(&blk_idx)
                    .expect("block just installed in the clean tier is missing");
                Ok(Some(BlockRef::cached(block)))
            },
            Some(block) => Ok(Some(BlockRef::owned(block))),
        }
    }

    /// Take the per-file permit, or report that it is taken.
    ///
    /// Used by every operation the reactor's handler task can run
    /// directly. Waiting for this permit on that task deadlocks: a
    /// write's retrieve carries the permit from `spawn_write` until
    /// `absorb_write`, and the handler is the only thing that can run
    /// the callback which releases it, so a handler blocked here waits
    /// on work only it could perform. The caller must put the request
    /// back instead.
    ///
    /// On the direct API this cannot fail. Those methods take
    /// `&mut self`, so no second operation on the same file can be in
    /// flight to hold the permit.
    #[cfg(feature = "reactor")]
    pub(crate) fn try_lock(&self) -> Result<OwnedSemaphorePermit> {
        self.sema.clone().try_acquire_owned()
            .map_err(|_| Error::new(ErrorKind::ResourceBusy, "per-file permit busy"))
    }

    /// Cache a block that a spawned read-only fetch filled.
    ///
    /// Dropped rather than cached if the index has since become
    /// resident some other way: a `block_mut` or a write between the
    /// spawn and here would have made it dirty, and that copy is the
    /// current one. Two concurrent misses on the same index also arrive
    /// here twice; the second is redundant but harmless.
    /// The cached block for `blk_idx`, if it is resident.
    ///
    /// A borrow of what the cache holds, so the caller must `unlock` it
    /// when done — the local-disk tier locks on `get` and asserts on the
    /// next `get` that it was not already locked.
    ///
    /// Nothing here reaches staging: an index that is not cached is simply
    /// absent, which is what makes a batch of these cheap enough to answer
    /// in one message.
    #[cfg(feature = "reactor")]
    pub(crate) fn cached_block(&mut self, blk_idx: BlockIndex) -> Option<&DataBlock> {
        self.cache.get(&blk_idx)
    }

    pub(crate) fn absorb_block(&mut self, blk_idx: BlockIndex, block: DataBlock) {
        if self.cache.has(&blk_idx) {
            debug!("absorb_block - block index {} already resident, dropping the loaded copy", blk_idx);
            return;
        }
        let _ = self.cache.insert_clean(blk_idx, block);
    }

    /// Borrow block `blk_idx` for modification, loading it from
    /// staging if it is not already cached.
    ///
    /// The block is marked dirty at acquisition, so writes through
    /// the guard need no write-back call and cannot be lost by an
    /// early return between acquire and drop. The next `flush`
    /// persists it. Borrowing the same block any number of times
    /// within one flush window produces exactly one new version,
    /// matching `write`.
    ///
    /// `create` decides what happens for a block that is not backed
    /// by data: `false` returns `Ok(None)`, `true` materializes a
    /// zero-filled block and returns a guard for it. A created
    /// block is guaranteed zeroed on both cache tiers.
    ///
    /// `i_size` is left alone — see the [module docs](super::block)
    /// for why, and for the consequence that a block above EOF is
    /// durable but unreachable through `read`. `i_blocks` is
    /// updated when a new bmap entry appears.
    ///
    /// A flush may be triggered on entry if the dirty set is
    /// already over threshold. It cannot be triggered on drop,
    /// since `Drop` cannot await, so a caller that dirties blocks
    /// only through this API and never calls `flush` will grow the
    /// dirty set without bound.
    pub async fn block_mut(&mut self, blk_idx: BlockIndex, create: bool) -> Result<Option<BlockMut<'_>>> {
        self.check_writable()?;
        #[cfg(feature = "wal")]
        self.check_txn_room()?;
        if !self.flags.is_writable() {
            return Err(Self::ebadf_bad_access_mode());
        }

        // Flush here if we are already over threshold: `Drop` is
        // not async, so this is the only point at which the guard
        // API can honor the auto-flush contract the byte writes
        // have.
        if let Err(e) = self.try_flush().await {
            let _ = self.rollback_from_persisted().await;
            return Err(e);
        }

        // Get the block into the dirty tier, loading or creating it
        // first if it is not cached at all.
        if !self.cache.has(&blk_idx) {
            // Probe with the side-effect-free `has`: `get_mut` is what
            // promotes a clean block into the dirty tier, and it is
            // called once, below.
            let state = self.block_state_inner(blk_idx).await?;
            if state.is_hole() && !create {
                return Ok(None);
            }

            #[cfg(feature = "reactor")]
            let permit = self.try_lock()?;
            #[cfg(not(feature = "reactor"))]
            let permit = self.sema.clone().acquire_owned().await.unwrap();
            // `new_block` allocates zeroed, which is what a created
            // block needs and also the correct starting point for an
            // explicit zero block being materialized. Deliberately not
            // the cache's own new-dirty-block path, which on the
            // local-disk tier hands back an unzeroed view of the cache
            // file.
            let block = self.cache.new_block(blk_idx);
            if state.is_mapped() {
                let blk_ptr = self.bmap.lookup(&blk_idx).await?;
                let buf = block.as_mut_slice();
                if let Err(e) = self.load_data_block_write_path(blk_idx, blk_ptr, 0, buf).await {
                    drop(permit);
                    return Err(e);
                }
            }
            let None = self.cache.insert(blk_idx, block) else {
                panic!("BlockIndex {} already on the dirty list", blk_idx);
            };
            drop(permit);
        }

        // Give the bmap a placeholder for this index, on *every* path
        // including the already-cached one.
        //
        // This is not only about telling flush to assign a real
        // pointer. `flush_process_build_segment` collects the dirty
        // meta nodes *before* it assigns pointers to data blocks, so a
        // node that is not already dirty when the flush starts is
        // never written — the flush would store the block's data and
        // update the map in memory, then persist neither the node
        // holding the new pointer nor any record of it. A cold reader
        // would follow the old pointer to the previous version, with
        // no error reported anywhere.
        //
        // While the whole map still fits in the inode's inline root
        // that is invisible, because the root travels with the inode
        // and every flush writes the inode. It only surfaces once the
        // map has spilled to a node, which is why skipping this on the
        // already-cached path looked harmless. The byte write path has
        // always inserted for every block it dirties.
        let prev = self.bmap.insert(blk_idx, BlockPtrFormat::dummy_value()).await?;
        if prev.is_none() {
            let data_block_size = self.config.meta.data_block_size;
            self.inode.update_blocks(data_block_size as isize);
        }
        self.inode.update_mtime();

        let block = self.cache.get_mut(&blk_idx)
            .expect("block is not in the cache after being loaded or created");
        // Normalize the dirty flag across cache tiers: the local-disk
        // `get_mut` sets it, the in-memory one only moves the block
        // into the dirty map.
        block.set_dirty();
        // Retain the block in the read cache once flushed. A full-block
        // byte write deliberately does not, so that streaming writes do
        // not evict the read cache, but this API exists for callers
        // that read-modify-write the same blocks repeatedly.
        block.set_should_cache();
        Ok(Some(BlockMut::new(block)))
    }

    pub(crate) fn need_flush(&self) -> bool {
        // check if dirty data bytes exceed segment buffer threshold
        let ndatadirty = self.cache.dirty_count();
        let data_block_size = self.config.meta.data_block_size;
        // trigger flush because we meet memory threshold
        let threshold_flush = ndatadirty > self.max_dirty_blocks
            || (ndatadirty * data_block_size) > self.config.runtime.segment_buffer_size;
        // trigger immediate flush if file opened in O_DIRECT or O_SYNC or O_DSYNC
        let immediate_flush = if self.flags.is_sync_flush_mode() {
            #[cfg(not(feature = "wal"))]
            { true }
            // in wal mode, we dont's need immediately flush
            // unless wal is not configured
            #[cfg(feature = "wal")]
            { self.wal.is_none() }
        } else {
            false
        };

        // Time alone does not publish when there is a log.
        //
        // Its purpose without one is to bound how long an acknowledged write
        // sits only in memory. A log already bounds that — the write is durable
        // when it returns — so all a timer adds is publishing at a moment
        // nobody declared. That matters because the newest published checkpoint
        // is what recovery must reach: if it can land in the middle of the
        // caller's own unit of work, then "recover to at least the newest
        // checkpoint" and "stop at a declared state" are in conflict, and
        // `WalRecoveryMode::Barrier` cannot honour both.
        //
        // The memory bound stays: `threshold_flush` still publishes, because
        // dropping it would trade a bounded cache for an unbounded one. A
        // caller that needs publishes to happen only at its own boundaries has
        // to raise those thresholds, and knows it is choosing that.
        //
        // Note `data_cache_dirty_max_flush_interval` set to 0 does not disable
        // the timer: `elapsed() >= Duration::from_millis(0)` is always true, so
        // 0 publishes on every check. Without a log, raising it is the way to
        // slow the timer down.
        // An open transaction publishes nothing. The caller has said its
        // writes are one unit, and a threshold crossing partway through would
        // make half of that unit the container's newest checkpoint — which the
        // next open would take as its baseline.
        #[cfg(feature = "wal")]
        if self.publishes_on_request_only() {
            return false;
        }
        #[cfg(feature = "wal")]
        let last_flush_expired = self.wal.is_none() && {
            let max_flush_interval = self.config.runtime.data_cache_dirty_max_flush_interval;
            self.state.get_last_flush().elapsed() >= Duration::from_millis(max_flush_interval)
        };
        #[cfg(not(feature = "wal"))]
        let last_flush_expired = {
            let max_flush_interval = self.config.runtime.data_cache_dirty_max_flush_interval;
            self.state.get_last_flush().elapsed() >= Duration::from_millis(max_flush_interval)
        };
        if last_flush_expired || threshold_flush || immediate_flush {
            return true;
        }
        false
    }

    // try flush out dirty data if all threshold condition meet
    pub(crate) async fn try_flush(&mut self) -> Result<bool> {
        if self.need_flush() {
            let _ = self.flush().await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Explicit flush with rollback-on-failure semantics.
    ///
    /// `HyperTrait::flush()` commits all pending in-memory mutations to
    /// staging. If it fails, in-memory state still reflects the mutations
    /// that never made it to disk, diverging from the persisted state. This
    /// wrapper rolls the in-memory state back to what's persisted on
    /// failure, so callers that observe `Err` also see an in-memory state
    /// that matches reality.
    ///
    /// Users exercise this via `fs_flush` / `fh_flush`.
    pub async fn flush_with_rollback(&mut self) -> Result<SegmentId> {
        // Refusals are settled before the rollback is on the table. A flush that
        // was declined never touched anything, so rolling back would discard
        // dirty data over a request that was simply not allowed — losing
        // acknowledged writes by asking the wrong question at the wrong time.
        //
        // `flush` performs this check too, for the callers that do not come
        // through here. It has to happen on both sides: there, so no caller can
        // skip it; here, so its failure does not reach the rollback.
        self.check_writable()?;
        #[cfg(feature = "wal")]
        if self.txn_open {
            return Err(Error::new(ErrorKind::ResourceBusy,
                "a transaction is open, so publishing was asked not to happen; commit it to publish"));
        }
        match self.flush().await {
            Ok(segid) => Ok(segid),
            Err(e) => {
                let _ = self.rollback_from_persisted().await;
                Err(e)
            }
        }
    }

    /// POSIX-`fdatasync` flavoured flush.
    ///
    /// Acts as `flush_with_rollback` but skips work that
    /// `fdatasync(2)` is allowed to skip per POSIX: flushing
    /// metadata that is not required to read the file's data
    /// correctly. In hyperfile terms, that means skipping the
    /// inode write when only attribute fields (`atime` / `mtime` /
    /// `ctime` / `mode` / `uid` / `gid`) are dirty and there is
    /// no dirty data block and no dirty bmap.
    ///
    /// If data or bmap is dirty, this delegates to
    /// `flush_with_rollback` — those updates are necessary for
    /// data correctness (`i_size` lives in the inode) and
    /// `fdatasync` must persist them too.
    ///
    /// Returns the last cno currently persisted on staging. When
    /// the call short-circuits (no data/bmap dirt), no new
    /// segment is written and the returned cno is the same as
    /// `last_cno()`.
    pub async fn flush_data(&mut self) -> Result<SegmentId> {
        if self.dirty_block_count() == 0 && !self.bmap_dirty() {
            // Inode may still be attr-dirty, but `fdatasync` is
            // explicitly allowed to skip persisting attr-only
            // changes. Return the last persisted cno unchanged.
            return Ok(self.inode.get_last_ondisk_cno());
        }
        self.flush_with_rollback().await
    }

    /// Roll back the in-memory state of this file to match what's persisted
    /// on staging. Used by the failure path of write/truncate/write_zero/flush
    /// to undo in-memory mutations when the flush fails. Steps:
    ///   1. Reload persisted inode and rebuild bmap from it.
    ///   2. Replace self.inode fields with the persisted values, preserving
    ///      read-only attrs like ino/uid/gid/mode/nlink which aren't mutated
    ///      by these code paths.
    ///   3. Reset bookkeeping (last_seq, ondisk_state) to the reloaded values
    ///      so subsequent operations see a consistent state.
    ///   4. Discard dirty data blocks and cached read blocks.
    ///
    /// Best-effort: if reloading the persisted inode itself fails, this
    /// function returns the error without attempting further recovery. The
    /// caller should propagate the original flush error regardless.
    pub(crate) async fn rollback_from_persisted(&mut self) -> Result<()> {
        // 1. reload persisted inode + rebuild bmap
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let inode_state = self.staging.load_inode(&mut raw_inode.as_mut_u8_slice()).await?;
        let meta_block_loader = self.staging.to_block_loader();
        let node_cache = self.bmap.get_node_cache();
        // Borrowed, not copied: see the note in `HyperTrait::refresh_bmap`.
        let new_bmap = BMap::<BlockIndex, BlockPtr, BlockPtr, L, C>::read(
            &raw_inode.i_bmap,
            self.config.meta.meta_block_size,
            meta_block_loader,
            node_cache,
        )?;
        new_bmap.set_cache_limit(self.config.runtime.node_cache_blocks);

        // 2. commit fresh bmap + inode fields from persisted state
        self.bmap = new_bmap;
        self.inode.restore_attr_from_raw(&raw_inode);
        self.inode.i_last_seq = raw_inode.i_last_seq;
        self.inode.i_last_cno = raw_inode.i_last_cno;
        self.inode.set_last_ondisk_cno(raw_inode.i_last_cno);
        self.inode.set_ondisk_state(inode_state);
        self.cache.set_size(self.inode.size());

        // 3. discard in-memory dirty / cached data
        self.cache.clear_dirty();
        self.cache.clear_data_blocks_cache();

        Ok(())
    }

    #[allow(dead_code)]
    #[cfg(all(feature = "wal", feature = "blocking"))]
    pub(crate) async fn kick_wal_protected_flush_blocking(&mut self) -> Result<SegmentId> {
        let lock = self.flush_lock().await;
        match self.wal_flush_process_blocking().await {
            Ok(segid) => {
                self.flush_unlock(lock);
                return Ok(segid);
            },
            Err(e) => {
                warn!("kick_wal_protected_flush_blocking failed: {:?}", e);
                return self.wal_flush_recovery(lock).await;
            },
        }
    }

    #[cfg(all(feature = "wal", feature = "reactor"))]
    pub(crate) async fn kick_wal_protected_flush_reactor(&mut self, fh: ChannelGroup<FileContext<'a>>) -> Result<SegmentId> {
        let Ok(lock) = self.flush_lock.clone().try_lock_owned() else {
            // FIXME: skip this flush by return ResourceBusy for now,
            // should this flush be re-queue?
            return Err(Error::new(ErrorKind::ResourceBusy, "another flush is in-progress"));
        };
        self.state.set_flushing();
        match self.wal_flush_process_reactor(fh, lock).await {
            Ok(segid) => {
                return Ok(segid);
            },
            Err((lock, e)) => {
                warn!("kick_wal_protected_flush_reactor failed: {:?}", e);
                return self.wal_flush_recovery(lock).await;
            },
        }
    }

    #[cfg(all(feature = "wal", feature = "reactor"))]
    pub(crate) async fn wal_flush_done(&mut self, lock: OwnedMutexGuard<()>, segid: SegmentId, od_state: OnDiskState, bmap_cache_limit: usize) {
        self.inode_mut().set_ondisk_state(Some(od_state));
        let last_cno = self.inode().get_last_cno();
        assert!(last_cno == segid);
        self.inode_mut().set_last_ondisk_cno(last_cno);
        self.wal_clear_mem_segment(segid).await;
        // Fire-and-forget delete of the persisted WAL objects.
        //
        // WAL chunks are written under the inode's last_seq at the
        // time of each write. The flush path calls get_next_seq(),
        // allocates the NEW segid for the segment, and the WAL
        // chunks stay under (segid - 1). Delete that prefix.
        //
        // A lost delete here leaves storage slightly bloated —
        // recovery filters by last_ondisk_cno, so old entries are
        // still correct — and is not worth blocking the flush ack.
        self.wal_drop_superseded_prefixes(segid);
        // restore cache limit
        self.restore_data_blocks_cache_limit();
        self.bmap_set_cache_limit(bmap_cache_limit);
        self.set_last_flush();
        self.flush_unlock(lock);
    }

    /// Where recovery stops. See [`WalRecoveryMode`].
    #[cfg(feature = "wal")]
    pub fn wal_recovery_mode(&self) -> WalRecoveryMode {
        self.config.wal.recovery_mode
    }

    /// Whether a checkpoint's log group may be applied.
    ///
    /// The barrier's manifest is checked against what is stored rather than
    /// trusted: it lists what was written so that this can be verified without
    /// depending on a listing being complete or on the order the records went
    /// out in.
    #[cfg(feature = "wal")]
    async fn wal_group_state(&self, segid: SegmentId) -> Result<WalGroupState> {
        let Some(ref wal) = self.wal else {
            return Err(Error::new(ErrorKind::Unsupported, "wal is not configured"));
        };
        let Some(barrier) = wal.read_barrier(segid).await? else {
            // No barrier, so nothing vouches for this group. A transaction marker
            // says more than that: part of it is work the caller declared to be
            // one unit and did not finish, and that part must not be applied in
            // either mode.
            if let Some(from_seq) = wal.read_txn_marker(segid).await? {
                return Ok(WalGroupState::TxnOpen { from_seq });
            }
            return Ok(WalGroupState::Unsealed);
        };
        let stored = wal.list_chunks(segid).await?;
        let missing = barrier.entries.iter()
            .filter(|(seq, off, len)| {
                match stored.get(seq) {
                    Some(desc) => desc.offset != *off || desc.len != *len,
                    None => true,
                }
            })
            .count();
        if missing > 0 {
            return Ok(WalGroupState::Incomplete { missing });
        }
        Ok(WalGroupState::Complete)
    }

    /// Whether a later session sealed a group above this one.
    ///
    /// A barrier above means some session wrote and sealed on top of a base that
    /// did not include this group. So this group belongs to a session that is
    /// gone, and the work above it is a delta on the very checkpoint recovery
    /// would land on — applying that is not inventing a state, it is the state
    /// the later session had.
    ///
    /// Cheap on purpose, and asked only of a group that would otherwise stop the
    /// replay: one read per group above it, none at all in the ordinary case
    /// where nothing stops.
    ///
    /// A barrier is the evidence rather than a whole group state because that is
    /// what makes the group abandoned; each group above is still judged on its
    /// own when the walk reaches it.
    #[cfg(feature = "wal")]
    async fn wal_group_superseded(&self, segid: SegmentId, above: &[SegmentId]) -> Result<bool> {
        let Some(ref wal) = self.wal else {
            return Err(Error::new(ErrorKind::Unsupported, "wal is not configured"));
        };
        for id in above.iter().filter(|id| **id > segid) {
            if wal.read_barrier(*id).await?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    // starting wal flush recovery process by reloading inode from backend storage
    // everything should be clean or give a panic if unrecoverable
    #[cfg(feature = "wal")]
    /// Replay the log to publish what a failed flush could not, retrying a
    /// bounded number of times before giving up on this file.
    ///
    /// The caller of the flush is long gone — a WAL flush answers as soon as
    /// the log holds the data, and the upload runs detached — so there is
    /// nobody to return an error to. Replaying is the remedy, and it can fail
    /// for the same reason the original publish did: the object store is
    /// refusing writes.
    ///
    /// When it will not go through, the file stops accepting modification and
    /// goes on serving reads. Every write that has been acknowledged is in the
    /// log, so nothing is lost and offline repair still has everything it
    /// needs — whereas continuing to accept writes would pile more data
    /// behind a publish that is not happening. This used to panic, which for
    /// a server built on this crate means the process, and takes down reads
    /// that were still being served correctly.
    pub(crate) async fn wal_flush_recovery(&mut self, lock: OwnedMutexGuard<()>) -> Result<SegmentId> {
        debug!("wal_flush_recovery - started");
        let mut last_err = None;
        for attempt in 1..=DEFAULT_FLUSH_RETRIES {
            match self.do_wal_flush_recovery().await {
                // Applying nothing is an outcome, not a failure, and under
                // `Barrier` it is the ordinary one: stopping at an unsealed
                // group means there was nothing this mode was willing to take.
                // Reading the zero as failure spent the retries and then set
                // the fail-stop flag, leaving a container that opened fine and
                // refused every write, with `do_open` discarding the only
                // explanation. What happened is in the report; the return value
                // does not have to say it as well.
                Ok(cno) => {
                    self.flush_unlock(lock);
                    if cno == 0 {
                        debug!("wal_flush_recovery - nothing to apply");
                    }
                    return Ok(cno);
                },
                Err(e) => {
                    warn!("wal_flush_recovery - attempt {}/{} failed: {}",
                        attempt, DEFAULT_FLUSH_RETRIES, e);
                    last_err = Some(e);
                },
            }
            if attempt < DEFAULT_FLUSH_RETRIES {
                tokio::time::sleep(std::time::Duration::from_secs(
                    DEFAULT_FLUSH_BACKOFF_SECS * attempt as u64)).await;
            }
        }

        // Out of attempts. Release the flush lock first, so reads that defer
        // on a flush being in progress are not held behind a flush that will
        // never finish.
        self.flush_unlock(lock);
        self.state.set_publish_failed();
        let msg = format!(
            "wal_flush_recovery - could not publish after {} attempts, \
             file is now read-only; unflushed data is in the wal and can be \
             recovered with offline tools{}",
            DEFAULT_FLUSH_RETRIES,
            last_err.map(|e| format!(": {}", e)).unwrap_or_default());
        warn!("{}", msg);
        Err(Error::new(ErrorKind::ReadOnlyFilesystem, msg))
    }

    // handler flush lock in caller
    #[cfg(feature = "wal")]
    async fn do_wal_flush_recovery(&mut self) -> Result<SegmentId> {
        let v = self.wal_list_segments().await?;
        let last_ondisk = self.inode().get_last_ondisk_cno();
        // filter out candidate segment id to playback
        let mut segids: Vec<_> = v.into_iter().filter(|id| *id >= last_ondisk).collect();
        segids.sort();
        debug!("do_wal_flush_recovery - replay segments {:?}", segids);

        // Nothing to replay leaves the container at its last published
        // checkpoint, and a publish only happens at a seal — so that is a
        // declared-consistent point, and the report says so.
        self.wal_recovery_report = WalRecoveryReport {
            replayed: false,
            landed_on_barrier: true,
            records_dropped: 0,
        };

        let mut cno = 0;
        let mut stopped_at: Option<SegmentId> = None;
        let mut skipped: Vec<SegmentId> = Vec::new();
        let mut last_applied_sealed = true;
        let mut remaining = segids.clone();
        for segid in segids {
            remaining.retain(|id| *id != segid);
            // In `Barrier` mode only a sealed, complete group may be applied,
            // and a group failing either test stops the replay rather than
            // being skipped: skipping one and applying a later one would
            // produce a state that never existed, which is worse than a torn
            // one — that at least was some moment's truth.
            //
            // `Latest` mode applies what it can, which is what keeps every
            // acknowledged write. It is the default for that reason.
            // Checked for every group, in both modes: `Barrier` acts on it,
            // and `Latest` needs it for the report — a caller decides whether
            // to repair by asking whether the landing point was sealed, and
            // that cannot be answered afterwards because a replayed group's
            // objects are deleted once it lands.
            let state = self.wal_group_state(segid).await?;

            // A group that is not whole gets one more question asked of it
            // before it is allowed to stop anything: is there a barrier above
            // it? If there is, this group was abandoned by a session that is
            // gone and a later one sealed past it — see `wal_group_superseded`.
            //
            // Stopping at it instead is a floor that nothing lifts. The group
            // does not go away, and until something publishes,
            // `last_ondisk_cno` does not move past it — so every later open
            // walks into the same group and stops in the same place while the
            // sealed groups behind it stay unreachable. Arriving here takes
            // nothing more than a crash in mid-flush, which makes it the
            // ordinary outcome rather than a corner.
            //
            // The invariant that makes the question answerable: a session seals
            // each group before advancing past it, and never writes into a
            // group it found already there. So the only group a session can
            // leave unsealed is the last one it wrote, and an unsealed group
            // with a barrier above it cannot belong to anyone still running.
            let superseded = if state == WalGroupState::Complete {
                false
            } else {
                self.wal_group_superseded(segid, &remaining).await?
            };
            let barrier_mode = self.wal_recovery_mode() == WalRecoveryMode::Barrier;

            // Applying part of a superseded group is the one thing `Barrier`
            // must not do, so there it is skipped whole — including the records
            // before an open transaction, which a later session never saw
            // either. `Latest` keeps every acknowledged write and lands on
            // mixed states by design, so what changes for it is only that such
            // a group no longer stops the replay.
            if superseded && barrier_mode {
                warn!("do_wal_flush_recovery - checkpoint {} was abandoned and a \
                      later session sealed past it, setting it aside", segid);
                skipped.push(segid);
                continue;
            }

            // An interval the caller left open is honoured in both modes: it is
            // an explicit declaration, not an inference about where the crash
            // fell. Records before it are ordinary and still applied; from it on
            // they are half a unit, and half is what the interval exists to
            // keep out.
            let mut upto: Option<usize> = None;
            if let WalGroupState::TxnOpen { from_seq } = state {
                warn!("do_wal_flush_recovery - checkpoint {} has an transaction \
                      left open at seq {}; applying what came before it and stopping \
                      there", segid, from_seq);
                upto = Some(from_seq);
                if !superseded {
                    stopped_at = Some(segid);
                }
            }
            if upto.is_none() && self.wal_recovery_mode() == WalRecoveryMode::Barrier {
                match state {
                    WalGroupState::Complete => {},
                    WalGroupState::Unsealed => {
                        warn!("do_wal_flush_recovery - checkpoint {} was never sealed, \
                              stopping here", segid);
                        stopped_at = Some(segid);
                        break;
                    },
                    WalGroupState::Incomplete { missing } => {
                        warn!("do_wal_flush_recovery - checkpoint {} is sealed but {} \
                              of its records are missing, stopping here", segid, missing);
                        stopped_at = Some(segid);
                        break;
                    },
                    WalGroupState::TxnOpen { .. } => unreachable!("handled above"),
                }
            }
            last_applied_sealed = state == WalGroupState::Complete;
            let stop_after_this = upto.is_some() && !superseded;
            match self.wal_replay_chunks_upto(segid, upto).await {
                Ok(c) => {
                    // The checkpoint a replay lands on bears no fixed relation
                    // to the number of the group being replayed, and asserting
                    // that it did has been wrong three times. It held only
                    // while `last_seq` tracked checkpoints one for one, and
                    // three things now move one without the other: a session
                    // that declines a group moves `last_seq` past it, a
                    // deferred publish moves it without publishing, and a group
                    // set aside is never published at all. Recovery also runs
                    // before this session advances `last_seq`, so a replay can
                    // land on a number below the group it came from.
                    //
                    // What has to be true is that the floor moved forward, or
                    // the next open finds the same work waiting.
                    assert!(c > last_ondisk,
                        "replay of {} produced {}, which is not past the floor \
                         recovery started from ({})", segid, c, last_ondisk);
                    cno = c;
                },
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    // A conditional put that fails with 412 on the segment
                    // object means that checkpoint is already on storage, and
                    // segment objects are immutable and named by checkpoint —
                    // so the one already there is the one this replay would
                    // have written. Counting it as a failure would take a file
                    // read-only over data that is present and correct, which
                    // is what the common case looks like: the segment landed
                    // and only the inode did not.
                    warn!("do_wal_flush_recovery - checkpoint {} is already on \
                          storage, skipping its replay: {}", segid, e);
                    cno = segid + 1;
                    // The log entries for it are redundant now, exactly as
                    // they are after a replay that did the work. Without this
                    // every open would list them again and repeat the skip.
                    self.wal_spawn_delete_segment(segid);
                },
                Err(e) => return Err(e),
            }
            if stop_after_this {
                // Everything past this point is inside the unfinished unit.
                break;
            }
        }

        // What the caller needs in order to decide whether to repair.
        //
        // `Latest` applies whatever it finds, so the landing point is a
        // declared-consistent one only if every group it applied happened to be
        // sealed and complete. `Barrier` stops at one by construction, and what
        // it left behind is counted so the caller knows work was set aside
        // rather than lost — the records are still in the log.
        let mut dropped = 0usize;
        let stopped = match stopped_at {
            Some(segid) => vec![segid],
            None => Vec::new(),
        };
        // Groups set aside count too. They were superseded rather than lost —
        // still in the log, and still readable by a caller that goes looking —
        // but nothing here applied them, and a report that said zero would be
        // claiming otherwise.
        for id in stopped.into_iter().chain(skipped.into_iter()).chain(remaining.into_iter()) {
            dropped += self.wal_list_chunks(id).await.map(|m| m.len()).unwrap_or(0);
        }
        // Stopping happens only at a seal, so that landing point is one by
        // construction. Otherwise it depends on the last group applied.
        let all_sealed = stopped_at.is_some() || last_applied_sealed;
        self.wal_recovery_report = WalRecoveryReport {
            replayed: cno != 0,
            landed_on_barrier: all_sealed,
            records_dropped: dropped,
        };

        Ok(cno)
    }

    #[cfg(feature = "wal")]
    pub async fn wal_list_segments(&self) -> Result<Vec<SegmentId>> {
        let Some(ref wal) = self.wal else {
            return Err(Error::new(ErrorKind::Unsupported, "wal is not configured"));
        };
        wal.list_segments().await
    }

    #[cfg(feature = "wal")]
    pub async fn wal_list_chunks(&self, segid: SegmentId) -> Result<BTreeMap<usize, WalChunkDesc>> {
        let Some(ref wal) = self.wal else {
            return Err(Error::new(ErrorKind::Unsupported, "wal is not configured"));
        };
        wal.list_chunks(segid).await
    }

    #[cfg(feature = "wal")]
    pub async fn wal_replay_chunks(&mut self, segid: SegmentId) -> Result<SegmentId> {
        self.wal_replay_chunks_upto(segid, None).await
    }

    /// Replay a checkpoint's records, optionally stopping before `upto`.
    ///
    /// The bound exists for a transaction left open: the records before it
    /// are ordinary writes that must still be applied, and the ones from it on
    /// are half of a declared unit that must not be.
    #[cfg(feature = "wal")]
    pub async fn wal_replay_chunks_upto(&mut self, segid: SegmentId, upto: Option<usize>) -> Result<SegmentId> {
        debug!("wal_replay_chunks - start to process {} upto {:?}", segid, upto);
        let mut map = self.wal_list_chunks(segid).await?;
        if let Some(from_seq) = upto {
            map.retain(|seq, _| *seq < from_seq);
        }

        // take out wal to avoid write path exec into wal again
        let wal = self.wal.take();

        // retrieve all chunks and write to file
        // TODO: currently one by one, make it concurrent in future
        //
        // The `_inner` variants bypass the write-access check on
        // purpose: recovery replays writes that were already
        // acknowledged before the crash, and it runs during `open`
        // regardless of the access mode the caller asked for. A
        // read-only open of a file that crashed mid-flush still has
        // to replay the WAL to present correct contents.
        for (_, chunk) in map.iter() {
            if chunk.is_zero {
                let _ = self.write_zero_inner(chunk.offset, chunk.len).await?;
            } else {
                let data = wal.as_ref().unwrap().read(chunk.seq, chunk.segid, chunk.offset, chunk.len).await?;
                let _ = self.write_inner(chunk.offset, &data).await?;
            }
        }

        // install wal back
        self.wal = wal;

        // force flush
        let res = self.flush_process().await;

        // Fire-and-forget cleanup of the replayed WAL objects. If
        // this fails, recovery on the next open will just skip them
        // (list_segments is filtered by last_ondisk_cno), so the
        // only cost of a lost delete is a small bit of storage
        // bloat; don't block on it.
        if res.is_ok() {
            self.wal_spawn_delete_segment(segid);
        }

        res
    }

    pub async fn flush_inode(&mut self, flag: FlushInodeFlag) -> Result<()> {
        // TODO update necessary inode fields
        let mut b: BMapRawType = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        b.copy_from_slice(self.bmap.as_slice());
        let raw_inode = self.inode.to_raw(b);
        let od_state = self.staging.flush_inode(raw_inode.as_u8_slice(), self.inode.get_ondisk_state(), flag).await?;
        self.inode.clear_attr_dirty();
        self.inode.set_ondisk_state(od_state);
        self.inode.set_last_ondisk_cno(self.inode.get_last_cno());
        Ok(())
    }

    // return: if last block data changed
    async fn truncate_last_data_block(&mut self, blk_idx: &BlockIndex, offset_to_discard: usize) -> Result<bool> {
        debug!("truncate_last_data_block - block index {}, offset_to_discard {}", blk_idx, offset_to_discard);
        if self.cache.truncate_data_block(blk_idx, offset_to_discard) {
            // The block is dirty now — `truncate_data_block` zeroed its
            // tail and promoted it out of the clean tier if that is
            // where it was — so the flush will give it a new pointer,
            // and the bmap node holding that pointer has to be dirty
            // *before* the flush starts collecting dirty meta nodes.
            // Without this the zeroing is written to the new segment
            // but the map still names the old one, and a cold reader
            // sees the pre-truncate tail. Invisible while the map fits
            // in the inode's inline root, which every flush writes.
            // Same reason as the load path below, and as every write
            // path.
            let _ = self.bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await?;
            return Ok(true);
        }

        // blk index not in both dirty and cache list
        // or block ptr is zero block in bmap or not exist in bmap, no need to discard data
        let blk_ptr = match self.bmap.lookup(blk_idx).await {
            Ok(blk_ptr) => {
                if BlockPtrFormat::is_zero_block(&blk_ptr) {
                    // no need to discard data for a zero block
                    debug!("truncate_last_data_block - block ptr is zero block, nothing changed");
                    return Ok(false);
                } else if BlockPtrFormat::is_on_staging(&blk_ptr) {
                    blk_ptr
                } else {
                    panic!("invalid block ptr {} of block index {}", self.blk_ptr_decode_display(&blk_ptr), blk_idx);
                }
            },
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    return Err(e);
                }
                // no need to discard data for a non exists data block
                debug!("truncate_last_data_block - block index {} not found in bmap, nothing changed", blk_idx);
                return Ok(false);
            },
        };
        debug!("retrive block ptr {} for block index {}", self.blk_ptr_decode_display(&blk_ptr), blk_idx);
        let block = self.cache.new_block(*blk_idx);
        let buf = block.as_mut_slice();
        let _ = self.load_data_block_read_path(*blk_idx, blk_ptr, 0, buf).await?;
        // discard rest of data in the block
        let (_, to_clear) = buf.split_at_mut(offset_to_discard);
        to_clear.fill(0);
        // back to dirty list
        self.cache.insert(*blk_idx, block);
        let _ = self.bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
        Ok(true)
    }

    // truncate
    /// Blocking-acquire wrapper; see `HyperFile::try_lock`.
    pub async fn truncate(&mut self, new_size: usize) -> Result<()> {
        self.check_writable()?;
        #[cfg(feature = "wal")]
        self.check_txn_room()?;
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        self.truncate_locked(new_size, permit).await
    }

    pub async fn truncate_locked(&mut self, new_size: usize, permit: OwnedSemaphorePermit) -> Result<()> {
        // A truncate drops cached blocks above the new size, which is what
        // makes an in-flight read-ahead's copies stale. See
        // `State::mutation_gen`.
        #[cfg(feature = "reactor")]
        self.state.bump_mutation_gen();
        // POSIX ftruncate: "If fildes is not a valid file descriptor
        // open for writing, the ftruncate() function shall fail."
        // The spec allows EBADF or EINVAL here; we use EBADF to match
        // the write path.
        if !self.flags.is_writable() {
            return Err(Self::ebadf_bad_access_mode());
        }
        let size = self.inode.size();
        debug!("truncate - file size from {} to {}", size, new_size);
        if new_size == size {
            // current size same as expected size
            return Ok(());
        }

        let data_block_size = self.config.meta.data_block_size;
        let tgt_blk_idx = (new_size / data_block_size) as BlockIndex;
        let cur_blk_idx = (size / data_block_size) as BlockIndex;
        let offset_to_discard = new_size % data_block_size;

        if tgt_blk_idx == cur_blk_idx {
            let data_changed = self.truncate_last_data_block(&tgt_blk_idx, offset_to_discard).await?;
            // no need to change metadata blocks, just update the new file size
            self.inode.set_size(new_size);
            self.cache.set_size(new_size);
            self.inode.update_mtime();
            if data_changed {
                debug!("truncate - data changed, trigger flush");
            } else {
                debug!("truncate - no bmap and data changed, update file attr only");
            }
            drop(permit);
            if let Err(e) = self.flush().await {
                let _ = self.rollback_from_persisted().await;
                return Err(e);
            }
            return Ok(());
        }

        // if need to extend file length
        if tgt_blk_idx > cur_blk_idx {
            // no need to modify bmap, just update new file size
            self.inode.set_size(new_size);
            self.cache.set_size(new_size);
            self.inode.update_mtime();
            debug!("truncate - extend file size with no bmap change, update file attr only");
            drop(permit);
            if let Err(e) = self.flush().await {
                let _ = self.rollback_from_persisted().await;
                return Err(e);
            }
            return Ok(());
        }

        debug!("truncate - shrink bmap to BlockIndex {}", tgt_blk_idx);
        // re-calc tgt_blk_idx for bmap truncate
        let tgt_blk_idx = ((new_size + data_block_size - 1) / data_block_size) as BlockIndex;

        // Count entries in [tgt_blk_idx, ∞) before we drop them, so
        // we can decrement i_blocks correctly. seek_key returns the
        // smallest key >= start, NotFound when none. Walking via
        // (found+1) gives a one-pass count of exactly the entries
        // about to be discarded.
        let mut removed_blocks: usize = 0;
        {
            let mut k = tgt_blk_idx;
            loop {
                match self.bmap.seek_key(&k).await {
                    Ok(found) => {
                        removed_blocks += 1;
                        let Some(next) = found.checked_add(1) else { break; };
                        k = next;
                    }
                    Err(_) => break,
                }
            }
        }

        // Drop every cached entry whose key is about to be
        // truncated from the bmap, dirty and clean alike.
        //
        // Dirty: without this the next flush would iterate the
        // dirty list, find a block with no matching bmap entry, and
        // fail `bmap.assign(blk_idx, ptr) -> NotFound`.
        //
        // Clean: a block that was already flushed is no longer part
        // of the file. Leaving it cached lets the read path serve
        // the pre-truncate bytes if the file is later grown back
        // past the old EOF, where POSIX requires a hole (zeros).
        let _ = self.cache.truncate_blocks_above(tgt_blk_idx);

        // if need to shrink bmap
        if let Err(e) = self.bmap.truncate(&tgt_blk_idx).await {
            if e.kind() != ErrorKind::NotFound {
                // bmap.truncate may have partially mutated the in-memory tree
                // even when returning error. Roll back to persisted state.
                let _ = self.rollback_from_persisted().await;
                return Err(e);
            }
            // NotFound is fine, let's continue
        }
        if new_size > 0 && offset_to_discard != 0 {
            // We only need to zero the tail of the LAST partially-
            // retained block. When new_size lands exactly on a block
            // boundary (offset_to_discard == 0), the block at
            // tgt_blk_idx-1 is fully retained and must NOT be
            // touched: passing offset_to_discard=0 to
            // `truncate_last_data_block` would interpret it as
            // "discard from offset 0", wiping the entire block.
            let tgt_blk_idx = tgt_blk_idx - 1;
            if let Err(e) = self.truncate_last_data_block(&tgt_blk_idx, offset_to_discard).await {
                let _ = self.rollback_from_persisted().await;
                return Err(e);
            }
        }
        self.inode.set_size(new_size);
        self.cache.set_size(new_size);
        if removed_blocks > 0 {
            self.inode.update_blocks(-((removed_blocks * data_block_size) as isize));
        }
        self.inode.update_mtime();
        drop(permit);
        if let Err(e) = self.flush().await {
            let _ = self.rollback_from_persisted().await;
            return Err(e);
        }
        Ok(())
    }

    pub async fn unlink(&self) -> Result<()> {
        let _ = self.staging.unlink().await?;
        Ok(())
    }

    // return last persistent cno on disk
    #[inline]
    pub fn last_cno(&self) -> u64 {
        #[cfg(not(feature = "wal"))]
        return self.inode.get_last_ondisk_cno();
        #[cfg(feature = "wal")]
        return self.inode.get_last_cno();
    }

    pub fn staging_config(&self) -> &StagingConfig {
        &self.config.staging
    }

    pub fn staging_interceptor(&mut self, i: impl StagingIntercept<T> + 'static) {
        self.staging.interceptor(i);
    }

    /// Test-only: number of dirty data blocks in the cache.
    #[doc(hidden)]
    pub fn dirty_block_count(&self) -> usize {
        self.cache.dirty_count()
    }

    /// Test-only: whether the inode has pending attr-only changes.
    #[doc(hidden)]
    pub fn is_attr_dirty(&self) -> bool {
        self.inode.is_attr_dirty()
    }

    /// Read-side counters for this file. See [`ReadTiming`].
    ///
    /// [`ReadTiming`]: super::ReadTiming
    pub fn read_timing(&self) -> &super::ReadTiming {
        self.staging.read_timing()
    }

    /// Zero the read counters, to bracket a measurement.
    pub fn read_timing_reset(&self) {
        self.staging.read_timing().reset();
    }

    /// Test-only: whether the bmap tree has dirty meta nodes.
    #[doc(hidden)]
    pub fn is_bmap_dirty(&self) -> bool {
        self.bmap.dirty()
    }

    /// Test-only: last committed checkpoint number (in-memory value).
    #[doc(hidden)]
    pub fn in_memory_last_cno(&self) -> u64 {
        self.inode.get_last_cno()
    }

    /// Test-only: last on-disk checkpoint number (in-memory tracking).
    #[doc(hidden)]
    pub fn in_memory_last_ondisk_cno(&self) -> u64 {
        self.inode.get_last_ondisk_cno()
    }

    /// Benchmark/regression-profiling only: cumulative per-phase
    /// timing counters accumulated across all completed flushes.
    /// Unstable interface, do not rely on it from production code.
    #[doc(hidden)]
    pub fn flush_timing(&self) -> &FlushTiming {
        &self.flush_timing
    }

    /// Benchmark/regression-profiling only: reset the flush timing
    /// counters to zero.
    #[doc(hidden)]
    pub fn flush_timing_reset(&self) {
        self.flush_timing.reset();
    }
}

impl<'a: 'static, T: Staging<L> + SegmentReadWrite + Send + Clone + 'static, L: BlockLoader<BlockPtr> + Clone + 'static, C: NodeCache<BlockPtr> + Clone> HyperFile<'a, T, L, C> {
    // we only care about incomplete blocks and not in dirty list
    // return:
    //   - vec of data block ptr we need to retrieve
    pub(crate) fn write_prepare(&mut self, off: usize, len: usize) -> Vec<BlockIndex> {
        self.cache.write_prepare(off, len)
    }

    // test if block of index need to be retrieve
    #[inline]
    pub(crate) fn write_prepare_block_index(&mut self, blk_idx: &BlockIndex) -> bool {
        !self.cache.contains(blk_idx)
    }

    async fn write_retrieve(&mut self, list: Vec<BlockIndex>) -> Result<Vec<DataBlock>> {
        let mut output = Vec::new();
        for blk_idx in list {
            match self.bmap.lookup(&blk_idx).await {
                Ok(blk_ptr) => {
                    debug!("retrive block ptr {} for block index {}", self.blk_ptr_decode_display(&blk_ptr), blk_idx);
                    let block = self.cache.new_block(blk_idx);
                    block.set_should_cache();
                    let buf = block.as_mut_slice();
                    if !BlockPtrFormat::is_zero_block(&blk_ptr) {
                        let _ = self.load_data_block_write_path(blk_idx, blk_ptr, 0, buf).await?;
                    }
                    output.push(block);
                },
                Err(e) => {
                    if e.kind() != ErrorKind::NotFound {
                        return Err(e);
                    }
                    debug!("block index {} not found in bmap, prepare a new block", blk_idx);
                    let block = self.cache.new_block(blk_idx);
                    block.set_should_cache();
                    output.push(block);
                },
            }
        }
        Ok(output)
    }

    pub(crate) fn update_cache(&mut self, blk_idx: BlockIndex, off: usize, buf: &[u8]) {
        self.cache.update_cache(&blk_idx, off, buf)
    }

    async fn load_data_block_read_path(&mut self, blk_idx: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8]) -> Result<()> {
        debug!("load_data_block_read_path - block index: {}, offset: {}, bytes: {}, block ptr: {}",
            blk_idx, offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        // in read path we would check both data and dirty cache before do real data load
        if let Some(block) = self.cache.get(&blk_idx) {
            let slice = block.as_slice();
            buf.copy_from_slice(&slice[offset..offset + buf.len()]);
            block.unlock();
            return Ok(());
        }
        #[cfg(feature = "wal")]
        if self.wal.is_some() && BlockPtrFormat::is_on_staging(&blk_ptr) && (self.inode().get_last_cno() > self.inode().get_last_ondisk_cno()) {
            let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
            if segid > self.inode().get_last_ondisk_cno() {
                let data_buf = unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
                };
                let copied = {
                    let lock = self.flushing_segments.read().await;
                    match lock.get(&segid).and_then(|weak_data| weak_data.upgrade()) {
                        Some(data) => {
                            let start_off = staging_off + offset;
                            let end = start_off + data_buf.len();
                            data_buf.copy_from_slice(&data[start_off..end]);
                            true
                        },
                        None => false,
                    }
                };
                if copied {
                    return Ok(());
                }
                // The flush landed on the way here, so the pinned buffer is
                // gone and the segment is on staging. Fall through and read
                // it from there: a flush completing is not a failure.
                debug!("segid {} no longer held in memory, reading it from staging", segid);
            }
        }
        if BlockPtrFormat::is_on_staging(&blk_ptr) {
            let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
            let _ = self.staging.load_data_block(segid, staging_off, offset, self.config.meta.data_block_size, buf).await?;
            return Ok(());
        } else if BlockPtrFormat::is_dummy_value(&blk_ptr) {
            panic!("failed to get block index: {} from data blocks dirty cache for dummy block ptr", blk_idx);
        } else if BlockPtrFormat::is_zero_block(&blk_ptr) {
            buf.fill(0);
            return Ok(());
        } else {
            panic!("load_data_block_read_path - block index: {}, offset: {}, bytes: {}, incorrect block ptr {} to load",
                blk_idx, offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        }
    }

    async fn load_data_block_write_path(&mut self, blk_idx: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8]) -> Result<()> {
        debug!("load_data_block_write_path - block index: {}, offset: {}, bytes: {}, block ptr: {}",
            blk_idx, offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        #[cfg(feature = "wal")]
        if self.wal.is_some() && BlockPtrFormat::is_on_staging(&blk_ptr) && (self.inode().get_last_cno() > self.inode().get_last_ondisk_cno()) {
            let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
            if segid > self.inode().get_last_ondisk_cno() {
                let data_buf = unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
                };
                let copied = {
                    let lock = self.flushing_segments.read().await;
                    match lock.get(&segid).and_then(|weak_data| weak_data.upgrade()) {
                        Some(data) => {
                            let start_off = staging_off + offset;
                            let end = start_off + data_buf.len();
                            data_buf.copy_from_slice(&data[start_off..end]);
                            true
                        },
                        None => false,
                    }
                };
                if copied {
                    return Ok(());
                }
                // The flush landed on the way here, so the pinned buffer is
                // gone and the segment is on staging. Fall through and read
                // it from there: a flush completing is not a failure.
                debug!("segid {} no longer held in memory, reading it from staging", segid);
            }
        }
        if BlockPtrFormat::is_on_staging(&blk_ptr) {
            let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
            let _ = self.staging.load_data_block(segid, staging_off, offset, self.config.meta.data_block_size, buf).await?;
            return Ok(());
        } else if BlockPtrFormat::is_dummy_value(&blk_ptr) {
            if let Some(block) = self.cache.get(&blk_idx) {
                let slice = block.as_slice();
                buf.copy_from_slice(&slice[offset..offset + buf.len()]);
                block.unlock();
                return Ok(());
            }
            panic!("failed to get block index: {} from data blocks dirty cache for dummy block ptr", blk_idx);
        } else if BlockPtrFormat::is_zero_block(&blk_ptr) {
            buf.fill(0);
            return Ok(());
        } else {
            panic!("load_data_block_write_path - block index: {}, offset: {}, bytes: {}, incorrect block ptr {} to load",
                blk_idx, offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        }
    }

    // return max dirty data blocks can hold
    fn calc_max_dirty_blocks(data_block_size: usize, max_dirty_bytes_threshold: usize, max_dirty_blocks_threshold: usize) -> usize {
        let max_blocks = max_dirty_bytes_threshold / data_block_size;
        std::cmp::max(max_blocks, max_dirty_blocks_threshold)
    }
}

impl<'a: 'static, T: Staging<L> + SegmentReadWrite + Send + Clone + 'static, L: BlockLoader<BlockPtr> + Clone + 'static, C: NodeCache<BlockPtr> + Clone> HyperFile<'a, T, L, C> {
    // write in batch style, input blocks could be incomplete
    /// Blocking-acquire wrapper; see `HyperFile::try_lock`.
    pub async fn write_batch(&mut self, blocks: Vec<BatchDataBlockWrapper>) -> Result<usize> {
        self.check_writable()?;
        #[cfg(feature = "wal")]
        self.check_txn_room()?;
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        self.write_batch_locked(blocks, permit).await
    }

    pub async fn write_batch_locked(&mut self, blocks: Vec<BatchDataBlockWrapper>, permit: OwnedSemaphorePermit) -> Result<usize> {
        // Checked here rather than only in the outer wrapper: the reactor's
        // batch arms call this directly, so a guard above it covers one surface
        // and not the other.
        #[cfg(feature = "wal")]
        self.check_txn_room()?;
        if !self.flags.is_writable() {
            return Err(Self::ebadf_bad_access_mode());
        }
        if blocks.len() == 0 {
            return Ok(0);
        }

        let data_block_size = self.config.meta.data_block_size;
        let mut bytes_write = 0;

        // group by block index
        let mut map: HashMap<BlockIndex, Vec<BatchDataBlockWrapper>> = HashMap::new();
        for block in blocks.into_iter() {
            let blk_idx = block.index() as BlockIndex;
            if let Some(v) = map.get_mut(&blk_idx) {
                v.push(block);
            } else {
                map.insert(blk_idx, vec![block]);
            }
        }

        // merge each block index group
        // (is one full block, vec of partial blocks or vec of one full block)
        let mut merged: BTreeMap<BlockIndex, (bool, Vec<BatchDataBlockWrapper>)> = BTreeMap::new();
        for (blk_idx, mut v) in map.into_iter() {
            // try find a full block from back to head
            let res = v.iter().rposition(|b| b.is_full_block());
            let m = if let Some(idx) = res {
                // if we found a full block
                let mut rest = v.split_off(idx);
                rest.reverse();
                let mut full_block = rest.pop().expect("invalid rest vec by split_off");
                while let Some(next_block) = rest.pop() {
                    full_block.merge_partial(&next_block);
                }
                (true, vec![full_block])
            } else {
                // if all partial blocks, we can't merge
                (false, v)
            };
            merged.insert(blk_idx, m);
        }

        // Logged before any state changes, for the reason given on
        // `wal_log_aligned_batch`: a write path the log never saw is
        // acknowledged while living only in memory, and the flush path is
        // allowed to answer early on the strength of the log holding it.
        #[cfg(feature = "wal")]
        if self.wal.is_some() {
            self.wal_log_batch(&merged, data_block_size).await?;
        }

        // write prepare
        let mut v_need_retrieve = Vec::new();
        for blk_idx in merged.keys().into_iter() {
            if self.write_prepare_block_index(blk_idx) {
                v_need_retrieve.push(*blk_idx);
            }
        }

        #[cfg(not(feature = "reactor"))]
        let fetched = self.write_retrieve(v_need_retrieve).await?;

        #[cfg(feature = "reactor")]
        let mut fetched = Vec::new();
        #[cfg(feature = "reactor")]
        let mut joins = Vec::new();
        #[cfg(feature = "reactor")]
        for blk_idx in v_need_retrieve {
            match self.bmap.lookup(&blk_idx).await {
                Ok(blk_ptr) => {
                    let block = self.cache.new_block(blk_idx);
                    block.set_should_cache();
                    let buf = block.as_mut_slice();
                    let join = self.spawn_load_data_block_write_path(blk_idx, blk_ptr, 0, buf)?;
                    joins.push(join);
                    fetched.push(block);
                },
                Err(e) => {
                    if e.kind() != ErrorKind::NotFound {
                        return Err(e);
                    }
                    let block = self.cache.new_block(blk_idx);
                    block.set_should_cache();
                    fetched.push(block);
                },
            }
        }
        #[cfg(feature = "reactor")]
        while let Some(o) = joins.pop() {
            match o {
                super::reactor::ImmOrJoinSize::ImmSize(_) => {},
                super::reactor::ImmOrJoinSize::JoinSize(j) => { let _ = j.await; },
            }
        }

        // insert fetched data blocks into dirty list
        for block in fetched.into_iter() {
            let blk_idx = block.index();
            let None = self.cache.insert(blk_idx, block) else {
                panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
            };
        }

        let mut new_blocks: usize = 0;
        for (blk_idx, (is_full_block, v_blocks)) in merged.iter() {
            if !is_full_block {
                // if not a full block, playback all partial data blocks
                let block = self.cache.get_mut(&blk_idx).expect("failed to get back data block from dirty list");
                for part in v_blocks.iter() {
                    if part.is_zero() {
                        let mut zero = Vec::new();
                        zero.resize(part.len(), 0);
                        block.copy(part.offset(), &zero);
                    } else {
                        block.copy(part.offset(), part.as_slice());
                    }
                    bytes_write += part.len();
                }
                block.unlock();
                let prev = self.bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
                if prev.is_none() {
                    new_blocks += 1;
                }
                continue;
            }
            // is full block
            assert!(v_blocks.len() == 1);
            let block_wrapper = &v_blocks[0];
            let blk_sz = block_wrapper.size();
            if block_wrapper.is_zero() {
                let _ = self.cache.remove(&blk_idx);
                bytes_write += blk_sz;
                let prev = self.bmap.insert(*blk_idx, BlockPtrFormat::new_zero_block()).await.expect("failed to insert new zero to bmap");
                if prev.is_none() {
                    new_blocks += 1;
                }
                continue;
            }
            self.update_cache(*blk_idx, 0, block_wrapper.as_slice());
            bytes_write += blk_sz;
            // force bmap update for dirty blocks
            let prev = self.bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
            if prev.is_none() {
                new_blocks += 1;
            }
        }
        // try update file size by offset and len from last block
        let (blk_idx, (is_full_block, v_blocks)) = merged.pop_last().expect("unable to get last block, input blocks is empty");
        let oldsize = self.inode.size();
        let off = (blk_idx as usize) * data_block_size;
        let len = if is_full_block {
            data_block_size
        } else {
            v_blocks.iter().max_by_key(|b| b.len()).expect("invalid vec of partial data block").len()
        };
        if off + len > oldsize {
            self.inode.set_size(off + len);
            self.cache.set_size(off + len);
        }
        if new_blocks > 0 {
            self.inode.update_blocks((new_blocks * data_block_size) as isize);
        }
        self.inode.update_mtime();
        drop(permit);
        let _flushed = self.try_flush().await?;
        Ok(bytes_write)
    }
}

impl<T, L, C> HyperTrait<T, L, C, BlockPtr> for HyperFile<'_, T, L, C>
    where
        T: Staging<L> + SegmentReadWrite + Send + Clone + 'static,
        L: BlockLoader<BlockPtr> + Clone,
        C: NodeCache<BlockPtr> + Clone,
{
    fn blk_ptr_encode(&self, segid: SegmentId, offset: SegmentOffset, seq: usize) -> BlockPtr {
        BlockPtrFormat::encode(segid, offset, seq, &self.bmap_ud.blk_ptr_format)
    }

    fn blk_ptr_decode(&self, blk_ptr: &BlockPtr) -> (SegmentId, SegmentOffset) {
        BlockPtrFormat::decode(blk_ptr, &self.bmap_ud.blk_ptr_format)
    }

    fn blk_ptr_decode_display(&self, blk_ptr: &BlockPtr) -> String {
        if BlockPtrFormat::is_dummy_value(blk_ptr) {
            return format!("[Dummy]");
        } else if BlockPtrFormat::is_invalid_value(blk_ptr) {
            return format!("[Invalid]");
        } else if BlockPtrFormat::is_zero_block(blk_ptr) {
            return format!("[Zero Block]");
        } else if BlockPtrFormat::is_on_staging(blk_ptr) {
            let (id, off) = self.blk_ptr_decode(blk_ptr);
            let group_id = BlockPtrFormat::decode_micro_group_id(blk_ptr);
            return format!("[Staging: id {} - offset {} - group {}]", id, off, group_id);
        } else {
            return format!("[Unkown: 0x{:x}]", blk_ptr);
        }
    }

    fn clear_data_blocks_cache(&mut self) {
        self.cache.clear_data_blocks_cache()
    }

    fn set_data_blocks_cache_unlimited(&mut self) {
        self.cache.set_unlimited()
    }

    fn restore_data_blocks_cache_limit(&mut self) {
        self.cache.restore_limit()
    }

    fn get_data_blocks_dirty(&self) -> DirtyDataBlocks<'_> {
        self.cache.get_dirty()
    }

    fn clear_data_blocks_dirty(&mut self) {
        self.cache.clear_dirty()
    }

    async fn lock(&self) -> OwnedSemaphorePermit {
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        permit
    }

    fn unlock(&self, permit: OwnedSemaphorePermit) {
        drop(permit);
    }

    async fn flush_lock(&self) -> OwnedMutexGuard<()> {
        let lock = self.flush_lock.clone().lock_owned().await;
        self.state.set_flushing();
        lock
    }

    fn flush_unlock(&self, lock: OwnedMutexGuard<()>) {
        drop(lock);
        self.state.clear_flushing();
    }

    fn bmap_as_slice(&self) -> &[u8] {
        self.bmap.as_slice()
    }

    fn bmap_get_block_loader(&self) -> L {
        self.bmap.get_block_loader()
    }

    fn bmap_get_node_cache(&self) -> C {
        self.bmap.get_node_cache()
    }

    fn bmap_dirty(&self) -> bool {
        self.bmap.dirty()
    }

    fn bmap_lookup_dirty(&self) -> Vec<BtreeNodeDirty<'_, BlockIndex, BlockPtr, BlockPtr>> {
        self.bmap.lookup_dirty()
    }

    async fn bmap_assign_meta_node(&self, blk_ptr: BlockPtr, node: BtreeNodeDirty<'_, BlockIndex, BlockPtr, BlockPtr>) -> Result<()> {
        self.bmap.assign_meta_node(blk_ptr, node).await
    }

    async fn bmap_assign_data_node(&self, blk_idx: &BlockIndex, blk_ptr: BlockPtr) -> Result<()> {
        self.bmap.assign_data_node(blk_idx, blk_ptr).await
    }

    fn bmap_clear_dirty(&mut self) {
        self.bmap.clear_dirty()
    }

    fn bmap_update(&mut self, bmap: BMap<'_, BlockIndex, BlockPtr, BlockPtr, L, C>) {
        *&mut self.bmap = unsafe {
            std::mem::transmute::<BMap<'_, BlockIndex, BlockPtr, BlockPtr, L, C>, BMap<'_, BlockIndex, BlockPtr, BlockPtr, L, C>>(bmap)
        };
    }

    async fn bmap_insert_dummy_value(bmap: &mut BMap<'_, BlockIndex, BlockPtr, BlockPtr, L, C>, blk_idx: &BlockIndex) -> Result<Option<BlockPtr>> {
        bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await
    }

    fn bmap_set_cache_unlimited(&self) -> usize {
        let limit = self.bmap.get_cache_limit();
        self.bmap.set_cache_limit(DEFAULT_CACHE_UNLIMITED);
        limit
    }

    fn bmap_set_cache_limit(&self, limit: usize) {
        self.bmap.set_cache_limit(limit);
    }

    fn staging(&self) -> &T {
        &self.staging
    }

    fn config(&self) -> &HyperFileConfig {
        &self.config
    }

    fn set_last_flush(&mut self) {
        self.state.set_last_flush();
    }

    /// Refuse a modification when publishing has failed unrecoverably.
    ///
    /// Only the WAL path can reach that state; without `wal` this is always
    /// `Ok`. See `State::publish_failed` for why the file stops accepting
    /// writes rather than continuing or panicking.
    #[inline]
    #[cfg(feature = "wal")]
    fn wal_mut(&mut self) -> Option<&mut Box<dyn crate::wal::WalReadWrite + Send>> {
        self.wal.as_mut()
    }

    #[cfg(feature = "wal")]
    fn in_txn_trait(&self) -> bool {
        self.txn_open
    }

    #[cfg(feature = "wal")]
    fn wal_may_defer_publish(&self) -> bool {
        // Never inside a transaction: its commit is a publish by definition,
        // and deferring it would leave the unit unsealed with the caller told
        // otherwise.
        if self.txn_open || self.wal.is_none() || self.wal_force_publish {
            return false;
        }
        let every = self.config.wal.publish_every.max(1);
        self.wal_deferred_barriers + 1 < every
    }

    #[cfg(feature = "wal")]
    fn wal_count_deferred_barrier(&mut self) {
        self.wal_deferred_barriers += 1;
    }

    #[cfg(feature = "wal")]
    fn wal_deferred_count(&self) -> usize {
        self.wal_deferred_barriers
    }

    #[cfg(feature = "wal")]
    fn wal_reset_deferred_count(&mut self) {
        self.wal_deferred_barriers = 0;
        self.wal_force_publish = false;
    }

    fn check_writable(&self) -> Result<()> {
        #[cfg(feature = "wal")]
        if self.state.is_publish_failed() {
            return Err(Error::new(ErrorKind::ReadOnlyFilesystem,
                "publishing failed unrecoverably; this file is read-only until \
                 the wal is recovered with offline tools"));
        }
        Ok(())
    }

    fn flush_timing(&self) -> &FlushTiming {
        &self.flush_timing
    }

    fn inode(&self) -> &Inode {
        &self.inode
    }

    #[allow(mutable_transmutes)]
    fn inode_mut(&self) -> &mut Inode {
        unsafe {
            std::mem::transmute::<&Inode, &mut Inode>(&self.inode)
        }
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }

    // wal
    #[cfg(feature = "wal")]
    async fn wal_set_mem_segment(&self, mem_segid: SegmentId, mem_segdata: Weak<Pin<Box<Vec<u8>>>>) {
        let mut lock = self.flushing_segments.write().await;
        if let Some(_) = lock.insert(mem_segid, mem_segdata) {
            panic!("wal set mem segment - segid {mem_segid} already exists in memory flushing segments");
        }
    }

    #[cfg(feature = "wal")]
    async fn wal_clear_mem_segment(&self, mem_segid: SegmentId) {
        let mut lock = self.flushing_segments.write().await;
        let Some(weak_mem_seg) = lock.remove(&mem_segid) else {
            panic!("wal clear mem segment - segid {mem_segid} did not exists in memory flushing segments");
        };
        let Some(mem_seg) = weak_mem_seg.upgrade() else {
            panic!("wal clear mem segment - segid {mem_segid} not be able to upgrade");
        };
        unsafe { Arc::decrement_strong_count(Arc::as_ptr(&mem_seg)) };
        // end of mem segment life
        drop(mem_seg);
    }

    #[cfg(feature = "wal")]
    fn wal_spawn_delete_segment(&self, segid: SegmentId) {
        if let Some(ref wal) = self.wal {
            let fut = wal.delete_segment(segid);
            tokio::task::spawn(async move {
                if let Err(e) = fut.await {
                    warn!("wal delete_segment {} failed: {:?}", segid, e);
                }
            });
        }
    }
}
