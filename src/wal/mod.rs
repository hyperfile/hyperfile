use std::pin::Pin;
use std::io::Result;
use std::collections::BTreeMap;
use crate::SegmentId;

pub mod config;
pub(crate) mod s3;

// ondisk chunk desc
#[derive(Debug)]
pub struct WalChunkDesc {
    pub seq: usize,
    pub segid: SegmentId,
    pub key: String,
    pub offset: usize,
    pub len: usize,
    pub is_zero: bool,
}

/// What a barrier says a checkpoint's record set contains.
///
/// A barrier exists to answer one question the record objects cannot: is this
/// group complete? A crash mid-flush leaves a partial set that looks exactly
/// like a whole one at the byte level, and applying it gives the filesystem
/// above half a transaction.
///
/// It carries the list rather than a count so that recovery can check it
/// against what is actually stored without relying on listing being complete
/// or on the order the records were written in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalBarrier {
    /// `(seq, offset, len)` of every record this barrier covers, in the order
    /// they were written.
    pub entries: Vec<(usize, usize, usize)>,
}

/// Magic and version live in the barrier body because it is a new on-disk
/// format with no existing objects to be compatible with — the version is
/// worth having from the start rather than retrofitted.
const WAL_BARRIER_MAGIC: u32 = 0x5741_4C42; // "WALB"
const WAL_BARRIER_VERSION: u32 = 1;

impl WalBarrier {
    pub fn new(entries: Vec<(usize, usize, usize)>) -> Self {
        Self { entries }
    }

    /// Fixed-width header then fixed-width entries, native endian, matching
    /// how the rest of this crate writes its own structures.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16 + self.entries.len() * 24);
        buf.extend_from_slice(&WAL_BARRIER_MAGIC.to_ne_bytes());
        buf.extend_from_slice(&WAL_BARRIER_VERSION.to_ne_bytes());
        buf.extend_from_slice(&(self.entries.len() as u64).to_ne_bytes());
        for (seq, off, len) in self.entries.iter() {
            buf.extend_from_slice(&(*seq as u64).to_ne_bytes());
            buf.extend_from_slice(&(*off as u64).to_ne_bytes());
            buf.extend_from_slice(&(*len as u64).to_ne_bytes());
        }
        buf
    }

    /// `None` for anything this build cannot make sense of. A barrier that
    /// cannot be read is treated as absent, which stops recovery rather than
    /// letting it apply a group it cannot vouch for.
    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < 16 {
            return None;
        }
        let magic = u32::from_ne_bytes(buf[0..4].try_into().ok()?);
        let version = u32::from_ne_bytes(buf[4..8].try_into().ok()?);
        if magic != WAL_BARRIER_MAGIC || version != WAL_BARRIER_VERSION {
            return None;
        }
        let count = u64::from_ne_bytes(buf[8..16].try_into().ok()?) as usize;
        if buf.len() != 16 + count * 24 {
            return None;
        }
        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            let at = 16 + i * 24;
            let seq = u64::from_ne_bytes(buf[at..at + 8].try_into().ok()?) as usize;
            let off = u64::from_ne_bytes(buf[at + 8..at + 16].try_into().ok()?) as usize;
            let len = u64::from_ne_bytes(buf[at + 16..at + 24].try_into().ok()?) as usize;
            entries.push((seq, off, len));
        }
        Some(Self { entries })
    }
}

/// What recovery did when the container was opened.
///
/// A caller that can repair its own contents needs this to decide whether to:
/// repairing is proportional to the whole container, so being able to skip it
/// when the landing point is already a declared-consistent one is the point of
/// reporting at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalRecoveryReport {
    /// Whether any log group was applied.
    ///
    /// `false` means the log had nothing this container had not already
    /// published — the ordinary case after a clean stop.
    pub replayed: bool,
    /// Whether the state the container came up in is one that was declared
    /// consistent by whoever wrote it.
    ///
    /// True when every group applied was sealed and complete, and nothing
    /// half-written was applied. That covers three cases: nothing needed
    /// replaying, so the container is at a published checkpoint and a publish
    /// only happens at a seal; `Barrier` recovery, which stops at a seal by
    /// construction; and `Latest` recovery that happened to find every group
    /// sealed.
    ///
    /// False means the last thing applied was an unsealed group — the writer
    /// was interrupted partway through whatever it considered one unit of work,
    /// and the contents are that midpoint.
    pub landed_on_barrier: bool,
    /// Records that were present but not applied.
    ///
    /// Non-zero only under `Barrier` recovery, which stops rather than
    /// applying past a group it cannot vouch for. These records are still in
    /// the log; nothing has been deleted.
    pub records_dropped: usize,
}

impl Default for WalRecoveryReport {
    /// "Recovery did not run", which is not all-false: a container that needed
    /// no replay is sitting at its last published checkpoint, and a publish only
    /// happens at a seal — so the landing point is a declared-consistent one.
    ///
    /// Worth being explicit about, because the ordinary case never reaches the
    /// recovery path at all and so never sets this. Deriving `Default` here
    /// would report every clean open as needing repair.
    fn default() -> Self {
        Self {
            replayed: false,
            landed_on_barrier: true,
            records_dropped: 0,
        }
    }
}

pub trait WalReadWrite {
    // write
    fn write(&mut self, segid: SegmentId, offset: usize, buf: &[u8]) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
    fn write_zero(&mut self, segid: SegmentId, offset: usize, len: usize) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
    // read
    fn read(&self, seq: usize, segid: SegmentId, offset: usize, len: usize) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + '_>>;
    /// Seal `segid`: record what its record set contains, so recovery can tell
    /// a complete group from a crash-truncated one.
    ///
    /// Must be called only after every record it covers is confirmed written,
    /// and before the segment is published — those two together are what make
    /// "a barrier exists" mean "this group is whole and was never published".
    ///
    /// Clears the accumulated list, so the next records belong to the next
    /// group.
    fn write_barrier(&mut self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

    /// Forget the records written since the last barrier, without sealing them.
    ///
    /// For a caller abandoning its own work: the records are being deleted, so a
    /// later barrier must not list them. Sealing instead would vouch for a group
    /// whose objects are on their way out, and recovery would then read it as a
    /// complete group with records missing.
    fn discard_pending(&mut self);

    /// The barrier for `segid`, or `None` if there is none — which recovery
    /// must treat as "this group was never sealed" rather than as an error.
    fn read_barrier(&self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<Option<WalBarrier>>> + Send + '_>>;

    /// The seq the next record written under `segid` would get.
    ///
    /// Needed so a transaction marker can name where it begins: records before
    /// that seq are ordinary and must still be applied, records from it on
    /// belong to the unit. Accounts for the counter resetting when the
    /// checkpoint changes, so asking before the first record of a new
    /// checkpoint gives the right answer rather than the previous one's.
    fn next_seq_peek(&self, segid: SegmentId) -> usize;

    /// Mark that a transaction is open under `segid`, starting at
    /// `from_seq`.
    ///
    /// Records from that seq on belong to work the caller has declared to be
    /// one unit. If the marker is still there at recovery time the unit did not
    /// finish, and those records must not be applied — applying half of it is
    /// what the transaction exists to prevent. Records *before* `from_seq` are
    /// ordinary and unaffected.
    fn write_txn_marker(&mut self, segid: SegmentId, from_seq: usize) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

    /// The open transaction's starting seq under `segid`, if one is open.
    fn read_txn_marker(&self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<Option<usize>>> + Send + '_>>;

    /// Remove the marker. Cleanup rather than a correctness step: a barrier for
    /// the same checkpoint already means the unit completed, and takes
    /// precedence.
    fn delete_txn_marker(&mut self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

    // list
    fn list_segments(&self) -> Pin<Box<dyn Future<Output = Result<Vec<SegmentId>>> + Send + '_>>;
    fn list_chunks(&self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<BTreeMap<usize, WalChunkDesc>>> + Send + '_>>;
    /// Delete every WAL object for the given segid from backing
    /// storage.
    ///
    /// Intended to be called after a successful flush has made the
    /// WAL chunks for `segid` redundant. Callers typically run this
    /// via `tokio::spawn` as a low-priority fire-and-forget cleanup:
    /// the WAL is safe to keep around (recovery filters by
    /// `last_ondisk_cno`), so a lost delete leaves storage slightly
    /// bloated but does not affect correctness. The returned future
    /// is `'static + Send` so callers can detach it freely.
    fn delete_segment(&self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn barrier_round_trips() {
        for entries in [
            vec![],
            vec![(0usize, 0usize, 4096usize)],
            vec![(0, 0, 4096), (1, 4096, 8192), (2, 65536, 200)],
        ] {
            let b = WalBarrier::new(entries.clone());
            let decoded = WalBarrier::decode(&b.encode()).expect("must round trip");
            assert_eq!(decoded.entries, entries);
        }
    }

    /// A body this build cannot vouch for reads as absent, so recovery stops
    /// instead of applying a group it cannot check.
    #[test]
    fn barrier_rejects_what_it_cannot_vouch_for() {
        assert!(WalBarrier::decode(&[]).is_none(), "empty");
        assert!(WalBarrier::decode(&[0u8; 8]).is_none(), "shorter than the header");

        let good = WalBarrier::new(vec![(1, 4096, 4096)]).encode();
        assert!(WalBarrier::decode(&good).is_some());

        let mut wrong_magic = good.clone();
        wrong_magic[0] ^= 0xFF;
        assert!(WalBarrier::decode(&wrong_magic).is_none(), "magic");

        let mut wrong_version = good.clone();
        wrong_version[4] = 0xFE;
        assert!(WalBarrier::decode(&wrong_version).is_none(), "version");

        // Count says one entry, body carries none: the length check catches it,
        // which is the case a crash mid-PUT would produce.
        let mut truncated = good.clone();
        truncated.truncate(16);
        assert!(WalBarrier::decode(&truncated).is_none(), "truncated body");

        let mut trailing = good.clone();
        trailing.push(0);
        assert!(WalBarrier::decode(&trailing).is_none(), "trailing bytes");
    }
}
