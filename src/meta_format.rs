use serde::{Deserialize, Serialize};
use crate::{BlockPtr, SegmentId, SegmentOffset};
use std::io::{Error, ErrorKind, Result};

const BLOCK_PTR_DUMMY: u64 = 0x3FFF_FFFF_FFFF_FFFF;
// a block ptr to a zero block
const BLOCK_PTR_ZERO_BLOCK: u64 = 0x3FFF_FFFF_0000_0000;

// mask
const BLOCK_PTR_LOCATION_MASK: u64 = 0xC000_0000_0000_0000;
const BLOCK_PTR_SEGMENT_ID_MASK: u64 = 0x3FFF_FFFF_0000_0000;
const BLOCK_PTR_SEGMENT_FLAT_OFFSET_MASK: u64 = 0x0000_0000_FFFF_FFFF;
const BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_MASK: u64 = 0x0000_0000_FFFF_C000; // in 4KiB
const BLOCK_PTR_SEGMENT_MG_GROUP_INDEX_MASK: u64 = 0x0000_0000_0000_3FFF;
// `PartedSegment` reuses the micro-group field as the part index. Same bits,
// different meaning, which is safe only because the format byte tells the two
// apart: a container is one or the other for its whole life.
const BLOCK_PTR_PARTED_PART_MASK: u64 = 0x0000_0000_0000_3FFF;

const BLOCK_PTR_SEGMENT_ID_BIT_SHIFT: u32 = 32;
const BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_SHIFT: u32 = 14;
const BLOCK_PTR_SEGMENT_MG_GROUP_INDEX_SHIFT: u32 = 4; // 16 entry per group
const BLOCK_PTR_SEGMENT_MG_OFFSET_ID_SHIFT: u32 = 12; // 4KiB

pub const BLOCK_PTR_SEGMENT_MG_GROUP_INDEX_BITS: u32 = BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_SHIFT;
pub const BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_BITS: u32 = 32 - BLOCK_PTR_SEGMENT_MG_GROUP_INDEX_BITS;

/// How many parts one checkpoint may be streamed as, under
/// [`BlockPtrFormat::PartedSegment`].
pub const BLOCK_PTR_PARTED_MAX_PARTS: usize = 1 << BLOCK_PTR_SEGMENT_MG_GROUP_INDEX_BITS;

/// The largest one part may be. The in-part block index is the same 18 bits the
/// unparted format uses for the whole segment, so a part is bounded by what a
/// whole segment used to be — and a checkpoint is bounded by that times the
/// number of parts.
pub const BLOCK_PTR_PARTED_MAX_PART_BYTES: usize =
    (1 << BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_BITS) << BLOCK_PTR_SEGMENT_MG_OFFSET_ID_SHIFT;

// data on staging
const BLOCK_PTR_STAGING: u64 = 0x4000_0000_0000_0000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[repr(u8)]
pub enum BlockPtrFormat {
    Nop = 0,
    Flat = 1,
    MicroGroup = 2,
    /// Like [`Self::MicroGroup`], but a checkpoint may be written as several
    /// objects — `<segid>.<part>` — instead of one.
    ///
    /// The part index goes where the micro-group index used to, which costs
    /// nothing because that field never took part in addressing: `decode` ignored
    /// it, and its only reader was a debug string. What a container gains is the
    /// ability to stream a checkpoint out in pieces and still name every block
    /// from the pointer alone.
    ///
    /// Part 0 is the summary, the metadata blocks and the inode, and it is
    /// written last — nothing else in the checkpoint can be described until the
    /// rest of it exists. Data blocks are in parts 1 and up. So part 0 being
    /// present is what says the stream finished, and a reader that finds parts
    /// without it is looking at a checkpoint that was abandoned.
    PartedSegment = 3,
}

impl BlockPtrFormat {
    /// Decode a format byte, or report one this build does not know.
    ///
    /// Separate from [`Self::from_u8`] because this byte can arrive from a
    /// persisted container written by another version, and a library has no
    /// business panicking over what it read from storage.
    #[inline]
    pub fn try_from_u8(data: u8) -> Result<Self> {
        match data {
            0 => Ok(Self::Nop),
            1 => Ok(Self::Flat),
            2 => Ok(Self::MicroGroup),
            3 => Ok(Self::PartedSegment),
            n => Err(Error::new(ErrorKind::InvalidData,
                format!("unknown block ptr format {}", n))),
        }
    }

    /// Decode a format byte, panicking on one this build does not know.
    ///
    /// For a byte this crate produced itself. Anything decoded from a
    /// container should use [`Self::try_from_u8`].
    #[inline]
    pub fn from_u8(data: u8) -> Self {
        Self::try_from_u8(data).expect("block ptr format")
    }

    #[inline]
    pub fn new_zero_block() -> BlockPtr {
        BLOCK_PTR_ZERO_BLOCK
    }

    #[inline]
    pub fn is_zero_block(blk_ptr: &BlockPtr) -> bool {
        *blk_ptr == BLOCK_PTR_ZERO_BLOCK
    }

    #[inline]
    pub fn dummy_value() -> BlockPtr {
        BLOCK_PTR_DUMMY
    }

    #[inline]
    pub fn is_dummy_value(blk_ptr: &BlockPtr) -> bool {
        *blk_ptr == BLOCK_PTR_DUMMY
    }

    #[inline]
    pub fn invalid_value() -> BlockPtr {
        BlockPtr::MIN
    }

    #[inline]
    pub fn is_invalid_value(blk_ptr: &BlockPtr) -> bool {
        *blk_ptr == BlockPtr::MIN
    }

    #[inline]
    pub fn is_on_staging(blk_ptr: &BlockPtr) -> bool {
        *blk_ptr & BLOCK_PTR_LOCATION_MASK == BLOCK_PTR_STAGING
    }

    /// Encode a location.
    ///
    /// `part` is which object of a streamed checkpoint the block landed in, and
    /// `offset` is its offset **within that object**. Every format but
    /// [`Self::PartedSegment`] writes one object per checkpoint and ignores
    /// `part`; for those, `offset` is the offset within the whole segment, which
    /// is the same thing.
    pub fn encode(segid: SegmentId, offset: SegmentOffset, seq: usize, fmt: &BlockPtrFormat) -> BlockPtr {
        let id = segid.seq_id() as u64;
        match fmt {
            Self::Nop => 0,
            Self::Flat => Self::encode_flat(id, offset, seq),
            Self::MicroGroup => Self::encode_micro_group(id, offset, seq),
            Self::PartedSegment => Self::encode_parted_segment(
                id, segid.part_id().unwrap_or(0), offset),
        }
    }

    /// Decode a location: which checkpoint, which of its objects, and where
    /// inside that object.
    ///
    /// The part is `None` for a format that writes one object per checkpoint,
    /// which is what says "the object has no part suffix" rather than "part 0".
    /// Those are different keys, and a container written before parting existed
    /// has the first kind.
    pub fn decode(blk_ptr: &BlockPtr, fmt: &BlockPtrFormat) -> (SegmentId, SegmentOffset) {
        match fmt {
            Self::Nop => (SegmentId::new(0), 0),
            Self::Flat => { let (s, o) = Self::decode_flat(blk_ptr); (SegmentId::new(s as u32), o) },
            Self::MicroGroup => { let (s, o) = Self::decode_micro_group(blk_ptr); (SegmentId::new(s as u32), o) },
            Self::PartedSegment => Self::decode_parted_segment(blk_ptr),
        }
    }

    /// Whether this format writes a checkpoint as several objects.
    #[inline]
    pub fn is_parted(&self) -> bool {
        matches!(self, Self::PartedSegment)
    }

    #[inline]
    fn encode_parted_segment(segid: u64, part: u16, offset: SegmentOffset) -> BlockPtr {
        debug_assert!((part as usize) < BLOCK_PTR_PARTED_MAX_PARTS,
            "part {} past the {} a pointer can name", part, BLOCK_PTR_PARTED_MAX_PARTS);
        debug_assert!(offset < BLOCK_PTR_PARTED_MAX_PART_BYTES,
            "offset {} past the {} bytes one part can hold", offset, BLOCK_PTR_PARTED_MAX_PART_BYTES);
        BLOCK_PTR_STAGING |
        (segid << BLOCK_PTR_SEGMENT_ID_BIT_SHIFT) & BLOCK_PTR_SEGMENT_ID_MASK |
        (offset as u64 >> BLOCK_PTR_SEGMENT_MG_OFFSET_ID_SHIFT << BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_SHIFT) & BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_MASK |
        (part as u64 & BLOCK_PTR_PARTED_PART_MASK)
    }

    #[inline]
    fn decode_parted_segment(blk_ptr: &BlockPtr) -> (SegmentId, SegmentOffset) {
        // clear bit 63 and bit 62
        let p = blk_ptr & !BLOCK_PTR_LOCATION_MASK;
        // format:
        //   - bits 0-13 part index
        //   - bits 14-31 block index within that part
        //   - bits 32-61 for segid
        let part = (p & BLOCK_PTR_PARTED_PART_MASK) as u16;
        let seg_offset_id = (p & BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_MASK) >> BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_SHIFT;
        let segid = (p & BLOCK_PTR_SEGMENT_ID_MASK) >> BLOCK_PTR_SEGMENT_ID_BIT_SHIFT;

        // convert to bytes offset
        let offset = seg_offset_id << BLOCK_PTR_SEGMENT_MG_OFFSET_ID_SHIFT; // multiple 4KiB
        (SegmentId::with_part(segid as u32, part), offset as SegmentOffset)
    }

    #[inline]
    fn encode_flat(segid: u64, offset: SegmentOffset, _seq: usize) -> BlockPtr {
        BLOCK_PTR_STAGING |
        (segid << BLOCK_PTR_SEGMENT_ID_BIT_SHIFT) & BLOCK_PTR_SEGMENT_ID_MASK |
        (offset as u64 & BLOCK_PTR_SEGMENT_FLAT_OFFSET_MASK)
    }

    #[inline]
    fn decode_flat(blk_ptr: &BlockPtr) -> (u64, SegmentOffset) {
        // clear bit 63 and bit 62
        let p = blk_ptr & !BLOCK_PTR_LOCATION_MASK;
        // format:
        //   - bits 0-31 offset
        //   - bits 32-61 for segid
        let offset = p & BLOCK_PTR_SEGMENT_FLAT_OFFSET_MASK;
        let segid = (p & BLOCK_PTR_SEGMENT_ID_MASK) >> BLOCK_PTR_SEGMENT_ID_BIT_SHIFT;
        (segid, offset as SegmentOffset)
    }

    #[inline]
    fn encode_micro_group(segid: u64, offset: SegmentOffset, seq: usize) -> BlockPtr {
        BLOCK_PTR_STAGING |
        (segid << BLOCK_PTR_SEGMENT_ID_BIT_SHIFT) & BLOCK_PTR_SEGMENT_ID_MASK |
        (offset as u64 >> BLOCK_PTR_SEGMENT_MG_OFFSET_ID_SHIFT << BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_SHIFT) & BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_MASK |
        ((seq as u64 >> BLOCK_PTR_SEGMENT_MG_GROUP_INDEX_SHIFT) & BLOCK_PTR_SEGMENT_MG_GROUP_INDEX_MASK) // convert to group id
    }

    #[inline]
    fn decode_micro_group(blk_ptr: &BlockPtr) -> (u64, SegmentOffset) {
        // clear bit 63 and bit 62
        let p = blk_ptr & !BLOCK_PTR_LOCATION_MASK;
        // format:
        //   - bits 0-13 micro group id
        //   - bits 14-31 segment block index
        //   - bits 32-61 for segid
        let seg_offset_id = (p & BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_MASK) >> BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_SHIFT;
        let segid = (p & BLOCK_PTR_SEGMENT_ID_MASK) >> BLOCK_PTR_SEGMENT_ID_BIT_SHIFT;

        // convert to bytes offset
        let offset = seg_offset_id << BLOCK_PTR_SEGMENT_MG_OFFSET_ID_SHIFT; // multiple 4KiB
        (segid, offset as SegmentOffset)
    }

    #[inline]
    pub fn decode_micro_group_id(blk_ptr: &BlockPtr) -> u64 {
        blk_ptr & BLOCK_PTR_SEGMENT_MG_GROUP_INDEX_MASK
    }

    // segid location is fixed, not depend on specific format
    #[inline]
    pub fn decode_segid(blk_ptr: &BlockPtr) -> SegmentId {
        let p = blk_ptr & !BLOCK_PTR_LOCATION_MASK;
        let segid = (p & BLOCK_PTR_SEGMENT_ID_MASK) >> BLOCK_PTR_SEGMENT_ID_BIT_SHIFT;
        SegmentId::new(segid as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- from_u8 ---

    #[test]
    fn from_u8_known_variants() {
        assert_eq!(BlockPtrFormat::from_u8(0), BlockPtrFormat::Nop);
        assert_eq!(BlockPtrFormat::from_u8(1), BlockPtrFormat::Flat);
        assert_eq!(BlockPtrFormat::from_u8(2), BlockPtrFormat::MicroGroup);
        assert_eq!(BlockPtrFormat::from_u8(3), BlockPtrFormat::PartedSegment);
    }

    #[test]
    #[should_panic(expected = "unknown block ptr format")]
    fn from_u8_invalid() {
        BlockPtrFormat::from_u8(4);
    }

    // --- sentinel values ---

    #[test]
    fn zero_block_sentinel() {
        let z = BlockPtrFormat::new_zero_block();
        assert!(BlockPtrFormat::is_zero_block(&z));
        assert!(!BlockPtrFormat::is_zero_block(&0));
        assert!(!BlockPtrFormat::is_dummy_value(&z));
    }

    #[test]
    fn dummy_value_sentinel() {
        let d = BlockPtrFormat::dummy_value();
        assert!(BlockPtrFormat::is_dummy_value(&d));
        assert!(!BlockPtrFormat::is_dummy_value(&0));
        assert!(!BlockPtrFormat::is_zero_block(&d));
    }

    #[test]
    fn invalid_value_sentinel() {
        let iv = BlockPtrFormat::invalid_value();
        assert!(BlockPtrFormat::is_invalid_value(&iv));
        assert_eq!(iv, BlockPtr::MIN);
    }

    // --- Flat encode/decode round-trip ---

    #[test]
    fn flat_round_trip_basic() {
        let segid = SegmentId::new(1);
        let offset: SegmentOffset = 4096;
        let ptr = BlockPtrFormat::encode(segid, offset, 0, &BlockPtrFormat::Flat);
        assert!(BlockPtrFormat::is_on_staging(&ptr));
        assert_eq!(BlockPtrFormat::decode(&ptr, &BlockPtrFormat::Flat), (segid, offset));
    }

    #[test]
    fn flat_round_trip_large_segid() {
        let segid = SegmentId::new(0x3FFF_FFFF); // max 30-bit segid
        let offset: SegmentOffset = 0xFFFF_FFFF; // max 32-bit offset
        let ptr = BlockPtrFormat::encode(segid, offset, 0, &BlockPtrFormat::Flat);
        assert_eq!(BlockPtrFormat::decode(&ptr, &BlockPtrFormat::Flat), (segid, offset));
    }

    #[test]
    fn flat_round_trip_zero() {
        let ptr = BlockPtrFormat::encode(SegmentId::new(0), 0, 0, &BlockPtrFormat::Flat);
        assert!(BlockPtrFormat::is_on_staging(&ptr));
        assert_eq!(BlockPtrFormat::decode(&ptr, &BlockPtrFormat::Flat), (SegmentId::new(0), 0));
    }

    // --- PartedSegment encode/decode round-trip ---

    #[test]
    fn parted_round_trip_carries_the_part() {
        for part in [0u16, 1, 2, 255, 4095, (BLOCK_PTR_PARTED_MAX_PARTS - 1) as u16] {
            let segid = SegmentId::with_part(12345, part);
            let offset: SegmentOffset = 8192;
            let ptr = BlockPtrFormat::encode(segid, offset, 0, &BlockPtrFormat::PartedSegment);
            assert!(BlockPtrFormat::is_on_staging(&ptr));
            let (got, o) = BlockPtrFormat::decode(&ptr, &BlockPtrFormat::PartedSegment);
            assert_eq!(got, segid, "part {} did not survive", part);
            assert_eq!(got.part_id(), Some(part));
            assert_eq!(o, offset);
        }
    }

    #[test]
    fn parted_round_trip_at_the_field_edges() {
        // Max 30-bit checkpoint, last part, and the largest 4 KiB-aligned offset
        // the in-part block index can name.
        let segid = SegmentId::with_part(0x3FFF_FFFF, (BLOCK_PTR_PARTED_MAX_PARTS - 1) as u16);
        let offset: SegmentOffset = BLOCK_PTR_PARTED_MAX_PART_BYTES - 4096;
        let ptr = BlockPtrFormat::encode(segid, offset, 0, &BlockPtrFormat::PartedSegment);
        assert_eq!(BlockPtrFormat::decode(&ptr, &BlockPtrFormat::PartedSegment), (segid, offset));
    }

    /// The part occupies the field the unparted format spends on a micro-group
    /// index, so the same bits mean different things and only the format byte
    /// tells them apart. A pointer read under the wrong format must not look
    /// plausible by accident.
    #[test]
    fn parted_and_unparted_do_not_share_a_reading() {
        let ptr = BlockPtrFormat::encode(
            SegmentId::with_part(7, 3), 8192, 0, &BlockPtrFormat::PartedSegment);
        let (parted, _) = BlockPtrFormat::decode(&ptr, &BlockPtrFormat::PartedSegment);
        assert_eq!(parted.part_id(), Some(3));
        // Read as unparted, the part bits are a group index and report no part
        // at all — which is why the two formats are never mixed in a container.
        let (plain, _) = BlockPtrFormat::decode(&ptr, &BlockPtrFormat::MicroGroup);
        assert_eq!(plain.part_id(), None);
    }

    #[test]
    fn only_the_parted_format_is_parted() {
        assert!(BlockPtrFormat::PartedSegment.is_parted());
        assert!(!BlockPtrFormat::MicroGroup.is_parted());
        assert!(!BlockPtrFormat::Flat.is_parted());
        assert!(!BlockPtrFormat::Nop.is_parted());
    }

    /// Part 0 is the summary's, and no data block is ever addressed there — the
    /// flush starts data at part 1. Nothing enforces that in the encoding, so
    /// this pins the constants the flush relies on.
    #[test]
    fn summary_part_is_zero_and_data_starts_after_it() {
        use crate::segment::Segment;
        assert_eq!(Segment::SUMMARY_PART, 0);
        assert_eq!(Segment::FIRST_DATA_PART, 1);
        assert!(Segment::FIRST_DATA_PART > Segment::SUMMARY_PART);
    }

    // --- MicroGroup encode/decode round-trip ---

    #[test]
    fn micro_group_round_trip_basic() {
        // offset must be 4KiB aligned for MicroGroup
        let segid = SegmentId::new(5);
        let offset: SegmentOffset = 8192; // 2 * 4KiB
        let seq = 32; // seq >= 16 to produce group_id >= 1
        let ptr = BlockPtrFormat::encode(segid, offset, seq, &BlockPtrFormat::MicroGroup);
        assert!(BlockPtrFormat::is_on_staging(&ptr));
        assert_eq!(BlockPtrFormat::decode(&ptr, &BlockPtrFormat::MicroGroup), (segid, offset));
    }

    #[test]
    fn micro_group_round_trip_zero_offset() {
        let ptr = BlockPtrFormat::encode(SegmentId::new(1), 0, 0, &BlockPtrFormat::MicroGroup);
        assert_eq!(BlockPtrFormat::decode(&ptr, &BlockPtrFormat::MicroGroup), (SegmentId::new(1), 0));
    }

    #[test]
    fn micro_group_group_id() {
        let seq = 48; // group_id = 48 >> 4 = 3
        let ptr = BlockPtrFormat::encode(SegmentId::new(1), 4096, seq, &BlockPtrFormat::MicroGroup);
        assert_eq!(BlockPtrFormat::decode_micro_group_id(&ptr), 3);
    }

    // --- decode_segid (format-independent) ---

    #[test]
    fn decode_segid_consistent_across_formats() {
        let segid = SegmentId::new(42);
        let flat_ptr = BlockPtrFormat::encode(segid, 4096, 0, &BlockPtrFormat::Flat);
        let mg_ptr = BlockPtrFormat::encode(segid, 4096, 0, &BlockPtrFormat::MicroGroup);
        assert_eq!(BlockPtrFormat::decode_segid(&flat_ptr), segid);
        assert_eq!(BlockPtrFormat::decode_segid(&mg_ptr), segid);
    }

    // --- Nop ---

    #[test]
    fn nop_encode_decode() {
        let ptr = BlockPtrFormat::encode(SegmentId::new(99), 1234, 0, &BlockPtrFormat::Nop);
        assert_eq!(ptr, 0);
        assert_eq!(BlockPtrFormat::decode(&ptr, &BlockPtrFormat::Nop), (SegmentId::new(0), 0));
    }
}
