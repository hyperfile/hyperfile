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

const BLOCK_PTR_SEGMENT_ID_BIT_SHIFT: u32 = 32;
const BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_SHIFT: u32 = 14;
const BLOCK_PTR_SEGMENT_MG_GROUP_INDEX_SHIFT: u32 = 4; // 16 entry per group
const BLOCK_PTR_SEGMENT_MG_OFFSET_ID_SHIFT: u32 = 12; // 4KiB

pub const BLOCK_PTR_SEGMENT_MG_GROUP_INDEX_BITS: u32 = BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_SHIFT;
pub const BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_BITS: u32 = 32 - BLOCK_PTR_SEGMENT_MG_GROUP_INDEX_BITS;

// data on staging
const BLOCK_PTR_STAGING: u64 = 0x4000_0000_0000_0000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[repr(u8)]
pub enum BlockPtrFormat {
    Nop = 0,
    Flat = 1,
    MicroGroup = 2,
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

    pub fn encode(segid: SegmentId, offset: SegmentOffset, seq: usize, fmt: &BlockPtrFormat) -> BlockPtr {
        match fmt {
            Self::Nop => 0,
            Self::Flat => Self::encode_flat(segid, offset, seq),
            Self::MicroGroup => Self::encode_micro_group(segid, offset, seq),
        }
    }

    pub fn decode(blk_ptr: &BlockPtr, fmt: &BlockPtrFormat) -> (SegmentId, SegmentOffset) {
        match fmt {
            Self::Nop => (0, 0),
            Self::Flat => Self::decode_flat(blk_ptr),
            Self::MicroGroup => Self::decode_micro_group(blk_ptr),
        }
    }

    #[inline]
    fn encode_flat(segid: SegmentId, offset: SegmentOffset, _seq: usize) -> BlockPtr {
        BLOCK_PTR_STAGING |
        (segid << BLOCK_PTR_SEGMENT_ID_BIT_SHIFT) & BLOCK_PTR_SEGMENT_ID_MASK |
        (offset as u64 & BLOCK_PTR_SEGMENT_FLAT_OFFSET_MASK)
    }

    #[inline]
    fn decode_flat(blk_ptr: &BlockPtr) -> (SegmentId, SegmentOffset) {
        // clear bit 63 and bit 62
        let p = blk_ptr & !BLOCK_PTR_LOCATION_MASK;
        // format:
        //   - bits 0-31 offset
        //   - bits 32-61 for segid
        let offset = p & BLOCK_PTR_SEGMENT_FLAT_OFFSET_MASK;
        let segid = (p & BLOCK_PTR_SEGMENT_ID_MASK) >> BLOCK_PTR_SEGMENT_ID_BIT_SHIFT;
        (segid as SegmentId, offset as SegmentOffset)
    }

    #[inline]
    fn encode_micro_group(segid: SegmentId, offset: SegmentOffset, seq: usize) -> BlockPtr {
        BLOCK_PTR_STAGING |
        (segid << BLOCK_PTR_SEGMENT_ID_BIT_SHIFT) & BLOCK_PTR_SEGMENT_ID_MASK |
        (offset as u64 >> BLOCK_PTR_SEGMENT_MG_OFFSET_ID_SHIFT << BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_SHIFT) & BLOCK_PTR_SEGMENT_MG_BLOCK_INDEX_MASK |
        ((seq as u64 >> BLOCK_PTR_SEGMENT_MG_GROUP_INDEX_SHIFT) & BLOCK_PTR_SEGMENT_MG_GROUP_INDEX_MASK) // convert to group id
    }

    #[inline]
    fn decode_micro_group(blk_ptr: &BlockPtr) -> (SegmentId, SegmentOffset) {
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
        (segid as SegmentId, offset as SegmentOffset)
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
        segid as SegmentId
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
    }

    #[test]
    #[should_panic(expected = "unknown block ptr format")]
    fn from_u8_invalid() {
        BlockPtrFormat::from_u8(3);
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
        let segid: SegmentId = 1;
        let offset: SegmentOffset = 4096;
        let ptr = BlockPtrFormat::encode(segid, offset, 0, &BlockPtrFormat::Flat);
        assert!(BlockPtrFormat::is_on_staging(&ptr));
        let (dec_segid, dec_offset) = BlockPtrFormat::decode(&ptr, &BlockPtrFormat::Flat);
        assert_eq!(dec_segid, segid);
        assert_eq!(dec_offset, offset);
    }

    #[test]
    fn flat_round_trip_large_segid() {
        let segid: SegmentId = 0x3FFF_FFFF; // max 30-bit segid
        let offset: SegmentOffset = 0xFFFF_FFFF; // max 32-bit offset
        let ptr = BlockPtrFormat::encode(segid, offset, 0, &BlockPtrFormat::Flat);
        let (dec_segid, dec_offset) = BlockPtrFormat::decode(&ptr, &BlockPtrFormat::Flat);
        assert_eq!(dec_segid, segid);
        assert_eq!(dec_offset, offset as usize);
    }

    #[test]
    fn flat_round_trip_zero() {
        let ptr = BlockPtrFormat::encode(0, 0, 0, &BlockPtrFormat::Flat);
        assert!(BlockPtrFormat::is_on_staging(&ptr));
        let (dec_segid, dec_offset) = BlockPtrFormat::decode(&ptr, &BlockPtrFormat::Flat);
        assert_eq!(dec_segid, 0);
        assert_eq!(dec_offset, 0);
    }

    // --- MicroGroup encode/decode round-trip ---

    #[test]
    fn micro_group_round_trip_basic() {
        // offset must be 4KiB aligned for MicroGroup
        let segid: SegmentId = 5;
        let offset: SegmentOffset = 8192; // 2 * 4KiB
        let seq = 32; // seq >= 16 to produce group_id >= 1
        let ptr = BlockPtrFormat::encode(segid, offset, seq, &BlockPtrFormat::MicroGroup);
        assert!(BlockPtrFormat::is_on_staging(&ptr));
        let (dec_segid, dec_offset) = BlockPtrFormat::decode(&ptr, &BlockPtrFormat::MicroGroup);
        assert_eq!(dec_segid, segid);
        assert_eq!(dec_offset, offset);
    }

    #[test]
    fn micro_group_round_trip_zero_offset() {
        let ptr = BlockPtrFormat::encode(1, 0, 0, &BlockPtrFormat::MicroGroup);
        let (dec_segid, dec_offset) = BlockPtrFormat::decode(&ptr, &BlockPtrFormat::MicroGroup);
        assert_eq!(dec_segid, 1);
        assert_eq!(dec_offset, 0);
    }

    #[test]
    fn micro_group_group_id() {
        let seq = 48; // group_id = 48 >> 4 = 3
        let ptr = BlockPtrFormat::encode(1, 4096, seq, &BlockPtrFormat::MicroGroup);
        assert_eq!(BlockPtrFormat::decode_micro_group_id(&ptr), 3);
    }

    // --- decode_segid (format-independent) ---

    #[test]
    fn decode_segid_consistent_across_formats() {
        let segid: SegmentId = 42;
        let flat_ptr = BlockPtrFormat::encode(segid, 4096, 0, &BlockPtrFormat::Flat);
        let mg_ptr = BlockPtrFormat::encode(segid, 4096, 0, &BlockPtrFormat::MicroGroup);
        assert_eq!(BlockPtrFormat::decode_segid(&flat_ptr), segid);
        assert_eq!(BlockPtrFormat::decode_segid(&mg_ptr), segid);
    }

    // --- Nop ---

    #[test]
    fn nop_encode_decode() {
        let ptr = BlockPtrFormat::encode(99, 1234, 0, &BlockPtrFormat::Nop);
        assert_eq!(ptr, 0);
        let (s, o) = BlockPtrFormat::decode(&ptr, &BlockPtrFormat::Nop);
        assert_eq!(s, 0);
        assert_eq!(o, 0);
    }
}
