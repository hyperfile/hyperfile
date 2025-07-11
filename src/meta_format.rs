use serde::{Deserialize, Serialize};
use crate::{BlockPtr, SegmentId, SegmentOffset};

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
    #[inline]
    pub fn from_u8(data: u8) -> Self {
        match data {
            0 => Self::Nop,
            1 => Self::Flat,
            2 => Self::MicroGroup,
            n @ _ => panic!("Unkown block ptr format {}", n),
        }
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
