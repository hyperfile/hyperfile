use std::pin::Pin;
use std::alloc::GlobalAlloc;
use std::alloc::{alloc_zeroed, dealloc, Layout};
use crate::BlockIndex;
use crate::utils;

const MIN_ALIGNED: usize = 4096;

#[allow(dead_code)]
struct AlignedAlloc;

unsafe impl GlobalAlloc for AlignedAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let aligned = match layout.align_to(MIN_ALIGNED) {
            Ok(l) => l,
            Err(_) => return std::ptr::null_mut(),
        };
        unsafe { alloc_zeroed(aligned) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { dealloc(ptr, layout) }
    }
}

pub struct AlignedDataBlock {
    ptr: *mut u8,
    layout: Layout,
}

unsafe impl Send for AlignedDataBlock {}
unsafe impl Sync for AlignedDataBlock {}

impl AlignedDataBlock {
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, MIN_ALIGNED).expect("unable to create layout for aligned block");
        Self {
            ptr: unsafe { alloc_zeroed(layout) },
            layout: layout,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.ptr, self.layout.size())
        }
    }

    pub fn as_mut_slice(&self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.ptr, self.layout.size())
        }
    }
}

impl Drop for AlignedDataBlock {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr, self.layout);
        }
    }
}

const DATA_BLOCK_FLAG_SHOULD_CACHE: u64 = 0x1;

pub struct DataBlock {
    data: Pin<Box<AlignedDataBlock>>,
    index: BlockIndex,
    flags: u64,
}

impl DataBlock {
    pub fn new(index: BlockIndex, size: usize) -> Self {
        Self {
            data: Box::pin(AlignedDataBlock::new(size)),
            index: index,
            flags: 0,
        }
    }

    // duplicate a data block by copy
    pub fn dup(&self) -> Self {
        let n = Self {
            data: Box::pin(AlignedDataBlock::new(self.size())),
            index: self.index(),
            flags: self.flags,
        };
        self.copy_out(0, n.as_mut_slice());
        n
    }

    #[inline]
    pub fn index(&self) -> BlockIndex {
        self.index
    }

    // unique id from value of inner buffer pointer
    #[inline]
    pub fn uid(&self) -> u64 {
        self.data.ptr as u64
    }

    // get buffer size
    #[inline]
    pub fn size(&self) -> usize {
        self.data.layout.size()
    }

    /// copy all data from slice into block buffer start with offset
    pub fn copy(&mut self, offset: usize, data: &[u8]) {
        let s = unsafe {
            let ptr = self.data.ptr.add(offset);
            std::slice::from_raw_parts_mut(ptr, data.len())
        };
        s.copy_from_slice(data);
    }

    /// copy data from block into supplied buffer
    pub fn copy_out(&self, offset: usize, buf: &mut [u8]) {
        let s = unsafe {
            let ptr = self.data.ptr.add(offset);
            std::slice::from_raw_parts_mut(ptr, buf.len())
        };
        buf.copy_from_slice(s);
    }

    // expose inner data as slice
    pub fn as_slice(&self) -> &[u8] {
        self.data.as_slice()
    }

    // expose inner data as slice
    pub fn as_mut_slice(&self) -> &mut [u8] {
        self.data.as_mut_slice()
    }

    pub fn set_should_cache(&mut self) {
        self.flags |= DATA_BLOCK_FLAG_SHOULD_CACHE;
    }

    pub fn is_should_cache(&self) -> bool {
        self.flags & DATA_BLOCK_FLAG_SHOULD_CACHE == DATA_BLOCK_FLAG_SHOULD_CACHE
    }
}

pub struct ZeroDataBlock {
    index: BlockIndex,
    size: usize,
}

impl ZeroDataBlock {
    pub(crate) fn new(index: BlockIndex, size: usize) -> Self {
        Self { index, size }
    }

    #[inline]
    pub(crate) fn index(&self) -> BlockIndex {
        self.index
    }

    #[inline]
    pub(crate) fn size(&self) -> usize {
        self.size
    }
}

pub enum AlignedDataBlockWrapper {
    Data(DataBlock),
    Zero(ZeroDataBlock),
}

impl AlignedDataBlockWrapper {
    pub fn new(index: BlockIndex, size: usize, is_zero: bool) -> Self {
        if is_zero {
            return Self::Zero(ZeroDataBlock::new(index, size));
        }
        Self::Data(DataBlock::new(index, size))
    }

    pub fn is_zero(&self) -> bool {
        match self {
            Self::Data(_) => false,
            Self::Zero(_) => true,
        }
    }

    pub fn index(&self) -> BlockIndex {
        match self {
            Self::Data(block) => block.index(),
            Self::Zero(block) => block.index(),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Data(block) => block.size(),
            Self::Zero(block) => block.size(),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Data(block) => block.as_slice(),
            Self::Zero(_) => panic!("no slice on zero data block"),
        }
    }

    pub fn as_mut_slice(&self) -> &mut [u8] {
        match self {
            Self::Data(block) => block.as_mut_slice(),
            Self::Zero(_) => panic!("no slice on zero data block"),
        }
    }
}

pub struct PartDataBlock {
    data: Option<Pin<Box<AlignedDataBlock>>>,
    index: BlockIndex,
    // offset of data within block
    offset: usize,
    // block size
    size: usize,
    // real data size
    len: usize,
}

impl PartDataBlock {
    pub(crate) fn new(index: BlockIndex, size: usize, offset: usize, len: usize, is_zero: bool) -> Self {
        Self {
            data: if is_zero { None } else { Some(Box::pin(AlignedDataBlock::new(len))) },
            index,
            offset,
            size,
            len,
        }
    }

    // expose inner data as slice
    pub(crate) fn as_slice(&self) -> &[u8] {
        self.data.as_ref().unwrap().as_slice()
    }

    pub(crate) fn as_mut_slice(&self) -> &mut [u8] {
        self.data.as_ref().unwrap().as_mut_slice()
    }

    #[inline]
    pub(crate) fn index(&self) -> BlockIndex {
        self.index
    }

    #[inline]
    pub(crate) fn size(&self) -> usize {
        self.size
    }

    #[inline]
    pub(crate) fn offset(&self) -> usize {
        self.offset
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub(crate) fn is_zero(&self) -> bool {
        self.data.is_none()
    }
}

pub enum BatchDataBlockWrapper {
    Data(DataBlock),
    Zero(ZeroDataBlock),
    Part(PartDataBlock),
}

impl BatchDataBlockWrapper {
    pub fn new(index: BlockIndex, size: usize, is_zero: bool) -> Self {
        if is_zero {
            return Self::Zero(ZeroDataBlock::new(index, size));
        }
        Self::Data(DataBlock::new(index, size))
    }

    pub fn new_partial_block(index: BlockIndex, size: usize, offset: usize, len: usize, is_zero: bool) -> Self {
        Self::Part(PartDataBlock::new(index, size, offset, len, is_zero))
    }

    pub fn index(&self) -> BlockIndex {
        match self {
            Self::Data(block) => block.index(),
            Self::Zero(block) => block.index(),
            Self::Part(block) => block.index(),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Data(block) => block.size(),
            Self::Zero(block) => block.size(),
            Self::Part(block) => block.size(),
        }
    }

    pub fn offset(&self) -> usize {
        match self {
            Self::Data(_) | Self::Zero(_) => 0,
            Self::Part(block) => block.offset(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Data(block) => block.size(),
            Self::Zero(block) => block.size(),
            Self::Part(block) => block.len(),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Data(block) => block.as_slice(),
            Self::Zero(_) => panic!("no slice on zero data block"),
            Self::Part(block) => {
                if block.is_zero() {
                    panic!("no slice on zero partial data block");
                }
                block.as_slice()
            },
        }
    }

    pub fn as_mut_slice(&self) -> &mut [u8] {
        match self {
            Self::Data(block) => block.as_mut_slice(),
            Self::Zero(_) => panic!("no slice on zero data block"),
            Self::Part(block) => {
                if block.is_zero() {
                    panic!("no slice on zero partial data block");
                }
                block.as_mut_slice()
            },
        }
    }

    // full block merge with other block
    pub(crate) fn merge_partial(&mut self, other: &Self) {
        assert!(self.is_full_block());
        assert!(!other.is_full_block());
        // handle other
        let (o_offset, o_len, o_is_zero, o_slice) = match other {
            Self::Data(_) | Self::Zero(_) => panic!("other block is NOT a partial block"),
            Self::Part(inner) => {
                if inner.is_zero() {
                    (inner.offset(), inner.len(), inner.is_zero(), None)
                } else {
                    (inner.offset(), inner.len(), inner.is_zero(), Some(inner.as_slice()))
                }
            },
        };
        // handle myself
        match self {
            Self::Data(inner) => {
                let inner_slice = inner.as_mut_slice();
                let target_slice = &mut inner_slice[o_offset..o_offset + o_len];
                if o_is_zero {
                    let blk_idx = inner.index();
                    let blk_size = inner.size();
                    let mut zero = Vec::with_capacity(o_len);
                    zero.resize(o_len, 0);
                    target_slice.copy_from_slice(&zero);
                    if utils::is_all_zeros(self.as_slice()) {
                        *self = Self::Zero(ZeroDataBlock::new(blk_idx, blk_size));
                    }
                } else {
                    target_slice.copy_from_slice(o_slice.unwrap());
                }
            },
            Self::Zero(inner) => {
                if o_is_zero {
                    // keep zero, do nothing
                    return;
                }
                // create a new data block
                let block = DataBlock::new(inner.index(), inner.size());
                let block_slice = block.as_mut_slice();
                let target_slice = &mut block_slice[o_offset..o_offset + o_len];
                target_slice.copy_from_slice(o_slice.unwrap());
                *self = Self::Data(block);
            },
            Self::Part(_) => panic!("this is not a full block"),
        }
    }

    #[inline]
    pub(crate) fn is_full_block(&self) -> bool {
        match self {
            Self::Data(_) => true,
            Self::Zero(_) => true,
            Self::Part(_) => false,
        }
    }

    #[inline]
    pub fn is_zero(&self) -> bool {
        match self {
            Self::Data(_) => false,
            Self::Zero(_) => true,
            Self::Part(block) => block.is_zero(),
        }
    }
}
