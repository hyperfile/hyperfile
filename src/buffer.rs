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

pub struct AllocDataBlock {
    ptr: *mut u8,
    layout: Layout,
}

unsafe impl Send for AllocDataBlock {}
unsafe impl Sync for AllocDataBlock {}

impl AllocDataBlock {
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

impl Drop for AllocDataBlock {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr, self.layout);
        }
    }
}

pub struct MmapDataBlock {
    ptr: *mut u8,
    size: usize,
}

unsafe impl Send for MmapDataBlock {}
unsafe impl Sync for MmapDataBlock {}

impl MmapDataBlock {
    pub fn new(ptr: *mut u8, size: usize) -> Self {
        Self { ptr, size }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.ptr, self.size)
        }
    }

    pub fn as_mut_slice(&self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.ptr, self.size)
        }
    }

    pub fn lock(&self) {
        unsafe {
            let ret = libc::mlock(
                self.ptr as *const libc::c_void,
                self.size as libc::size_t
            );
            if ret == -1 {
                panic!("failed to mulock {:p} - {}, err: {}",
                    self.ptr, self.size, std::io::Error::last_os_error());
            }
        }
    }

    pub fn unlock(&self) {
        unsafe {
            let ret = libc::munlock(
                self.ptr as *const libc::c_void,
                self.size as libc::size_t,
            );
            if ret == -1 {
                panic!("failed to mulock {:p} - {}, err: {}",
                    self.ptr, self.size, std::io::Error::last_os_error());
            }
        }
    }
}

pub enum AlignedDataBlock {
    Alloc(Pin<Box<AllocDataBlock>>),
    Mmap(MmapDataBlock),
}

const DATA_BLOCK_FLAG_DIRTY: u64 = 0x1;
const DATA_BLOCK_FLAG_MMAP_LOCKED: u64 = 0x2;
const DATA_BLOCK_FLAG_SHOULD_CACHE: u64 = 0x4;

pub struct DataBlock {
    data: AlignedDataBlock,
    index: BlockIndex,
    flags: u64,
}

impl DataBlock {
    pub fn new(index: BlockIndex, size: usize) -> Self {
        Self::new_alloc(index, size)
    }

    pub fn new_alloc(index: BlockIndex, size: usize) -> Self {
        Self {
            data: AlignedDataBlock::Alloc(Box::pin(AllocDataBlock::new(size))),
            index: index,
            flags: 0,
        }
    }

    pub fn new_mmap(index: BlockIndex, ptr: *mut u8, size: usize) -> Self {
        Self {
            data: AlignedDataBlock::Mmap(MmapDataBlock::new(ptr, size)),
            index: index,
            flags: 0,
        }
    }

    // duplicate a data block by copy
    pub fn dup(&self) -> Self {
        let n = Self {
            data: AlignedDataBlock::Alloc(Box::pin(AllocDataBlock::new(self.size()))),
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
        match &self.data {
            AlignedDataBlock::Alloc(alloc) => alloc.ptr as u64,
            AlignedDataBlock::Mmap(mmap) => mmap.ptr as u64,
        }
    }

    // get buffer size
    #[inline]
    pub fn size(&self) -> usize {
        match &self.data {
            AlignedDataBlock::Alloc(alloc) => alloc.layout.size(),
            AlignedDataBlock::Mmap(mmap) => mmap.size,
        }
    }

    /// copy all data from slice into block buffer start with offset
    pub fn copy(&mut self, offset: usize, data: &[u8]) {
        let ptr = match &self.data {
            AlignedDataBlock::Alloc(alloc) => alloc.ptr,
            AlignedDataBlock::Mmap(mmap) => mmap.ptr,
        };
        let s = unsafe {
            let ptr = ptr.add(offset);
            std::slice::from_raw_parts_mut(ptr, data.len())
        };
        s.copy_from_slice(data);
    }

    /// copy data from block into supplied buffer
    pub fn copy_out(&self, offset: usize, buf: &mut [u8]) {
        let ptr = match &self.data {
            AlignedDataBlock::Alloc(alloc) => alloc.ptr,
            AlignedDataBlock::Mmap(mmap) => mmap.ptr,
        };
        let s = unsafe {
            let ptr = ptr.add(offset);
            std::slice::from_raw_parts_mut(ptr, buf.len())
        };
        buf.copy_from_slice(s);
    }

    // expose inner data as slice
    pub fn as_slice(&self) -> &[u8] {
        match &self.data {
            AlignedDataBlock::Alloc(alloc) => alloc.as_slice(),
            AlignedDataBlock::Mmap(mmap) => mmap.as_slice(),
        }
    }

    // expose inner data as slice
    pub fn as_mut_slice(&self) -> &mut [u8] {
        match &self.data {
            AlignedDataBlock::Alloc(alloc) => alloc.as_mut_slice(),
            AlignedDataBlock::Mmap(mmap) => mmap.as_mut_slice(),
        }
    }

    pub fn set_dirty(&self) {
        let flags = self.flags | DATA_BLOCK_FLAG_DIRTY;
        let ptr = std::ptr::addr_of!(self.flags) as *mut u64;
        unsafe {
            std::ptr::write_volatile(ptr, flags);
        }
    }

    pub fn clear_dirty(&self) {
        let flags = self.flags & !DATA_BLOCK_FLAG_DIRTY;
        let ptr = std::ptr::addr_of!(self.flags) as *mut u64;
        unsafe {
            std::ptr::write_volatile(ptr, flags);
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.flags & DATA_BLOCK_FLAG_DIRTY == DATA_BLOCK_FLAG_DIRTY
    }

    pub fn set_locked(&self) {
        let flags = self.flags | DATA_BLOCK_FLAG_MMAP_LOCKED;
        let ptr = std::ptr::addr_of!(self.flags) as *mut u64;
        unsafe {
            std::ptr::write_volatile(ptr, flags);
        }
    }

    pub fn clear_locked(&self) {
        let flags = self.flags & !DATA_BLOCK_FLAG_MMAP_LOCKED;
        let ptr = std::ptr::addr_of!(self.flags) as *mut u64;
        unsafe {
            std::ptr::write_volatile(ptr, flags);
        }
    }

    pub fn is_locked(&self) -> bool {
        self.flags & DATA_BLOCK_FLAG_MMAP_LOCKED == DATA_BLOCK_FLAG_MMAP_LOCKED
    }

    pub fn set_should_cache(&mut self) {
        self.flags |= DATA_BLOCK_FLAG_SHOULD_CACHE;
    }

    pub fn is_should_cache(&self) -> bool {
        self.flags & DATA_BLOCK_FLAG_SHOULD_CACHE == DATA_BLOCK_FLAG_SHOULD_CACHE
    }

    pub fn lock(&self) {
        match &self.data {
            AlignedDataBlock::Alloc(_) => {},
            AlignedDataBlock::Mmap(mmap) => {
                if self.is_locked() { return; }
                mmap.lock();
                self.set_locked();
            },
        }
    }

    pub fn unlock(&self) {
        match &self.data {
            AlignedDataBlock::Alloc(_) => {},
            AlignedDataBlock::Mmap(mmap) => {
                if !self.is_locked() || self.is_dirty() { return; }
                mmap.unlock();
                self.clear_locked();
            },
        }
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
    data: Option<Pin<Box<AllocDataBlock>>>,
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
            data: if is_zero { None } else { Some(Box::pin(AllocDataBlock::new(len))) },
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

#[cfg(test)]
mod tests {
    use super::*;

    // --- AllocDataBlock ---

    #[test]
    fn alloc_data_block_zeroed() {
        let blk = AllocDataBlock::new(4096);
        assert!(blk.as_slice().iter().all(|&b| b == 0));
        assert_eq!(blk.as_slice().len(), 4096);
    }

    #[test]
    fn alloc_data_block_write_read() {
        let blk = AllocDataBlock::new(4096);
        blk.as_mut_slice()[0] = 0xAB;
        blk.as_mut_slice()[4095] = 0xCD;
        assert_eq!(blk.as_slice()[0], 0xAB);
        assert_eq!(blk.as_slice()[4095], 0xCD);
    }

    // --- DataBlock ---

    #[test]
    fn data_block_new_and_size() {
        let blk = DataBlock::new(5, 8192);
        assert_eq!(blk.index(), 5);
        assert_eq!(blk.size(), 8192);
        assert!(!blk.is_dirty());
    }

    #[test]
    fn data_block_copy_and_copy_out() {
        let mut blk = DataBlock::new(0, 4096);
        let data = [1u8, 2, 3, 4];
        blk.copy(100, &data);
        let mut out = [0u8; 4];
        blk.copy_out(100, &mut out);
        assert_eq!(out, [1, 2, 3, 4]);
    }

    #[test]
    fn data_block_dirty_flag() {
        let blk = DataBlock::new(0, 4096);
        assert!(!blk.is_dirty());
        blk.set_dirty();
        assert!(blk.is_dirty());
        blk.clear_dirty();
        assert!(!blk.is_dirty());
    }

    #[test]
    fn data_block_should_cache_flag() {
        let mut blk = DataBlock::new(0, 4096);
        assert!(!blk.is_should_cache());
        blk.set_should_cache();
        assert!(blk.is_should_cache());
    }

    #[test]
    fn data_block_dup() {
        let mut blk = DataBlock::new(3, 4096);
        blk.copy(0, &[0xAA; 4096]);
        let dup = blk.dup();
        assert_eq!(dup.index(), 3);
        assert_eq!(dup.size(), 4096);
        assert_eq!(dup.as_slice()[0], 0xAA);
        assert_eq!(dup.as_slice()[4095], 0xAA);
        // dup should be independent
        assert_ne!(blk.uid(), dup.uid());
    }

    #[test]
    fn data_block_as_slice() {
        let blk = DataBlock::new(0, 4096);
        assert_eq!(blk.as_slice().len(), 4096);
        assert_eq!(blk.as_mut_slice().len(), 4096);
    }

    // --- ZeroDataBlock ---

    #[test]
    fn zero_data_block_properties() {
        let zb = ZeroDataBlock::new(10, 4096);
        assert_eq!(zb.index(), 10);
        assert_eq!(zb.size(), 4096);
    }

    // --- AlignedDataBlockWrapper ---

    #[test]
    fn aligned_wrapper_data() {
        let w = AlignedDataBlockWrapper::new(0, 4096, false);
        assert!(!w.is_zero());
        assert_eq!(w.size(), 4096);
        assert_eq!(w.index(), 0);
    }

    #[test]
    fn aligned_wrapper_zero() {
        let w = AlignedDataBlockWrapper::new(1, 4096, true);
        assert!(w.is_zero());
        assert_eq!(w.index(), 1);
    }

    #[test]
    #[should_panic(expected = "no slice on zero data block")]
    fn aligned_wrapper_zero_no_slice() {
        let w = AlignedDataBlockWrapper::new(0, 4096, true);
        let _ = w.as_slice();
    }

    // --- BatchDataBlockWrapper ---

    #[test]
    fn batch_wrapper_full_block() {
        let w = BatchDataBlockWrapper::new(0, 4096, false);
        assert!(w.is_full_block());
        assert!(!w.is_zero());
        assert_eq!(w.offset(), 0);
        assert_eq!(w.len(), 4096);
    }

    #[test]
    fn batch_wrapper_zero_block() {
        let w = BatchDataBlockWrapper::new(0, 4096, true);
        assert!(w.is_full_block());
        assert!(w.is_zero());
    }

    #[test]
    fn batch_wrapper_partial_block() {
        let w = BatchDataBlockWrapper::new_partial_block(5, 4096, 100, 200, false);
        assert!(!w.is_full_block());
        assert!(!w.is_zero());
        assert_eq!(w.index(), 5);
        assert_eq!(w.size(), 4096);
        assert_eq!(w.offset(), 100);
        assert_eq!(w.len(), 200);
        assert_eq!(w.as_slice().len(), 200);
    }

    #[test]
    fn batch_wrapper_partial_zero() {
        let w = BatchDataBlockWrapper::new_partial_block(0, 4096, 0, 100, true);
        assert!(!w.is_full_block());
        assert!(w.is_zero());
    }

    #[test]
    fn batch_wrapper_merge_partial_into_data() {
        let mut full = BatchDataBlockWrapper::new(0, 4096, false);
        // write known data into full block
        full.as_mut_slice().fill(0);

        let part = BatchDataBlockWrapper::new_partial_block(0, 4096, 10, 4, false);
        part.as_mut_slice().copy_from_slice(&[1, 2, 3, 4]);

        full.merge_partial(&part);
        assert_eq!(&full.as_slice()[10..14], &[1, 2, 3, 4]);
        assert_eq!(full.as_slice()[0], 0); // untouched
    }

    #[test]
    fn batch_wrapper_merge_zero_partial_into_data() {
        let mut full = BatchDataBlockWrapper::new(0, 4096, false);
        full.as_mut_slice().fill(0xFF);

        let part = BatchDataBlockWrapper::new_partial_block(0, 4096, 0, 10, true);
        full.merge_partial(&part);
        // first 10 bytes should be zeroed
        assert!(full.as_slice()[..10].iter().all(|&b| b == 0));
        assert_eq!(full.as_slice()[10], 0xFF);
    }

    #[test]
    fn batch_wrapper_merge_partial_into_zero() {
        let mut full = BatchDataBlockWrapper::new(0, 4096, true);
        let part = BatchDataBlockWrapper::new_partial_block(0, 4096, 0, 4, false);
        part.as_mut_slice().copy_from_slice(&[0xAA; 4]);

        full.merge_partial(&part);
        // should have been promoted to Data
        assert!(!full.is_zero());
        assert_eq!(&full.as_slice()[0..4], &[0xAA; 4]);
        // rest should be zero
        assert!(full.as_slice()[4..].iter().all(|&b| b == 0));
    }
}