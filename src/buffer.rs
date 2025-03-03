use std::pin::Pin;
use std::alloc::GlobalAlloc;
use std::alloc::{alloc_zeroed, dealloc, Layout};
use crate::BlockIndex;

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

struct AlignedDataBlock {
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

pub(crate) struct DataBlock {
    data: Pin<Box<AlignedDataBlock>>,
    index: BlockIndex,
}

impl DataBlock {
    pub(crate) fn new(index: BlockIndex, size: usize) -> Self {
        Self {
            data: Box::pin(AlignedDataBlock::new(size)),
            index: index,
        }
    }

    pub(crate) fn dup(&self) -> Self {
        let n = Self {
            data: Box::pin(AlignedDataBlock::new(self.size())),
            index: self.index(),
        };
        self.copy_out(0, n.as_mut_slice());
        n
    }

    #[inline]
    pub(crate) fn index(&self) -> BlockIndex {
        self.index
    }

    // unique id from value of inner buffer pointer
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn uid(&self) -> u64 {
        self.data.ptr as u64
    }

    // get buffer size
    #[inline]
    pub(crate) fn size(&self) -> usize {
        self.data.layout.size()
    }

    /// copy all data from slice into block buffer start with offset
    pub(crate) fn copy(&mut self, offset: usize, data: &[u8]) {
        let s = unsafe {
            let ptr = self.data.ptr.add(offset);
            std::slice::from_raw_parts_mut(ptr, data.len())
        };
        s.copy_from_slice(data);
    }

    /// copy data from block into supplied buffer
    pub(crate) fn copy_out(&self, offset: usize, buf: &mut [u8]) {
        let s = unsafe {
            let ptr = self.data.ptr.add(offset);
            std::slice::from_raw_parts_mut(ptr, buf.len())
        };
        buf.copy_from_slice(s);
    }

    // expose inner data as slice
    pub(crate) fn as_slice(&self) -> &[u8] {
        self.data.as_slice()
    }

    // expose inner data as slice
    pub(crate) fn as_mut_slice(&self) -> &mut [u8] {
        self.data.as_mut_slice()
    }
}
