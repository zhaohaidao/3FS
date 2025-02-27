use super::super::Size;

pub const ALIGN_SIZE: Size = Size::new(512);

pub fn create_aligned_vec(size: Size) -> Vec<u8> {
    let s: usize = size.into();
    let layout = std::alloc::Layout::from_size_align(s, ALIGN_SIZE.into()).unwrap();
    unsafe { Vec::from_raw_parts(std::alloc::alloc(layout), s, s) }
}

pub fn is_aligned_buf(data: &[u8]) -> bool {
    data.as_ptr() as u64 % ALIGN_SIZE.0 == 0 && data.len() as u64 % ALIGN_SIZE.0 == 0
}

pub fn is_aligned_len(len: u32) -> bool {
    len % ALIGN_SIZE.0 as u32 == 0
}

pub fn is_aligned_io(data: &[u8], offset: u32) -> bool {
    is_aligned_buf(data) && is_aligned_len(offset)
}
