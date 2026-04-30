pub fn is_all_zeros(buf: &[u8]) -> bool {
    let (prefix, aligned, suffix) = unsafe { buf.align_to::<u128>() };

    prefix.iter().all(|&x| x == 0)
        && suffix.iter().all(|&x| x == 0)
        && aligned.iter().all(|&x| x == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_zeros_empty() {
        assert!(is_all_zeros(&[]));
    }

    #[test]
    fn all_zeros_true() {
        assert!(is_all_zeros(&[0u8; 4096]));
    }

    #[test]
    fn all_zeros_false_first_byte() {
        let mut buf = [0u8; 4096];
        buf[0] = 1;
        assert!(!is_all_zeros(&buf));
    }

    #[test]
    fn all_zeros_false_last_byte() {
        let mut buf = [0u8; 4096];
        buf[4095] = 1;
        assert!(!is_all_zeros(&buf));
    }

    #[test]
    fn all_zeros_false_middle() {
        let mut buf = [0u8; 4096];
        buf[2048] = 0xFF;
        assert!(!is_all_zeros(&buf));
    }

    #[test]
    fn all_zeros_unaligned_size() {
        assert!(is_all_zeros(&[0u8; 7]));
        let mut buf = [0u8; 7];
        buf[6] = 1;
        assert!(!is_all_zeros(&buf));
    }

    #[test]
    fn all_zeros_single_byte() {
        assert!(is_all_zeros(&[0u8]));
        assert!(!is_all_zeros(&[1u8]));
    }
}