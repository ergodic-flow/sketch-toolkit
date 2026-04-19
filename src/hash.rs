const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

// FNV-1a
pub fn hashfn(data: &[u8], seed: u64) -> u64 {
    let mut h = seed;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

pub fn hashfn_default(data: &[u8]) -> u64 {
    hashfn(data, FNV_OFFSET)
}

pub fn row_seed(row: usize) -> u64 {
    FNV_OFFSET.wrapping_add((row as u64).wrapping_mul(0x9e3779b97f4a7c15))
}
