//! Bloom filter — a space-efficient probabilistic data structure used to test
//! whether an element is a member of a set. False positives are possible, but
//! false negatives are not.

use wincode::{SchemaRead, SchemaWrite};

use crate::hash::{hashfn, row_seed};

#[derive(Debug, Clone, SchemaWrite, SchemaRead)]
pub struct BloomFilter {
    width: usize,
    depth: usize,
    bits: Vec<u64>,
    count: u64,
}

impl BloomFilter {
    pub fn new(width: usize, depth: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(width > 0, "Bloom filter width must be >= 1");
        anyhow::ensure!(depth > 0, "Bloom filter depth must be >= 1");

        let words = width.div_ceil(64);
        Ok(Self {
            width,
            depth,
            bits: vec![0u64; words],
            count: 0,
        })
    }

    fn bit_index(&self, row: usize, value: &str) -> usize {
        let seed = row_seed(row);
        let h = hashfn(value.as_bytes(), seed);
        (h % self.width as u64) as usize
    }

    pub fn insert(&mut self, value: &str) {
        for row in 0..self.depth {
            let idx = self.bit_index(row, value);
            self.bits[idx / 64] |= 1u64 << (idx % 64);
        }
        self.count += 1;
    }

    pub fn contains(&self, value: &str) -> bool {
        for row in 0..self.depth {
            let idx = self.bit_index(row, value);
            if (self.bits[idx / 64] >> (idx % 64)) & 1 == 0 {
                return false;
            }
        }
        true
    }

    pub fn merge(&mut self, other: &Self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.width == other.width,
            "cannot merge bloom filters with different width ({} vs {})",
            self.width,
            other.width
        );
        anyhow::ensure!(
            self.depth == other.depth,
            "cannot merge bloom filters with different depth ({} vs {})",
            self.depth,
            other.depth
        );
        for i in 0..self.bits.len() {
            self.bits[i] |= other.bits[i];
        }
        self.count = 0;
        Ok(())
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn estimated_count(&self) -> u64 {
        let set_bits: u64 = self.bits.iter().map(|w| w.count_ones() as u64).sum();
        if set_bits == 0 {
            return 0;
        }
        if set_bits >= self.width as u64 {
            return u64::MAX;
        }

        let fill_ratio = set_bits as f64 / self.width as f64;
        let n = -(self.width as f64 / self.depth as f64) * (1.0 - fill_ratio).ln();
        n as u64
    }

    pub fn width(&self) -> usize {
        self.width
    }

    pub fn depth(&self) -> usize {
        self.depth
    }

    pub fn estimated_fp_rate(&self) -> f64 {
        let set_bits: u64 = self.bits.iter().map(|w| w.count_ones() as u64).sum();
        let ratio = set_bits as f64 / self.width as f64;
        ratio.powi(self.depth as i32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contains_inserted() {
        let mut bf = BloomFilter::new(10000, 7).unwrap();
        bf.insert("hello");
        bf.insert("world");
        assert!(bf.contains("hello"));
        assert!(bf.contains("world"));
    }

    #[test]
    fn test_no_false_negatives() {
        let mut bf = BloomFilter::new(10000, 7).unwrap();
        for i in 0..1000 {
            bf.insert(&format!("key-{i}"));
        }
        for i in 0..1000 {
            assert!(
                bf.contains(&format!("key-{i}")),
                "false negative for key-{i}"
            );
        }
    }

    #[test]
    fn test_false_positive_rate() {
        let mut bf = BloomFilter::new(100000, 7).unwrap();
        for i in 0..10000 {
            bf.insert(&format!("key-{i}"));
        }
        let mut false_positives = 0;
        for i in 0..10000 {
            if bf.contains(&format!("nonexistent-{i}")) {
                false_positives += 1;
            }
        }
        let fp_rate = false_positives as f64 / 10000.0;
        assert!(fp_rate < 0.05, "FP rate {fp_rate} too high");
    }

    #[test]
    fn test_merge() {
        let mut bf1 = BloomFilter::new(10000, 7).unwrap();
        let mut bf2 = BloomFilter::new(10000, 7).unwrap();
        for i in 0..500 {
            bf1.insert(&format!("key-{i}"));
        }
        for i in 500..1000 {
            bf2.insert(&format!("key-{i}"));
        }
        bf1.merge(&bf2).unwrap();
        for i in 0..1000 {
            assert!(
                bf1.contains(&format!("key-{i}")),
                "false negative after merge for key-{i}"
            );
        }
    }

    #[test]
    fn test_merge_preserves_original() {
        let mut bf1 = BloomFilter::new(10000, 7).unwrap();
        let mut bf2 = BloomFilter::new(10000, 7).unwrap();
        bf1.insert("only-in-first");
        bf2.insert("only-in-second");
        bf1.merge(&bf2).unwrap();
        assert!(bf1.contains("only-in-first"));
        assert!(bf1.contains("only-in-second"));
    }

    #[test]
    fn test_merge_different_params() {
        let mut bf1 = BloomFilter::new(10000, 7).unwrap();
        let bf2 = BloomFilter::new(5000, 7).unwrap();
        assert!(bf1.merge(&bf2).is_err());
    }

    #[test]
    fn test_estimated_count() {
        let mut bf = BloomFilter::new(100000, 7).unwrap();
        for i in 0..10000 {
            bf.insert(&format!("key-{i}"));
        }
        let estimate = bf.estimated_count();
        let err = ((estimate as f64 - 10000.0) / 10000.0).abs();
        assert!(err < 0.05, "estimate {estimate}, error {err}");
    }

    #[test]
    fn test_invalid_params() {
        assert!(BloomFilter::new(0, 7).is_err());
        assert!(BloomFilter::new(10000, 0).is_err());
    }
}
