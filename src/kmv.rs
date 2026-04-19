//! K-Minimum Values (KMV) sketch — a cardinality estimation data structure
//! that approximates the number of distinct elements in a stream by tracking
//! the k smallest hash values observed.

use std::collections::BTreeSet;
use wincode::{SchemaRead, SchemaWrite};

use crate::hash::hashfn_default;

#[derive(Debug, Clone, SchemaWrite, SchemaRead)]
pub struct KmvSketch {
    k: usize,
    min_hashes: BTreeSet<u64>,
}

impl KmvSketch {
    pub fn new(k: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(
            k >= 2,
            "KMV sketch requires k >= 2 for the unbiased estimator to work"
        );
        Ok(Self {
            k,
            min_hashes: BTreeSet::new(),
        })
    }

    /// Internal helper to maintain bounded capacity dynamically
    fn insert_hash(&mut self, h: u64) {
        if self.min_hashes.len() < self.k {
            self.min_hashes.insert(h);
        } else if let Some(&max) = self.min_hashes.last() {
            if h < max {
                self.min_hashes.insert(h);
                // BTreeSet doesn't grow if `h` was a duplicate, so only pop
                // if we actually exceeded our `k` limit.
                if self.min_hashes.len() > self.k {
                    let _ = self.min_hashes.pop_last();
                }
            }
        }
    }

    pub fn insert(&mut self, value: &str) {
        self.insert_hash(hashfn_default(value.as_bytes()));
    }

    pub fn estimate(&self) -> u64 {
        if self.min_hashes.is_empty() {
            return 0;
        }
        if self.min_hashes.len() < self.k {
            return self.min_hashes.len() as u64;
        }

        let v_k = *self.min_hashes.last().unwrap() as u128;
        if v_k == 0 {
            return u64::MAX;
        }

        let k = (self.k - 1) as u128;
        let space = (u64::MAX as u128) + 1;
        ((k * space) / v_k) as u64
    }

    pub fn k(&self) -> usize {
        self.k
    }

    pub fn hashes(&self) -> &BTreeSet<u64> {
        &self.min_hashes
    }

    fn merged_hashes(&self, other: &Self) -> BTreeSet<u64> {
        let mut merged: BTreeSet<u64> = self.min_hashes.clone();
        for &h in &other.min_hashes {
            merged.insert(h);
        }
        while merged.len() > self.k {
            let _ = merged.pop_last();
        }
        merged
    }

    pub fn estimate_union(&self, other: &Self) -> anyhow::Result<u64> {
        anyhow::ensure!(
            self.k == other.k,
            "cannot compute union of KMV sketches with different k ({} vs {})",
            self.k,
            other.k
        );
        if self.min_hashes.is_empty() && other.min_hashes.is_empty() {
            return Ok(0);
        }
        let merged = self.merged_hashes(other);
        if merged.len() < self.k {
            return Ok(merged.len() as u64);
        }
        let v_k = *merged.last().unwrap() as u128;
        if v_k == 0 {
            return Ok(u64::MAX);
        }
        let space = (u64::MAX as u128) + 1;
        Ok(((self.k as u128 - 1) * space / v_k) as u64)
    }

    pub fn estimate_intersection(&self, other: &Self) -> anyhow::Result<u64> {
        anyhow::ensure!(
            self.k == other.k,
            "cannot intersect KMV sketches with different k ({} vs {})",
            self.k,
            other.k
        );

        if self.min_hashes.is_empty() || other.min_hashes.is_empty() {
            return Ok(0);
        }

        let merged = self.merged_hashes(other);

        // If the total combined unique elements are less than k,
        // we can just calculate the exact intersection size.
        if merged.len() < self.k {
            let exact = self.min_hashes.intersection(&other.min_hashes).count();
            return Ok(exact as u64);
        }

        // Count how many of the top-k union hashes are present in both original sets
        let l = merged
            .iter()
            .filter(|h| self.min_hashes.contains(h) && other.min_hashes.contains(h))
            .count();

        let v_k = *merged.last().unwrap() as u128;
        if v_k == 0 {
            return Ok(0);
        }

        let space = (u64::MAX as u128) + 1;
        let union_est = ((self.k as u128 - 1) * space / v_k) as u64;

        let j_hat = l as f64 / self.k as f64;
        let result = (j_hat * union_est as f64) as u64;

        Ok(result)
    }

    pub fn merge(&mut self, other: &Self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.k == other.k,
            "cannot merge KMV sketches with different k ({} vs {})",
            self.k,
            other.k
        );
        for &h in &other.min_hashes {
            self.insert_hash(h);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_when_below_k() {
        let mut s = KmvSketch::new(100).unwrap();
        for i in 0..10 {
            s.insert(&format!("item-{i}"));
        }
        assert_eq!(s.estimate(), 10);
    }

    #[test]
    fn test_approximate_count() {
        let mut s = KmvSketch::new(1024).unwrap();
        let n = 10_000;
        for i in 0..n {
            s.insert(&format!("item-{i}"));
        }
        let est = s.estimate();
        let err = ((est as f64 - n as f64) / n as f64).abs();
        assert!(err < 0.1, "estimate {est}, error {err}");
    }

    #[test]
    fn test_duplicates_ignored() {
        let mut s = KmvSketch::new(1024).unwrap();
        for _ in 0..1000 {
            s.insert("hello");
        }
        for _ in 0..1000 {
            s.insert("world");
        }
        assert_eq!(s.estimate(), 2);
    }

    #[test]
    fn test_merge() {
        let mut s1 = KmvSketch::new(1024).unwrap();
        let mut s2 = KmvSketch::new(1024).unwrap();
        for i in 0..5000 {
            s1.insert(&format!("item-{i}"));
        }
        for i in 5000..10000 {
            s2.insert(&format!("item-{i}"));
        }
        s1.merge(&s2).unwrap();
        let merged = s1.estimate();
        let err = ((merged as f64 - 10000.0) / 10000.0).abs();
        assert!(err < 0.15, "merged estimate {merged}, error {err}");
    }

    #[test]
    fn test_intersection_disjoint() {
        let mut s1 = KmvSketch::new(1024).unwrap();
        let mut s2 = KmvSketch::new(1024).unwrap();
        for i in 0..5000 {
            s1.insert(&format!("set-a-{i}"));
        }
        for i in 0..5000 {
            s2.insert(&format!("set-b-{i}"));
        }
        let inter = s1.estimate_intersection(&s2).unwrap();
        assert!(
            inter < 200,
            "disjoint sets should have near-zero intersection, got {inter}"
        );
    }

    #[test]
    fn test_intersection_full_overlap() {
        let mut s1 = KmvSketch::new(1024).unwrap();
        let mut s2 = KmvSketch::new(1024).unwrap();
        for i in 0..10000 {
            s1.insert(&format!("item-{i}"));
            s2.insert(&format!("item-{i}"));
        }
        let inter = s1.estimate_intersection(&s2).unwrap();
        let err = ((inter as f64 - 10000.0) / 10000.0).abs();
        assert!(err < 0.2, "full overlap estimate {inter}, error {err}");
    }

    #[test]
    fn test_intersection_partial_overlap() {
        let mut s1 = KmvSketch::new(1024).unwrap();
        let mut s2 = KmvSketch::new(1024).unwrap();
        for i in 0..5000 {
            s1.insert(&format!("item-{i}"));
        }
        for i in 2500..7500 {
            s2.insert(&format!("item-{i}"));
        }
        let inter = s1.estimate_intersection(&s2).unwrap();
        let err = ((inter as f64 - 2500.0) / 2500.0).abs();
        assert!(
            err < 0.3,
            "partial overlap estimate {inter}, expected ~2500, error {err}"
        );
    }

    #[test]
    fn test_invalid_k() {
        assert!(KmvSketch::new(1).is_err());
    }
}
