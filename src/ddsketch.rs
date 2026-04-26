//! DDSketch — a distributed quantile sketch data structure that provides
//! approximate quantile queries with guaranteed relative error bounds. It is
//! fully mergeable, making it suitable for distributed aggregation.

use std::collections::BTreeMap;
use wincode::{SchemaRead, SchemaWrite};

#[derive(Debug, Clone, SchemaWrite, SchemaRead)]
pub struct DDSketch {
    gamma: f64,
    ln_gamma: f64,
    positive_buckets: BTreeMap<i64, u64>,
    negative_buckets: BTreeMap<i64, u64>,
    count: u64,
    zero_count: u64,
}

impl DDSketch {
    pub fn new(relative_error: f64) -> anyhow::Result<Self> {
        anyhow::ensure!(
            relative_error.is_finite() && relative_error > 0.0 && relative_error < 1.0,
            "relative error must be between 0 and 1"
        );
        let gamma = (1.0 + relative_error) / (1.0 - relative_error);
        Ok(Self {
            gamma,
            ln_gamma: gamma.ln(),
            positive_buckets: BTreeMap::new(),
            negative_buckets: BTreeMap::new(),
            count: 0,
            zero_count: 0,
        })
    }

    pub fn insert(&mut self, value: f64) {
        if value == 0.0 {
            self.zero_count += 1;
        } else if value > 0.0 {
            let bucket = (value.ln() / self.ln_gamma).floor() as i64;
            *self.positive_buckets.entry(bucket).or_insert(0) += 1;
        } else {
            let bucket = ((-value).ln() / self.ln_gamma).floor() as i64;
            *self.negative_buckets.entry(bucket).or_insert(0) += 1;
        }
        self.count += 1;
    }

    pub fn quantile(&self, q: f64) -> f64 {
        assert!((0.0..=1.0).contains(&q), "quantile must be in [0, 1]");
        if self.count == 0 {
            return 0.0;
        }

        let target = ((q * self.count as f64).ceil() as u64).max(1);
        let mut accumulated = 0;

        // 1. Iterate negative buckets from largest magnitude (most negative) to smallest
        for (&bucket, &count) in self.negative_buckets.iter().rev() {
            accumulated += count;
            if accumulated >= target {
                return -self.bucket_value(bucket);
            }
        }

        // 2. Iterate zeros
        accumulated += self.zero_count;
        if accumulated >= target {
            return 0.0;
        }

        // 3. Iterate positive buckets from smallest to largest
        for (&bucket, &count) in &self.positive_buckets {
            accumulated += count;
            if accumulated >= target {
                return self.bucket_value(bucket);
            }
        }

        // Fallback for floating point / rounding weirdness
        if let Some((&bucket, _)) = self.positive_buckets.last_key_value() {
            self.bucket_value(bucket)
        } else {
            0.0
        }
    }

    /// Returns the statistically optimal value for a bucket that guarantees the relative error bound
    fn bucket_value(&self, bucket: i64) -> f64 {
        let lower_bound = (self.ln_gamma * bucket as f64).exp();
        lower_bound * 2.0 * self.gamma / (self.gamma + 1.0)
    }

    pub fn merge(&mut self, other: &Self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (self.gamma - other.gamma).abs() < 1e-10,
            "cannot merge sketches with different gamma ({} vs {})",
            self.gamma,
            other.gamma
        );

        for (&bucket, &count) in &other.positive_buckets {
            *self.positive_buckets.entry(bucket).or_insert(0) += count;
        }
        for (&bucket, &count) in &other.negative_buckets {
            *self.negative_buckets.entry(bucket).or_insert(0) += count;
        }
        self.count += other.count;
        self.zero_count += other.zero_count;
        Ok(())
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn gamma(&self) -> f64 {
        self.gamma
    }

    pub fn relative_error(&self) -> f64 {
        (self.gamma - 1.0) / (self.gamma + 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_small() {
        let mut sk = DDSketch::new(0.01).unwrap();
        for v in [1.0, 2.0, 3.0, 4.0, 5.0] {
            sk.insert(v);
        }
        assert!((sk.quantile(0.5) - 3.0).abs() < 0.5);
        assert!((sk.quantile(0.8) - 4.0).abs() < 1.0);
    }

    #[test]
    fn test_uniform_distribution() {
        let mut sk = DDSketch::new(0.01).unwrap();
        for i in 1..=10000 {
            sk.insert(i as f64);
        }
        let p50 = sk.quantile(0.5);
        assert!((p50 - 5000.0).abs() / 5000.0 < 0.05, "p50={p50}");

        let p99 = sk.quantile(0.99);
        assert!((p99 - 9900.0).abs() / 9900.0 < 0.05, "p99={p99}");
    }

    #[test]
    fn test_negative_distribution() {
        let mut sk = DDSketch::new(0.01).unwrap();
        for i in 1..=1000 {
            sk.insert(-i as f64);
        }
        let p50 = sk.quantile(0.5);
        // p50 should be around -500.0
        assert!((p50 - (-500.0)).abs() / 500.0 < 0.05, "p50={p50}");
    }

    #[test]
    fn test_merge() {
        let mut s1 = DDSketch::new(0.01).unwrap();
        let mut s2 = DDSketch::new(0.01).unwrap();
        for i in 1..=5000 {
            s1.insert(i as f64);
        }
        for i in 5001..=10000 {
            s2.insert(i as f64);
        }
        s1.merge(&s2).unwrap();
        assert_eq!(s1.count(), 10000);
        let p50 = s1.quantile(0.5);
        assert!((p50 - 5000.0).abs() / 5000.0 < 0.05, "merged p50={p50}");
    }

    #[test]
    fn test_zero_handling() {
        let mut sk = DDSketch::new(0.01).unwrap();
        sk.insert(0.0);
        sk.insert(0.0);
        sk.insert(1.0);
        sk.insert(2.0);
        assert_eq!(sk.quantile(0.0), 0.0);
        assert_eq!(sk.quantile(0.5), 0.0);
        assert!(sk.quantile(0.75) > 0.0);
    }
}
