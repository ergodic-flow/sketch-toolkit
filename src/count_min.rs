//! Count-Min Sketch — a sub-linear space probabilistic data structure that
//! provides approximate frequency counts for items in a data stream. Counts
//! may be over-estimated due to hash collisions, but never under-estimated.

use std::collections::HashMap;
use wincode::{SchemaRead, SchemaWrite};

use crate::hash::{hashfn, row_seed};

/// A Count-Min Sketch probabilistic data structure.
///
/// It provides approximate frequency counts for a stream of items using
/// sub-linear space. It may over-estimate counts due to collisions,
/// but it will never under-estimate.
#[derive(Debug, Clone, SchemaWrite, SchemaRead)]
pub struct CountMinSketch {
    /// Number of columns in the 2D bank of counters.
    width: usize,
    /// Number of rows (independent hash functions) in the bank of counters.
    depth: usize,
    /// The actual counter storage, flattened into a single contiguous block.
    counters: Vec<u64>,
}

impl CountMinSketch {
    /// Creates a new `CountMinSketch` with the specified dimensions.
    ///
    /// Larger `width` reduces the probability of collisions (error magnitude).
    /// Larger `depth` reduces the probability of a "bad" estimate (error frequency).
    pub fn new(width: usize, depth: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(width > 0, "Count-Min width must be >= 1");
        anyhow::ensure!(depth > 0, "Count-Min depth must be >= 1");
        let size = width
            .checked_mul(depth)
            .ok_or_else(|| anyhow::anyhow!("Count-Min dimensions are too large"))?;

        Ok(Self {
            width,
            depth,
            counters: vec![0; size],
        })
    }

    /// Maps a value to a specific column for a given row.
    ///
    /// Uses a seeded FNV hash where the seed is derived from the row index
    /// and a large prime to ensure independence between rows.
    fn column(&self, row: usize, value: &str) -> usize {
        let seed = row_seed(row);
        let h = hashfn(value.as_bytes(), seed);
        (h % self.width as u64) as usize
    }

    /// Inserts the value into the sketch and returns the newly estimated count.
    ///
    /// Every row's corresponding counter is incremented. The estimate returned
    /// is the minimum of all those counters.
    pub fn insert(&mut self, value: &str) -> u64 {
        let mut min_count = u64::MAX;
        for row in 0..self.depth {
            let col = self.column(row, value);
            let idx = row * self.width + col;

            self.counters[idx] += 1;
            min_count = min_count.min(self.counters[idx]);
        }
        min_count
    }

    /// Returns the approximate frequency of the given value.
    ///
    /// The result is the minimum value across all rows for the item's hashed columns.
    pub fn estimate(&self, value: &str) -> u64 {
        let mut min = u64::MAX;
        for row in 0..self.depth {
            let col = self.column(row, value);
            let idx = row * self.width + col;

            min = min.min(self.counters[idx]);
        }
        min
    }

    pub fn merge(&mut self, other: &Self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.width == other.width,
            "cannot merge count-min sketches with different width ({} vs {})",
            self.width,
            other.width
        );
        anyhow::ensure!(
            self.depth == other.depth,
            "cannot merge count-min sketches with different depth ({} vs {})",
            self.depth,
            other.depth
        );

        // Contiguous memory allows us to optimize the merge using a flat zipped iterator
        for (a, b) in self.counters.iter_mut().zip(other.counters.iter()) {
            *a += b;
        }

        Ok(())
    }

    pub fn width(&self) -> usize {
        self.width
    }

    pub fn depth(&self) -> usize {
        self.depth
    }
}

/// A "Heavy Hitters" tracker that uses a Count-Min Sketch to maintain the top-K items.
///
/// This structure tracks the most frequent items seen so far. It combines the
/// space-efficiency of a sketch with a small map for high-accuracy tracking
/// of high-frequency items.
#[derive(Debug, Clone, SchemaWrite, SchemaRead)]
pub struct TopKSketch {
    /// The underlying frequency estimator.
    cm: CountMinSketch,
    /// The maximum number of items to track in the "heavy hitters" list.
    k: usize,
    /// A bounded map storing at most K elements and their current frequency estimates.
    top_items: HashMap<String, u64>,
}

impl TopKSketch {
    /// Creates a new `TopKSketch`.
    ///
    /// * `k`: The number of frequent items to track.
    /// * `width` / `depth`: Parameters for the underlying `CountMinSketch`.
    pub fn new(k: usize, width: usize, depth: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(k > 0, "TopK requires k >= 1");
        Ok(Self {
            cm: CountMinSketch::new(width, depth)?,
            k,
            top_items: HashMap::with_capacity(k),
        })
    }

    /// Inserts an item into the sketch and updates the Top-K list.
    ///
    /// If the item's new frequency exceeds the frequency of the least-frequent
    /// item currently in the Top-K list, it will be added (and the former minimum removed).
    pub fn insert(&mut self, value: &str) {
        let count = self.cm.insert(value);

        // 1. If we are already tracking this item or have room, store the current estimate.
        if self.top_items.contains_key(value) || self.top_items.len() < self.k {
            self.top_items.insert(value.to_string(), count);
        }
        // 2. Otherwise, check if it deserves to replace the current minimum item.
        else {
            let mut min_key = None;
            let mut min_val = u64::MAX;

            // Find the current "weakest" item in our top-K set.
            for (k, &v) in self.top_items.iter() {
                if v < min_val {
                    min_val = v;
                    min_key = Some(k.clone());
                }
            }

            // If the current item is now more frequent than our weakest heavy hitter, swap them.
            if count > min_val {
                if let Some(key) = min_key {
                    self.top_items.remove(&key);
                }
                self.top_items.insert(value.to_string(), count);
            }
        }
    }

    /// Returns the current Top-K items sorted by estimated frequency (descending).
    pub fn top_k(&self) -> Vec<(String, u64)> {
        let mut estimated: Vec<(String, u64)> = self
            .top_items
            .iter()
            .map(|(k, &v)| (k.clone(), v))
            .collect();
        estimated.sort_by(|a, b| b.1.cmp(&a.1));
        estimated
    }

    pub fn k(&self) -> usize {
        self.k
    }

    pub fn width(&self) -> usize {
        self.cm.width()
    }

    pub fn depth(&self) -> usize {
        self.cm.depth()
    }

    pub fn merge(&mut self, _other: &Self) -> anyhow::Result<()> {
        anyhow::bail!("top-k sketches are not mergeable")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_min_invalid_params() {
        assert!(CountMinSketch::new(0, 7).is_err());
        assert!(CountMinSketch::new(100, 0).is_err());
    }

    #[test]
    fn test_top_k_merge_disabled() {
        let mut a = TopKSketch::new(1, 100, 3).unwrap();
        let b = TopKSketch::new(1, 100, 3).unwrap();
        assert!(a.merge(&b).is_err());
    }
}
