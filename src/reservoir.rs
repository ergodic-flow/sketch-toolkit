use rand::Rng;

#[derive(Debug, Clone)]
pub struct ReservoirSample {
    k: usize,
    reservoir: Vec<String>,
    seen: u64,
}

impl ReservoirSample {
    pub fn new(k: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(k >= 1, "reservoir size must be >= 1");
        Ok(Self {
            k,
            reservoir: Vec::with_capacity(k),
            seen: 0,
        })
    }

    pub fn insert(&mut self, value: &str) {
        self.seen += 1;
        if (self.seen as usize) <= self.k {
            self.reservoir.push(value.to_string());
        } else {
            let j = rand::rng().random_range(0..self.seen) as usize;
            if j < self.k {
                self.reservoir[j] = value.to_string();
            }
        }
    }

    pub fn k(&self) -> usize {
        self.k
    }

    pub fn seen(&self) -> u64 {
        self.seen
    }

    pub fn sample(&self) -> &[String] {
        &self.reservoir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_stream() {
        let mut r = ReservoirSample::new(5).unwrap();
        for i in 0..3 {
            r.insert(&format!("item-{i}"));
        }
        assert_eq!(r.sample().len(), 3);
        assert_eq!(r.seen(), 3);
    }

    #[test]
    fn test_larger_stream() {
        let mut r = ReservoirSample::new(100).unwrap();
        for i in 0..10_000 {
            r.insert(&format!("item-{i}"));
        }
        assert_eq!(r.sample().len(), 100);
        assert_eq!(r.seen(), 10_000);
    }

    #[test]
    fn test_k1() {
        let mut r = ReservoirSample::new(1).unwrap();
        for i in 0..1000 {
            r.insert(&format!("item-{i}"));
        }
        assert_eq!(r.sample().len(), 1);
        assert_eq!(r.seen(), 1000);
    }

    #[test]
    fn test_invalid_k() {
        assert!(ReservoirSample::new(0).is_err());
    }
}
