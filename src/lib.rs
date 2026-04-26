pub mod bloom;
pub mod count_min;
pub mod ddsketch;
pub mod hash;
pub mod kmv;
pub mod reservoir;

use anyhow::Context;
use wincode::{SchemaRead, SchemaWrite};

#[derive(SchemaWrite, SchemaRead)]
pub enum SketchData {
    CountDistinctKmv(kmv::KmvSketch),
    FrequencyCountMin(count_min::CountMinSketch),
    TopKCountMin(count_min::TopKSketch),
    QuantilesDDSketch(ddsketch::DDSketch),
    MembershipBloom(bloom::BloomFilter),
}

impl SketchData {
    pub fn insert(&mut self, value: &str) {
        match self {
            Self::CountDistinctKmv(kmv) => kmv.insert(value),
            Self::FrequencyCountMin(cm) => {
                cm.insert(value);
            }
            Self::TopKCountMin(topk) => topk.insert(value),
            Self::MembershipBloom(bf) => bf.insert(value),
            Self::QuantilesDDSketch(dds) => {
                if let Ok(v) = value.parse::<f64>()
                    && v.is_finite()
                {
                    dds.insert(v);
                }
            }
        }
    }

    pub fn merge(&mut self, other: &Self) -> anyhow::Result<()> {
        match (self, other) {
            (Self::CountDistinctKmv(a), Self::CountDistinctKmv(b)) => a.merge(b),
            (Self::FrequencyCountMin(a), Self::FrequencyCountMin(b)) => a.merge(b),
            (Self::TopKCountMin(a), Self::TopKCountMin(b)) => a.merge(b),
            (Self::QuantilesDDSketch(a), Self::QuantilesDDSketch(b)) => a.merge(b),
            (Self::MembershipBloom(a), Self::MembershipBloom(b)) => a.merge(b),
            _ => anyhow::bail!("cannot merge different sketch types"),
        }
    }
}

pub fn load_sketch(path: &str) -> anyhow::Result<SketchData> {
    let data = std::fs::read(path).with_context(|| format!("error reading {path}"))?;
    let config = wincode::config::Configuration::default().disable_preallocation_size_limit();
    wincode::config::deserialize(&data, config).with_context(|| format!("error parsing {path}"))
}

pub fn save_sketch(sketch: &SketchData, path: &str) -> anyhow::Result<()> {
    let config = wincode::config::Configuration::default().disable_preallocation_size_limit();
    let data = wincode::config::serialize(sketch, config).context("error serializing sketch")?;
    std::fs::write(path, data).with_context(|| format!("error writing {path}"))
}

pub fn query_result(sketch: &SketchData) -> serde_json::Value {
    match sketch {
        SketchData::CountDistinctKmv(kmv) => serde_json::json!({
            "estimated_count": kmv.estimate(),
            "k": kmv.k(),
            "relative_error_pct": 100.0 / (kmv.k() as f64).sqrt(),
        }),
        SketchData::FrequencyCountMin(cm) => serde_json::json!({
            "width": cm.width(),
            "depth": cm.depth(),
        }),
        SketchData::TopKCountMin(topk) => {
            let items: Vec<_> = topk
                .top_k()
                .into_iter()
                .map(|(v, c)| serde_json::json!({"value": v, "estimated_count": c}))
                .collect();
            serde_json::json!({
                "k": topk.k(),
                "items": items,
            })
        }
        SketchData::QuantilesDDSketch(dds) => serde_json::json!({
            "count": dds.count(),
            "relative_error": dds.relative_error(),
        }),
        SketchData::MembershipBloom(bf) => serde_json::json!({
            "item_count": bf.count(),
            "estimated_count": bf.estimated_count(),
            "width": bf.width(),
            "depth": bf.depth(),
            "estimated_fp_rate_pct": bf.estimated_fp_rate() * 100.0,
        }),
    }
}

pub fn query_quantiles(
    sketch: &SketchData,
    percentiles: &[f64],
) -> anyhow::Result<serde_json::Value> {
    match sketch {
        SketchData::QuantilesDDSketch(dds) => {
            let mut quantiles = serde_json::Map::new();
            for &p in percentiles {
                let key = format!("p{}", (p * 100.0) as u64);
                quantiles.insert(key, serde_json::json!(dds.quantile(p)));
            }
            Ok(serde_json::json!({
                "count": dds.count(),
                "relative_error": dds.relative_error(),
                "quantiles": quantiles,
            }))
        }
        _ => anyhow::bail!("sketch does not support quantile queries"),
    }
}

pub fn query_membership(sketch: &SketchData, value: &str) -> anyhow::Result<serde_json::Value> {
    match sketch {
        SketchData::MembershipBloom(bf) => Ok(serde_json::json!({
            "value": value,
            "likely_present": bf.contains(value),
            "estimated_fp_rate_pct": bf.estimated_fp_rate() * 100.0,
        })),
        SketchData::FrequencyCountMin(cm) => Ok(serde_json::json!({
            "value": value,
            "estimated_count": cm.estimate(value),
        })),
        _ => anyhow::bail!("sketch does not support value queries"),
    }
}

pub fn query_intersection(a: &SketchData, b: &SketchData) -> anyhow::Result<serde_json::Value> {
    match (a, b) {
        (SketchData::CountDistinctKmv(s1), SketchData::CountDistinctKmv(s2)) => {
            let est_a = s1.estimate();
            let est_b = s2.estimate();
            let est_intersection = s1.estimate_intersection(s2)?;
            let est_union = s1.estimate_union(s2)?;
            let jaccard = if est_union > 0 {
                est_intersection as f64 / est_union as f64
            } else {
                0.0
            };
            Ok(serde_json::json!({
                "estimated_intersection": est_intersection,
                "estimated_union": est_union,
                "jaccard": jaccard,
                "estimated_a": est_a,
                "estimated_b": est_b,
            }))
        }
        _ => anyhow::bail!("intersection requires two count-distinct sketches"),
    }
}
