use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::process;

use anyhow::Context;
use clap::{Parser, Subcommand};
use sketch_toolkit::{self as sk, SketchData};

#[derive(Parser)]
#[command(name = "sketch", about = "data sketch toolkit")]
struct Cli {
    #[arg(long, global = true)]
    pretty: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    CountDistinct {
        #[arg(short, long, default_value_t = 16384)]
        k: usize,
        #[arg(short, long)]
        output: Option<String>,
    },
    TopK {
        #[arg(short, long, default_value_t = 10)]
        k: usize,
        #[arg(long, default_value_t = 2000)]
        width: usize,
        #[arg(long, default_value_t = 7)]
        depth: usize,
        #[arg(short, long)]
        output: Option<String>,
    },
    Frequency {
        #[arg(long, default_value_t = 2000)]
        width: usize,
        #[arg(long, default_value_t = 7)]
        depth: usize,
        #[arg(short, long)]
        output: Option<String>,
    },
    Quantiles {
        #[arg(short, long, value_delimiter = ',', default_values_t = vec![0.0, 50.0, 90.0, 95.0, 99.0, 100.0])]
        percentiles: Vec<f64>,
        #[arg(short, long, default_value_t = 0.01)]
        error: f64,
        #[arg(short, long)]
        output: Option<String>,
    },
    Membership {
        #[arg(long, default_value_t = 440_000_000)]
        width: usize,
        #[arg(long, default_value_t = 7)]
        depth: usize,
        #[arg(short, long)]
        output: Option<String>,
    },
    Sample {
        #[arg(short, long, default_value_t = 100)]
        k: usize,
        #[arg(short, long)]
        output: Option<String>,
    },
    Query {
        file: String,
        #[arg(short, long, value_delimiter = ',')]
        percentiles: Vec<f64>,
        #[arg(long)]
        value: Option<String>,
        #[arg(long)]
        intersect_with: Option<String>,
    },
    Merge {
        files: Vec<String>,
        #[arg(short, long)]
        output: Option<String>,
    },
}

fn with_stdin_lines(mut f: impl FnMut(&str)) {
    let stdin = io::stdin();
    let mut reader = BufReader::with_capacity(256 * 1024, stdin.lock());
    let mut buf = String::new();
    while reader.read_line(&mut buf).unwrap_or(0) > 0 {
        let line = buf.trim_end_matches('\n').trim_end_matches('\r');
        if !line.is_empty() {
            f(line);
        }
        buf.clear();
    }
}

fn print_json(pretty: bool, value: &serde_json::Value) {
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    if pretty {
        serde_json::to_writer_pretty(&mut out, value).unwrap();
    } else {
        serde_json::to_writer(&mut out, value).unwrap();
    }
    out.write_all(b"\n").unwrap();
}

fn parse_percentiles(raw: &[f64]) -> Vec<f64> {
    raw.iter()
        .map(|&p| {
            let q = if p > 1.0 { p / 100.0 } else { p };
            assert!(
                (0.0..=100.0).contains(&q),
                "percentile {p} out of range [0, 100]"
            );
            q
        })
        .collect()
}

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e:#}");
        process::exit(1);
    }
}

fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let pretty = cli.pretty;

    match cli.command {
        Commands::CountDistinct { k, output } => {
            let mut sketch = sk::kmv::KmvSketch::new(k)?;
            with_stdin_lines(|line| sketch.insert(line));
            let data = SketchData::CountDistinctKmv(sketch);
            if let Some(path) = output {
                sk::save_sketch(&data, &path)?;
            } else {
                print_json(pretty, &sk::query_result(&data));
            }
        }
        Commands::TopK {
            k,
            width,
            depth,
            output,
        } => {
            let mut sketch = sk::count_min::TopKSketch::new(k, width, depth)?;
            with_stdin_lines(|line| sketch.insert(line));
            let data = SketchData::TopKCountMin(sketch);
            if let Some(path) = output {
                sk::save_sketch(&data, &path)?;
            } else {
                print_json(pretty, &sk::query_result(&data));
            }
        }
        Commands::Quantiles {
            percentiles,
            error,
            output,
        } => {
            let mut sketch = sk::ddsketch::DDSketch::new(error)?;
            with_stdin_lines(|line| {
                if let Ok(v) = line.parse::<f64>() {
                    sketch.insert(v);
                }
            });
            let data = SketchData::QuantilesDDSketch(sketch);
            if let Some(path) = output {
                sk::save_sketch(&data, &path)?;
            } else {
                let ps = parse_percentiles(&percentiles);
                print_json(pretty, &sk::query_quantiles(&data, &ps)?);
            }
        }
        Commands::Membership {
            width,
            depth,
            output,
        } => {
            let mut sketch = sk::bloom::BloomFilter::new(width, depth);
            with_stdin_lines(|line| sketch.insert(line));
            let data = SketchData::MembershipBloom(sketch);
            if let Some(path) = output {
                sk::save_sketch(&data, &path)?;
            } else {
                print_json(pretty, &sk::query_result(&data));
            }
        }
        Commands::Frequency {
            width,
            depth,
            output,
        } => {
            let mut sketch = sk::count_min::CountMinSketch::new(width, depth);
            with_stdin_lines(|line| {
                sketch.insert(line);
            });
            let data = SketchData::FrequencyCountMin(sketch);
            if let Some(path) = output {
                sk::save_sketch(&data, &path)?;
            } else {
                print_json(pretty, &sk::query_result(&data));
            }
        }
        Commands::Sample { k, output } => {
            let mut sampler = sk::reservoir::ReservoirSample::new(k)?;
            with_stdin_lines(|line| sampler.insert(line));
            let items = sampler.sample();
            if let Some(path) = output {
                let mut out = BufWriter::new(
                    std::fs::File::create(&path)
                        .with_context(|| format!("error creating {path}"))?,
                );
                for item in items {
                    out.write_all(item.as_bytes())?;
                    out.write_all(b"\n")?;
                }
            } else {
                let stdout = io::stdout();
                let mut out = BufWriter::new(stdout.lock());
                for item in items {
                    out.write_all(item.as_bytes())?;
                    out.write_all(b"\n")?;
                }
            }
        }
        Commands::Query {
            file,
            percentiles,
            value,
            intersect_with,
        } => {
            let data = sk::load_sketch(&file)?;
            if let Some(other_path) = intersect_with {
                let other = sk::load_sketch(&other_path)?;
                print_json(pretty, &sk::query_intersection(&data, &other)?);
            } else if let Some(v) = value {
                print_json(pretty, &sk::query_membership(&data, &v)?);
            } else if !percentiles.is_empty() {
                let ps = parse_percentiles(&percentiles);
                print_json(pretty, &sk::query_quantiles(&data, &ps)?);
            } else {
                print_json(pretty, &sk::query_result(&data));
            }
        }
        Commands::Merge { files, output } => {
            anyhow::ensure!(files.len() >= 2, "need at least 2 files to merge");
            let mut merged = sk::load_sketch(&files[0])?;
            for path in &files[1..] {
                let other = sk::load_sketch(path)?;
                merged.merge(&other)?;
            }
            if let Some(path) = output {
                sk::save_sketch(&merged, &path)?;
            } else {
                print_json(pretty, &sk::query_result(&merged));
            }
        }
    }

    Ok(())
}
