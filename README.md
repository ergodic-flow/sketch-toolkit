# Sketch Toolkit

A lightweight CLI toolkit for summarizing and estimating high-cardinality data streams.
It reads newline-delimited data from stdin, computes space-efficient sketches, and outputs JSON to stdout.

## Why Do I Want This?

You have a large dataset for which you want to answer the following questions:

1) How many distinct items exist?
2) What are the Top K occuring items?
3) What is the frequency of occurence of a given item?
4) What are the quantiles of my dataset?
5) For a given item, does it exist in the dataset?

Each of these questions has an approximate answer that can be computed
in linear time and sub-linear space. As a bonus, sketches are mergeable, meaning
you can compute Sketch `S` on stream `A`, Sketch `T` on stream `B`, and compute
answers from `S + T`!

## Install

```bash
cargo build --release
```

The binary is at `./target/release/sketch-toolkit`.

## Sketches

The following data sketches are implemented and supported.

| Sketch | Subcommand | Algorithm | What it does | Mergeable |
|--------|------------|-----------|--------------|----------|
| Count Distinct | `count-distinct` | KMV | Estimate number of unique items | Yes      |
| Top-K | `top-k` | Count-Min | Find the top most frequent items | No       |
| Frequency | `frequency` | Count-Min | Estimate the frequency of each item | Yes      |
| Quantiles | `quantiles` | DDSketch | Estimate value at a given percentile | Yes      | 
| Membership | `membership` | Bloom | Probabilistic set membership test | Yes      |
| Sample | `sample` | Reservoir | Draw a uniform random sample from a stream | No       |

## Options

### `count-distinct`

| Flag | Default | Description |
|---|---|---|
| `-k` | 1024 | Number of hashes to track. Higher = more accurate, more memory. |
| `-o` | — | Save sketch to file instead of printing results. |

### `top-k`

| Flag | Default | Description |
|---|---|---|
| `-k` | 10 | Number of top items to return. |
| `--width` | 2000 | Count-Min table width. Higher = more accurate. |
| `--depth` | 7 | Count-Min table depth. Higher = more accurate. |
| `-o` | — | Save sketch to file instead of printing results. |

### `quantiles`

| Flag | Default | Description |
|---|---|---|
| `-p` | 50,90,95,99 | Percentiles to compute (comma-separated). |
| `--error` | 0.01 | Relative error bound. Lower = more accurate, more buckets. |
| `-o` | — | Save sketch to file instead of printing results. |

### `membership`

| Flag | Default | Description |
|---|---|---|
| `--width` | 440000000 | Number of bits in the filter. Higher = lower FP rate. |
| `--depth` | 7 | Number of hash functions. Higher = lower FP rate (up to a point). |
| `-o` | — | Save filter to file instead of printing results. |

Membership sketches can be merged with the `merge` command (bitwise OR of the arrays).

### `query`

| Flag | Default | Description |
|---|---|---|
| `-p` | — | Override percentiles (quantile sketches only). |
| `--value` | — | Check membership (membership sketch only). |
| `--intersect-with` | — | Estimate intersection with another count-distinct sketch. |

### `merge`

Takes 2+ sketch files. All sketches must be the same type with matching parameters.

| Flag | Default | Description |
|---|---|---|
| `-o` | — | Save merged sketch to file instead of printing results. |

### `sample`

Reads a stream from stdin and outputs a uniform random sample using Reservoir sampling.
Each line of the input stream is considered one item; the sampled lines are written to stdout (or a file).

| Flag | Default | Description |
|---|---|---|
| `-k` | 100 | Number of items to sample. |
| `-o` | — | Save sampled lines to file instead of printing to stdout. |
