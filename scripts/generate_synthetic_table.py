#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dlfx.io import write_table
from dlfx.synthetic import SyntheticConfig, make_synthetic_analysis_table


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate a synthetic analysis table for smoke testing.")
    p.add_argument("--out", required=True, help="Output path (.csv or .parquet).")
    p.add_argument("--n", type=int, default=2000, help="Number of synthetic rows.")
    p.add_argument("--seed", type=int, default=11, help="Random seed.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    df = make_synthetic_analysis_table(SyntheticConfig(n=args.n, seed=args.seed))
    write_table(df, args.out)
    print(f"Wrote synthetic table to: {args.out}")


if __name__ == "__main__":
    main()

