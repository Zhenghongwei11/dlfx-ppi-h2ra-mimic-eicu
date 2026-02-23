#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dlfx.study import load_config, run_study


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run full study: diagnostics + Table 1 + effect plots + audit manifest.")
    p.add_argument("--input", required=True, help="Path to analysis-ready table (.csv or .parquet).")
    p.add_argument("--outdir", required=True, help="Output directory.")
    p.add_argument(
        "--config",
        default=str(ROOT / "configs" / "study_default.yaml"),
        help="Path to YAML config (default: configs/study_default.yaml).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    audit = run_study(input_path=args.input, outdir=args.outdir, config=cfg, repo_root=ROOT)
    print(f"Wrote outputs to: {args.outdir}")
    print(f"Audit: {Path(args.outdir) / 'audit' / 'run_audit.json'}")


if __name__ == "__main__":
    main()

