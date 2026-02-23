from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd


def test_primary_analysis_script_runs(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]

    inp = tmp_path / "synthetic.parquet"
    outdir = tmp_path / "out_death"

    # Generate synthetic dataset
    subprocess.check_call(
        [sys.executable, str(root / "scripts" / "generate_synthetic_table.py"), "--out", str(inp), "--n", "800"],
        cwd=root,
    )

    # Run time-to-event analysis (death has time column)
    subprocess.check_call(
        [
            sys.executable,
            str(root / "scripts" / "run_primary_analysis.py"),
            "--input",
            str(inp),
            "--outdir",
            str(outdir),
            "--outcome",
            "death",
        ],
        cwd=root,
    )

    results_path = outdir / "results.json"
    assert results_path.exists()
    results = json.loads(results_path.read_text(encoding="utf-8"))
    assert "cox_hr" in results
    assert results["cox_hr"]["hr"] > 0


def test_binary_outcome_path(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]

    inp = tmp_path / "synthetic.parquet"
    outdir = tmp_path / "out_ugib"

    subprocess.check_call(
        [sys.executable, str(root / "scripts" / "generate_synthetic_table.py"), "--out", str(inp), "--n", "600"],
        cwd=root,
    )

    # ugib_broad has no time column in synthetic -> should use binary risk path.
    subprocess.check_call(
        [
            sys.executable,
            str(root / "scripts" / "run_primary_analysis.py"),
            "--input",
            str(inp),
            "--outdir",
            str(outdir),
            "--outcome",
            "ugib_broad",
        ],
        cwd=root,
    )

    results = json.loads((outdir / "results.json").read_text(encoding="utf-8"))
    assert "risk_binary" in results
    assert results["risk_binary"]["rr"] > 0

    bal = pd.read_csv(outdir / "balance_smd.csv")
    assert {"feature", "smd_unweighted", "smd_weighted"}.issubset(set(bal.columns))

