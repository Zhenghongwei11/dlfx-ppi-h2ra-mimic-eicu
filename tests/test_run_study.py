from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_run_study_produces_audit_and_figures(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    inp = tmp_path / "synthetic.parquet"
    outdir = tmp_path / "run"

    subprocess.check_call(
        [sys.executable, str(root / "scripts" / "generate_synthetic_table.py"), "--out", str(inp), "--n", "800"],
        cwd=root,
    )
    subprocess.check_call(
        [
            sys.executable,
            str(root / "scripts" / "run_study.py"),
            "--input",
            str(inp),
            "--outdir",
            str(outdir),
        ],
        cwd=root,
    )

    audit_path = outdir / "audit" / "run_audit.json"
    assert audit_path.exists()
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    assert audit["input"]["sha256"]
    assert (outdir / "figures" / "love_plot.png").exists()
    assert (outdir / "figures" / "forest_ratio.png").exists()
    assert (outdir / "tables" / "effect_estimates.csv").exists()
    assert (outdir / "tables" / "table1.csv").exists()


def test_run_multicohort_script(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    inp_a = tmp_path / "a.parquet"
    inp_b = tmp_path / "b.parquet"
    outdir = tmp_path / "multi"

    subprocess.check_call(
        [sys.executable, str(root / "scripts" / "generate_synthetic_table.py"), "--out", str(inp_a), "--n", "700", "--seed", "11"],
        cwd=root,
    )
    subprocess.check_call(
        [sys.executable, str(root / "scripts" / "generate_synthetic_table.py"), "--out", str(inp_b), "--n", "700", "--seed", "22"],
        cwd=root,
    )
    subprocess.check_call(
        [
            sys.executable,
            str(root / "scripts" / "run_multicohort.py"),
            "--primary",
            str(inp_a),
            "--external",
            str(inp_b),
            "--outdir",
            str(outdir),
        ],
        cwd=root,
    )

    assert (outdir / "combined" / "effect_estimates_combined.csv").exists()
    assert (outdir / "audit_multicohort.json").exists()
