#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import subprocess
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class CopyRule:
    src: Path
    dst_rel: Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Build a sanitized public release bundle zip (GitHub release asset / supplementary zip). "
            "Excludes patient-level data and local-only drafting materials."
        )
    )
    p.add_argument(
        "--outdir",
        default=str(ROOT / "docs" / "release_bundle"),
        help="Output directory for bundle artifacts (zip + manifests).",
    )
    p.add_argument(
        "--name",
        default=f"dlfx_release_bundle_{_today()}",
        help="Base name for the zip (without .zip).",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite an existing zip with the same name.",
    )
    return p.parse_args()


def _today() -> str:
    # Keep deterministic, local-only; good enough for bundle naming.
    import datetime as _dt

    return _dt.date.today().isoformat()


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _iter_files(base: Path) -> list[Path]:
    return [p for p in sorted(base.rglob("*")) if p.is_file()]


def _iter_git_tracked_files() -> list[Path]:
    """Return Paths for files tracked by git.

    This keeps the bundle aligned with the repository snapshot and avoids
    accidentally packaging untracked local files.
    """

    res = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=str(ROOT),
        check=True,
        stdout=subprocess.PIPE,
    )
    out = res.stdout.decode("utf-8", errors="replace")
    paths = [p for p in out.split("\x00") if p]
    return [ROOT / p for p in sorted(paths)]


def _should_exclude(path: Path) -> bool:
    rel = path.relative_to(ROOT).as_posix()

    # Avoid recursive bundling of previous bundle outputs.
    if rel.startswith("docs/release_bundle/"):
        return True
    # Legacy path (keep excluded if present locally).
    if rel.startswith("docs/review_bundle/"):
        return True

    # Keep local-only drafts out even if tracked on a workstation.
    if rel.startswith("docs/submissions/") or rel.startswith("docs/manuscript/"):
        return True
    if rel in {"docs/MANUSCRIPT_LINT_REPORT.md", "docs/WRITING_GUIDE.md", "scripts/lint_manuscript.py"}:
        return True

    return False


def _should_exclude_output_artifact(path: Path) -> bool:
    # Exclude any Parquet (may contain patient-level rows) and any "analysis_table_used" artifacts.
    name = path.name.lower()
    if name.endswith(".parquet"):
        return True
    if "analysis_table_used" in name:
        return True
    return False


def _bundle_policy_text() -> str:
    return (
        "# Public Release Bundle Policy\n\n"
        "This folder contains the public release bundle zip intended for:\n"
        "- GitHub release distribution, and\n"
        "- journal supplementary upload (if needed).\n\n"
        "## Included\n"
        "- Source code and scripts required to reproduce the analysis (requires PhysioNet credentialed access).\n"
        "- Protocol and codebook describing the analysis-table contract and analysis plan.\n"
        "- Non-patient-level derived artifacts (aggregate tables/figures) copied into the bundle under `artifacts/`.\n"
        "- Run audits (JSON) where they do not contain patient-level data.\n\n"
        "## Excluded (intentional)\n"
        "- Patient-level data and extracts (all `data/` and all Parquet outputs).\n"
        "- Patient-level analysis tables (e.g., `analysis_table_used.*`).\n"
        "- Local-only drafts, notes, and submission documents not required to reproduce the analysis.\n"
    )


def _reproduction_guide_text() -> str:
    return (
        "# Reproduction Guide\n\n"
        "This repository distributes code and aggregated (non-identifying) outputs for a cohort study using "
        "access-controlled ICU EHR datasets.\n\n"
        "## What you can reproduce from this package\n"
        "- Aggregate effect tables, sensitivity summaries, and diagnostic figures are provided under `artifacts/` inside the bundle zip.\n"
        "- Full end-to-end regeneration requires credentialed access to MIMIC-IV and eICU-CRD via PhysioNet.\n\n"
        "## How to reproduce (requires PhysioNet access)\n"
        "1) Create a Python environment (Python 3.12 recommended).\n"
        "2) Install dependencies: `pip install -r requirements.txt`.\n"
        "3) Obtain the PhysioNet zip archives for MIMIC-IV and eICU-CRD and place them under `data/raw/physionet/`.\n"
        "4) Run the extraction scripts to generate analysis-ready tables.\n"
        "5) Run the multicohort pipeline and sensitivity suite.\n\n"
        "## Notes\n"
        "- Patient-level data are not distributed in this repository or the bundle.\n"
    )


def _zip_dir(src_dir: Path, zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for p in _iter_files(src_dir):
            rel = p.relative_to(src_dir).as_posix()
            zf.write(p, arcname=rel)


def main() -> None:
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    zip_name = str(args.name).removesuffix(".zip") + ".zip"
    zip_path = outdir / zip_name
    staging = outdir / "_staging"

    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)

    # 1) Copy repo sources from the version-controlled snapshot.
    for p in _iter_git_tracked_files():
        if _should_exclude(p):
            continue
        rel = p.relative_to(ROOT)
        _safe_copy_file(p, staging / rel)

    # 2) Copy safe aggregate artifacts from output/ into a dedicated folder.
    artifact_rules: list[CopyRule] = []
    safe_outputs = [
        # Combined anchors
        ("output/multicohort_run/combined/effect_estimates_combined.csv", "artifacts/combined/effect_estimates_combined.csv"),
        ("output/multicohort_run/combined/sensitivity_summary.tsv", "artifacts/combined/sensitivity_summary.tsv"),
        ("output/multicohort_run/combined/attrition_flow.tsv", "artifacts/combined/attrition_flow.tsv"),
        ("output/multicohort_run/combined/forest_ratio_pooled.png", "artifacts/combined/forest_ratio_pooled.png"),
        ("output/multicohort_run/combined/figure4_sensitivity.png", "artifacts/combined/figure4_sensitivity.png"),
        ("output/multicohort_run/combined/figure4_sensitivity.pdf", "artifacts/combined/figure4_sensitivity.pdf"),
        ("output/multicohort_run/combined/supplement_table_S1_strict_cigib.tsv", "artifacts/combined/supplement_table_S1_strict_cigib.tsv"),
        ("output/multicohort_run/combined/supplement_table_S2_secondary_outcomes.tsv", "artifacts/combined/supplement_table_S2_secondary_outcomes.tsv"),
        # Per-cohort aggregates
        ("output/multicohort_run/mimic/tables/table1.csv", "artifacts/mimic/table1.csv"),
        ("output/multicohort_run/mimic/tables/balance_smd.csv", "artifacts/mimic/balance_smd.csv"),
        ("output/multicohort_run/mimic/tables/effect_estimates.csv", "artifacts/mimic/effect_estimates.csv"),
        ("output/multicohort_run/eicu/tables/table1.csv", "artifacts/eicu/table1.csv"),
        ("output/multicohort_run/eicu/tables/balance_smd.csv", "artifacts/eicu/balance_smd.csv"),
        ("output/multicohort_run/eicu/tables/effect_estimates.csv", "artifacts/eicu/effect_estimates.csv"),
        # Audits (safe metadata)
        ("output/multicohort_run/audit_multicohort.json", "artifacts/audit/audit_multicohort.json"),
        ("output/multicohort_run/mimic/audit/run_audit.json", "artifacts/audit/mimic_run_audit.json"),
        ("output/multicohort_run/eicu/audit/run_audit.json", "artifacts/audit/eicu_run_audit.json"),
    ]
    for src_rel, dst_rel in safe_outputs:
        src = ROOT / src_rel
        if not src.exists():
            continue
        if _should_exclude_output_artifact(src):
            continue
        artifact_rules.append(CopyRule(src=src, dst_rel=Path(dst_rel)))

    for r in artifact_rules:
        _safe_copy_file(r.src, staging / r.dst_rel)

    # 3) Add policy + reproduction guide.
    (outdir / "policy.md").write_text(_bundle_policy_text(), encoding="utf-8")
    (outdir / "REPRODUCTION_GUIDE.md").write_text(_reproduction_guide_text(), encoding="utf-8")

    # Include these two docs inside the zip as well.
    _safe_copy_file(outdir / "policy.md", staging / "release_bundle" / "policy.md")
    _safe_copy_file(outdir / "REPRODUCTION_GUIDE.md", staging / "release_bundle" / "REPRODUCTION_GUIDE.md")

    # 4) Build zip.
    if zip_path.exists() and not args.overwrite:
        raise SystemExit(f"Refusing to overwrite existing zip: {zip_path} (pass --overwrite)")
    if zip_path.exists():
        zip_path.unlink()
    _zip_dir(staging, zip_path)

    # 5) Manifests: zip checksum + file list + per-file checksums.
    with zipfile.ZipFile(zip_path, "r") as zf:
        contents = sorted(zf.namelist())
    (outdir / "bundle_contents.txt").write_text("\n".join(contents) + "\n", encoding="utf-8")

    # Per-file checksums are computed on staging files (equivalent to zip extraction).
    checksums = []
    for p in _iter_files(staging):
        rel = p.relative_to(staging).as_posix()
        checksums.append((rel, _sha256(p)))
    checksums.sort(key=lambda x: x[0])
    (outdir / "bundle_checksums.tsv").write_text(
        "path\tsha256\n" + "\n".join([f"{rel}\t{h}" for rel, h in checksums]) + "\n",
        encoding="utf-8",
    )
    (outdir / "zip_sha256.txt").write_text(f"{_sha256(zip_path)}  {zip_name}\n", encoding="utf-8")

    # Clean staging.
    shutil.rmtree(staging)


if __name__ == "__main__":
    main()
