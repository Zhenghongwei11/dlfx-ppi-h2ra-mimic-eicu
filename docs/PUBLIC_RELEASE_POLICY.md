# Public Release Policy (GitHub + Zenodo)

This repository is intended for reproducible methods and aggregated, non-identifying outputs.

## What is included
- Protocol and data contract:
  - `protocol/`
- Analysis code:
  - `src/`
  - `scripts/` (analysis + extraction utilities)
- Data-source manifest (no patient data):
  - `data/manifest.tsv`
- Aggregated release materials:
  - `docs/release_bundle/`

## What is NOT included
- Patient-level data or extracts (PhysioNet DUA restricted):
  - `data/raw/`
  - `data/*.parquet`
  - any other derived patient-level tables
- Local access/browsing artifacts:
  - `.playwright-cli/`, `wget-log*`

## How to reproduce results
1. Obtain PhysioNet credentialing and dataset access for MIMIC-IV and eICU-CRD.
2. Follow `docs/DATA_ACCESS.md` and `docs/EXTRACTION_LOCAL_DUCKDB.md` to extract analysis-ready tables locally.
3. Run `python3 scripts/run_multicohort.py` to regenerate tables/figures.
4. Compare regenerated aggregate outputs against the reference outputs under `docs/release_bundle/`.
