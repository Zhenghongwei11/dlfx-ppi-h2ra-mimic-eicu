# Reviewer Guide

This bundle is designed to support review of code and aggregate results.

## What you can reproduce from this bundle
- Aggregate effect tables, sensitivity summaries, and diagnostic figures are provided under `artifacts/`.
- Full end-to-end regeneration requires access-controlled data (MIMIC-IV and eICU-CRD) via PhysioNet.

## How to reproduce (requires PhysioNet access)
1) Create a Python environment (Python 3.12 recommended).
2) Install dependencies: `pip install -r requirements.txt`.
3) Place PhysioNet zip archives in `data/raw/physionet/` (paths documented in the extraction scripts).
4) Run extraction scripts to generate analysis-ready tables.
5) Run the multicohort pipeline and sensitivity suite.

## Notes
- Patient-level data are not distributed in this bundle.
