# Reproduction Guide

This repository distributes code and aggregated (non-identifying) outputs for a cohort study using access-controlled ICU EHR datasets.

## What you can reproduce from this package
- Aggregate effect tables, sensitivity summaries, and diagnostic figures are provided under `artifacts/` inside the bundle zip.
- Full end-to-end regeneration requires credentialed access to MIMIC-IV and eICU-CRD via PhysioNet.

## How to reproduce (requires PhysioNet access)
1) Create a Python environment (Python 3.12 recommended).
2) Install dependencies: `pip install -r requirements.txt`.
3) Obtain the PhysioNet zip archives for MIMIC-IV and eICU-CRD and place them under `data/raw/physionet/`.
4) Run the extraction scripts to generate analysis-ready tables.
5) Run the multicohort pipeline and sensitivity suite.

## Notes
- Patient-level data are not distributed in this repository or the bundle.
