# Data Sources (for Methods + Supplement)

This file lists data sources and what we will cite/describe in the manuscript and supplementary materials.

## Primary cohorts (publishable)
- MIMIC-IV Clinical Database v3.1 (discovery cohort): https://physionet.org/content/mimiciv/3.1/
- eICU-CRD v2.0 (external validation cohort): https://physionet.org/content/eicu-crd/2.0/

These datasets are public but access-controlled on PhysioNet; follow `docs/DATA_ACCESS.md` and comply with DUAs.

### Local raw archives (do not commit)
- `data/raw/physionet/mimiciv-3.1.zip`
- `data/raw/physionet/eicu-crd-2.0.zip`

## Demo/synthetic data (non-publishable for main results)
- MIMIC-IV Demo / eICU Demo: used only for rehearsal and smoke testing.
- Synthetic analysis tables: used only for pipeline tests (see `scripts/generate_synthetic_table.py`).

## Definitions and code lists
- Study design and estimands: `protocol/protocol.md`
- Variable definitions, time windows, and initial code lists: `protocol/codebook.md`

## Machine-readable manifest
See `data/manifest.tsv`.
