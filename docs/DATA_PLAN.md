# Data Plan (MIMIC-IV discovery + eICU validation)

## Cohorts
- Discovery cohort: MIMIC-IV (ICU + hospital EHR), extracted to one-row-per-ICU-stay analysis table.
- Validation cohort: eICU-CRD (multi-center ICU EHR), extracted to a comparable analysis table.

## Access and compliance
Follow `docs/DATA_ACCESS.md`. Do not commit or share any patient-level extracts. Keep local extracts under `data/` (gitignored) and comply with PhysioNet DUAs.

## Local raw archives (downloaded)
- `data/raw/physionet/mimiciv-3.1.zip`
- `data/raw/physionet/eicu-crd-2.0.zip`

## Extraction routes (preferred)
1) BigQuery route (recommended for MIMIC-IV): run/adapt `sql/mimic/build_analysis_table.sql` logic in BigQuery and export Parquet/CSV.
2) Local Postgres route: load datasets locally and run `sql/` templates.

## Analysis-table contract
The analysis table MUST follow `protocol/codebook.md`:
- one row per ICU stay
- fixed baseline and exposure windows
- time-to-event columns measured in days from index time

## Expected file locations (local, not committed)
- `data/mimic_sup_ppi_h2ra.parquet`
- `data/eicu_sup_ppi_h2ra.parquet`
