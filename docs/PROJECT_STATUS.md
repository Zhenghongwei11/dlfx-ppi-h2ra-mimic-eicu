# Project Status (dlfx)

Last updated: 2026-02-22

## What this project is
ICU EHR observational cohort study using target trial emulation: SUP indication patients, PPI vs H2RA, outcomes CIGIB/CDI/mortality, MIMIC-IV discovery + eICU external validation.

Canonical plan: `protocol/protocol.md` + `protocol/codebook.md`.

## Where we are (evidence-based)
| Stage | Status | Evidence |
| --- | --- | --- |
| Data access approved | DONE | PhysioNet access approved for required datasets (records kept by authors; not distributed here) |
| Human-subjects training (CITI) | DONE | Completed 10-Feb-2026; expires 10-Feb-2029 (records kept by authors; not distributed here) |
| Raw archives downloaded | DONE | `data/raw/physionet/mimiciv-3.1.zip`, `data/raw/physionet/eicu-crd-2.0.zip` |
| Analysis-ready cohort table exported | DONE | `data/mimic_sup_ppi_h2ra.parquet`, `data/eicu_sup_ppi_h2ra.parquet` |
| Real-cohort mainline analysis run | DONE | `output/mimic_run/`, `output/eicu_run/` |
| Sensitivity suite run (real cohorts) | DONE | `output/multicohort_run/combined/sensitivity_summary.tsv` (S1–S6 + G1–G3); per-sensitivity runs under `output/multicohort_run/sensitivity/` |
| External validation (eICU) run | DONE | `output/multicohort_run/` (per-cohort + combined) |
| Manuscript drafting | IN PROGRESS | Manuscript files are maintained separately (not distributed in this repo). |
| Journal submission pack | IN PROGRESS | Submission pack files are maintained separately (not distributed in this repo). |
| Figure 4 + supplement anchors generated | DONE | `output/multicohort_run/combined/figure4_sensitivity.png` + `output/multicohort_run/combined/supplement_table_S1_strict_cigib.tsv` + `output/multicohort_run/combined/supplement_table_S2_secondary_outcomes.tsv` |

## Current state
- Protocol: drafted (`protocol/protocol.md`)
- Codebook/schema: drafted (`protocol/codebook.md`)
- Data access: approved (MIMIC-IV + eICU) and raw archives downloaded (see below)
- Training: CITI “Data or Specimens Only Research” completed 10-Feb-2026; expires 10-Feb-2029 (records kept by authors; not distributed here)
- Pipeline scaffold: present (synthetic smoke tests already run under `output/`)

### Local raw data (do not commit)
- `data/raw/physionet/mimiciv-3.1.zip` (MIMIC-IV v3.1)
- `data/raw/physionet/eicu-crd-2.0.zip` (eICU-CRD v2.0)

### Analysis-ready extracts (publishable tables)
- `data/mimic_sup_ppi_h2ra.parquet` (MIMIC discovery cohort)
- `data/eicu_sup_ppi_h2ra.parquet` (eICU external validation cohort)

### Evidence of runs (non-publishable)
- Synthetic end-to-end runs exist under `output/synth_run*` (tables/figures/audit produced).
- PhysioNet access automation snapshots exist under `output/playwright/` (screenshots/yaml), but no real-cohort analysis outputs were detected under `output/`.
  - This is superseded now by real-cohort runs under `output/mimic_run/`, `output/eicu_run/`, and `output/multicohort_run/`.

## Open items (fill in as you progress)
- PhysioNet approval status: APPROVED (MIMIC-IV + eICU)
- Extraction route: Local DuckDB (recommended on this machine; see `docs/EXTRACTION_LOCAL_DUCKDB.md`)
- First real cohort analysis table exported: DONE (`data/*.parquet` present)
- Protocol freeze status: FROZEN (see `docs/PROTOCOL_FREEZE.md`)
- Data sources manifest: see `data/manifest.tsv`
- Target journal: determined by the authors during submission (journal planning notes are not distributed here)

## Recent changes (important for reproducibility)
- 2026-02-18: MIMIC lab extraction in `sql/mimic/build_analysis_table.sql` was corrected from label matching to explicit `itemid` mapping (prevents LDH being misclassified as lactate and other label collisions). All real-cohort outputs were regenerated after this fix (mainline + sensitivity suite + Figure 4 anchors).

## Local environment signals
- Postgres CLI was not detected (`psql` not found), so any Postgres-based extraction likely has not been set up on this machine yet.
- BigQuery CLI is installed (`bq` present), but the active account does not currently have permission to query `physionet-data` datasets.
- Local extraction route: see `docs/EXTRACTION_LOCAL_DUCKDB.md`.
