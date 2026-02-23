# Review Bundle Policy

This folder contains a single canonical public review bundle zip intended to be byte-identical for:
- journal supplementary submission, and
- GitHub release distribution.

## Included
- Source code and scripts required to reproduce the analysis (requires PhysioNet credentialed access).
- Protocol and codebook describing the analysis-table contract and prespecified analyses.
- Non-patient-level derived artifacts (aggregate tables/figures) copied into the bundle under `artifacts/`.
- Run audits (JSON) where they do not contain patient-level data.

## Excluded (intentional)
- Patient-level data and extracts (all `data/` and all Parquet outputs).
- Patient-level analysis tables (e.g., `analysis_table_used.*`).
- Submission-only materials (cover letter, submission checklist, manuscript source).
- Planning/spec scaffolding.

Bundle zip name: `dlfx_review_bundle_2026-02-23.zip`.
