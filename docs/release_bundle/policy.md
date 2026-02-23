# Public Release Bundle Policy

This folder contains the public release bundle zip intended for:
- GitHub release distribution, and
- journal supplementary upload (if needed).

## Included
- Source code and scripts required to reproduce the analysis (requires PhysioNet credentialed access).
- Protocol and codebook describing the analysis-table contract and analysis plan.
- Non-patient-level derived artifacts (aggregate tables/figures) copied into the bundle under `artifacts/`.
- Run audits (JSON) where they do not contain patient-level data.

## Excluded (intentional)
- Patient-level data and extracts (all `data/` and all Parquet outputs).
- Patient-level analysis tables (e.g., `analysis_table_used.*`).
- Local-only drafts, notes, and submission documents not required to reproduce the analysis.
