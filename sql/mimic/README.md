# MIMIC-IV SQL templates (PostgreSQL)

These SQL files are **templates** to build an analysis-ready, one-row-per-ICU-stay table for:

> SUP in ICU: **PPI vs H2RA**, landmark at 24h, target trial emulation.

## Assumptions

- You have loaded MIMIC-IV into PostgreSQL using the standard schemas:
  - `mimiciv_hosp`
  - `mimiciv_icu`
- If you have `mimiciv_derived` (from the community `mimic-code` project), you can enrich the table with SOFA/comorbidity scores. The first version here **does not require** derived tables, but leaves explicit TODOs where derived tables are preferred.

## Output

Run `build_analysis_table.sql` to create:

- `dlfx_mimic_sup_ppi_h2ra` — analysis-ready table with:
  - eligibility flags,
  - baseline labs (0–24h),
  - exposure (PPI vs H2RA, 0–24h),
  - outcome flags (initial ICD-based versions; refine as needed).

Then export it to `csv/parquet` for analysis with the Python pipeline under `scripts/`.

## Important note about outcome timing

Many MIMIC diagnosis codes are assigned at discharge and do not have precise timestamps. For the primary CIGIB endpoint, a **strict, time-aware** definition should ideally use time-stamped evidence (PRBC transfusion, Hb dynamics, endoscopy/hemostasis procedure timing). The provided SQL includes conservative ICD-based flags as a starting point and marks time-aware components as TODOs.

