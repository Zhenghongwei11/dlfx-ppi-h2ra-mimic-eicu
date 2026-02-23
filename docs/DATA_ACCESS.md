# Data access (from zero) — MIMIC-IV & eICU

This project is designed for **public, access-controlled ICU EHR datasets** on PhysioNet.
You can develop and smoke-test the pipeline using **open demo datasets**, but to produce publishable results you will need **full cohort access**.

## Option A (recommended): Full MIMIC-IV + eICU via PhysioNet

### What you need

1) A PhysioNet account
2) **Credentialed user** status
3) Completion of required human-subjects training
4) Signed Data Use Agreement (DUA) for each dataset you access

### Required training (as listed on PhysioNet)

- CITI: **Data or Specimens Only Research**

### Local evidence (not committed)

The following training evidence PDFs exist on the submission workstation desktop and are not included in this repository:
- `citiCompletionCertificate_15351349_75289558.pdf` (completion 10-Feb-2026; expires 10-Feb-2029; record ID 75289558)
- `citiCompletionReport_15351349_75289558.pdf` (details of modules; reported score 97)

For submission packaging, see `docs/submissions/JIMR/PHYSIONET_ACCESS_EVIDENCE.md`.

### Steps (practical)

1) Create a PhysioNet account and log in.
2) Become a **credentialed user** (follow the PhysioNet credentialing flow).
3) Complete the required CITI course and upload your certificate to PhysioNet.
4) Request access and sign the DUA for:
   - MIMIC-IV Clinical Database (v3.1 or latest)
   - eICU-CRD (v2.0)
5) Choose one of the two compute routes:
   - **BigQuery route (often easiest):** request BigQuery access via PhysioNet, run SQL there, export an analysis-ready table to Parquet/CSV.
   - **Local Postgres route:** download files and load into Postgres, then run the SQL templates in `sql/`.

### BigQuery route (fastest to “first real result”)

PhysioNet provides MIMIC-IV v3.1 on BigQuery (schemas are published on their news page). You still need PhysioNet access approval; BigQuery just avoids installing/loading a huge local database.

High-level workflow:

1) Run `sql/mimic/build_analysis_table.sql` logic as BigQuery SQL (you will need to adapt schema/table names).
2) Export the resulting table to a file:
   - `dlfx/data/mimic_sup_ppi_h2ra.parquet`
3) Run analysis:
   - `python3 scripts/run_study.py --input dlfx/data/mimic_sup_ppi_h2ra.parquet --outdir dlfx/output/mimic_run`

## Option B (open, immediate): Demo datasets for pipeline rehearsal (NOT publishable main results)

These are useful for:
- verifying your local environment,
- practicing SQL joins / variable definitions,
- ensuring your pipeline produces the expected tables/figures/audit outputs.

Demo datasets:
- MIMIC-IV Clinical Database Demo (100 patients; open)
- eICU-CRD Demo (open)

After downloading a demo dataset you can either:
- build a tiny analysis-ready table yourself (recommended as practice), or
- continue using `scripts/generate_synthetic_table.py` for smoke tests.

## Practical expectations (time)

- Training: a few hours.
- PhysioNet credentialing + dataset approval: often days, sometimes longer depending on review/queue.
- BigQuery first-pass extraction: 0.5–2 days once access is granted (depends on your SQL comfort).

## Important compliance note

Do not commit or share any patient-level extracts.
Keep all extracted tables under `dlfx/data/` (already gitignored) and follow DUA restrictions.

