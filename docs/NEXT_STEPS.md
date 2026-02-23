# Next Steps (Q2-ready cohort study)

## 1) Data access + extraction
1. Data access complete: PhysioNet credentialing + DUAs approved; raw archives downloaded (see `docs/PROJECT_STATUS.md`).
2. Pick extraction route:
   - BigQuery route (still ok, even if you also downloaded archives)
   - Local Postgres route (natural next step since archives exist)
   - Local DuckDB route (recommended on this machine; no Postgres/BigQuery required)
3. Build/export analysis-ready tables that follow `protocol/codebook.md`:
   - `data/mimic_sup_ppi_h2ra.parquet`
   - `data/eicu_sup_ppi_h2ra.parquet`

### DuckDB commands (recommended here)
```bash
python3 scripts/extract_mimic_duckdb.py --zip data/raw/physionet/mimiciv-3.1.zip --out data/mimic_sup_ppi_h2ra.parquet
python3 scripts/extract_eicu_duckdb.py --zip data/raw/physionet/eicu-crd-2.0.zip --out data/eicu_sup_ppi_h2ra.parquet
```

## 2) Freeze protocol before modeling
1. Update `docs/PROTOCOL_FREEZE.md` to FROZEN with a freeze reference.
2. Ensure `docs/CLAIMS.tsv`, `docs/FIGURE_STORYBOARD.tsv`, and `docs/SENSITIVITY_REGISTRY.tsv` match `protocol/protocol.md`.

## 3) Mainline analysis run (MIMIC)
1. Validate the analysis table contract.
2. Produce cohort flow + Table 1 + balance/overlap diagnostics.
   - Flow/attrition table: `uv run python scripts/generate_attrition_flow.py --landmark-hours 24`
3. Produce main effect estimates with uncertainty for CIGIB/CDI/mortality.

## 4) Sensitivity suite + external validation
1. Run the registered sensitivity suite (see `docs/SENSITIVITY_REGISTRY.tsv`).
   - Scripted runner (landmark 12h/6h): `uv run python scripts/run_sensitivity_suite.py --threads 4`
2. Run the full validation cohort (eICU) with the same definitions.
3. Produce cross-cohort comparison tables/plots and a heterogeneity note.

## 5) Submission prep (later)
1. Complete `protocol/strobe_record_checklist.md`.
2. Generate publication figures and a figure provenance map.
3. Build a review bundle for peer review and GitHub release.
