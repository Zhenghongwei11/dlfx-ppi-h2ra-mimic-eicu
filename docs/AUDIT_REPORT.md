# Audit Report (Traceability)

This file maps each planned claim to the pre-specified outputs used as supporting evidence.

## Reference protocol
- Protocol: `protocol/protocol.md`
- Codebook/contract: `protocol/codebook.md`
- Freeze record: `docs/PROTOCOL_FREEZE.md`

## Claims -> Evidence Map

### C1_CIGIB_STRICT
- Claim: PPI vs H2RA (initiators, landmark at ICU+24h) and 14-day strict CIGIB.
- Primary evidence:
  - Effects (per-cohort + combined): `output/multicohort_run/combined/effect_estimates_combined.csv`
  - Per-cohort tables: `output/multicohort_run/mimic/tables/effect_estimates.csv`, `output/multicohort_run/eicu/tables/effect_estimates.csv`
  - Flow/attrition: `output/multicohort_run/combined/attrition_flow.tsv`
  - Balance diagnostics: `output/multicohort_run/mimic/tables/balance_smd.csv`, `output/multicohort_run/eicu/tables/balance_smd.csv`
  - Baseline summaries: `output/multicohort_run/mimic/tables/table1.csv`, `output/multicohort_run/eicu/tables/table1.csv`
- Audit provenance:
  - Main multicohort audit: `output/multicohort_run/audit_multicohort.json`
  - Per-cohort run audits: `output/multicohort_run/mimic/audit/run_audit.json`, `output/multicohort_run/eicu/audit/run_audit.json`

### C2_CDI
- Claim: PPI vs H2RA and 14-day CDI risk.
- Evidence: `output/multicohort_run/combined/effect_estimates_combined.csv`
- Note: eICU CDI ascertainment is currently limited (event count may be zero under current mapping). Treat as secondary/exploratory unless improved.

### C3_MORTALITY
- Claim: PPI vs H2RA and 28-day mortality.
- Evidence: `output/multicohort_run/combined/effect_estimates_combined.csv`

## Sensitivity suite status
- Registry: `docs/SENSITIVITY_REGISTRY.tsv`
- Implemented (scripted):
  - Landmark 12h: `output/multicohort_run/sensitivity/S1_LANDMARK_12H/`
  - Landmark 6h: `output/multicohort_run/sensitivity/S2_LANDMARK_6H/`
  - Alternative exposure definition (strict start within baseline window): `output/multicohort_run/sensitivity/S3_ALT_EXPOSURE_SOURCE/`
  - Exclude early bleeding evidence proxy (baseline-window transfusion): `output/multicohort_run/sensitivity/S4_EXCLUDE_EARLY_BLEED/`
  - Negative-control window (1 day horizon): `output/multicohort_run/sensitivity/S5_NEG_CONTROL_WINDOW/`
  - Competing risk (death): `output/multicohort_run/sensitivity/S6_COMPETING_RISK_DEATH/`
  - Subgroups (G1–G3): `output/multicohort_run/sensitivity/G1_SUBGROUP_SUP_DRIVER/`, `output/multicohort_run/sensitivity/G2_SUBGROUP_LIVER_DX/`, `output/multicohort_run/sensitivity/G3_SUBGROUP_ANTITHROMBOTIC/`
- Summary table: `output/multicohort_run/combined/sensitivity_summary.tsv`

## Implementation note (bug fix)
During audit, we detected negative follow-up times in MIMIC strict-CIGIB time variables due to admissions discharge timestamps occurring before the ICU timeline (data-quality artifact). The censoring logic in `sql/mimic/build_analysis_table.sql` was updated to ignore discharge/death timestamps that occur before `index_time`. After this fix, `cigib_strict_time_days` is non-negative for all rows.

During audit, we also detected implausible MIMIC baseline lab values caused by label-prefix matching in `labs_24h` (e.g., lactate inadvertently capturing LDH). `sql/mimic/build_analysis_table.sql` was updated to use explicit `itemid` mapping (with conservative plausibility bounds) for hemoglobin/platelets/INR/creatinine/lactate. All real-cohort outputs should be considered valid only if generated after 2026-02-18.
