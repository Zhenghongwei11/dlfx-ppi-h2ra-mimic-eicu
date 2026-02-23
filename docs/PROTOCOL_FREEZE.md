# Protocol Freeze (ICU SUP: PPI vs H2RA)

## Status
- Freeze status: FROZEN
- Last reviewed: 2026-02-18
- Freeze date: 2026-02-17
- Candidate freeze reference (integrity): SHA256 of canonical files
  - `protocol/protocol.md`: f59dd5448442ceb5d3cbe0b8d49ab2b8a570b2a7ca5ee21a76adcb71c698d15f
  - `protocol/codebook.md`: 4a8860f13732374cfc3eead197e9a47731726a8ae3eebc830feddcefd2a8a132

## Runs using this freeze
- MIMIC mainline run: `output/mimic_run/`
- eICU run: `output/eicu_run/`
- Multicohort run (MIMIC + eICU + combined): `output/multicohort_run/`

## Canonical Study Files
- `protocol/protocol.md` (study design, estimands, eligibility, exposure, outcomes, covariates, analyses)
- `protocol/codebook.md` (analysis-ready table contract: columns, windows, code lists)

## Freeze Rule
Before running outcome modeling on real cohorts (MIMIC-IV or eICU), set:
- Freeze status: FROZEN
- Freeze date
- Freeze reference

## Allowed Deviations Policy
Any change that affects eligibility, exposure, outcome definitions, time windows, covariates, estimands, or the sensitivity suite is a protocol deviation.

If a deviation is required:
- Record it in a deviation log (append to this file under "Deviations")
- State the reason (data limitation, mapping differences, bug fix, reviewer request)
- State the impact (which claims, outcomes, or figures are affected)
- Add a replacement/mitigation analysis if needed

## Deviations
2026-02-17:
- **Bug fix (implementation, non-protocol change):** MIMIC follow-up censoring previously used hospital discharge timestamps that could occur before the ICU timeline (data-quality artifact), which yielded negative follow-up times for some non-events. We updated `sql/mimic/build_analysis_table.sql` to ignore discharge/death timestamps that occur before `index_time` when computing censoring. After this fix, `cigib_strict_time_days` is non-negative for all rows.

2026-02-17:
- **Bug fix / protocol alignment (implementation):** eICU extraction previously set `sup_indication_mv_24h=0` for all stays (MV proxy not implemented), which effectively restricted eligibility to coagulopathy-only. We implemented an MV proxy using `respiratoryCare` (ventilation interval overlap with the baseline window) in `scripts/extract_eicu_duckdb.py`. This changes eICU eligibility counts and downstream estimates; all real-cohort runs should be regenerated after this change.

2026-02-18:
- **Bug fix / protocol alignment (implementation):** MIMIC `labs_24h` extraction previously used `d_labitems.label` matching (e.g., `label ilike 'Lactate%'`), which can misclassify labs with similar prefixes (e.g., Lactate Dehydrogenase) and create implausible values. We replaced label matching with explicit `itemid` mapping for hemoglobin/platelets/INR/creatinine/lactate in `sql/mimic/build_analysis_table.sql`, with conservative plausibility bounds applied per lab. This changes baseline covariate values and any downstream adjusted estimates; all real-cohort runs were regenerated after this fix.
