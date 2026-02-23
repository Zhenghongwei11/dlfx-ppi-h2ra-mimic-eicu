# eICU-CRD SQL templates

These SQL files are templates to build the same analysis-ready table schema used for MIMIC, but mapped to **eICU-CRD** tables/fields.

## Assumptions

- You have loaded eICU-CRD into PostgreSQL (or another SQL engine) using the standard eICU schema.
- eICU uses time offsets (minutes) from ICU admission in many tables; we use those offsets to define:
  - exposure window: 0–1440 minutes
  - landmark: 1440 minutes
  - follow-up windows: 14 days / 28 days

## Output

- `dlfx_eicu_sup_ppi_h2ra` (one row per `patientunitstayid`)

## TODOs

eICU table names and fields vary by install. The template marks the key mapping points you must verify:

- Mechanical ventilation evidence within 24h
- PPI/H2RA medication records within 24h
- Outcome algorithms (CIGIB strict + broad; CDI; mortality)

