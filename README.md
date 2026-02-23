# dlfx — ICU EHR cohort (PPI vs H2RA) target trial emulation

DOI: 10.5281/zenodo.18743991 (concept DOI: 10.5281/zenodo.18743017)

This project is a **from-zero, reproducible** scaffold to run a publishable ICU cohort study:

> In adult ICU patients with stress ulcer prophylaxis (SUP) indications, what are the real‑world benefits/harms of **PPI vs H2RA**?

**Primary outcome**: clinically important upper GI bleeding (CIGIB; strict + broad definitions).  
**Key secondary outcomes**: *C. difficile* and mortality.

## Datasets (public, but access-controlled)

- MIMIC-IV (primary / discovery cohort)
- eICU-CRD (external validation cohort)

This repo **does not include** any patient-level data.

## Getting the data (start from zero)

See `docs/DATA_ACCESS.md`.

## What you run (once you have data access)

1) Create an *analysis-ready* one-row-per-ICU-stay table using the provided SQL templates:
   - `sql/mimic/`
   - `sql/eicu/`
2) Run the analysis pipeline (diagnostics + Table 1 + effect plots + audit):
   - Single cohort: `python3 scripts/run_study.py --help`
   - Primary + external validation: `python3 scripts/run_multicohort.py --help`

## Quick smoke demo (no real data)

This generates a synthetic analysis table and runs the pipeline end-to-end.

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt

python3 scripts/generate_synthetic_table.py --out /tmp/dlfx_synth.parquet --n 2000
python3 scripts/run_study.py --input /tmp/dlfx_synth.parquet --outdir output/synth_run
```

## Repo map

- Protocol & reporting templates:
  - `protocol/protocol.md`
  - `protocol/codebook.md`
  - `protocol/strobe_record_checklist.md`
- SQL extraction templates:
  - `sql/mimic/build_analysis_table.sql`
  - `sql/eicu/build_analysis_table.sql`
- Analysis code:
  - `src/dlfx/`
  - `scripts/`
- Tests on synthetic data:
  - `tests/`

## Quickstart (developer)

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
pytest -q
```
