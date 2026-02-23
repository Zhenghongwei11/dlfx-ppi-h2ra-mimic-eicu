# STROBE + RECORD checklist

> Fill this out when drafting the manuscript. Keep it as a living appendix for submission.

## STROBE (cohort studies)

### Title and abstract
- [x] Indicate the study’s design with a commonly used term in the title or the abstract.
  - Manuscript: Title and Abstract (target trial emulation; observational cohort framing)
- [x] Provide an informative and balanced summary of what was done and what was found.
  - Manuscript: Abstract (Background/Methods/Results/Conclusions)

### Introduction
- [x] Background/rationale
  - Manuscript: Introduction
- [x] Objectives and planned hypotheses
  - Protocol: `protocol/protocol.md` (Study question + estimands)
  - Manuscript: Introduction (objective statement)

### Methods
- [x] Study design (early in the paper)
  - Manuscript: Methods -> Study design
- [x] Setting (locations, dates, data sources)
  - Manuscript: Methods -> Data sources
  - Protocol: `protocol/protocol.md` (Data sources)
- [x] Participants (eligibility, selection methods, follow-up)
  - Manuscript: Methods -> Participants
  - Flow: Figure 1 (Source Data 2)
- [x] Variables (definitions of outcomes, exposures, predictors, confounders)
  - Protocol: `protocol/codebook.md`
  - Manuscript: Methods -> Exposure strategies; Outcomes; Covariates
- [x] Data sources/measurement
  - Protocol: `protocol/protocol.md` + `protocol/codebook.md`
- [x] Bias (efforts to address)
  - Manuscript: Methods -> Statistical analysis (landmark time zero; active comparator; IPTW; balance)
  - Sensitivity: Figure 4 (Source Data 6)
- [x] Study size (how arrived at)
  - Flow: Figure 1 (Source Data 2)
- [x] Quantitative variables (handling)
  - Manuscript: Methods -> Statistical analysis (splines; categorical encoding)
- [x] Statistical methods (confounding control, missing data, sensitivity)
  - Manuscript: Methods -> Statistical analysis; Sensitivity analyses
  - Sensitivity summary: Figure 4 (Source Data 6)

### Results
- [x] Participants (flow diagram recommended)
  - Figure 1 (Source Data 2)
- [x] Descriptive data (baseline table)
  - Table 1 and Table 2 (Source Data 3 and Source Data 4)
- [x] Outcome data
  - Figure 3 (Source Data 1)
- [x] Main results (effect estimates with precision)
  - Figure 3 (Source Data 1)
- [x] Other analyses (subgroups, interactions, sensitivity)
  - Sensitivity: Figure 4 (Source Data 6)
  - Subgroups: planned (see `docs/SENSITIVITY_REGISTRY.tsv`)

### Discussion
- [x] Key results
  - Manuscript: Discussion (Principal findings)
- [x] Limitations (bias, imprecision)
  - Manuscript: Discussion (Limitations)
- [x] Interpretation (cautious)
  - Manuscript: Discussion (Interpretation)
- [x] Generalisability
  - Manuscript: Discussion (External validation + transportability caveats)

### Other information
- [x] Funding
  - No external funding: "This research received no specific grant from any funding agency in the public, commercial, or not-for-profit sectors."

## RECORD (routinely-collected health data) additions

- [x] Data access and cleaning methods described
  - Manuscript: Methods -> Data sources (to be expanded for final submission)
- [x] Codes/algorithms for exposures and outcomes provided (or accessible)
  - Protocol: `protocol/codebook.md` (code lists / rules)
- [x] Database linkage described (if applicable)
  - Not applicable (single-database analyses; no patient-level linkage across databases)
- [x] Validation studies referenced (or proxy limitations stated)
  - This study uses operational definitions/proxies implemented consistently across cohorts (see `protocol/codebook.md`).
  - Where validated phenotypes/algorithms are not available in both databases, we:
    - use a planned proxy definition,
    - report it as a limitation (residual misclassification risk),
    - and include robustness checks (Figure 4 / Source Data 6).
  - If a validated algorithm is adopted later (e.g., for CIGIB/CDI), add citations and document mapping differences across MIMIC vs eICU.
- [x] Availability of protocol / raw code / programming scripts stated
  - Data availability: MIMIC-IV and eICU-CRD are public but access-controlled datasets available via PhysioNet credentialing and DUAs (see `docs/DATA_SOURCES.md`, `data/manifest.tsv`).
  - Patient-level extracts are not shared in this repository (all `data/**` and `output/**` are gitignored).
  - Protocol + codebook: included in-repo under `protocol/`.
  - Analysis code: included in-repo under `sql/`, `scripts/`, `src/` (share repository link in the submission).
