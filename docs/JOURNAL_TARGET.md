# Journal Target

## Primary target
- Journal: Biomedicine & Pharmacotherapy (B&P)
- Article type: Original Research (observational cohort study)

## Backup target
- Journal: Journal of International Medical Research (JIMR) (do not confuse with JMIR)

## What to optimize to reduce rejection risk
1) Clear target-trial emulation framing (eligibility, time zero/landmark, exposure window, follow-up, estimands).
2) Cohort-study transparency: attrition/flow, Table 1, missingness summary, outcome definitions (strict + sensitivity).
3) Confounding-control credibility: balance + overlap/positivity diagnostics; weight truncation policy; effective sample size.
4) Robustness: pre-specified sensitivity suite + subgroup checks with cautious interpretation.
5) External validity: eICU external validation with heterogeneity discussion.
6) Reporting: STROBE + RECORD completed and consistent with protocol and outputs.
7) Reproducibility: share code (SQL + Python) and a reviewer-friendly run bundle that excludes patient data.

## Repo files that MUST align before submission
- Protocol/canonical definitions: `protocol/protocol.md`, `protocol/codebook.md`
- Freeze + deviation log: `docs/PROTOCOL_FREEZE.md`
- Claims/storyboard/sensitivity registry: `docs/CLAIMS.tsv`, `docs/FIGURE_STORYBOARD.tsv`, `docs/SENSITIVITY_REGISTRY.tsv`
- Data sources: `docs/DATA_SOURCES.md`, `data/manifest.tsv`
- Reporting checklist: `protocol/strobe_record_checklist.md`

