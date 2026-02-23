# Q2 Reporting Package (Cohort Study)

This project is a target-trial emulation cohort analysis (MIMIC-IV discovery + eICU external validation). For a Q2-level submission, the goal is:
1) keep the main paper readable (high-signal), and
2) push long/technical tables into a structured supplement with clear anchors.

## Main Manuscript (keep tight)
- **Table 1 (baseline)**: one table per cohort or one combined table; show weighted balance headline (max/median SMD after weighting) in caption or text.
- **Figure 1 (attrition/flow)**: cohort assembly + exclusions.
- **Figure 2 (positivity & balance)**:
  - PS overlap (unweighted + weighted)
  - Love plot (post-weighting balance)
- **Figure 3 (clinical endpoints)**:
  - Weighted KM/CIF curves for the primary endpoint and death (as appropriate)
- **Figure 4 (robustness)**:
  - Sensitivity suite summary (what changes and what does not)

## Supplement (where long tables belong)
- **Supplement Table S1**: strict CIGIB sensitivity suite (per-cohort + pooled where compatible).
- **Supplement Table S2**: secondary outcomes (death, CDI) and key robustness checks; avoid pooling incompatible estimands.
- **Supplement Methods Appendix**:
  - exact cohort definitions (eligibility/exposure windows)
  - complete covariate list + encoding rules (link to `protocol/codebook.md`)
  - PS model spec, truncation, and diagnostics thresholds
- **Supplement Diagnostics**:
  - missingness table (per covariate, per cohort, by arm)
  - weight distribution summary (min/p1/median/p99/max; ESS treated/control)
  - negative-control/falsification rationale (what it tests and what failure implies)

## Existing Artifact Anchors (this repo)
- Primary/combined effects: `output/multicohort_run/combined/effect_estimates_combined.csv`
- Sensitivity summary: `output/multicohort_run/combined/sensitivity_summary.tsv`
- Figure 4 draft: `output/multicohort_run/combined/figure4_sensitivity.png`
- Supplement tables: `output/multicohort_run/combined/supplement_table_S1_strict_cigib.tsv`, `output/multicohort_run/combined/supplement_table_S2_secondary_outcomes.tsv`
- Per-cohort diagnostics:
  - `output/multicohort_run/mimic/figures/ps_overlap_unweighted.png`, `output/multicohort_run/mimic/figures/ps_overlap_weighted.png`
  - `output/multicohort_run/mimic/figures/love_plot.png`, `output/multicohort_run/mimic/figures/weights_hist.png`
  - same paths under `output/multicohort_run/eicu/`

