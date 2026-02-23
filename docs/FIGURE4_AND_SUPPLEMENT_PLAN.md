# Figure 4 and Supplement Structure (Submission-Ready)

This document fixes what goes into **Figure 4 (main manuscript)** versus **supplementary materials** for the ICU SUP cohort study (PPI vs H2RA).

Source-of-truth table for all sensitivity/subgroup results:
- `output/multicohort_run/combined/sensitivity_summary.tsv` (Source Data 6)

## Figure 4 (Main Manuscript): Primary Outcome Robustness Only

**Goal:** Demonstrate robustness of the **primary outcome (strict CIGIB)** association without overloading the main paper with secondary endpoints or exploratory subgroup fishing.

### Figure 4A: Planned sensitivity suite (HR scale; strict CIGIB)
Include **strict CIGIB** only, reporting **pooled** effect (and optionally cohort-specific markers in lighter color):

- Mainline (landmark 24h; reference)
  - Use the pooled strict CIGIB row from `S6_COMPETING_RISK_DEATH` (same mainline definitions).
- S1: Landmark 12h (`S1_LANDMARK_12H`)
- S2: Landmark 6h (`S2_LANDMARK_6H`)
- S3: Alternative exposure definition (`S3_ALT_EXPOSURE_SOURCE`)
- S4: Exclude early bleeding evidence proxy (`S4_EXCLUDE_EARLY_BLEED`)

**Display rule:** only rows where `outcome == cigib_strict`.

### Negative-control (falsification) window
The negative-control early window (`S5_NEG_CONTROL_WINDOW`) is a falsification test and is best shown in the **Supplement** (or explicitly separated from robustness sensitivities), because a strong early association is expected under residual confounding by indication.

### Figure 4B: Competing risk sensitivity (different estimand scale)
Competing-risk analysis uses a different effect measure; do not mix it into the HR forest without labeling.

- S6: Death as competing event (`S6_COMPETING_RISK_DEATH`)
  - Use `outcome == cigib_strict_competing_risk_death` (CIF-RR at 14d)

### Figure 4C (optional, main): Planned heterogeneity check (one subgroup only)
Include only the most clinically interpretable, pre-registered subgroup as a single main-figure panel:

- G1: SUP indication driver proxy (`G1_SUBGROUP_SUP_DRIVER`)
  - strata: `mv_1` vs `mv_0` (baseline mechanical ventilation evidence)
  - `outcome == cigib_strict`

If journal figure limits are tight, move Figure 4C to Supplement and keep only 4A + 4B in main.

## Supplement: Full Sensitivity + Subgroup Results

### Supplement Table S1 (recommended): Strict CIGIB across all sensitivities and all subgroups
Include **all** rows with:
- `outcome in {cigib_strict, cigib_strict_competing_risk_death}`
- all `sensitivity_id` in the registry (S1–S6, G1–G3)
- all `cohort` values (`mimic`, `eicu`, `pooled`)

### Supplement Table S2 (recommended): Secondary outcomes (mortality, broad UGIB, CDI)
Include all rows with:
- `outcome in {death, ugib_broad, cdi}`
- all `sensitivity_id` in the registry (S1–S6, G1–G3)
- all `cohort` values where finite

Also include the negative-control early window:
- `sensitivity_id == S5_NEG_CONTROL_WINDOW` and `outcome == cigib_strict`

**Interpretation rule:** treat CDI as secondary/exploratory unless outcome ascertainment is strengthened; the eICU mapping currently yields unstable/infinite estimates in some strata.

### Supplement Figure S1 (recommended): Subgroup forest plots (G2 and G3)
If space allows, add subgroup-only strict CIGIB forests:
- G2: liver disease (`G2_SUBGROUP_LIVER_DX`)
- G3: antithrombotic exposure (`G3_SUBGROUP_ANTITHROMBOTIC`)

## Text Placement (Results)

Main Results section should:
- mention Figure 4 as **robustness checks for strict CIGIB**,
- call out that S4 (early bleeding exclusion proxy) is expected to attenuate if baseline bleeding drives confounding,
- mention competing-risk sensitivity (Figure 4B) as a robustness check of conclusions rather than a replacement estimand,
- keep subgroup statements descriptive (direction + uncertainty), avoid subgroup-driven claims.
