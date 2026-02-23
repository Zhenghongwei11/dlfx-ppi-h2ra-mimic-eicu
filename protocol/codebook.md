# Codebook — analysis-ready table schema (MIMIC + eICU)

This document fixes **variable definitions**, **time windows**, and **code lists** used in the target trial emulation.

> All variables are defined at the **ICU stay** level (one row per stay).

## 1) Identifiers and timestamps

- `dataset`: `"mimic"` or `"eicu"`
- `stay_id`: ICU stay identifier (MIMIC: `stay_id`; eICU: `patientunitstayid`)
- `patient_id`: patient identifier (MIMIC: `subject_id`; eICU: `uniquepid` or equivalent)
- `icu_intime`: ICU admission timestamp
- `index_time`: `icu_intime + landmark_hours` (default landmark = 24)

## 2) Eligibility / design variables

Baseline window: `[icu_intime, icu_intime + 24h)` (unless landmark differs in sensitivity runs).

- `age_years`: age at hospital admission
- `is_first_icu_stay`: 1 if first ICU stay for patient
- `alive_in_icu_at_landmark`: 1 if alive and still in ICU at landmark
- `sup_indication_mv_24h`: 1 if mechanical ventilation evidence within baseline window
- `sup_indication_coagulopathy_24h`: 1 if coagulopathy within baseline window
- `eligible_sup_high_risk`: 1 if `sup_indication_mv_24h==1 OR sup_indication_coagulopathy_24h==1`

### Coagulopathy thresholds (fixed)

- `platelet_min_24h` < 50 (×10⁹/L), OR
- `inr_max_24h` > 1.5

## 3) Treatment assignment (active comparator)

Exposure window: `[icu_intime, icu_intime + 24h)`.

- `treatment`: categorical, one of:
  - `"ppi"`
  - `"h2ra"`
- `ppi_any_24h`: 1 if any PPI in exposure window
- `h2ra_any_24h`: 1 if any H2RA in exposure window
- `dual_ppi_h2ra_24h`: 1 if both

### Medication lists (high-level)

PPIs (examples): omeprazole, pantoprazole, esomeprazole, lansoprazole, rabeprazole.  
H2RAs (examples): famotidine, ranitidine, cimetidine, nizatidine.

> Exact string matching rules are implemented in code and can be adapted per dataset medication dictionary.

## 4) Outcomes (time-to-event)

All outcomes are assessed **after index_time**.

Time unit in analysis table: **days** from index.

### 4.1 Primary outcome: strict CIGIB

- `cigib_strict_event`: 1 if strict definition met within 14 days
- `cigib_strict_time_days`: time from index to strict CIGIB event (censored otherwise)

Strict CIGIB components (fixed, post-index window):

- UGIB diagnosis evidence (ICD-based), AND at least one of:
  - PRBC transfusion evidence, OR
  - hemoglobin drop (ΔHb) meeting threshold, OR
  - endoscopy/hemostasis procedure evidence

> Implementation detail: the first version uses a **conservative** combination rule; if a lab/procedure source is missing in a dataset, we fall back to a pre-defined alternative and document the limitation.

### 4.2 Broad UGIB (sensitivity)

- `ugib_broad_event`
- `ugib_broad_time_days`

UGIB diagnosis evidence only (ICD-based).

### 4.3 *C. difficile* infection (CDI)

- `cdi_event`
- `cdi_time_days`

Primary CDI: microbiology/toxin positive if available; ICD-based CDI as sensitivity.

### 4.4 Mortality

- `death_event_28d`: 1 if death within 28 days of index (or in-hospital)
- `death_time_days`: time to death from index

## 5) Baseline covariates (0–24h)

### Demographics

- `sex`: categorical
- `race`: categorical

### Severity and support

- `sofa_24h`: SOFA score (preferred: derived table)
- `vasopressor_any_24h`: 1 if any vasopressor
- `rrt_any_24h`: 1 if renal replacement therapy
- `mv_any_24h`: 1 if mechanical ventilation evidence

### Labs (fixed extraction rule)

Use a single pre-defined rule (choose **one** and apply consistently):

- Rule A (recommended): first measurement within 0–24h
- Rule B: worst value within 0–24h (pre-specified direction)

Variables:

- `hgb_first_24h`, `hgb_min_24h`
- `platelet_first_24h`, `platelet_min_24h`
- `inr_first_24h`, `inr_max_24h`
- `creatinine_first_24h`, `creatinine_max_24h`
- `lactate_first_24h`, `lactate_max_24h`

### Comorbidities (ICD-derived)

- `liver_disease`: 1/0
- `ckd`: 1/0
- `prior_pud_or_gib`: 1/0
- `diabetes`: 1/0
- Optional: Charlson/Elixhauser index.

### Concomitant meds (0–24h)

- `antithrombotic_any_24h`: 1/0 (combined anticoagulant or antiplatelet proxy; used for subgroup analyses)
- `steroid_any_24h`
- `nsaid_any_24h`
- `antibiotic_any_24h`

## 6) Censoring / follow-up

- `followup_end_time`: min(outcome, death, ICU discharge, hospital discharge, admin censor)
- `followup_time_days`: duration from index to follow-up end

## 7) Code lists (initial minimal set)

> These lists are intentionally conservative; extend only with justification and keep versioned.

### 7.1 UGIB ICD-10 (examples)

- K92.0 Hematemesis
- K92.1 Melena
- K92.2 Gastrointestinal hemorrhage, unspecified
- K25.* with hemorrhage, K26.* with hemorrhage, K27.*, K28.*

### 7.2 UGIB ICD-9 (examples)

- 578.0 Hematemesis
- 578.1 Blood in stool
- 578.9 GI hemorrhage unspecified
- 531.* / 532.* / 533.* / 534.* with hemorrhage

### 7.3 CDI ICD-10 / ICD-9 (examples)

- ICD-10: A04.7
- ICD-9: 008.45
