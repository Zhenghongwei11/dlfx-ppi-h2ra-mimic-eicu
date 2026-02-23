# Protocol (Target Trial Emulation) — ICU SUP: PPI vs H2RA

> 中文摘要：这是一个“公开 ICU 电子病历队列”的可复现方案，用 **target trial emulation** 框架评估在符合应激性溃疡预防（SUP）指征的成人 ICU 患者中，**PPI vs H2RA** 对“临床重要上消化道出血（CIGIB）”的获益与对院内感染（至少 *C. difficile*）的潜在风险；并用 eICU 做外部验证，以降低“公开库公式化论文”被拒稿概率。

## 1. Study question (fixed)

Among adult ICU patients with SUP indications, what are the real‑world comparative effectiveness and safety of **proton pump inhibitors (PPIs)** versus **histamine‑2 receptor antagonists (H2RAs)**?

### Primary estimand

- Effect of initiating PPI (vs H2RA) within the first 24 hours of ICU stay on **14‑day risk** of **clinically important upper GI bleeding (CIGIB)**.

### Key secondary estimands

- Effect on **14‑day risk** of *Clostridioides difficile* infection (CDI).
- Effect on **28‑day (or in‑hospital) all‑cause mortality**.

## 2. Data sources

- **Primary (discovery):** MIMIC-IV (ICU + hospital EHR).
- **External validation:** eICU-CRD (multi‑center ICU EHR).

This repository contains **no patient-level data**. All analyses are performed locally after obtaining dataset access per PhysioNet requirements.

## 3. Target trial specification

### 3.1 Eligibility criteria

Population: adult ICU patients (≥18 years) who:

1) Have a first ICU stay (index ICU stay per patient; one stay per patient), and  
2) Are alive and still in ICU at **24 hours** after ICU admission (landmark), and  
3) Meet **SUP indication (high-risk) criteria** within the first 24 hours:
   - **Mechanical ventilation evidence** within 24 hours, **OR**
   - **Coagulopathy** within 24 hours (pre‑specified thresholds).

#### Pre-specified coagulopathy thresholds (baseline window: 0–24h)

- Platelets **< 50 × 10⁹/L** *or* INR **> 1.5**.

> Note: thresholds are fixed a priori and should not be tuned post hoc.

### 3.2 Treatment strategies (active comparator)

Exposure assessment window: **0–24 hours** from ICU admission.

- **PPI strategy:** receipt of any PPI medication administration (or an equivalent medication record source) within 0–24h.
- **H2RA strategy:** receipt of any H2RA medication administration within 0–24h.

Exclusions:

- Received **both** PPI and H2RA within 0–24h (dual exposure; ambiguous strategy).
- Received neither within 0–24h (not “initiators”; outside target trial).

### 3.3 Assignment procedure

Not randomized. We emulate randomization via:

- New‑initiator, active‑comparator design, and
- Propensity score (PS) weighting with pre‑specified covariate set measured pre‑index.

### 3.4 Time zero (index)

- **Index time = ICU admission + 24h** (landmark).

Rationale: avoids immortal time bias and ensures covariates and exposure window are completed prior to start of follow‑up.

### 3.5 Follow-up

Start: index time (ICU admission + 24h).

End: earliest of:

- outcome event,
- death,
- ICU discharge,
- hospital discharge,
- administrative censoring at **14 days** (for CIGIB/CDI), or **28 days** (for mortality).

## 4. Outcomes (pre-specified)

### 4.1 Primary outcome: CIGIB (two-tier definition)

We pre‑specify **two definitions** to address outcome misclassification concerns:

1) **Strict CIGIB** (primary): UGIB diagnosis evidence **plus** objective severity/management signal (e.g., PRBC transfusion and/or hemoglobin drop and/or endoscopy/hemostasis procedure) within a defined time window after index.
2) **Broad UGIB** (sensitivity): UGIB diagnosis evidence only.

> Exact code lists and timing windows are fixed in `protocol/codebook.md`.

### 4.2 Key secondary: CDI

Primary CDI definition prioritizes microbiology/toxin test evidence if available; ICD-coded CDI is used as a sensitivity definition.

### 4.3 Mortality

- In‑hospital mortality (primary, if available),
- 28‑day mortality (secondary).

## 5. Covariates (baseline window: 0–24h)

We pre‑specify a minimum sufficient set of confounders measured before index:

- Demographics: age, sex, race/ethnicity.
- Comorbidities: liver disease, CKD, prior GI ulcer/bleeding, diabetes (standard ICD-derived indices).
- Severity: SOFA/SAPS (prefer derived scores; fixed extraction method), shock/vasopressors, mechanical ventilation, RRT.
- Labs: hemoglobin, platelets, INR, creatinine, lactate (first value or worst value within 0–24h; fixed rule).
- Concomitant meds: anticoagulants/antiplatelets, systemic steroids, NSAIDs, antibiotics within 0–24h.

No post‑index variables are used in the PS model.

## 6. Primary analysis (causal inference)

### 6.1 Propensity score model

- Logistic regression PS with **restricted cubic splines / B‑splines** for continuous covariates (age, severity score, key labs).
- Categorical covariates modeled via indicator variables.

### 6.2 Weighting

- Use **stabilized inverse probability of treatment weights (IPTW)**.
- Weight truncation is **pre‑specified** at the **1st/99th percentiles** (and reported).

### 6.3 Balance diagnostics (must pass)

- Standardized mean differences (SMD) for all covariates pre/post weighting.
- Pass criterion: **|SMD| < 0.1** after weighting (primary threshold).
- Provide a love plot and balance table.

### 6.4 Effect estimation

Time-to-event outcomes:

- Primary: **weighted Cox proportional hazards model** with robust variance.
- Report HR with 95% CI.

Absolute risk at 14 days:

- Weighted Kaplan–Meier per group at day 14: risk = 1 − S(14d).
- Report risk difference (RD) and risk ratio (RR).

## 7. Sensitivity analyses (pre-specified)

1) **Landmark time**: repeat with landmark at 12h and 6h (same inclusion logic).
2) **Alternative exposure source**: medication administration vs prescription/orders (if both exist).
3) **Exclude early bleeding evidence**: exclude stays with UGIB evidence or PRBC transfusion in 0–24h (potentially therapeutic/active bleed).
4) **Negative-control time window**: evaluate an “early window” where a true causal effect is implausible to detect residual confounding.
5) **Competing risk (death)**: treat death as competing for CIGIB in a sensitivity model (Fine–Gray or Aalen–Johansen), if feasible in both datasets.

## 8. Subgroup analyses (limited, pre-registered)

- SUP indication driver: mechanical ventilation vs coagulopathy.
- Baseline liver disease: yes vs no.
- Baseline antithrombotic exposure: yes vs no.

Interpretation is cautious; no fishing across many subgroups.

## 9. External validation (eICU)

Replicate:

- same eligibility, exposure window, index time, follow-up,
- same outcome definitions and covariate set (mapped to available eICU fields),
- same PS/IPTW and effect estimation workflow.

Report:

- effect direction consistency,
- effect size similarity,
- heterogeneity discussion (coding systems, practice patterns, measurement differences).

## 10. Reporting & reproducibility

- Protocol frozen and shared (e.g., OSF) **before** outcome modeling.
- STROBE + RECORD checklist completed (`protocol/strobe_record_checklist.md`).
- Full code release: SQL, analysis scripts, environment lock, and deterministic outputs.

