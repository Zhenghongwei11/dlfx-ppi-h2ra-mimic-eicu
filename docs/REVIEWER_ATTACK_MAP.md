# Reviewer Attack Map (ICU EHR cohort: PPI vs H2RA)

This file lists likely reviewer criticisms and the pre-specified outputs that address them.

| Risk | Why it matters | Mitigation | Expected outputs |
| --- | --- | --- | --- |
| Residual confounding / confounding by indication | Non-random treatment assignment | Active-comparator new-initiator design; IPTW; balance diagnostics; negative-control window | `results/balance/*`, `plots/balance/*`, `results/sensitivity/S5_negative_control_window.tsv` |
| Poor overlap / positivity violations | Extreme weights can dominate results | Weight truncation (pre-specified); overlap plots; report effective sample size | `results/balance/*`, `results/diagnostics/*` |
| Immortal time bias | Wrong time zero inflates effects | Landmark design; sensitivity with 12h/6h landmark | `results/sensitivity/S1_landmark_12h.tsv`, `results/sensitivity/S2_landmark_6h.tsv` |
| Outcome misclassification (CIGIB/CDI) | EHR outcomes are noisy | Dual definitions (strict + broad); sensitivity analyses; transparent code lists | `results/sensitivity/*`, protocol code lists (`protocol/codebook.md`) |
| Competing risk of death | Death prevents observing bleeding | Competing-risk sensitivity if feasible | `results/sensitivity/S6_competing_risk.tsv` |
| External validity | Single-center results may not generalize | External validation in eICU; heterogeneity discussion | `results/replication/*`, `plots/replication/*` |
| Selective analyses / p-hacking | Unplanned analyses reduce credibility | Pre-registered sensitivity registry; protocol freeze + deviation log | `docs/PROTOCOL_FREEZE.md`, `docs/SENSITIVITY_REGISTRY.tsv` |

