from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SyntheticConfig:
    n: int = 2000
    seed: int = 11


def make_synthetic_analysis_table(cfg: SyntheticConfig = SyntheticConfig()) -> pd.DataFrame:
    """
    Create a synthetic, analysis-ready table that matches the expected schema
    for `scripts/run_primary_analysis.py`.

    This is ONLY for pipeline testing; it does not represent real clinical data.
    """
    rng = np.random.default_rng(cfg.seed)
    n = int(cfg.n)

    age = np.clip(rng.normal(62, 14, size=n), 18, 95)
    sex = rng.choice(["M", "F"], size=n, p=[0.6, 0.4])
    race = rng.choice(["WHITE", "BLACK", "ASIAN", "HISPANIC", "OTHER"], size=n, p=[0.55, 0.12, 0.08, 0.15, 0.10])

    sofa = np.clip(rng.normal(6.5, 3.0, size=n), 0, 24)
    hgb_min = np.clip(rng.normal(10.5, 2.0, size=n), 5, 16)
    platelet_min = np.clip(rng.lognormal(mean=np.log(180), sigma=0.5, size=n), 5, 600)
    inr_max = np.clip(rng.lognormal(mean=np.log(1.2), sigma=0.25, size=n), 0.8, 6.0)
    creat_max = np.clip(rng.lognormal(mean=np.log(1.1), sigma=0.4, size=n), 0.3, 12.0)
    lact_max = np.clip(rng.lognormal(mean=np.log(2.0), sigma=0.5, size=n), 0.4, 18.0)

    # Treatment assignment with confounding (PPI more likely in sicker patients).
    lin = (
        -0.2
        + 0.03 * (age - 60)
        + 0.10 * (sofa - 6)
        + 0.25 * (inr_max - 1.2)
        + 0.15 * (creat_max - 1.1)
        + 0.05 * (lact_max - 2.0)
        + (sex == "M") * 0.15
    )
    p_ppi = 1.0 / (1.0 + np.exp(-lin))
    treat_ppi = rng.binomial(1, p_ppi, size=n).astype(int)
    treatment = np.where(treat_ppi == 1, "ppi", "h2ra")

    # Time-to-event outcome: death (hazard depends on covariates; modest treatment effect).
    # HR(PPI vs H2RA) ~ 0.95 (slight benefit), but confounded.
    base_hazard = 0.05  # per day
    true_hr = 0.95
    hazard = base_hazard * np.exp(
        0.015 * (age - 60)
        + 0.08 * (sofa - 6)
        + 0.18 * (creat_max - 1.1)
        + 0.08 * (lact_max - 2.0)
    ) * np.where(treat_ppi == 1, true_hr, 1.0)

    t_event = rng.exponential(scale=1.0 / hazard)
    t_censor = rng.uniform(1, 28, size=n)
    death_time = np.minimum(t_event, t_censor)
    death_event = (t_event <= t_censor).astype(int)

    # Time-to-event outcome: strict CIGIB (rare; driven by coagulopathy/low Hb; treatment effect).
    base_hazard_gib = 0.006  # per day
    true_hr_gib = 0.90  # PPI reduces bleeding modestly
    hazard_gib = base_hazard_gib * np.exp(
        0.10 * (sofa - 6)
        + 0.55 * (inr_max - 1.2)
        + 0.002 * (300 - np.clip(platelet_min, 0, 300))
        + 0.12 * (10.5 - hgb_min)
    ) * np.where(treat_ppi == 1, true_hr_gib, 1.0)
    t_event_gib = rng.exponential(scale=1.0 / hazard_gib)
    t_censor_gib = rng.uniform(1, 14, size=n)
    gib_time = np.minimum(t_event_gib, t_censor_gib)
    gib_event = (t_event_gib <= t_censor_gib).astype(int)

    # Binary outcomes as additional placeholders (not time-stamped).
    ugib_broad = rng.binomial(1, p=0.06 + 0.01 * (sofa > 10), size=n)
    cdi = rng.binomial(1, p=0.03 + 0.01 * (lact_max > 4), size=n)

    df = pd.DataFrame(
        {
            "dataset": "synthetic",
            "stay_id": np.arange(1, n + 1),
            "patient_id": np.arange(1, n + 1),
            "age_years": age,
            "sex": sex,
            "race": race,
            "sofa_24h": sofa,
            "hgb_min_24h": hgb_min,
            "platelet_min_24h": platelet_min,
            "inr_max_24h": inr_max,
            "creatinine_max_24h": creat_max,
            "lactate_max_24h": lact_max,
            "treatment": treatment,
            "ugib_broad_event": ugib_broad,
            "ugib_broad_time_days": np.nan,
            "cdi_event": cdi,
            "cdi_time_days": np.nan,
            "death_event_28d": death_event,
            "death_time_days": death_time,
            "cigib_strict_event": gib_event,
            "cigib_strict_time_days": gib_time,
        }
    )
    return df
