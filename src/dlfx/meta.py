from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class MetaResult:
    ratio: float
    ratio_lo: float
    ratio_hi: float
    tau2: float


def _log_se_from_ci(est: float, lo: float, hi: float) -> float:
    return float((np.log(hi) - np.log(lo)) / (2.0 * 1.96))


def random_effects_meta_ratio(
    *,
    ratios: np.ndarray,
    ratio_los: np.ndarray,
    ratio_his: np.ndarray,
) -> Optional[MetaResult]:
    """
    DerSimonian-Laird random-effects meta-analysis on log ratio scale.
    Requires >=2 studies with finite CIs.
    """
    ratios = np.asarray(ratios, dtype=float)
    los = np.asarray(ratio_los, dtype=float)
    his = np.asarray(ratio_his, dtype=float)
    mask = np.isfinite(ratios) & np.isfinite(los) & np.isfinite(his) & (ratios > 0) & (los > 0) & (his > 0)
    ratios = ratios[mask]
    los = los[mask]
    his = his[mask]
    if ratios.size < 2:
        return None

    yi = np.log(ratios)
    sei = np.array([_log_se_from_ci(r, lo, hi) for r, lo, hi in zip(ratios, los, his)], dtype=float)
    vi = sei**2
    wi = 1.0 / vi
    y_fixed = float(np.sum(wi * yi) / np.sum(wi))
    q = float(np.sum(wi * (yi - y_fixed) ** 2))
    df = ratios.size - 1
    c = float(np.sum(wi) - (np.sum(wi**2) / np.sum(wi)))
    tau2 = max(0.0, (q - df) / c) if c > 0 else 0.0

    wi_star = 1.0 / (vi + tau2)
    y_re = float(np.sum(wi_star * yi) / np.sum(wi_star))
    se_re = float(np.sqrt(1.0 / np.sum(wi_star)))
    lo = float(np.exp(y_re - 1.96 * se_re))
    hi = float(np.exp(y_re + 1.96 * se_re))
    return MetaResult(ratio=float(np.exp(y_re)), ratio_lo=lo, ratio_hi=hi, tau2=float(tau2))


def combine_effect_tables(
    effects_a: pd.DataFrame,
    effects_b: pd.DataFrame,
    *,
    label_a: str,
    label_b: str,
) -> pd.DataFrame:
    """
    Merge two effect tables (from `tables/effect_estimates.csv`) and compute a pooled RE estimate when possible.
    """
    need = {"outcome", "outcome_label", "ratio", "ratio_lo", "ratio_hi", "effect_type"}
    for name, df in [("A", effects_a), ("B", effects_b)]:
        missing = need - set(df.columns)
        if missing:
            raise ValueError(f"Effects table {name} missing columns: {sorted(missing)}")

    a = effects_a.copy()
    b = effects_b.copy()
    a = a.rename(
        columns={
            "ratio": f"ratio_{label_a}",
            "ratio_lo": f"ratio_lo_{label_a}",
            "ratio_hi": f"ratio_hi_{label_a}",
            "effect_type": f"effect_type_{label_a}",
        }
    )
    b = b.rename(
        columns={
            "ratio": f"ratio_{label_b}",
            "ratio_lo": f"ratio_lo_{label_b}",
            "ratio_hi": f"ratio_hi_{label_b}",
            "effect_type": f"effect_type_{label_b}",
        }
    )
    keep_a = ["outcome", "outcome_label", f"ratio_{label_a}", f"ratio_lo_{label_a}", f"ratio_hi_{label_a}", f"effect_type_{label_a}"]
    keep_b = ["outcome", f"ratio_{label_b}", f"ratio_lo_{label_b}", f"ratio_hi_{label_b}", f"effect_type_{label_b}"]
    m = a[keep_a].merge(b[keep_b], on="outcome", how="outer")

    pooled_rows = []
    for _, row in m.iterrows():
        ratios = []
        los = []
        his = []
        effect_types = []
        for lab in [label_a, label_b]:
            r = row.get(f"ratio_{lab}")
            lo = row.get(f"ratio_lo_{lab}")
            hi = row.get(f"ratio_hi_{lab}")
            if pd.notna(r) and pd.notna(lo) and pd.notna(hi):
                ratios.append(float(r))
                los.append(float(lo))
                his.append(float(hi))
                et = row.get(f"effect_type_{lab}")
                if pd.notna(et):
                    effect_types.append(str(et))

        # Do not meta-analyze across incompatible estimands (e.g., HR vs RR).
        # We only pool when all included studies share the same effect_type.
        if len(set(effect_types)) > 1:
            meta = None
        else:
            meta = random_effects_meta_ratio(ratios=np.array(ratios), ratio_los=np.array(los), ratio_his=np.array(his))
        pooled_rows.append(
            {
                "outcome": row["outcome"],
                "pooled_ratio": meta.ratio if meta else np.nan,
                "pooled_ratio_lo": meta.ratio_lo if meta else np.nan,
                "pooled_ratio_hi": meta.ratio_hi if meta else np.nan,
                "pooled_tau2": meta.tau2 if meta else np.nan,
            }
        )
    pooled = pd.DataFrame(pooled_rows)
    return m.merge(pooled, on="outcome", how="left")
