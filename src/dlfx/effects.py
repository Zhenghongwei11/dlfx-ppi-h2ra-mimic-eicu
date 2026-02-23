from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class RiskEstimate:
    risk_treated: float
    risk_control: float
    rd: float
    rr: float
    rd_ci95: tuple[float, float] | None = None
    rr_ci95: tuple[float, float] | None = None


def _kish_effective_n(w: np.ndarray) -> float:
    w = w.astype(float)
    s1 = float(np.sum(w))
    s2 = float(np.sum(w**2))
    if s2 == 0:
        return 0.0
    return (s1**2) / s2


def _risk_ci_normal(p: float, n_eff: float) -> float:
    if n_eff <= 0:
        return float("nan")
    return float(np.sqrt(max(p * (1 - p), 1e-12) / n_eff))


def weighted_binary_risks(
    df: pd.DataFrame,
    *,
    treatment_indicator_col: str,
    event_col: str,
    weight_col: str,
) -> RiskEstimate:
    t = df[treatment_indicator_col].to_numpy(dtype=int)
    e = df[event_col].to_numpy(dtype=float)
    w = df[weight_col].to_numpy(dtype=float)

    mask1 = t == 1
    mask0 = t == 0

    r1 = float(np.sum(w[mask1] * e[mask1]) / np.sum(w[mask1]))
    r0 = float(np.sum(w[mask0] * e[mask0]) / np.sum(w[mask0]))

    rd = r1 - r0
    rr = float(r1 / r0) if r0 > 0 else float("inf")

    n1 = _kish_effective_n(w[mask1])
    n0 = _kish_effective_n(w[mask0])
    se1 = _risk_ci_normal(r1, n1)
    se0 = _risk_ci_normal(r0, n0)
    se_rd = float(np.sqrt(se1**2 + se0**2))
    rd_ci = (rd - 1.96 * se_rd, rd + 1.96 * se_rd)

    # Log(RR) delta method (approx)
    se_log_rr = float(np.sqrt((se1 / max(r1, 1e-12)) ** 2 + (se0 / max(r0, 1e-12)) ** 2))
    rr_ci = (
        float(np.exp(np.log(max(rr, 1e-12)) - 1.96 * se_log_rr)),
        float(np.exp(np.log(max(rr, 1e-12)) + 1.96 * se_log_rr)),
    )

    return RiskEstimate(
        risk_treated=r1,
        risk_control=r0,
        rd=rd,
        rr=rr,
        rd_ci95=rd_ci,
        rr_ci95=rr_ci,
    )


@dataclass(frozen=True)
class CoxEstimate:
    hr: float
    hr_ci95: tuple[float, float]


def fit_weighted_cox(
    df: pd.DataFrame,
    *,
    duration_col: str,
    event_col: str,
    weight_col: str,
    treatment_indicator_col: str,
) -> CoxEstimate:
    """
    Weighted Cox model with a single binary covariate (treatment indicator).

    This implementation avoids external survival-model dependencies so the
    scaffold runs on older Python versions. It uses a weighted partial
    likelihood with Breslow handling of ties.
    """

    cols = [duration_col, event_col, weight_col, treatment_indicator_col]
    work = df.loc[:, cols].copy()
    work = work.dropna(subset=cols)

    time = np.maximum(pd.to_numeric(work[duration_col], errors="coerce").to_numpy(dtype=float), 1e-12)
    event = pd.to_numeric(work[event_col], errors="coerce").to_numpy(dtype=int)
    w = pd.to_numeric(work[weight_col], errors="coerce").to_numpy(dtype=float)
    x = pd.to_numeric(work[treatment_indicator_col], errors="coerce").to_numpy(dtype=int)

    if not np.isin(x, [0, 1]).all():
        raise ValueError(f"{treatment_indicator_col} must be 0/1 for Cox fit.")

    # Build per-time summaries for Breslow ties.
    df0 = pd.DataFrame({"time": time, "event": event, "w": w, "x": x})
    df0 = df0.sort_values("time", ascending=False).reset_index(drop=True)

    by_time = (
        df0.assign(
            w_risk1=lambda d: d["w"] * (d["x"] == 1),
            w_risk0=lambda d: d["w"] * (d["x"] == 0),
            w_event1=lambda d: d["w"] * ((d["x"] == 1) & (d["event"] == 1)),
            w_event0=lambda d: d["w"] * ((d["x"] == 0) & (d["event"] == 1)),
        )
        .groupby("time", as_index=False)[["w_risk1", "w_risk0", "w_event1", "w_event0"]]
        .sum()
        .sort_values("time", ascending=False)
        .reset_index(drop=True)
    )

    # Cumulative risk-set weights for each event time (descending times).
    by_time["R1"] = by_time["w_risk1"].cumsum()
    by_time["R0"] = by_time["w_risk0"].cumsum()
    by_time["d1"] = by_time["w_event1"]
    by_time["d0"] = by_time["w_event0"]
    by_time["d"] = by_time["d1"] + by_time["d0"]

    event_times = by_time[by_time["d"] > 0].copy()
    if event_times.empty:
        raise ValueError("No events present for Cox fit.")

    r1 = event_times["R1"].to_numpy(dtype=float)
    r0 = event_times["R0"].to_numpy(dtype=float)
    d1 = event_times["d1"].to_numpy(dtype=float)
    d = event_times["d"].to_numpy(dtype=float)

    # Newton-Raphson for 1D beta
    beta = 0.0
    for _ in range(60):
        eb = float(np.exp(beta))
        denom = r1 * eb + r0
        u = float(np.sum(d1 - d * (r1 * eb / denom)))
        h = -float(np.sum(d * (r1 * eb * r0) / (denom**2)))
        step = u / h
        beta_new = beta - step
        if abs(beta_new - beta) < 1e-10:
            beta = beta_new
            break
        beta = beta_new

    # Robust (sandwich) variance using individual score contributions.
    eb = float(np.exp(beta))
    event_times = event_times.assign(ex_bar=(event_times["R1"] * eb) / (event_times["R1"] * eb + event_times["R0"]))
    ex_map = dict(zip(event_times["time"].to_numpy(dtype=float), event_times["ex_bar"].to_numpy(dtype=float)))

    ev = df0[df0["event"] == 1].copy()
    ev["ex_bar"] = ev["time"].map(ex_map).astype(float)
    score_i = ev["w"].to_numpy(dtype=float) * (ev["x"].to_numpy(dtype=float) - ev["ex_bar"].to_numpy(dtype=float))

    denom = r1 * eb + r0
    i_obs = float(np.sum(d * (r1 * eb * r0) / (denom**2)))
    meat = float(np.sum(score_i**2))
    var_robust = meat / (i_obs**2) if i_obs > 0 else float("nan")
    se = float(np.sqrt(max(var_robust, 1e-24)))

    hr = float(np.exp(beta))
    hr_ci = (float(np.exp(beta - 1.96 * se)), float(np.exp(beta + 1.96 * se)))
    return CoxEstimate(hr=hr, hr_ci95=hr_ci)


def weighted_km_risk_at(
    df: pd.DataFrame,
    *,
    duration_col: str,
    event_col: str,
    weight_col: str,
    horizon_days: float,
) -> float:
    """
    Weighted Kaplan–Meier risk at a fixed horizon.

    Uses frequency-style weights:
      n(t) = sum(weights of subjects at risk just before t)
      d(t) = sum(weights of events at t)
      S(t) = Π (1 - d(t)/n(t))
    """
    work = df[[duration_col, event_col, weight_col]].dropna().copy()
    if work.empty:
        return float("nan")

    work[duration_col] = pd.to_numeric(work[duration_col], errors="coerce").astype(float)
    work[event_col] = pd.to_numeric(work[event_col], errors="coerce").astype(int)
    work[weight_col] = pd.to_numeric(work[weight_col], errors="coerce").astype(float)

    work = work.sort_values(duration_col, ascending=True)
    total_w = float(work[weight_col].sum())
    if total_w <= 0:
        return float("nan")

    grouped = (
        work.assign(
            w_total=lambda d: d[weight_col],
            w_event=lambda d: d[weight_col] * (d[event_col] == 1),
        )
        .groupby(duration_col, as_index=False)[["w_total", "w_event"]]
        .sum()
        .sort_values(duration_col, ascending=True)
        .reset_index(drop=True)
    )

    s = 1.0
    risk = total_w
    for _, row in grouped.iterrows():
        t = float(row[duration_col])
        if t > horizon_days:
            break
        d = float(row["w_event"])
        if risk > 0 and d > 0:
            s *= max(0.0, 1.0 - (d / risk))
        risk -= float(row["w_total"])

    return 1.0 - float(s)


def _weighted_aj_cif_at(
    *,
    time: np.ndarray,
    status: np.ndarray,
    weight: np.ndarray,
    horizon_days: float,
) -> float:
    """
    Weighted Aalen–Johansen cumulative incidence function (CIF) for cause 1.

    status coding:
      0 = censored
      1 = event of interest
      2 = competing event
    """
    t = np.asarray(time, dtype=float)
    s = np.asarray(status, dtype=int)
    w = np.asarray(weight, dtype=float)

    mask = np.isfinite(t) & np.isfinite(w) & (w > 0)
    if not np.any(mask):
        return float("nan")
    t = np.minimum(t[mask], float(horizon_days))
    s = s[mask]
    w = w[mask]

    order = np.argsort(t, kind="mergesort")
    t = t[order]
    s = s[order]
    w = w[order]

    uniq, idx = np.unique(t, return_index=True)
    w_total = np.add.reduceat(w, idx)
    w_d1 = np.add.reduceat(w * (s == 1), idx)
    w_d2 = np.add.reduceat(w * (s == 2), idx)

    total_w = float(np.sum(w_total))
    if total_w <= 0:
        return float("nan")

    cum_before = np.concatenate([[0.0], np.cumsum(w_total)[:-1]])
    y = total_w - cum_before  # at-risk weight just before each uniq time

    surv = 1.0
    cif = 0.0
    for ti, yi, d1i, d2i in zip(uniq, y, w_d1, w_d2):
        if float(ti) > float(horizon_days):
            break
        yi = float(yi)
        d1i = float(d1i)
        d2i = float(d2i)
        if yi <= 0:
            continue
        if d1i > 0:
            cif += surv * (d1i / yi)
        if (d1i + d2i) > 0:
            surv *= max(0.0, 1.0 - ((d1i + d2i) / yi))

    return float(cif)


def weighted_competing_risk_cif_rr_at(
    df: pd.DataFrame,
    *,
    treatment_indicator_col: str,
    interest_event_col: str,
    interest_time_col: str,
    competing_event_col: str,
    competing_time_col: str,
    weight_col: str,
    horizon_days: float,
    n_bootstrap: int = 0,
    seed: int = 0,
) -> RiskEstimate:
    """
    Two-arm competing-risk summary for cause-1 CIF at a fixed horizon:
    - CIF_treated, CIF_control (Aalen–Johansen, weighted)
    - RD = CIF_treated - CIF_control
    - RR = CIF_treated / CIF_control

    If n_bootstrap > 0, compute percentile 95% CIs for RD and RR via stratified bootstrap by treatment group.
    """
    cols = [
        treatment_indicator_col,
        interest_event_col,
        interest_time_col,
        competing_event_col,
        competing_time_col,
        weight_col,
    ]
    work = df.loc[:, cols].copy()
    work = work.dropna(subset=[treatment_indicator_col, interest_event_col, interest_time_col, weight_col])

    t = pd.to_numeric(work[treatment_indicator_col], errors="coerce").to_numpy(dtype=int)
    ei = pd.to_numeric(work[interest_event_col], errors="coerce").fillna(0).to_numpy(dtype=int)
    ti = pd.to_numeric(work[interest_time_col], errors="coerce").to_numpy(dtype=float)
    ec = pd.to_numeric(work[competing_event_col], errors="coerce").fillna(0).to_numpy(dtype=int)
    tc = pd.to_numeric(work[competing_time_col], errors="coerce").to_numpy(dtype=float)
    w = pd.to_numeric(work[weight_col], errors="coerce").to_numpy(dtype=float)

    horizon = float(horizon_days)
    censor_time = np.minimum(ti, horizon)
    interest_time = np.where(ei == 1, np.minimum(ti, horizon), np.inf)
    comp_time = np.where((ec == 1) & np.isfinite(tc) & (tc <= horizon), np.minimum(tc, horizon), np.inf)

    time = censor_time.copy()
    status = np.zeros_like(ei, dtype=int)
    m1 = (interest_time <= comp_time) & (interest_time <= censor_time)
    m2 = (comp_time < interest_time) & (comp_time <= censor_time)
    status[m1] = 1
    time[m1] = interest_time[m1]
    status[m2] = 2
    time[m2] = comp_time[m2]

    mask1 = t == 1
    mask0 = t == 0
    cif1 = _weighted_aj_cif_at(time=time[mask1], status=status[mask1], weight=w[mask1], horizon_days=horizon)
    cif0 = _weighted_aj_cif_at(time=time[mask0], status=status[mask0], weight=w[mask0], horizon_days=horizon)

    rd = float(cif1 - cif0)
    rr = float(cif1 / cif0) if cif0 > 0 else float("inf")

    rd_ci: tuple[float, float] | None = None
    rr_ci: tuple[float, float] | None = None
    if int(n_bootstrap) > 0:
        rng = np.random.default_rng(int(seed))
        idx1 = np.flatnonzero(mask1)
        idx0 = np.flatnonzero(mask0)
        if idx1.size > 0 and idx0.size > 0:
            rd_s = []
            rr_s = []
            for _ in range(int(n_bootstrap)):
                b1 = rng.integers(0, idx1.size, size=idx1.size)
                b0 = rng.integers(0, idx0.size, size=idx0.size)
                s1 = idx1[b1]
                s0 = idx0[b0]
                bcif1 = _weighted_aj_cif_at(time=time[s1], status=status[s1], weight=w[s1], horizon_days=horizon)
                bcif0 = _weighted_aj_cif_at(time=time[s0], status=status[s0], weight=w[s0], horizon_days=horizon)
                if np.isfinite(bcif1) and np.isfinite(bcif0):
                    rd_s.append(float(bcif1 - bcif0))
                    rr_s.append(float(bcif1 / bcif0) if bcif0 > 0 else float("inf"))
            if rd_s:
                rd_ci = (float(np.quantile(rd_s, 0.025)), float(np.quantile(rd_s, 0.975)))
            rr_s_f = [x for x in rr_s if np.isfinite(x) and x > 0]
            if rr_s_f:
                rr_ci = (float(np.quantile(rr_s_f, 0.025)), float(np.quantile(rr_s_f, 0.975)))

    return RiskEstimate(
        risk_treated=float(cif1),
        risk_control=float(cif0),
        rd=rd,
        rr=rr,
        rd_ci95=rd_ci,
        rr_ci95=rr_ci,
    )
