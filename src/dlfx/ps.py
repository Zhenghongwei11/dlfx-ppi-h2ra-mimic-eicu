from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, SplineTransformer, StandardScaler


@dataclass(frozen=True)
class PSConfig:
    spline_continuous: bool = True
    spline_n_knots: int = 5
    spline_degree: int = 3
    weight_truncation: Tuple[float, float] = (0.01, 0.99)  # quantiles
    ps_clip: Tuple[float, float] = (1e-6, 1 - 1e-6)
    random_state: int = 7


def fit_propensity_score(
    df: pd.DataFrame,
    *,
    treatment_indicator: pd.Series,
    covariates: Sequence[str],
    categorical: Sequence[str],
    continuous: Sequence[str],
    config: PSConfig = PSConfig(),
) -> tuple[np.ndarray, np.ndarray, Pipeline]:
    # Drop columns that are entirely missing to avoid imputer failures.
    categorical = [c for c in categorical if df[c].notna().any()]
    continuous = [c for c in continuous if pd.to_numeric(df[c], errors="coerce").notna().any()]
    covariates = [c for c in covariates if (c in categorical) or (c in continuous)]
    if not covariates:
        raise ValueError("No usable covariates remain after dropping all-missing columns.")

    x = df.loc[:, list(covariates)].copy()
    y = treatment_indicator.to_numpy(dtype=int)

    num_steps = [("impute", SimpleImputer(strategy="median"))]
    if config.spline_continuous and continuous:
        num_steps.append(
            (
                "spline",
                SplineTransformer(
                    n_knots=config.spline_n_knots,
                    degree=config.spline_degree,
                    include_bias=False,
                ),
            )
        )
    num_steps.append(("scale", StandardScaler(with_mean=False)))
    num_pipe = Pipeline(steps=num_steps)

    cat_pipe = Pipeline(
        steps=[
            ("impute", SimpleImputer(strategy="constant", fill_value="missing")),
            (
                "onehot",
                OneHotEncoder(handle_unknown="ignore", sparse_output=True),
            ),
        ]
    )

    transformers = []
    if continuous:
        transformers.append(("num", num_pipe, list(continuous)))
    if categorical:
        transformers.append(("cat", cat_pipe, list(categorical)))
    pre = ColumnTransformer(transformers=transformers, remainder="drop", sparse_threshold=0.3)

    clf = LogisticRegression(
        solver="saga",
        max_iter=5000,
        random_state=config.random_state,
    )

    model = Pipeline(steps=[("pre", pre), ("clf", clf)])
    model.fit(x, y)

    ps = model.predict_proba(x)[:, 1]
    ps = np.clip(ps, config.ps_clip[0], config.ps_clip[1])

    p_treated = float(np.mean(y))
    weights = np.where(y == 1, p_treated / ps, (1.0 - p_treated) / (1.0 - ps))

    lo_q, hi_q = config.weight_truncation
    if not (0.0 <= lo_q < hi_q <= 1.0):
        raise ValueError(f"Invalid truncation quantiles: {config.weight_truncation}")
    lo = float(np.quantile(weights, lo_q))
    hi = float(np.quantile(weights, hi_q))
    weights = np.clip(weights, lo, hi)

    return ps, weights, model
