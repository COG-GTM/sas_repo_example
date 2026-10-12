"""Checks survey_estimate() against R survey output on a small synthetic design.

The design has two singleton (lonely-PSU) strata, so it exercises
options(survey.lonely.psu = "adjust") as well as nest = TRUE, domains (svyby),
subpopulations (subset), totals, means, and ratios. Expected values were generated
with make_expected_r.R (R survey package).

Run with:  python -m pytest Python/tests
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

HERE = Path(__file__).resolve().parent
sys.path.append(str(HERE.parent))
from meps_utils import survey_estimate

DATA = pd.read_csv(HERE / "synthetic_design.csv")
EXPECTED = pd.read_csv(HERE / "synthetic_expected_r.csv", dtype={"level": str})


@pytest.mark.parametrize("case", EXPECTED.groupby(["stat", "by", "subpop"]).groups.keys())
def test_matches_r(case):
    stat, by, subpop = case
    expected = EXPECTED[(EXPECTED.stat == stat) & (EXPECTED.by == by) & (EXPECTED.subpop == subpop)]
    out = survey_estimate(
        DATA, "Y", weight="WT", stat=stat,
        domain=None if by == "none" else by,
        subpop=None if subpop == "none" else DATA[subpop] == 1,
        denominator="X" if stat == "ratio" else None,
    )
    out["level"] = out[by] if by != "none" else "all"
    merged = expected.merge(out, on="level", suffixes=("_r", "_py"), validate="1:1")
    assert len(merged) == len(expected)
    np.testing.assert_allclose(merged.estimate_py, merged.estimate_r, rtol=1e-6)
    np.testing.assert_allclose(merged.se_py, merged.se_r, rtol=1e-6)


def test_domain_estimation_keeps_full_design():
    """Subpopulation SEs must differ from (naively) subsetting rows first.

    The subpopulation excludes a whole PSU, so dropping rows would also drop that PSU
    from the design and understate the variance.
    """
    sub = (DATA["FLAG"] == 1) & ~((DATA["VARSTR"] == 1) & (DATA["VARPSU"] == 1))
    full = survey_estimate(DATA, "Y", weight="WT", stat="total", subpop=sub)
    naive = survey_estimate(DATA[sub], "Y", weight="WT", stat="total")
    assert full.estimate.iloc[0] == pytest.approx(naive.estimate.iloc[0])
    assert full.se.iloc[0] != pytest.approx(naive.se.iloc[0])


def test_lonely_psu_options():
    with pytest.raises(ValueError, match="single PSU"):
        survey_estimate(DATA, "Y", weight="WT", lonely_psu="fail")
    adjust = survey_estimate(DATA, "Y", weight="WT").se.iloc[0]
    remove = survey_estimate(DATA, "Y", weight="WT", lonely_psu="remove").se.iloc[0]
    assert adjust > remove


def test_zero_estimate_domain():
    df = DATA.assign(Z=np.where(DATA.GRP == 1, 0.0, DATA.Y))
    out = survey_estimate(df, "Z", weight="WT", domain="GRP")
    assert out.loc[out.GRP == "1", ["estimate", "se"]].to_numpy().tolist() == [[0.0, 0.0]]
