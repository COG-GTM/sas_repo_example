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
from meps_utils import survey_estimate, survey_quantile

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


def test_missing_domain_values_are_excluded_not_reported():
    df = DATA.copy()
    df["grp"] = pd.Series(np.where(df.index % 2 == 0, "a", "b"), dtype=object)
    df.loc[df.index % 3 == 0, "grp"] = None
    out = survey_estimate(df, "Y", weight="WT", stat="total", domain="grp")
    assert sorted(out["grp"]) == ["a", "b"]


def test_domain_values_containing_delimiter_stay_distinct():
    df = DATA.copy()
    df["d1"] = np.where(df.index % 2 == 0, "A|B", "A")
    df["d2"] = np.where(df.index % 2 == 0, "C", "B|C")
    out = survey_estimate(df, "Y", weight="WT", stat="total", domain=["d1", "d2"])
    assert len(out) == 2
    assert set(zip(out["d1"], out["d2"])) == {("A|B", "C"), ("A", "B|C")}


def test_empty_subpopulation_returns_empty_frame():
    out = survey_estimate(DATA, "Y", weight="WT", stat="mean", subpop=DATA["Y"] < -1e9)
    assert out.empty
    assert list(out.columns) == ["variable", "estimate", "se"]


def test_quantile_matches_r_svyquantile():
    expected = pd.read_csv(HERE / "synthetic_expected_quantile_r.csv", dtype={"level": str})
    overall = survey_quantile(DATA, "Y", weight="WT", quantile=0.5)
    by_grp = survey_quantile(DATA, "Y", weight="WT", quantile=0.5, domain="GRP", subpop=DATA["FLAG"] == 1)
    got = pd.concat([overall.assign(level="all"), by_grp.assign(level=by_grp["GRP"])])
    merged = expected.merge(got, on="level", suffixes=("_r", "_py"), validate="one_to_one")
    assert len(merged) == len(expected)
    np.testing.assert_allclose(merged["estimate_py"], merged["estimate_r"], rtol=1e-10)
    np.testing.assert_allclose(merged["se_py"], merged["se_r"], rtol=1e-8)


def test_quantile_skips_zero_weight_domain_and_uses_exact_boundary():
    df = pd.DataFrame({"VARSTR": [1, 1, 1, 1], "VARPSU": [1, 2, 1, 2], "WT": [1.0, 2.0, 0.0, 0.0],
                       "Y": [1.0, 2.0, 3.0, 4.0], "D": ["A", "A", "B", "B"]})
    res = survey_quantile(df, "Y", weight="WT", domain="D")
    assert res["D"].tolist() == ["A"]
    from meps_utils import _qrule_math
    x, w = np.array([100.0, 200.0]), np.array([499999999999999.0, 500000000000001.0])
    assert _qrule_math(x, w, 0.5) == 200.0
    assert _qrule_math(np.array([1.0, 2.0]), np.array([1.0, 1.0]), 0.5) == 1.0


def test_survey_glm_matches_r_svyglm_quasibinomial():
    # Reference: R survey 4.1.1, lonely.psu="adjust" (stratum 6 has one PSU)
    #   summary(svyglm(Y ~ AGE + as.factor(G), design = subset(des, SUB), family = quasibinomial))
    from meps_utils import survey_glm
    d = pd.read_csv(Path(__file__).with_name("glm_synth_data.csv"))
    res = survey_glm(d, "Y", weight="WT", numeric=["AGE"], factors=["G"], subpop=d["SUB"])
    r_est = [-2.43955357154747, 0.0382180683236095, 0.893380372895196, -0.104051785098702]
    r_se = [1.30266909781822, 0.018195773254631, 0.570525652199618, 0.456818446970699]
    assert res["variable"].tolist() == ["(Intercept)", "AGE", "as.factor(G)2", "as.factor(G)3"]
    np.testing.assert_allclose(res["estimate"], r_est, rtol=1e-7)
    np.testing.assert_allclose(res["se"], r_se, rtol=1e-6)


def test_adjust_domain_lonely_matches_r():
    # Reference: R survey 4.1.1 with options(survey.lonely.psu = "adjust",
    #   survey.adjust.domain.lonely = TRUE); svyby(~Y, ~D, des, svymean / svytotal)
    df = pd.DataFrame({
        "VARSTR": np.repeat([1, 2, 3], 6),
        "VARPSU": np.tile(np.repeat([1, 2], 3), 3),
        "WT": [10, 20, 30, 15, 25, 35, 12, 22, 32, 18, 28, 38, 11, 21, 31, 17, 27, 37],
        "Y": [1, 4, 2, 5, 3, 6, 2, 7, 1, 8, 3, 9, 4, 2, 6, 1, 5, 3],
        "D": list("AABBBBABABABAAABBB"),
    })
    kw = {"weight": "WT", "domain": "D", "adjust_domain_lonely": True}
    mean = survey_estimate(df, "Y", stat="mean", **kw).sort_values("D")
    total = survey_estimate(df, "Y", stat="total", **kw).sort_values("D")
    np.testing.assert_allclose(mean["estimate"], [3.04242424242424, 5.01136363636364], rtol=1e-12)
    np.testing.assert_allclose(mean["se"], [0.830719154021954, 1.02005850634226], rtol=1e-12)
    np.testing.assert_allclose(total["estimate"], [502, 1323], rtol=1e-12)
    np.testing.assert_allclose(total["se"], [406.142832018491, 581.860808097607], rtol=1e-12)


def test_survey_glm_drops_incomplete_cases_like_r():
    # R: same design with AGE[c(3, 40, 77)] <- NA and G[c(5, 90)] <- NA (1-based rows);
    # svyglm drops incomplete rows from the model frame
    from meps_utils import survey_glm
    d = pd.read_csv(Path(__file__).with_name("glm_synth_data.csv"))
    d.loc[[2, 39, 76], "AGE"] = np.nan
    d.loc[[4, 89], "G"] = np.nan
    res = survey_glm(d, "Y", weight="WT", numeric=["AGE"], factors=["G"], subpop=d["SUB"])
    assert res["variable"].tolist() == ["(Intercept)", "AGE", "as.factor(G)2", "as.factor(G)3"]
    np.testing.assert_allclose(
        res["estimate"], [-2.34953696063341, 0.0357501514908902, 0.941174925467802, -0.0495565828023312], rtol=1e-7)
    np.testing.assert_allclose(
        res["se"], [1.29192260476308, 0.0175922479843676, 0.58541732494744, 0.478125775553552], rtol=1e-6)
