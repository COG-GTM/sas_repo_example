# -----------------------------------------------------------------------------
# Python port of R/workshop_exercises/cond_mv_2020.R (no SAS counterpart)
#
# Office-based medical visits for mental health in 2020, using the condition
# (COND), condition-event link (CLNK), office-based events (OB) and full-year
# consolidated (FYC) files. Estimates:
#   - Number of mental-health OB visits, total and mean expenditure per visit
#   - Number/percent of people with a mental-health OB visit
#   - Mean expenditure per person, number of visits and total expenditure
#
# Input files: C:/MEPS/h220g.dta (2020 OB), h222.dta (2020 COND),
#              h220if1.dta (2020 CLNK), h224.dta (2020 FYC)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate

MH_CCSR = "MBD|FAC002|FAC007|NVS011|SYM008|SYM009"
DESIGN = ["DUPERSID", "PERWT20F", "VARSTR", "VARPSU"]


def main(meps_dir=None):
    ob = read_meps(meps_path("h220g.dta", meps_dir),
                   columns=["PANEL", "DUPERSID", "EVNTIDX", "EVENTRN", "OBDATEYR", "OBDATEMM",
                            "TELEHEALTHFLAG", "OBXP20X", "PERWT20F", "VARPSU", "VARSTR"])
    cond = read_meps(meps_path("h222.dta", meps_dir),
                     columns=["DUPERSID", "CONDIDX", "ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X"])
    clnk = read_meps(meps_path("h220if1.dta", meps_dir))
    fyc = read_meps(meps_path("h224.dta", meps_dir), columns=DESIGN)

    # Mental-health conditions: any CCSR code matching the MH categories
    all_ccsr = cond[["CCSR1X", "CCSR2X", "CCSR3X"]].astype(str).agg("_".join, axis=1)
    mental_health = cond[all_ccsr.str.contains(MH_CCSR)]

    # Link to office-based events (EVENTYPE = 1), one row per event
    clnk_ob = clnk[clnk["EVENTYPE"] == 1]
    mh_clnk = mental_health.merge(clnk_ob, on=["DUPERSID", "CONDIDX"])
    mh_nodup = mh_clnk[["DUPERSID", "EVNTIDX", "EVENTYPE"]].drop_duplicates()
    ob_mh = ob.merge(mh_nodup, on=["DUPERSID", "EVNTIDX"]).assign(mh_ob_visit=1, mh_ob=1)

    # Stack MH visits onto the FYC file so every person stays in the design
    ob_mh_fyc = ob_mh.merge(fyc.assign(fyc=1), on=DESIGN, how="outer")
    print(ob_mh_fyc.groupby(["mh_ob", "mh_ob_visit", "fyc"], dropna=False).size())

    # Person-level file
    g = ob_mh_fyc.groupby(DESIGN, dropna=False)
    pers = g.agg(persXP=("OBXP20X", "sum"), pers_nevents=("mh_ob_visit", "sum"),
                 mh_ob_visit_pers=("mh_ob_visit", "mean"), mh_ob_pers=("mh_ob", "mean")).reset_index()
    pers[["pers_nevents", "mh_ob_visit_pers", "mh_ob_pers"]] = pers[
        ["pers_nevents", "mh_ob_visit_pers", "mh_ob_pers"]].fillna(0)
    assert len(pers) == len(fyc)

    ev = ob_mh_fyc.fillna({"mh_ob_visit": 0, "OBXP20X": 0})
    ev_sub = ob_mh_fyc["mh_ob"] == 1
    p_sub = pers["mh_ob_pers"] == 1
    w = {"weight": "PERWT20F"}
    results = {
        "total_visits": survey_estimate(ev, "mh_ob_visit", stat="total", subpop=ev_sub, **w),
        "total_exp": survey_estimate(ev, "OBXP20X", stat="total", subpop=ev_sub, **w),
        "mean_exp_per_visit": survey_estimate(ev, "OBXP20X", stat="mean", subpop=ev_sub, **w),
        "n_people": survey_estimate(pers, "mh_ob_visit_pers", stat="total", **w),
        "pct_people": survey_estimate(pers, "mh_ob_visit_pers", stat="mean", **w),
        "mean_exp_per_person": survey_estimate(pers, "persXP", stat="mean", subpop=p_sub, **w),
        "n_visits_person_file": survey_estimate(pers, "pers_nevents", stat="total", subpop=p_sub, **w),
        "total_exp_person_file": survey_estimate(pers, "persXP", stat="total", subpop=p_sub, **w),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
