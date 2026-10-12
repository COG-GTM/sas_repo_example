# -----------------------------------------------------------------------------
# Python port of R/workshop_exercises/cond_pmed_2020.R (no SAS counterpart)
#
# Prescribed medicine purchases for hyperlipidemia (CCSR END010) in 2020, using the
# condition (COND), condition-event link (CLNK), prescribed medicines (RX) and
# full-year consolidated (FYC) files. Estimates:
#   - Number of people with a PMED purchase for hyperlipidemia, total fills and expenditures
#   - Average fills and expenditures per person with a purchase, overall, by sex and by poverty
#
# Input files: C:/MEPS/h220a.dta (2020 RX), h222.dta (2020 COND),
#              h220if1.dta (2020 CLNK), h224.dta (2020 FYC)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate


def main(meps_dir=None):
    pmed = read_meps(meps_path("h220a.dta", meps_dir),
                     columns=["DUPERSID", "RXRECIDX", "LINKIDX", "RXDRGNAM", "RXXP20X"])
    pmed = pmed.rename(columns={"LINKIDX": "EVNTIDX"})
    cond = read_meps(meps_path("h222.dta", meps_dir),
                     columns=["DUPERSID", "CONDIDX", "ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X"])
    clnk = read_meps(meps_path("h220if1.dta", meps_dir))
    fyc = read_meps(meps_path("h224.dta", meps_dir),
                    columns=["DUPERSID", "AGELAST", "SEX", "POVCAT20", "CHOLDX", "VARSTR", "VARPSU", "PERWT20F"])

    # Hyperlipidemia conditions and their linked events (one row per event)
    hl = cond[(cond[["CCSR1X", "CCSR2X", "CCSR3X"]] == "END010").any(axis=1)]
    hl_clnk = hl.merge(clnk, on=["DUPERSID", "CONDIDX"])
    hl_distinct = hl_clnk[["DUPERSID", "EVNTIDX", "ICD10CDX", "CCSR1X", "EVENTYPE"]].drop_duplicates()
    assert not hl_distinct["EVNTIDX"].duplicated().any()
    hl_merged = hl_distinct.merge(pmed, on=["DUPERSID", "EVNTIDX"])
    print(hl_merged["RXDRGNAM"].value_counts().head(10))

    # Person-level fills and expenditures, merged onto the full FYC file
    drugs = (hl_merged.groupby("DUPERSID")
             .agg(n_hl_fills=("RXRECIDX", "nunique"), hl_drug_exp=("RXXP20X", "sum"))
             .reset_index().assign(hl_pmed_flag=1))
    fy = fyc.merge(drugs, on="DUPERSID", how="outer")
    assert len(fy) == len(fyc)
    fy = fy.fillna({"hl_pmed_flag": 0, "n_hl_fills": 0, "hl_drug_exp": 0})
    print(fy[fy["CHOLDX"] >= 0].groupby(["hl_pmed_flag", "CHOLDX"]).size())

    sub = fy["hl_pmed_flag"] == 1
    w = {"weight": "PERWT20F", "subpop": sub}
    pers_vars = ["n_hl_fills", "hl_drug_exp"]
    results = {
        "totals": survey_estimate(fy, ["hl_pmed_flag"] + pers_vars, stat="total", **w),
        "means": survey_estimate(fy, pers_vars, stat="mean", **w),
        "means_by_sex": survey_estimate(fy, pers_vars, stat="mean", domain="SEX", **w),
        "means_by_povcat": survey_estimate(fy, pers_vars, stat="mean", domain="POVCAT20", **w),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
