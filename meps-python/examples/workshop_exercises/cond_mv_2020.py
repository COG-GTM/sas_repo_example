"""
Example code linking MEPS-HC Medical Conditions file to the Office-based
medical visits file, data year 2020:

Event-level estimates:
 - Number of office-based visits for mental health
 - Total expenditures for office-based mental health treatment
 - Mean expenditure per office-based mental health visit

Person-level estimates:
 - Number of people with office-based mental health visits
 - Percent of people with office-based mental health visits
 - Mean expenditure per person for office-based mental health visits

Input files:
 - h220g (2020 Office-based event file)
 - h222 (2020 Conditions file)
 - h220if1 (2020 CLNK: Condition-event link file)
 - h224 (2020 Full-Year Consolidated file)

This is the Python equivalent of R/workshop_exercises/cond_mv_2020.R
"""

import sys
sys.path.insert(0, "../..")

import pandas as pd
import numpy as np

from meps import read_meps, MEPSSurveyDesign, svymean, svytotal
from meps.utils import filter_ccsr_pattern, aggregate_to_person


def main():
    print("Loading datasets...")
    print("  - Office-based visits (OB)")
    ob20 = read_meps(year=2020, type="OB")
    print("  - Conditions (COND)")
    cond20 = read_meps(year=2020, type="COND")
    print("  - Condition-Event Link (CLNK)")
    clnk20 = read_meps(year=2020, type="CLNK")
    print("  - Full-Year Consolidated (FYC)")
    fyc20 = read_meps(year=2020, type="FYC")

    print("\nKeeping only needed variables...")
    ob20x = ob20[["PANEL", "DUPERSID", "EVNTIDX", "EVENTRN", "OBDATEYR", "OBDATEMM",
                  "TELEHEALTHFLAG", "OBXP20X", "PERWT20F", "VARPSU", "VARSTR"]].copy()

    cond20x = cond20[["DUPERSID", "CONDIDX", "ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X"]].copy()

    fyc20x = fyc20[["DUPERSID", "PERWT20F", "VARSTR", "VARPSU"]].copy()

    print("\nFiltering conditions to Mental Disorders...")
    mental_health = filter_ccsr_pattern(
        cond20x,
        pattern="MBD|FAC002|FAC007|NVS011|SYM008|SYM009",
        ccsr_vars=["CCSR1X", "CCSR2X", "CCSR3X"],
    )

    print(f"  Found {len(mental_health)} mental health conditions")

    print("\nViewing ICD10-CCSR combinations for mental health:")
    print(mental_health.groupby(["ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X"]).size().head(20))

    print("\nFiltering CLNK to office-based visits (EVENTYPE=1)...")
    clnk_ob = clnk20[clnk20["EVENTYPE"] == 1].copy()
    print(f"  CLNK records for OB visits: {len(clnk_ob)}")

    print("\nMerging conditions with CLNK...")
    mh_clnk = pd.merge(
        mental_health,
        clnk_ob,
        on=["DUPERSID", "CONDIDX"],
        how="inner",
    )
    print(f"  Merged records: {len(mh_clnk)}")

    print("\nDe-duplicating by EVNTIDX...")
    mh_clnk_nodup = mh_clnk.drop_duplicates(subset=["DUPERSID", "EVNTIDX"])
    print(f"  After de-duplication: {len(mh_clnk_nodup)}")

    print("\nMerging with office-based events...")
    ob_mental_health = pd.merge(
        ob20x,
        mh_clnk_nodup[["DUPERSID", "EVNTIDX", "EVENTYPE"]],
        on=["DUPERSID", "EVNTIDX"],
        how="inner",
    )
    ob_mental_health["mh_ob_visit"] = 1
    print(f"  Mental health OB visits: {len(ob_mental_health)}")

    print("\nMerging with FYC for complete strata/PSUs...")
    ob_mental_health["mh_ob"] = 1
    fyc20x["fyc"] = 1

    ob_mh_fyc = pd.merge(
        ob_mental_health,
        fyc20x,
        on="DUPERSID",
        how="outer",
        suffixes=("", "_fyc"),
    )

    for col in ["VARSTR", "VARPSU", "PERWT20F"]:
        if f"{col}_fyc" in ob_mh_fyc.columns:
            ob_mh_fyc[col] = ob_mh_fyc[col].fillna(ob_mh_fyc[f"{col}_fyc"])

    print("\n" + "=" * 70)
    print("EVENT-LEVEL ESTIMATES")
    print("=" * 70)

    evnt_design = MEPSSurveyDesign(
        data=ob_mh_fyc,
        id_var="VARPSU",
        strata_var="VARSTR",
        weight_var="PERWT20F",
    )

    sub_condition = ob_mh_fyc["mh_ob"] == 1
    sub_design = evnt_design.subset(sub_condition)

    print("\nNumber of office-based visits for mental health:")
    result = svytotal(sub_design, "mh_ob_visit")
    print(result.to_string(index=False))

    print("\nTotal expenditures for office-based mental health visits:")
    result = svytotal(sub_design, "OBXP20X")
    print(result.to_string(index=False))

    print("\nMean expenditure per office-based mental health visit:")
    result = svymean(sub_design, "OBXP20X")
    print(result.to_string(index=False))

    print("\n" + "=" * 70)
    print("PERSON-LEVEL ESTIMATES")
    print("=" * 70)

    print("\nAggregating to person-level...")
    pers_mh = ob_mh_fyc.groupby(["DUPERSID", "VARSTR", "VARPSU", "PERWT20F"]).agg({
        "OBXP20X": "sum",
        "mh_ob_visit": "sum",
        "mh_ob": "mean",
    }).reset_index()

    pers_mh = pers_mh.rename(columns={
        "OBXP20X": "persXP",
        "mh_ob_visit": "pers_nevents",
        "mh_ob": "mh_ob_pers",
    })

    pers_mh["pers_nevents"] = pers_mh["pers_nevents"].fillna(0)
    pers_mh["mh_ob_pers"] = pers_mh["mh_ob_pers"].fillna(0)
    pers_mh["mh_ob_visit_pers"] = (pers_mh["pers_nevents"] > 0).astype(int)

    print(f"  Person-level records: {len(pers_mh)}")
    print(f"  FYC records: {len(fyc20)}")

    pers_design = MEPSSurveyDesign(
        data=pers_mh,
        id_var="VARPSU",
        strata_var="VARSTR",
        weight_var="PERWT20F",
    )

    print("\nNumber of people with office visit for mental health:")
    result = svytotal(pers_design, "mh_ob_visit_pers")
    print(result.to_string(index=False))

    print("\nPercent of people with office visit for mental health:")
    result = svymean(pers_design, "mh_ob_visit_pers")
    print(result.to_string(index=False))

    pers_mh_sub = pers_design.subset(pers_mh["mh_ob_pers"] == 1)
    print("\nMean expenditure per person for office-based mental health visits:")
    result = svymean(pers_mh_sub, "persXP")
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
