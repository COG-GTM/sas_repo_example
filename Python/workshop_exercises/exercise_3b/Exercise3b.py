# -----------------------------------------------------------------------------
# Python port of SAS/workshop_exercises/exercise_3b/Exercise3b.sas
# (no R counterpart; validated against Exercise3b_OUTPUT.TXT)
#
# DESCRIPTION: THIS PROGRAM ILLUSTRATES HOW TO CALCULATE EXPENDITURES FOR ALL EVENTS
#              ASSOCIATED WITH A CONDITION. THE CONDITION USED IN THIS EXERCISE IS
#              DIABETES (CCS CODE=049 OR 050).
#
# INPUT FILES: 1) C:/MEPS/h181.ssp    (2015 FY PUF DATA)
#              2) C:/MEPS/h180.ssp    (2015 CONDITION PUF DATA)
#              3) C:/MEPS/h178a.ssp   (2015 PMED PUF DATA)
#              4) C:/MEPS/h178d.ssp   (2015 INPATIENT VISITS PUF DATA)
#              5) C:/MEPS/h178e.ssp   (2015 EROM VISITS PUF DATA)
#              6) C:/MEPS/h178f.ssp   (2015 OUTPATIENT VISITS PUF DATA)
#              7) C:/MEPS/h178g.ssp   (2015 OFFICE-BASED VISITS PUF DATA)
#              8) C:/MEPS/h178h.ssp   (2015 HOME HEALTH PUF DATA)
#              9) C:/MEPS/h178if1.ssp (2015 CONDITION-EVENT LINK PUF DATA)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate

SOURCES = ["SF", "MR", "MD", "PV", "VA", "TR", "OF", "SL", "WC", "OR", "OU", "OT"]
EXP = ["TOTEXP"] + SOURCES

# file, event type, payment-variable prefixes (facility + doctor parts are summed), total expense
EVENT_FILES = [
    ("h178g.ssp", "AMBU", ["OB"], "OBXP15X"),
    ("h178e.ssp", "EROM", ["ERF", "ERD"], "ERXP15X"),
    ("h178d.ssp", "IPAT", ["IPF", "IPD"], "IPXP15X"),
    ("h178h.ssp", "HVIS", ["HH"], "HHXP15X"),
    ("h178f.ssp", "AMBU", ["OPF", "OPD"], "OPXP15X"),
]


def align(df, prefixes, total):
    """Rename event-specific expenditure variables to common names (SF, MR, ..., TOTEXP)."""
    out = pd.DataFrame({"EVNTIDX": df["EVNTIDX"]})
    for s in SOURCES:
        out[s] = sum(df[f"{p}{s}15X"] for p in prefixes)
    out["TOTEXP"] = df[total]
    return out[out["TOTEXP"] >= 0]


def main(meps_dir=None):
    def load(f):
        return read_meps(meps_path(f, meps_dir))

    fyc = read_meps(meps_path("h181.ssp", meps_dir), columns=["DUPERSID", "VARSTR", "VARPSU", "PERWT15F"])

    # 1) Pull out conditions with diabetes (CCS code '049', '050')
    cond = load("h180.ssp")
    diab = cond.loc[cond["CCCODEX"].isin(["049", "050"]), ["DUPERSID", "CONDIDX", "CCCODEX"]]

    # 2) Get event IDs for the diabetic conditions from the condition-event link file
    clnk = load("h178if1.ssp")[["CONDIDX", "EVNTIDX"]]
    diab2 = diab.merge(clnk, on="CONDIDX", how="inner")

    # 3) Delete duplicate cases per event
    diab3 = diab2[["DUPERSID", "EVNTIDX"]].drop_duplicates("EVNTIDX")

    # 4) Sum PMED purchase-level data to event level
    rx = load("h178a.ssp").rename(columns={"LINKIDX": "EVNTIDX"})
    rx_cols = ["RXXP15X"] + [f"RX{s}15X" for s in SOURCES]
    pmed = rx.groupby("EVNTIDX")[rx_cols].sum().reset_index()
    pmed = align(pmed, ["RX"], "RXXP15X").assign(EVNTYP="PMED")

    # 5-6) Align expenditure variables across event types and combine all events
    events = [align(load(f), prefixes, total).assign(EVNTYP=typ) for f, typ, prefixes, total in EVENT_FILES]
    allevent = pd.concat(events + [pmed], ignore_index=True)

    # 7) Subset events to those with diabetes
    diab4 = diab3.merge(allevent, on="EVNTIDX", how="inner")

    # 8) Estimates on expenditures and use, all types of service
    per = diab4.groupby("DUPERSID")[EXP].sum().reset_index().assign(SUB=1)
    fy1 = fyc.merge(per, on="DUPERSID", how="left")
    fy1["SUB"] = fy1["SUB"].fillna(2)
    fy1[EXP] = fy1[EXP].fillna(0)
    fy1 = fy1[fy1["PERWT15F"] > 0]

    # 9) Estimates by type of service (one row per person and event type)
    tos = (diab4.groupby(["DUPERSID", "EVNTYP"])[EXP].agg("sum")
           .join(diab4.groupby(["DUPERSID", "EVNTYP"]).size().rename("N_VISITS")).reset_index())
    fytos = fyc.merge(tos.assign(SUB=1), on="DUPERSID", how="left")
    fytos["SUB"] = fytos["SUB"].fillna(2)
    fytos["EVNTYP"] = fytos["EVNTYP"].fillna("NA")
    fytos[["N_VISITS"] + EXP] = fytos[["N_VISITS"] + EXP].fillna(0)
    fytos = fytos[fytos["PERWT15F"] > 0]

    w = {"weight": "PERWT15F"}
    tos_vars = ["N_VISITS"] + EXP
    results = {
        "total_sub": survey_estimate(fy1, EXP, stat="total", subpop=fy1["SUB"] == 1, **w),
        "mean_sub": survey_estimate(fy1, EXP, stat="mean", subpop=fy1["SUB"] == 1, **w),
        "total_by_tos": survey_estimate(fytos, tos_vars, stat="total", subpop=fytos["SUB"] == 1,
                                        domain="EVNTYP", **w),
        "mean_by_tos": survey_estimate(fytos, tos_vars, stat="mean", subpop=fytos["SUB"] == 1,
                                       domain="EVNTYP", **w),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
