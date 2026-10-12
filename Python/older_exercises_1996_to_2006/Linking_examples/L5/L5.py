# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L5/L5.sas
# (validated against the SAS listing in L5.pdf)
#
# DESCRIPTION: (1) USE THE CONDITION FILE TO IDENTIFY EVENTS FOR ASTHMA (ICD9CODX = '493')
#              (2) LINK CONDITIONS TO EVENTS (CLNK)
#              (3) MERGE THE CONDITION-EVENT LINKED FILE TO EVENT FILES
#              (4) FOR EACH EVENT, CONSTRUCT VARIABLES WITH THE SAME NAME ACROSS EVENTS
#              (5) COMBINE FACILITY AND DOCTOR EXPENDITURES
#              (6) COMBINE EVENT FILES, IDENTIFY TYPE OF EVENT
#              (7) AGGREGATE EVENT-LEVEL RECORDS TO PERSON LEVEL, THEN WEIGHTED MEANS (PROC MEANS)
#
# INPUT FILES: C:/MEPS/h61.ssp (2001 CONDITIONS), C:/MEPS/h59if1.ssp (2001 CLNK),
#              C:/MEPS/h59a.ssp (PRESCRIBED MEDICINES), h59e/h59f/h59g.ssp (ER, OUTPATIENT,
#              OFFICE-BASED), C:/MEPS/h60.ssp (2001 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps

INSCOV = {1: "Any private", 2: "Public only", 3: "Uninsured"}
VISTYPE = {"ob": "Office-Based", "op": "Outpatient", "er": "Emergency", "pm": "Drug Purchase"}
SEX = {1: "Male", 2: "Female"}
RACETHNX = {1: "Hispanic", 2: "Black, not Hispanic", 3: "Other"}
AGE_BINS, AGE_LABELS = [-0.5, 4.5, 17.5, 24.5, 44.5, 64.5, 90.5], ["0-4", "5-17", "18-24", "25-44", "45-64", "65-90"]
PAID = ["AMBTOTPD", "AMBFAMPD"]


def _wmean(df, by, weight="PERWT01F"):
    """PROC MEANS MEAN with WEIGHT, one row per class level and variable."""
    g = df[PAID].mul(df[weight], axis=0).groupby([df[b] for b in by]).sum()
    out = g.div(df.groupby(by)[weight].sum(), axis=0).reset_index()
    out["DOMAIN"] = out[by].astype(str).agg(" / ".join, axis=1)
    return out.melt(id_vars="DOMAIN", value_vars=PAID, var_name="variable", value_name="estimate")


def _per_person(ev):
    return (ev.groupby("DUPERSID", as_index=False)
            .agg(AMBTOTPD=("AMBTOTEV", "sum"), AMBFAMPD=("AMBFAMEV", "sum")))


def main(meps_dir=None):
    def load(f, cols):
        return read_meps(meps_path(f, meps_dir), columns=cols)

    cond = load("h61.ssp", ["CONDIDX", "ICD9CODX", "DUPERSID"])
    cond = cond.loc[cond["ICD9CODX"] == "493", ["CONDIDX", "DUPERSID"]]
    clnk = load("h59if1.ssp", ["CONDIDX", "EVNTIDX"])
    condev = cond.merge(clnk, on="CONDIDX").sort_values(["CONDIDX"], kind="stable").drop_duplicates("EVNTIDX")

    rx = load("h59a.ssp", ["RXRECIDX", "LINKIDX", "RXSF01X", "RXXP01X"]).rename(columns={"LINKIDX": "EVNTIDX"})
    pm = condev.merge(rx, on="EVNTIDX").assign(AMBTOTEV=lambda d: d["RXXP01X"], AMBFAMEV=lambda d: d["RXSF01X"])
    fy = load("h60.ssp", ["DUPERSID", "INSCOV01", "PERWT01F", "AGE31X", "AGE42X", "AGE53X", "SEX", "RACETHNX"])

    pers1 = _per_person(pm).merge(fy, on="DUPERSID", how="left")
    pers1["INSCOV01"] = pers1["INSCOV01"].map(INSCOV)
    by_inscov = _wmean(pers1, ["INSCOV01"])

    ob = condev.merge(load("h59g.ssp", ["EVNTIDX", "OBXP01X", "OBSF01X"]), on="EVNTIDX")
    ob = ob.assign(AMBTOTEV=ob["OBXP01X"], AMBFAMEV=ob["OBSF01X"])
    op = condev.merge(load("h59f.ssp", ["EVNTIDX", "OPXP01X", "OPFSF01X", "OPDSF01X"]), on="EVNTIDX")
    op = op.assign(AMBTOTEV=op["OPXP01X"], AMBFAMEV=op[["OPFSF01X", "OPDSF01X"]].sum(axis=1))  # facility + doctor
    er = condev.merge(load("h59e.ssp", ["EVNTIDX", "ERXP01X", "ERFSF01X", "ERDSF01X"]), on="EVNTIDX")
    er = er.assign(AMBTOTEV=er["ERXP01X"], AMBFAMEV=er[["ERFSF01X", "ERDSF01X"]].sum(axis=1))
    allevnt = pd.concat([d.assign(VISTYPE=t) for t, d in (("ob", ob), ("op", op), ("er", er), ("pm", pm))])
    print(allevnt["VISTYPE"].map(VISTYPE).value_counts().to_string())

    pers = _per_person(allevnt).merge(fy, on="DUPERSID", how="left")
    age = pers["AGE53X"].where(pers["AGE53X"] >= 0, pers["AGE42X"].where(pers["AGE42X"] >= 0,
                                                                        pers["AGE31X"].where(pers["AGE31X"] >= 0)))
    pers["AGE"] = pd.cut(age, AGE_BINS, labels=AGE_LABELS).astype(object)
    pers["RACETHNX"], pers["SEX"] = pers["RACETHNX"].map(RACETHNX), pers["SEX"].map(SEX)
    by_demo = _wmean(pers.dropna(subset=["AGE", "RACETHNX", "SEX"]), ["AGE", "RACETHNX", "SEX"])

    results = {"pm_by_inscov": by_inscov, "by_age_race_sex": by_demo,
               "vistype": allevnt["VISTYPE"].map(VISTYPE).value_counts().rename_axis("variable")
               .reset_index(name="estimate").assign(estimate=lambda d: d["estimate"].astype(float))}
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
