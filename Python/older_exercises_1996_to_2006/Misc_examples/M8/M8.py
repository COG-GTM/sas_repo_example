# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M8/M8.sas
# (M8.pdf contains only the SAS log, so there are no printed values to validate against)
#
# DESCRIPTION: 2005 PRESCRIBED MEDICINE EXPENDITURES FOR CANCER (CCS CODES 011-045): ROLL PMED
#              ACQUISITIONS UP TO PURCHASE EVENTS, LINK THEM TO CANCER CONDITIONS THROUGH CLNK
#              (COUNTING EACH EVENT ONCE PER PERSON), AND TABULATE WEIGHTED TOTALS ($1,000) BY
#              SOURCE OF PAYMENT, OVERALL AND BY AGE GROUP AND RACE/ETHNICITY.
#
# INPUT FILES: C:/MEPS/h96.ssp (2005 CONDITIONS), C:/MEPS/h94a.ssp (2005 PRESCRIBED MEDICINES),
#              C:/MEPS/h94if1.ssp (2005 CLNK; SAS: H94I1), C:/MEPS/h97.ssp (2005 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps

AGECAT = {1: "UNDER 18", 2: "18 - 64", 3: "65 AND OLDER"}
RACETHNB = {1: "HISPANIC", 2: "BLACK-NO OTH RACE/NOT HISPANIC", 3: "ASIAN-NO OTH RACE/NOT HISPANIC",
            4: "OTHER/NOT HISPANIC"}
EXAMPLE = ["32663024", "33634027"]
PAY = ["TOTRX05", "OOP", "PRIVATE", "MEDICARE", "MEDICAID", "OTHER"]


def main(meps_dir=None):
    cond = read_meps(meps_path("h96.ssp", meps_dir), columns=["DUPERSID", "CCCODEX", "CONDIDX"])
    cond = cond.loc[cond["CCCODEX"].between("011", "045"), ["DUPERSID", "CONDIDX"]]

    rx = read_meps(meps_path("h94a.ssp", meps_dir),
                   columns=["DUPERSID", "RXRECIDX", "LINKIDX", "RXSF05X", "RXMD05X", "RXPV05X", "RXMR05X", "RXXP05X",
                            "RXNDC"])
    ev = (rx.groupby(["DUPERSID", "LINKIDX"], as_index=False)
          .agg(OOP=("RXSF05X", "sum"), MEDICARE=("RXMR05X", "sum"), MEDICAID=("RXMD05X", "sum"),
               PRIVATE=("RXPV05X", "sum"), TOTRX05=("RXXP05X", "sum"), RXNDC=("RXNDC", "last"))
          .rename(columns={"LINKIDX": "EVNTIDX"}))
    ev["OTHER"] = np.floor(ev["TOTRX05"] - ev[["MEDICARE", "MEDICAID", "PRIVATE", "OOP"]].sum(axis=1) + 0.5)
    print(ev[ev["DUPERSID"].isin(EXAMPLE)].to_string(index=False))

    clnk = read_meps(meps_path("h94if1.ssp", meps_dir), columns=["CONDIDX", "EVNTIDX"])
    condpmed = cond.merge(clnk, on="CONDIDX", how="left").merge(ev, on=["DUPERSID", "EVNTIDX"])
    print(condpmed[condpmed["DUPERSID"].isin(EXAMPLE)].to_string(index=False))
    condpmed = condpmed.drop_duplicates(["DUPERSID", "EVNTIDX"])    # an event can treat several cancers
    print(condpmed[condpmed["DUPERSID"].isin(EXAMPLE)].to_string(index=False))

    fy = read_meps(meps_path("h97.ssp", meps_dir),
                   columns=["DUPERSID", "AGE05X", "AGE53X", "AGE42X", "AGE31X", "RACETHNX", "PERWT05F"])
    age = fy["AGE05X"].where(fy["AGE05X"] >= 0, fy["AGE53X"].where(fy["AGE53X"] >= 0, fy["AGE42X"].where(
        fy["AGE42X"] >= 0, fy["AGE31X"].where(fy["AGE31X"] >= 0, fy["AGE05X"]))))
    fy["AGECAT"] = pd.Series(np.select([age.between(0, 17.999), age.between(18, 64.999), age >= 65], [1, 2, 3], 0),
                             index=fy.index).map(AGECAT)
    fy["RACETHNX"] = fy["RACETHNX"].map(RACETHNB)
    temp = condpmed.merge(fy[["DUPERSID", "AGECAT", "RACETHNX", "PERWT05F"]], on="DUPERSID", how="left")
    temp[PAY] = temp[PAY] / 1000

    tables = {}
    for by in [None, "AGECAT", "RACETHNX"]:     # PROC TABULATE ... SUM, WEIGHT PERWT05F
        g = temp.assign(_all="TOTAL", **{c: temp[c].mul(temp["PERWT05F"]) for c in PAY}).groupby(by or "_all")
        tables[by or "TOTAL"] = g[PAY].sum()
        print(f"\nCancer PMED expenditures ($1,000) by {by or 'TOTAL'}:\n{tables[by or 'TOTAL'].round(0).to_string()}")
    return tables


if __name__ == "__main__":
    main()
