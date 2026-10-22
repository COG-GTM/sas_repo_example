# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M5/M5.sas
# (M5.pdf contains only the SAS log, so there are no printed values to validate against)
#
# DESCRIPTION: COMPARES PERSONS WITH DIABETES, HYPERTENSION, EMPHYSEMA AND ASTHMA IDENTIFIED
#              FROM CONDITION-FILE ICD-9 CODES (250, 401, 492, 493) WITH THE PRIORITY-CONDITION
#              QUESTIONS ON THE FULL-YEAR FILE (DIABDX53, HIBPDX53, EMPHDX53, ASTHDX53), 2005.
#
# INPUT FILES: C:/MEPS/h96.ssp (2005 CONDITIONS), C:/MEPS/h97.ssp (2005 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps

ICD9 = {"250": "DIABETES", "401": "HYPERTENSION", "492": "EMPHYSEMA", "493": "ASTHMA"}
PC = {"DIABDX53": "DIABETES", "HIBPDX53": "HYPERTENSION", "EMPHDX53": "EMPHYSEMA", "ASTHDX53": "ASTHMA"}


def main(meps_dir=None):
    cond = read_meps(meps_path("h96.ssp", meps_dir), columns=["DUPERSID", "CONDIDX", "ICD9CODX", "PRIOLIST"])
    cond = cond[cond["ICD9CODX"].isin(list(ICD9))]
    print(cond["ICD9CODX"].map(ICD9).value_counts().to_string())
    print(cond.groupby([cond["ICD9CODX"].map(ICD9), "PRIOLIST"]).size().to_string())

    # person-level flags from the condition file
    flags = pd.crosstab(cond["DUPERSID"], cond["ICD9CODX"].map(ICD9)).clip(upper=1)
    flags = flags.reindex(columns=list(ICD9.values()), fill_value=0).add_prefix("C_").reset_index()

    fy = read_meps(meps_path("h97.ssp", meps_dir), columns=["DUPERSID", *PC, "VARPSU", "VARSTR", "PERWT05F"])
    for v, name in PC.items():
        fy[f"PC_{name}"] = fy[v].where(~((fy[v] == 2) | (fy[v] < -1)), 0)   # 2 (no) and -7/-8/-9 -> 0
        print(fy[f"PC_{name}"].value_counts(dropna=False).sort_index().to_string())

    merged = flags.merge(fy, on="DUPERSID", how="outer", indicator=True)
    print("BOTH={} JUSTA={} JUSTB={}".format(*[(merged["_merge"] == k).sum() for k in ("both", "left_only", "right_only")]))
    fycond = merged[merged["_merge"] != "left_only"].copy()            # IF B
    for name in ICD9.values():
        fycond[f"C_{name}"] = fycond[f"C_{name}"].fillna(0).astype(int)
        print(pd.crosstab(fycond[f"C_{name}"], fycond[f"PC_{name}"]).to_string())
    return {"fycond05": fycond.drop(columns="_merge")}


if __name__ == "__main__":
    main()
