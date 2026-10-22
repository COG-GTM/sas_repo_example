# -----------------------------------------------------------------------------
# PROGRAM:      EXERCISE4.SAS
#
#  This program illustrates how to pool MEPS data files from different years. It
#  highlights one example of a discontinuity that may be encountered when
#  working with data from before and after the 2018 MEPS CAPI re-design.
#
#  The program pools 2017, 2018 and 2019 data and calculates
#   - percentage of people with Joint Pain / Arthritis (JTPAIN**, ARTHDX)
#   - average expenditures per person, by Joint Pain status (TOTEXP, TOTSLF)
#   - standard errors by specifying common variance structure when pooling data.
#  for the U.S. civilian noninstitutionized population.
#
#  Input files:
#   - 2017 Full-year consolidated file (h201)
#   - 2018 Full-year consolidated file (h209)
#   - 2019 Full-year consolidated file (h216)
#   - 1996-2019 pooled linkage variance estimation file (h36u19)
#
# Python port of SAS/workshop_exercises/exercise_4d/Exercise4.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps, svy_freq, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

# Formats (PROC FORMAT) -------------------------------------------------------

AGECAT_FMT = [(19, 49, "19-49"), (50, 64, "50-64"), (65, np.inf, "65+")]
YES_NO_FMT = {1: "Yes", 2: "No", -1: "DK/REF/INAPP/MISSING", -7: "DK/REF/INAPP/MISSING",
              -8: "DK/REF/INAPP/MISSING", -15: "DK/REF/INAPP/MISSING"}  # fmt: skip
SPOP_FMT = {1: "Age 18+", 0: "Age 0-17"}

# Concatenate 2017, 2018 and 2019 Full Year Consolidated Files ----------------

years = {2017: ("h201", "JTPAIN31"), 2018: ("h209", "JTPAIN31_M18"), 2019: ("h216", "JTPAIN31_M18")}
frames = []
for year, (name, jtpain) in years.items():
    yy = str(year)[2:]
    cols = ["DUPERSID", "PANEL", "VARSTR", "VARPSU", f"PERWT{yy}F", "AGELAST", "ARTHDX", jtpain,
            f"TOTEXP{yy}", f"TOTSLF{yy}"]  # fmt: skip
    df = load_meps(name, columns=cols).rename(
        columns={f"TOTEXP{yy}": "TOTEXP", f"TOTSLF{yy}": "TOTSLF", jtpain: "JTPAIN"}
    )
    df["YEAR"] = year
    # Create a new weight variable by dividing the original weight by 3 for the pooled data set
    df["PERWTF"] = df[f"PERWT{yy}F"] / 3
    frames.append(df)
MEPS_171819 = pd.concat(frames, ignore_index=True)
M = MEPS_171819
M["DUPERSID"] = M["DUPERSID"].astype(str).str.strip()

# Create new variables: JOINT_PAIN, SPOP (subpopulation indicator), ZERO_WEIGHT
# (zero survey weight for QC purposes). For 2017 the 2018+ inapplicable rule
# differs (ARTHDX <= 0 vs ARTHDX < 0), as in the SAS program.
is17 = M["YEAR"] == 2017
arth_inapp = np.where(is17, M["ARTHDX"] <= 0, M["ARTHDX"] < 0)
spop = (M["AGELAST"] >= 18) & ~(arth_inapp & (M["JTPAIN"] < 0))
M["SPOP"] = np.where(spop, 1, 0)
M["JOINT_PAIN"] = np.where(spop, np.where((M["ARTHDX"] == 1) | (M["JTPAIN"] == 1), 1, 2), np.nan)
M["ZERO_WEIGHT"] = (M["PERWTF"] == 0).astype(int)

# Change the 8-character DUPERSID to a 10-character one for 2017 (PANEL + ID).
# As in the SAS program this is only done for SPOP = 1 records; the other 2017
# records keep a blank DUPERSID and so get no linkage variance variables.
M.loc[is17, "DUPERSID"] = np.where(
    M.loc[is17, "SPOP"] == 1, M.loc[is17, "PANEL"].astype(int).astype(str) + M.loc[is17, "DUPERSID"], ""
)

# Pooled linkage variance estimation file: 10-character DUPERSID, panels 21-24
VS = load_meps("h36u19")
VS["DUPERSID"] = VS["DUPERSID"].astype(str).str.strip()
short = VS["DUPERSID"].str.len() == 8
VS.loc[short, "DUPERSID"] = VS.loc[short, "PANEL"].astype(int).map("{:02d}".format) + VS.loc[short, "DUPERSID"]
sorted_VS = VS[VS["PANEL"].isin([21, 22, 23, 24])].drop_duplicates("DUPERSID")

# Match-merge the 2017-19 file with the pooled linkage variance estimation file
MEPS_171819_m = M.merge(sorted_VS[["DUPERSID", "STRA9619", "PSU9619"]], on="DUPERSID", how="left")

# QC (commented out in the SAS program) ---------------------------------------
qc = MEPS_171819_m[(MEPS_171819_m["SPOP"] == 1) & (MEPS_171819_m["PERWTF"] > 0)]
print("MEPS 2017-19 combined, spop=1 & perwtf>0 for QC purposes")
print(crosstab_list(qc.assign(JOINT_PAIN=apply_format(qc["JOINT_PAIN"], YES_NO_FMT)), ["JOINT_PAIN"]))
print(qc[["STRA9619", "PSU9619"]].agg(["count", lambda s: s.isna().sum(), "min", "max"]), "\n")

# Pooled estimates using the common variance structure (STRA9619 / PSU9619) ---
# SURVEYMEANS drops records with missing strata/cluster, so do the same here.
P = MEPS_171819_m.dropna(subset=["STRA9619", "PSU9619"]).copy()
P["JOINT_PAIN"] = apply_format(P["JOINT_PAIN"], YES_NO_FMT)
design = dict(weight="PERWTF", strata="STRA9619", psu="PSU9619")

print("Pooled estimates for MEPS 2017-19")
res = svy_freq(P, "JOINT_PAIN", domain="SPOP", **design)
print(res[(res["SPOP"] == 1) & (res["JOINT_PAIN"] != "Missing")].to_string(index=False), "\n")

dom = ["SPOP", "JOINT_PAIN"]
res = svy_mean(P, ["TOTEXP", "TOTSLF"], domain=dom, **design).merge(
    svy_total(P, ["TOTEXP", "TOTSLF"], domain=dom, **design),
    on=dom + ["variable", "n"],
    suffixes=("_mean", "_sum"),
)
print(res[res["SPOP"] == 1].to_string(index=False))
