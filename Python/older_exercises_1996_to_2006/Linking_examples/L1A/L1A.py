"""Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L1A/L1A.sas



<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

YN = {-9: "-9 Not Asc", -8: "-8 DK", -7: "-7 Refised", -1: "-1 Inappl", 1: " 1 Yes", 2: " 2 No"}
IDS = ["80001021", "80005018", "80005025", "80006012", "80006029"]

h40 = read_meps("h40")  # 2000 JOBS file
h56 = read_meps("h56")  # 2001 JOBS file
PAY = list(h56.loc[:, "SICKPAY":"PAYVACTN"].columns)  # SICKPAY--PAYVACTN
KEEP = ["DUPERSID", "PANEL", "RN", "JOBSN", "SUBTYPE", "STILLAT"] + PAY

# Print the job records of a few persons across both years ------------------------------
s00 = h40[h40.DUPERSID.isin(IDS) & h40.RN.isin([1, 2])].assign(YEAR=2000)
s01 = h56[h56.DUPERSID.isin(IDS)].assign(YEAR=2001)
both = pd.concat([s00, s01]).sort_values(["DUPERSID", "JOBSN"], kind="stable")  # SET ... BY (interleave)
print(both[["DUPERSID", "YEAR", "RN", "JOBSN", "SUBTYPE", "STILLAT"] + PAY].to_string(index=False), "\n")


def freqs(df, title):
    for v in PAY:
        f = sas_format(df[v], YN).value_counts(dropna=False).sort_index().rename("Frequency").to_frame()
        f["Percent"] = 100 * f.Frequency / f.Frequency.sum()
        show(f"{title}: {v}", f.reset_index(), "{:,.2f}")


# 2001 current main jobs (panel 5, rn 3) that began in 2000 ---------------------------
cmj01 = h56.loc[(h56.PANEL == 5) & (h56.RN == 3) & (h56.SUBTYPE == 1) & (h56.STILLAT == 1), KEEP]
cmj01 = cmj01.sort_values(["DUPERSID", "JOBSN", "RN"])
freqs(cmj01, "2001 CMJs continuing from 2000")
print(cmj01.head(20).to_string(index=False), "\n")

# Newly-reported 2000 current main jobs (panel 5, rn 1/2) -----------------------------
cmj00 = h40.loc[(h40.PANEL == 5) & h40.RN.isin([1, 2]) & (h40.SUBTYPE == 1) & (h40.STILLAT == -1), KEEP]
cmj00 = cmj00.sort_values(["DUPERSID", "JOBSN", "RN"])
freqs(cmj00, "2000 newly-reported CMJs")
print(cmj00.head(20).to_string(index=False), "\n")

# Concatenate (SET ... BY interleave) and print -----------------------------------------
cat = pd.concat([cmj00, cmj01]).sort_values(["DUPERSID", "JOBSN"], kind="stable")
print(cat.head(40).to_string(index=False), "\n")

# MERGE cmj01 (IN=A) cmj00 (IN=B, RENAME 2000 vars, DROP STILLAT) BY DUPERSID JOBSN; IF A & B
new = cmj01.merge(cmj00.drop(columns=["STILLAT", "PANEL", "SUBTYPE", "RN"])
                  .rename(columns={"SICKPAY": "SICKPAYX", "PAYDRVST": "PAYDRVSTX", "PAYVACTN": "PAYVACTNX"})
                  [["DUPERSID", "JOBSN", "SICKPAYX", "PAYDRVSTX", "PAYVACTNX"]],
                  on=["DUPERSID", "JOBSN"], how="inner").drop(columns="RN")
print(new[["DUPERSID", "PANEL", "JOBSN", "SUBTYPE", "STILLAT", "SICKPAYX", "PAYDRVSTX", "PAYVACTNX"]].head(40)
      .to_string(index=False), "\n")
for a, b in [("SICKPAYX", "SICKPAY"), ("PAYDRVSTX", "PAYDRVST"), ("PAYVACTNX", "PAYVACTN")]:
    t = new.assign(**{a: sas_format(new[a], YN), b: sas_format(new[b], YN)})
    print(freq_list(t, [a, b]).to_string(index=False), "\n")
