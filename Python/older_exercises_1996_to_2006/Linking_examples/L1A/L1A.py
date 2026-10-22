# ----------------------------------------------------------------------------
# Program:      c:\meps\prog\Example_L1A.sas
#
# Description:  This example shows how to:
#               (1) Identify 2001 jobs in that began in 2000
#               (2) Identify first-reported 2000 jobs
#               (3) Update missing 2001 values with 2000 values
#
# Input Files:  c:\meps\data\h40.sas7bdat (2000 Jobs)
#               c:\meps\data\h56.sas7bdat (2001 Jobs)
#
# Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L1A/L1A.sas
# ----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps  # noqa: E402

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 40)

YN = {-9: "-9 Not Asc", -8: "-8 DK", -7: "-7 Refised", -1: "-1 Inappl", 1: " 1 Yes", 2: " 2 No"}

H40 = load_meps("h40")
H56 = load_meps("h56")


def var_range(df, first, last):
    """SAS variable list ``first--last`` (all columns between two names, inclusive)."""
    cols = list(df.columns)
    return cols[cols.index(first) : cols.index(last) + 1]


PAYVARS = var_range(H56, "SICKPAY", "PAYVACTN")
KEEP = ["DUPERSID", "PANEL", "RN", "JOBSN", "SUBTYPE", "STILLAT"] + PAYVARS
by = ["DUPERSID", "JOBSN", "RN"]


def freqs(df):
    for v in PAYVARS:
        print(crosstab_list(df.assign(**{v: apply_format(df[v], YN)}), [v]).T.to_string(header=False))


# Example persons: jobs records from both years ------------------------------
ids = ["80001021", "80005018", "80005025", "80006012", "80006029"]
s00 = H40[H40["DUPERSID"].isin(ids) & H40["RN"].isin([1, 2])].assign(YEAR=2000)
s01 = H56[H56["DUPERSID"].isin(ids)].assign(YEAR=2001)
both = pd.concat([s00, s01]).sort_values(["DUPERSID", "JOBSN"], kind="stable")
print(both[["DUPERSID", "YEAR", "RN", "JOBSN", "SUBTYPE", "STILLAT"] + PAYVARS].to_string(index=False), "\n")

# Identify 2001 CMJs (panel 5, rn 3) that began in 2000 ------------------------
cmj01 = H56.loc[(H56["PANEL"] == 5) & (H56["RN"] == 3) & (H56["SUBTYPE"] == 1) & (H56["STILLAT"] == 1), KEEP]
cmj01 = cmj01.sort_values(by, kind="stable")
freqs(cmj01)
print(cmj01.head(60).to_string(index=False), "\n")

# Identify newly-reported 2000 CMJs (panel 5, rn 1,2) --------------------------
cmj00 = H40.loc[(H40["PANEL"] == 5) & H40["RN"].isin([1, 2]) & (H40["SUBTYPE"] == 1) & (H40["STILLAT"] == -1), KEEP]
cmj00 = cmj00.sort_values(by, kind="stable")
freqs(cmj00)
print(cmj00.head(60).to_string(index=False), "\n")

# Concatenate records from both years (interleaved BY DUPERSID JOBSN)
print(pd.concat([cmj00, cmj01]).sort_values(["DUPERSID", "JOBSN"], kind="stable").head(60).to_string(index=False), "\n")

# Merge records from both years, renaming 2000 variables (drop STILLAT so it
# doesn't overwrite the 2001 value): xxxX = xxx from First-Reported Round
ren = {"SICKPAY": "SICKPAYX", "PAYDRVST": "PAYDRVSTX", "PAYVACTN": "PAYVACTNX"}
new = cmj01.merge(
    cmj00.rename(columns=ren)[["DUPERSID", "JOBSN"] + list(ren.values())], on=["DUPERSID", "JOBSN"], how="inner"
).drop(columns="RN")
print(new[["DUPERSID", "PANEL", "JOBSN", "SUBTYPE", "STILLAT"] + list(ren.values())].head(60).to_string(index=False))
for old, x in ren.items():
    print(crosstab_list(new.assign(**{c: apply_format(new[c], YN) for c in (x, old)}), [x, old]).to_string(index=False))
