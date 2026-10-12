# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L1A/L1A.sas
# (L1A.pdf contains only the SAS log, so there are no printed values to validate against)
#
# DESCRIPTION: (1) IDENTIFY 2001 CURRENT MAIN JOBS (PANEL 5 ROUND 3) THAT BEGAN IN 2000
#              (2) IDENTIFY THE FIRST-REPORTED 2000 RECORD OF THOSE JOBS (PANEL 5 ROUNDS 1-2)
#              (3) UPDATE MISSING 2001 VALUES (SICKPAY, PAYDRVST, PAYVACTN) WITH 2000 VALUES
#
# INPUT FILES: C:/MEPS/h40.ssp (2000 JOBS), C:/MEPS/h56.ssp (2001 JOBS)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps

YN = {-9: "-9 Not Asc", -8: "-8 DK", -7: "-7 Refised", -1: "-1 Inappl", 1: "1 Yes", 2: "2 No"}
EXAMPLE = ["80001021", "80005018", "80005025", "80006012", "80006029"]
KEYS = ["DUPERSID", "PANEL", "RN", "JOBSN", "SUBTYPE", "STILLAT"]
BENEFITS = ["SICKPAY", "PAYDRVST", "PAYVACTN"]


def main(meps_dir=None):
    j00 = read_meps(meps_path("h40.ssp", meps_dir), columns=[*KEYS, *BENEFITS]).assign(YEAR=2000)
    j01 = read_meps(meps_path("h56.ssp", meps_dir), columns=[*KEYS, *BENEFITS]).assign(YEAR=2001)

    ex = pd.concat([j00[j00["DUPERSID"].isin(EXAMPLE) & j00["RN"].isin([1, 2])], j01[j01["DUPERSID"].isin(EXAMPLE)]])
    print(ex.sort_values(["DUPERSID", "JOBSN", "YEAR", "RN"])[["DUPERSID", "YEAR", *KEYS[2:], *BENEFITS]]
          .to_string(index=False))

    # 2001 current main jobs held since a previous round; benefits are asked only when the job is first reported
    cmj01 = j01[(j01["PANEL"] == 5) & (j01["RN"] == 3) & (j01["SUBTYPE"] == 1) & (j01["STILLAT"] == 1)]
    cmj00 = j00[(j00["PANEL"] == 5) & j00["RN"].isin([1, 2]) & (j00["SUBTYPE"] == 1) & (j00["STILLAT"] == -1)]
    for name, df in (("2001", cmj01), ("2000 first-reported", cmj00)):
        print(f"\n{name} current main jobs:")
        for b in BENEFITS:
            print(df[b].map(YN).value_counts(dropna=False).sort_index().to_string())

    new = cmj01.drop(columns=["RN", "YEAR"]).merge(
        cmj00[["DUPERSID", "JOBSN", *BENEFITS]].rename(columns={b: f"{b}X" for b in BENEFITS}),
        on=["DUPERSID", "JOBSN"])
    print(new.head(60).to_string(index=False))
    tables = {b: new.groupby([new[f"{b}X"].map(YN), new[b].map(YN)]).size() for b in BENEFITS}
    for b, t in tables.items():
        print(f"\n{b}X * {b}:\n{t.to_string()}")
    return {"new": new, **tables}


if __name__ == "__main__":
    main()
