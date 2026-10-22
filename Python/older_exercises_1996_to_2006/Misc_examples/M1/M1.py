# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M1/M1.sas
# (validated against the SAS listing in M1.pdf)
#
# DESCRIPTION: COMPARES UNWEIGHTED AND WEIGHTED PERCENTAGES OF PERSONS BY
#              RACE/ETHNICITY (RACETHNX), 2005 -- WHY THE WEIGHTS MATTER.
#
# INPUT FILE:  C:/MEPS/h97.ssp (2005 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps

RACETHNB = {1: "HISPANIC", 2: "BLACK-NO OTH RACE/NOT HISPANIC", 3: "ASIAN-NO OTH RACE/NOT HISPANIC",
            4: "OTHER/NOT HISPANIC"}


def main(meps_dir=None):
    puf = read_meps(meps_path("h97.ssp", meps_dir), columns=["RACETHNX", "PERWT05F"])
    race = puf["RACETHNX"].map(RACETHNB)
    unwt = race.value_counts(normalize=True) * 100                       # PROC FREQ
    wt = puf.groupby(race)["PERWT05F"].sum() / puf["PERWT05F"].sum() * 100  # PROC FREQ ... WEIGHT
    pct = pd.DataFrame({"PERCENT": unwt, "WTPCT": wt}).reindex(list(RACETHNB.values()))
    print(pct.round(2).to_string())
    results = {"pct_race": pd.DataFrame({"RACETHNX": pct.index, "variable": "PERCENT", "estimate": pct["PERCENT"].to_numpy(),
                                         "se": float("nan")}),
               "wtpct_race": pd.DataFrame({"RACETHNX": pct.index, "variable": "WTPCT", "estimate": pct["WTPCT"].to_numpy(),
                                           "se": float("nan")})}
    return results


if __name__ == "__main__":
    main()
