# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L1/L1.sas
# (validated against the SUDAAN/SAS listing in L1.pdf)
#
# DESCRIPTION: (1) IDENTIFY JOBS IN THE FIRST PART OF 2001 (PANEL 5 ROUND 3, PANEL 6 ROUND 1)
#              (2) COUNT THE NUMBER OF EACH TYPE OF JOB FOR EACH PERSON
#              (3) MERGE JOBS AND FULL-YEAR FILES
#              (4) ESTIMATE PERSONS AGE 18+ BY NUMBER OF CURRENT JOBS WITH STANDARD ERRORS AND
#                  RELATIVE STANDARD ERRORS (SUDAAN PROC CROSSTAB IN THE ORIGINAL)
#
# INPUT FILES: C:/MEPS/h56.ssp (2001 JOBS), C:/MEPS/h60.ssp (2001 FULL-YEAR PERSONS)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate

SUBTYPE = {1: "CURRENT MAIN", 2: "CURRENT MISC", 3: "FORMER MAIN", 4: "FORMER MISC", 5: "LAST JOB OUTSIDE RN",
           6: "RETIREMENT JOB"}
NCURR = {1: "None", 2: "1", 3: "2", 4: "3+"}
# SUDAAN drops records with nonpositive weights; strata then left with one PSU are omitted
DESIGN = {"weight": "PERWT01F", "stratum": "VARSTR01", "psu": "VARPSU01", "lonely_psu": "remove"}


def main(meps_dir=None):
    jobs = read_meps(meps_path("h56.ssp", meps_dir), columns=["DUPERSID", "PANEL", "RN", "SUBTYPE"])
    jobs = jobs[((jobs["PANEL"] == 5) & (jobs["RN"] == 3)) | ((jobs["PANEL"] == 6) & (jobs["RN"] == 1))]
    print(jobs["SUBTYPE"].map(SUBTYPE).value_counts().to_string())

    jobsper = pd.crosstab(jobs["DUPERSID"], jobs["SUBTYPE"]).reindex(columns=range(1, 7), fill_value=0)
    jobsper.columns = [f"N{c}" for c in jobsper.columns]
    jobsper["TOTJOBS"] = jobsper.sum(axis=1)
    jobsper = jobsper.reset_index()
    print(jobsper.groupby(["TOTJOBS", *[f"N{c}" for c in range(1, 7)]]).size().to_string())

    fy = read_meps(meps_path("h60.ssp", meps_dir),
                   columns=["DUPERSID", "PANEL01", "AGE31X", "PERWT01F", "VARSTR01", "VARPSU01"])
    curr = fy.merge(jobsper, on="DUPERSID", how="left")
    curr = curr[curr["PERWT01F"] > 0].copy()
    curr["NUMCURR"] = curr["N1"] + curr["N2"]                     # missing when no job record
    curr["NCURR"] = np.minimum(curr["NUMCURR"] + 1, 4)
    for k, lab in NCURR.items():
        curr[f"NCURR_{lab}"] = (curr["NCURR"] == k).astype(float)
    curr["TOTAL"] = 1.0

    subpop = (curr["AGE31X"] >= 18) & curr["NCURR"].notna()       # SUBPOPN AGE31X >= 18
    cols = ["TOTAL", *[f"NCURR_{lab}" for lab in NCURR.values()]]
    pop = survey_estimate(curr, cols, stat="total", subpop=subpop, **DESIGN)
    pct = survey_estimate(curr, cols, stat="mean", subpop=subpop, **DESIGN)
    pct[["estimate", "se"]] *= 100
    sample = curr.loc[subpop, cols].sum()
    table = pd.DataFrame({"ncurr": ["TOTAL", *NCURR.values()], "sample": sample.to_numpy(),
                          "population": pop["estimate"].to_numpy(), "se_pop": pop["se"].to_numpy(),
                          "column_pct": pct["estimate"].to_numpy(), "se_col_pct": pct["se"].to_numpy()})
    table["rse_pop"] = table["se_pop"] / table["population"]
    table["rse_col_pct"] = (table["se_col_pct"] / table["column_pct"]).where(table["ncurr"] != "TOTAL", 0.0)
    print(table.to_string(index=False))
    return {"pop": pop.assign(variable=table["ncurr"].to_numpy()),
            "pct": pct.assign(variable=table["ncurr"].to_numpy())}


if __name__ == "__main__":
    main()
