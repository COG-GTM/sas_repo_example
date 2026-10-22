# ----------------------------------------------------------------
# PROGRAM:      C:\MEPS\PROG\EXAMPLE_EM2.SAS
#
# DESCRIPTION:  THIS EXAMPLE SHOWS HOW TO USE THE 2002 MEPS JOBS
#               FILE (HC-063) TO DETERMINE HOW MANY PERSONS
#               WORKING AT THE START OF THE YEAR CHANGED JOBS.
#
# INPUT FILES:  (1) C:\MEPS\DATA\H63.SAS7BDAT (2002 MEPS JOBS FILE)
#               (2) C:\MEPS\DATA\H62.SAS7BDAT (2002 MEPS FULL-YEAR FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Employment_examples/EM2/EM2.sas
# ----------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps  # noqa: E402

pd.set_option("display.width", 200)

AGEF = [(18, 44, "18-44"), (45, 64, "45-64"), (65, 85, "65-85")]

H63 = load_meps("h63", columns=["DUPERSID", "PANEL", "RN", "SUBTYPE", "JSTRTM", "JSTRTD", "JSTRTY", "JOBSIDX"])

# Persons who had a job on or before January 1, 2002 (person-level)
j = H63
start = (
    (((j["PANEL"] == 6) & (j["RN"] == 3)) | ((j["PANEL"] == 7) & (j["RN"] == 1)))
    & j["SUBTYPE"].isin([1, 2, 3, 4])
    & ((j["JSTRTY"] < 2002) | ((j["JSTRTM"] == 1) & (j["JSTRTD"] == 1) & (j["JSTRTY"] == 2002)))
)
empstart_pop = set(j.loc[start, "DUPERSID"])

# Of those, persons who either added a job or changed jobs during 2002
jan1 = (j["JSTRTD"] == 1) & (j["JSTRTM"] == 1)
chng = j["SUBTYPE"].isin([3, 4]) | (j["SUBTYPE"].isin([1, 2]) & (j["JSTRTY"] == 2002) & ~jan1)
chngjob_pop = set(j.loc[chng & j["DUPERSID"].isin(empstart_pop), "DUPERSID"])

# Adults (latest valid age >= 18) from the full-year file
hc62 = load_meps("h62", columns=["DUPERSID", "AGE02X", "AGE42X", "AGE31X", "PERWT02P"])
hc62["AGE"] = np.select([hc62["AGE02X"] >= 0, hc62["AGE42X"] >= 0, hc62["AGE31X"] >= 0],
                        [hc62["AGE02X"], hc62["AGE42X"], hc62["AGE31X"]], default=np.nan)  # fmt: skip
hc62 = hc62[hc62["AGE"] >= 18][["DUPERSID", "AGE", "PERWT02P"]]
t = crosstab_list(hc62.assign(AGE=apply_format(hc62["AGE"], AGEF)), ["AGE"])
print(t.assign(PERCENT=(100 * t["COUNT"] / t["COUNT"].sum()).round(2)).to_string(index=False), "\n")

# Combined data set with EMPSTART / CHNGJOB indicators
chnginfo = hc62.assign(
    EMPSTART=np.where(hc62["DUPERSID"].isin(empstart_pop), "YES", "NO"),
    CHNGJOB=np.where(hc62["DUPERSID"].isin(chngjob_pop), "YES", "NO"),
)
for cols in (["EMPSTART"], ["CHNGJOB"], ["EMPSTART", "CHNGJOB"], ["CHNGJOB"]):
    t = crosstab_list(chnginfo, cols)
    print(t.assign(PERCENT=(100 * t["COUNT"] / t["COUNT"].sum()).round(2)).to_string(index=False), "\n")

# Weighted: persons employed at the start of the year
w = chnginfo[chnginfo["EMPSTART"] == "YES"].groupby("CHNGJOB")["PERWT02P"].sum().rename("FREQUENCY").to_frame()
w["PERCENT"] = 100 * w["FREQUENCY"] / w["FREQUENCY"].sum()
print(w)
