# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Employment_examples/EM2/EM2.sas
#
# DESCRIPTION: AMONG PERSONS 18 AND OLDER WITH A CURRENT JOB STARTING ON OR BEFORE JANUARY 1,
#              2002, THE PERCENT WHO EITHER ADDED OR STOPPED A JOB IN 2002.
#
# INPUT FILES: C:/MEPS/h63.ssp (2002 JOBS FILE), C:/MEPS/h70.ssp (2002 FULL-YEAR FILE). The SAS
#              program used H62, a preliminary 2002 full-year file with preliminary weights
#              (PERWT02P) that is no longer distributed; the final file and weight (PERWT02F) are
#              used instead, so weighted results differ from EM2.pdf (see Python/VALIDATION.md).
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps

AGEF_BINS, AGEF_LABELS = [18, 45, 65, np.inf], ["18-44", "45-64", "65-85"]
WEIGHT = "PERWT02F"


def main(meps_dir=None):
    jobs = read_meps(meps_path("h63.ssp", meps_dir),
                     columns=["DUPERSID", "PANEL", "RN", "SUBTYPE", "JSTRTM", "JSTRTD", "JSTRTY", "JOBSIDX"])
    jan1 = (jobs["JSTRTM"] == 1) & (jobs["JSTRTD"] == 1) & (jobs["JSTRTY"] == 2002)
    first_rn = ((jobs["PANEL"] == 6) & (jobs["RN"] == 3)) | ((jobs["PANEL"] == 7) & (jobs["RN"] == 1))
    empstart = set(jobs.loc[first_rn & jobs["SUBTYPE"].isin([1, 2, 3, 4]) & ((jobs["JSTRTY"] < 2002) | jan1),
                            "DUPERSID"])
    # former jobs, or current jobs started in 2002 after January 1
    changed = jobs["SUBTYPE"].isin([3, 4]) | (jobs["SUBTYPE"].isin([1, 2]) & (jobs["JSTRTY"] == 2002) & ~jan1)
    chngjob = set(jobs.loc[changed & jobs["DUPERSID"].isin(empstart), "DUPERSID"])

    fy = read_meps(meps_path("h70.ssp", meps_dir), columns=["DUPERSID", "AGE02X", "AGE42X", "AGE31X", WEIGHT])
    age = fy["AGE02X"].where(fy["AGE02X"] >= 0, fy["AGE42X"].where(fy["AGE42X"] >= 0,
                                                                  fy["AGE31X"].where(fy["AGE31X"] >= 0)))
    hc = fy.assign(AGE=age)[age >= 18].copy()
    print(pd.cut(hc["AGE"], AGEF_BINS, right=False, labels=AGEF_LABELS).value_counts(sort=False).to_string())

    hc["EMPSTART"] = np.where(hc["DUPERSID"].isin(empstart), "YES", "NO")
    hc["CHNGJOB"] = np.where(hc["DUPERSID"].isin(chngjob), "YES", "NO")
    unweighted = {v: hc[v].value_counts().sort_index() for v in ("EMPSTART", "CHNGJOB")}
    print(hc.groupby(["EMPSTART", "CHNGJOB"]).size().to_string())

    pop = hc[hc["EMPSTART"] == "YES"]
    weighted = pop.groupby("CHNGJOB")[WEIGHT].sum()
    table = pd.DataFrame({"frequency": weighted, "percent": weighted / weighted.sum() * 100})
    print(f"\nweighted CHNGJOB among EMPSTART = YES:\n{table.to_string()}")
    return {**unweighted, "chngjob_weighted": table}


if __name__ == "__main__":
    main()
