# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M4/M4.sas
# (M4.pdf contains only the SAS log, so there are no printed values to validate against)
#
# DESCRIPTION: QC OF THE 2003 CONDITION FILE EVENT COUNTS (ERNUM ... RXNUM) AGAINST THE NUMBER
#              OF EVENTS LINKED TO EACH CONDITION IN THE CONDITION-EVENT LINK FILE (CLNK).
#
# INPUT FILES: C:/MEPS/h78.ssp (2003 CONDITIONS), C:/MEPS/h77if1.ssp (2003 CLNK; SAS: H77I1)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps

EVENTYPE = {1: "OBCNT", 2: "OPCNT", 3: "ERCNT", 4: "IPCNT", 7: "HHCNT", 8: "RXCNT"}
CNT = ["ERCNT", "HHCNT", "IPCNT", "OBCNT", "OPCNT", "RXCNT"]


def evnumf(n):
    return pd.Series(np.select([n == 0, n.between(1, 10), n.between(11, 25), n.between(26, 50), n > 50],
                               [" 0", " 1-10", "11-25", "26-50", "51+"], ""), index=n.index)


def main(meps_dir=None):
    cond = read_meps(meps_path("h78.ssp", meps_dir), columns=["CONDIDX", "ERNUM", "HHNUM", "IPNUM", "OBNUM", "OPNUM", "RXNUM"])
    cond["TOTNUM"] = cond[["ERNUM", "HHNUM", "IPNUM", "OBNUM", "OPNUM", "RXNUM"]].sum(axis=1)
    clnk = read_meps(meps_path("h77if1.ssp", meps_dir), columns=["CONDIDX", "EVENTYPE"])
    cnt = pd.crosstab(clnk["CONDIDX"], clnk["EVENTYPE"].map(EVENTYPE)).reindex(columns=CNT, fill_value=0).reset_index()

    merged = cond.merge(cnt, on="CONDIDX", how="outer", indicator=True)
    merged[CNT] = merged[CNT].fillna(0)
    merged["TOTCNT"] = merged[CNT].sum(axis=1)
    both, justa, justb = [(merged["_merge"] == k).sum() for k in ("both", "left_only", "right_only")]
    print(f"BOTH={both} JUSTA={justa} JUSTB={justb}")
    print(pd.crosstab(evnumf(merged["TOTNUM"].fillna(0)), evnumf(merged["TOTCNT"])))
    print("conditions where TOTNUM != TOTCNT:", int((merged["TOTNUM"].fillna(0) != merged["TOTCNT"]).sum()))
    return {"cond2003": merged.drop(columns="_merge")}


if __name__ == "__main__":
    main()
