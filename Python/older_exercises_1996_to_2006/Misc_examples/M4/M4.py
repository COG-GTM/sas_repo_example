# ----------------------------------------------------------------------------
# PROGRAM:      C:\MEPS\PROG\EXAMPLE04.SAS
#
# DESCRIPTION:  THIS EXAMPLE ILLUSTRATES TWO WAYS OF DETERMINING THE NUMBER
#               OF EVENTS ASSOCIATED WITH CONDITIONS.
#               (1) USING THE evNUM VARIABLES ON THE CONDITIONS FILE.
#               (2) USING THE NUMBER OF MATCHES BETWEEN THE CONDITIONS FILE
#                   AND THE CLNK FILE.
#               ABOUT 76% OF THE CONDITIONS LISTED ON THE 2003 MEDICAL
#               CONDITIONS FILE ARE ASSOCIATED WITH ONE OR MORE EVENTS.
#               NOTE: BECAUSE PERSONS CAN HAVE MORE THAN ONE REPORTED CONDITION,
#                     AND A CONDITION CAN BE ASSOCIATED WITH MORE THAN ONE EVENT,
#                     THE COUNTS COMPUTED HERE FOR THE CONDITION LEVEL WILL NOT
#                     EQUAL PERSON- OR EVENT-LEVEL COUNTS.
#                     FOR EXAMPLE, ONE HOSPITAL STAY MIGHT BE ASOCIATED WITH
#                     TWO CONDITIONS (BROKEN ARM, BROKEN LEG).
#                     ALSO, ONE CONDITION (STROKE) MIGHT BE ASSOCIATED WITH
#                     TWO EVENTS (HOSPITAL STAY, OFFICE VISIT).
#
# INPUT FILES:  (1) C:\MEPS\DATA\H78.SAS7BDAT (2003 MEDICAL CONDITIONS FILE)
#               (2) C:\MEPS\DATA\H77I1.SAS7BDAT (2003 CLNK FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M4/M4.sas
# ----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, crosstab_list, load_meps  # noqa: E402

pd.set_option("display.width", 200)

EVNUMF = [(0, 0, " 0"), (1, 10, " 1-10"), (11, 25, "11-25"), (26, 50, "26-50"), (51, np.inf, "51+")]
EVS = ["ER", "HH", "IP", "OB", "OP", "RX"]

cond_num = load_meps("h78", columns=["CONDIDX"] + [f"{e}NUM" for e in EVS])
cond_num["TOTNUM"] = cond_num[[f"{e}NUM" for e in EVS]].sum(axis=1, min_count=len(EVS))

# Count CLNK matches by event type (EVENTYPE 1=OB 2=OP 3=ER 4=IP 7=HH 8=RX)
clnk = load_meps("h77if1", columns=["CONDIDX", "EVENTYPE"])
evtype = {1: "OBCNT", 2: "OPCNT", 3: "ERCNT", 4: "IPCNT", 7: "HHCNT", 8: "RXCNT"}
cond_cnt = pd.crosstab(clnk["CONDIDX"], clnk["EVENTYPE"].map(evtype))
cond_cnt = cond_cnt.reindex(columns=[f"{e}CNT" for e in EVS], fill_value=0).reset_index()

cond2003 = cond_num.merge(cond_cnt, on="CONDIDX", how="outer", indicator=True)
cnts = [f"{e}CNT" for e in EVS]
cond2003[cnts] = cond2003[cnts].fillna(0)
cond2003["TOTCNT"] = cond2003[cnts].sum(axis=1)
m = cond2003.pop("_merge").value_counts()
print(f"BOTH={m.get('both', 0)} JUSTA={m.get('left_only', 0)} JUSTB={m.get('right_only', 0)}\n")

for a, b in [("TOTNUM", "TOTCNT")] + [(f"{e}NUM", f"{e}CNT") for e in EVS]:
    t = cond2003.assign(**{c: apply_format(cond2003[c], EVNUMF, missing=".") for c in (a, b)})
    tab = crosstab_list(t, [a, b])
    tab["PERCENT"] = (100 * tab["COUNT"] / tab["COUNT"].sum()).round(2)
    print(tab.to_string(index=False), "\n")
