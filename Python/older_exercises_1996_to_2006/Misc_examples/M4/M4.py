"""Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M4/M4.sas

/

PROGRAM:        C://MEPS/PROG/EXAMPLE04.SAS

DESCRIPTION:      THIS EXAMPLE ILLUSTRATES TWO WAYS OF DETERMINING THE NUMBER
               OF EVENTS ASSOCIATED WITH CONDITIONS.
               (1) USING THE evNUM VARIABLES ON THE CONDITIONS FILE.
               (2) USING THE NUMBER OF MATCHES BETWEEN THE CONDITIONS FILE
                   AND THE CLNK FILE.

               ABOUT 76% OF THE CONDITIONS LISTED ON THE 2003 MEDICAL
               CONDITIONS FILE ARE ASSOCIATED WITH ONE OR MORE EVENTS.

               NOTE: BECAUSE PERSONS CAN HAVE MORE THAN ONE REPORTED CONDITION,
                     AND A CONDITION CAN BE ASSOCIATED WITH MORE THAN ONE EVENT,
                     THE COUNTS COMPUTED HERE FOR THE CONDITION LEVEL WILL NOT
                     EQUAL PERSON- OR EVENT-LEVEL COUNTS.

                     FOR EXAMPLE, ONE HOSPITAL STAY MIGHT BE ASOCIATED WITH
                     TWO CONDITIONS (BROKEN ARM, BROKEN LEG).
                     ALSO, ONE CONDITION (STROKE) MIGHT BE ASSOCIATED WITH
                     TWO EVENTS (HOSPITAL STAY, OFFICE VISIT).


INPUT FILES:      (1) <MEPS_DIR>/DATA/H78.SAS7BDAT (2003 MEDICAL CONDITIONS FILE)
               (2) <MEPS_DIR>/DATA/H77I1.SAS7BDAT (2003 CLNK FILE)


/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

EVNUMF = {0: " 0", slice(1, 10): " 1-10", slice(11, 25): "11-25", slice(26, 50): "26-50", slice(51, None): "51+"}
TYPES = ["ER", "HH", "IP", "OB", "OP", "RX"]

# 2003 conditions file (HC-078): number of events of each type linked to the condition
cond_num = read_meps("h78", ["CONDIDX", "ERNUM", "HHNUM", "IPNUM", "OBNUM", "OPNUM", "RXNUM"])
cond_num["TOTNUM"] = cond_num[[t + "NUM" for t in TYPES]].sum(axis=1, min_count=6)

# 2003 condition-event link file (HC-077I1 / HC-077IF1): count linked events by EVENTYPE ------------
try:
    clnk = read_meps("h77i1", ["CONDIDX", "EVENTYPE"], download=False)
except FileNotFoundError:  # HC-077I1 has been re-released as HC-077IF1
    clnk = read_meps("h77if1", ["CONDIDX", "EVENTYPE"])
code = {1: "OB", 2: "OP", 3: "ER", 4: "IP", 7: "HH", 8: "RX"}
cond_cnt = pd.crosstab(clnk.CONDIDX, clnk.EVENTYPE.map(code)).reindex(columns=TYPES, fill_value=0)
cond_cnt = cond_cnt.add_suffix("CNT").reset_index()

# MERGE COND_NUM (IN=A) COND_CNT (IN=B) BY CONDIDX; missing counts -> 0 ------------------
cond2003 = cond_num.merge(cond_cnt, on="CONDIDX", how="outer", indicator=True)
cnt = [t + "CNT" for t in TYPES]
cond2003[cnt] = cond2003[cnt].fillna(0)
cond2003["TOTCNT"] = cond2003[cnt].sum(axis=1)
m = cond2003._merge.value_counts()
print(f"BOTH={m['both']} JUSTA={m['left_only']} JUSTB={m['right_only']}\n")

for a, b in [("TOTNUM", "TOTCNT")] + [(t + "NUM", t + "CNT") for t in TYPES]:
    t = cond2003.assign(**{a: sas_format(cond2003[a], EVNUMF), b: sas_format(cond2003[b], EVNUMF)})
    print(freq_list(t, [a, b]).to_string(index=False), "\n")
