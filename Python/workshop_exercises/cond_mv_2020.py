"""Python port of SAS/workshop_exercises/cond_mv_2020.sas

Example code linking MEPS-HC Medical Conditions file to the Office-based
 medical visits file, data year 2020:

Event-level estimates:
  - Number of office-based visits for mental health
  - Total expenditures for office-based mental health treatment
  - Mean expenditure per office-based mental health visit

Person-level estimates
  - Number of people with office-based mental health visits
  - Percent of people with office-based mental health visits
  - Mean expenditure per person for office-based mental health visits


Input files:
  - h220g.sas7bdat   (2020 Office-based event file)
  - h222.sas7bdat    (2020 Conditions file)
  - h220if1.sas7bdat (2020 CLNK: Condition-event link file)
  - h224.sas7bdat    (2020 Full-Year Consolidated file)

Resources:
 - CCSR codes:
         https://github.com/HHS-AHRQ/MEPS/blob/master/Quick_Reference_Guides/meps_ccsr_conditions.csv

 - MEPS-HC Public Use Files:
        https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp

 - MEPS-HC data tools:
        https://datatools.ahrq.gov/meps-hc

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Load datasets ---------------------------------------------------------------------
ob20 = read_meps("h220g")    # Office-based medical visits
cond20 = read_meps("h222")   # Medical conditions
clnk20 = read_meps("h220if1")  # Condition-event linkage file
fyc20 = read_meps("h224")    # Person-level full-year file

ob20x = ob20[["PANEL", "DUPERSID", "EVNTIDX", "EVENTRN"] + [c for c in ob20 if c.startswith(("OBDATE", "TELE"))]
             + ["OBXP20X", "PERWT20F", "VARSTR", "VARPSU"]]
cond20x = cond20[["DUPERSID", "CONDIDX"] + [c for c in cond20 if c.startswith(("ICD10", "CCSR"))]].copy()
fyc20x = fyc20[["DUPERSID", "PERWT20F", "VARSTR", "VARPSU"]]

# Mental health conditions (CCSR codes) --------------------------------------------
cond20x["ALL_CCSR"] = cond20x.CCSR1X.fillna("") + cond20x.CCSR2X.fillna("") + cond20x.CCSR3X.fillna("")
mh_codes = ["MBD", "FAC002", "FAC007", "NVS011", "SYM008", "SYM009"]
mental_health = cond20x[cond20x.ALL_CCSR.str.contains("|".join(mh_codes))]
print(freq_list(mental_health, ["ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X"]))

# Office-based visits in the CLNK file (EVENTYPE = 1), merged to MH conditions ------
clnk_ob = clnk20[clnk20.EVENTYPE == 1]
print(freq_list(clnk_ob, ["EVENTYPE"]))
mh_clnk = mental_health.merge(clnk_ob, on=["DUPERSID", "CONDIDX"], how="inner")

# De-duplicate by event ID: a visit can be linked to more than one MH condition
mh_clnk_nodup = mh_clnk[["DUPERSID", "EVNTIDX", "EVENTYPE"]].drop_duplicates()

ob_mental_health = mh_clnk_nodup.merge(ob20x, on=["DUPERSID", "EVNTIDX"], how="inner")
ob_mental_health["MH_OB_VISIT"] = 1
print(freq_list(ob_mental_health, ["EVENTYPE", "MH_OB_VISIT"]))

# Merge with FYC so every person's VARSTR/VARPSU are in the design (full outer merge)
ob_mh_fyc = ob_mental_health.drop(columns=["PERWT20F", "VARSTR", "VARPSU"]).merge(
    fyc20x, on="DUPERSID", how="outer", indicator=True)
ob_mh_fyc["MH_OB"] = np.where(ob_mh_fyc._merge != "right_only", 1, 0)
ob_mh_fyc["FYC"] = np.where(ob_mh_fyc._merge != "left_only", 1, 0)
ob_mh_fyc["MH_OB_VISIT"] = ob_mh_fyc.MH_OB_VISIT.fillna(0)
ob_mh_fyc = ob_mh_fyc.drop(columns="_merge")
print(freq_list(ob_mh_fyc, ["MH_OB", "MH_OB_VISIT", "FYC"]))

# Event-level estimates --------------------------------------------------------------
#  - Number of office-based visits for mental health:       343,810,085 (SE: 22,252,863)
#  - Total exp. for office-based mental health visits:  $60,209,392,314 (SE: 4,437,433,004)
show("Event-level (no domain)", surveymeans(ob_mh_fyc, ["MH_OB_VISIT", "OBXP20X"], weight="PERWT20F")
     [["VarName", "Mean", "StdErr", "Sum", "StdDev"]])
ev = surveymeans(ob_mh_fyc, ["MH_OB_VISIT", "OBXP20X"], weight="PERWT20F", domain="MH_OB")
show("Event-level, DOMAIN MH_OB", ev[ev.MH_OB == 1][["MH_OB", "VarName", "Mean", "StdErr", "Sum", "StdDev"]])

print(pd.crosstab(ob20.OBDATEMM, ob20.TELEHEALTHFLAG))

# Person-level estimates ------------------------------------------------------------
#  - Number of people with office visit for MH:  29,816,984 (SE: 1,192,676)
#  - Percent of people with office visit for MH:      9.08% (SE: 0.29%)
#  - Mean exp per person for office visits for MH: $2019.30 (SE: 126.16)
# (SAS: PROC MEANS BY DUPERSID VARSTR VARPSU; OUTPUT mean()= sum()=)
pers_mh = ob_mh_fyc.groupby(["DUPERSID", "VARSTR", "VARPSU"]).agg(
    PERWT20F=("PERWT20F", "mean"), persXP=("OBXP20X", lambda s: s.sum(min_count=1)),
    pers_nevents=("MH_OB_VISIT", "sum"), mh_ob_visit_pers=("MH_OB_VISIT", "mean"),
    mh_ob_pers=("MH_OB", "mean")).reset_index()
print(freq_list(pers_mh, ["mh_ob_pers"]))
pm = surveymeans(pers_mh, ["mh_ob_visit_pers", "persXP", "pers_nevents"], weight="PERWT20F", domain="mh_ob_pers")
show("Person-level, DOMAIN mh_ob_pers", pm[["mh_ob_pers", "VarName", "N", "Mean", "StdErr", "Sum", "StdDev"]])
