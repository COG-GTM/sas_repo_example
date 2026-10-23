"""Python port of SAS/summary_tables_examples/cond_expenditures_2018.sas

Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Medical Conditions, 2018:
 - Number of people with care
 - Number of events
 - Total expenditures
 - Mean expenditure per person

Note: Starting in 2016, conditions were converted from ICD-9 and CCS codes
 to ICD-10 and CCSR codes

Input files:
    - <MEPS_DIR>/h206a.sas7bdat (2018 RX event file)
    - <MEPS_DIR>/h206d.sas7bdat (2018 IP event file)
    - <MEPS_DIR>/h206e.sas7bdat (2018 ER event file)
    - <MEPS_DIR>/h206f.sas7bdat (2018 OP event file)
    - <MEPS_DIR>/h206g.sas7bdat (2018 OB event file)
    - <MEPS_DIR>/h206h.sas7bdat (2018 HH event file)
    - <MEPS_DIR>/h206if1.sas7bdat (2018 CLNK: Condition-event link file)
    - <MEPS_DIR>/h207.sas7bdat    (2018 Conditions file)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Load event files (SAS: LIBNAME mylib "C:/MEPS"; SET mylib.h206x) ---------------
rx = read_meps("h206a")
ip = read_meps("h206d")
er = read_meps("h206e")
op = read_meps("h206f")
ob = read_meps("h206g")
hh = read_meps("h206h")

# Aggregate RX fills to the prescribed-medicine (LINKIDX) level -------------------
design = ["VARSTR", "VARPSU", "PERWT18F"]
rx_pers = (rx.groupby(["DUPERSID", "LINKIDX"] + design, dropna=False)
           .agg(XP18X=("RXXP18X", "sum"), n_fills=("RXXP18X", "count")).reset_index())

# Stack events (SAS: SET ... (IN=) with RENAME) -----------------------------------
keep = ["DUPERSID", "EVNTIDX", "XP18X", "n_fills"] + design
parts = [rx_pers.rename(columns={"LINKIDX": "EVNTIDX"}).assign(data="RX")]
for df, typ in [(ip, "IP"), (er, "ER"), (op, "OP"), (ob, "OB"), (hh, "HH")]:
    parts.append(df.rename(columns={f"{typ}XP18X": "XP18X"}).reindex(columns=keep).assign(data=typ))
stacked_events = pd.concat([p.reindex(columns=keep + ["data"]) for p in parts], ignore_index=True)
stacked_events["n_events"] = stacked_events.n_fills.fillna(1).clip(lower=1)  # max(n_fills, 1)
stacked_events = stacked_events.drop(columns="n_fills")

# Load conditions file, CLNK file, and CCSR -> collapsed condition crosswalk -------
clnk1 = read_meps("h206if1")
cond_puf = read_meps("h207")
ccsr_file = MEPS_DIR / "meps_ccsr_conditions.csv"
ccsr_url = ("https://raw.githubusercontent.com/HHS-AHRQ/MEPS/master/Quick_Reference_Guides/"
            "meps_ccsr_conditions.csv")
condition_codes = pd.read_csv(ccsr_file if ccsr_file.exists() else ccsr_url)
condition_codes = condition_codes.rename(columns={"CCSR Code": "CCSR",
                                                  "MEPS collapsed condition category": "Condition"})
condition_codes = condition_codes[["CCSR", "Condition"]]

# Merge conditions with CLNK; convert CCSR1X-CCSR3X to long (PROC TRANSPOSE) -------
ccsr_cols = [c for c in cond_puf.columns if c.startswith("CCSR")]
cond_clink = clnk1[["DUPERSID", "CONDIDX", "EVNTIDX"]].merge(
    cond_puf[["DUPERSID", "CONDIDX"] + ccsr_cols], on=["DUPERSID", "CONDIDX"], how="outer")
cond_long = cond_clink.melt(id_vars=["DUPERSID", "CONDIDX", "EVNTIDX"], value_vars=ccsr_cols, value_name="CCSR")
cond_long = cond_long[cond_long.CCSR != "-1"].drop(columns="variable")

# Add collapsed condition categories (left join; drop unmatched CCSR codes)
cond = cond_long.merge(condition_codes, on="CCSR", how="left")
cond = cond[cond.Condition.fillna("") != ""]
cond = cond[["DUPERSID", "EVNTIDX", "Condition"]].drop_duplicates()  # PROC SORT NODUPKEY

# Merge events with conditions; keep events with a condition and valid expenditure
all_events = stacked_events.merge(cond, on=["DUPERSID", "EVNTIDX"], how="outer")
all_events = all_events[all_events.Condition.fillna("").ne("") & (all_events.XP18X >= 0)]

# Aggregate to person-condition level ----------------------------------------------
all_pers = (all_events.groupby(["DUPERSID", "VARSTR", "VARPSU", "Condition"])
            .agg(mean_XP=("XP18X", "mean"), avg_events=("n_events", "mean"), PERWT18F=("PERWT18F", "mean"),
                 pers_XP=("XP18X", "sum"), n_events=("n_events", "sum")).reset_index())
all_pers["person"] = 1

# Calculate estimates using survey procedures (DOMAIN Condition) ------------------
out = surveymeans(all_pers, ["person", "n_events", "pers_XP"], weight="PERWT18F", strata="VARSTR",
                  cluster="VARPSU", domain="Condition")
show("Number of people with care", out[out.VarName == "person"][["Condition", "Sum", "StdDev"]])
show("Number of events", out[out.VarName == "n_events"][["Condition", "Sum", "StdDev"]])
show("Total and mean expenditures", out[out.VarName == "pers_XP"][["Condition", "Sum", "StdDev", "Mean", "StdErr"]])
