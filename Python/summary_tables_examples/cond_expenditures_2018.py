# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Medical Conditions, 2018:
#  - Number of people with care
#  - Number of events
#  - Total expenditures
#  - Mean expenditure per person
#
# Note: Starting in 2016, conditions were converted from ICD-9 and CCS codes
#  to ICD-10 and CCSR codes
#
# Input files:
#   - C:/MEPS/h206a.sas7bdat (2018 RX event file)
#   - C:/MEPS/h206d.sas7bdat (2018 IP event file)
#   - C:/MEPS/h206e.sas7bdat (2018 ER event file)
#   - C:/MEPS/h206f.sas7bdat (2018 OP event file)
#   - C:/MEPS/h206g.sas7bdat (2018 OB event file)
#   - C:/MEPS/h206h.sas7bdat (2018 HH event file)
#   - C:/MEPS/h206if1.sas7bdat (2018 CLNK: Condition-event link file)
#   - C:/MEPS/h207.sas7bdat    (2018 Conditions file)
#
# Python port of SAS/summary_tables_examples/cond_expenditures_2018.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import MEPS_DIR, read_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)
pd.set_option("display.max_rows", 200)

# Load datasets ---------------------------------------------------------------

# Event files
rx = read_meps(f"{MEPS_DIR}/h206a.sas7bdat")
ip = read_meps(f"{MEPS_DIR}/h206d.sas7bdat")
er = read_meps(f"{MEPS_DIR}/h206e.sas7bdat")
op = read_meps(f"{MEPS_DIR}/h206f.sas7bdat")
ob = read_meps(f"{MEPS_DIR}/h206g.sas7bdat")
hh = read_meps(f"{MEPS_DIR}/h206h.sas7bdat")

# For RX events, count number of fills per event
rx_pers = (
    rx.groupby(["DUPERSID", "LINKIDX", "VARSTR", "VARPSU", "PERWT18F"])
    .agg(XP18X=("RXXP18X", "sum"), n_fills=("RXXP18X", "count"))
    .reset_index()
)

# Stack event files
keep = ["data", "EVNTIDX", "DUPERSID", "n_events", "XP18X", "VARSTR", "VARPSU", "PERWT18F"]
frames = [rx_pers.rename(columns={"LINKIDX": "EVNTIDX"}).assign(data="RX")]
for name, ev in (("IP", ip), ("ER", er), ("OP", op), ("OB", ob), ("HH", hh)):
    frames.append(ev.rename(columns={f"{name}XP18X": "XP18X"}).assign(data=name))
stacked_events = pd.concat(frames, ignore_index=True)
# Count events (for RX, each fill is an event)
stacked_events["n_events"] = stacked_events["n_fills"].fillna(1).clip(lower=1)
stacked_events = stacked_events[keep]

# Load in event-condition linking file
clnk1 = read_meps(f"{MEPS_DIR}/h206if1.sas7bdat")

# Load in conditions file
cond_puf = read_meps(f"{MEPS_DIR}/h207.sas7bdat")

# Load crosswalk for CCSR and collapsed conditions codes
ccsr_url = (
    "https://raw.githubusercontent.com/HHS-AHRQ/MEPS/master/"
    "Quick_Reference_Guides/meps_ccsr_conditions.csv"
)
condition_codes = (
    pd.read_csv(ccsr_url)
    .rename(columns={"CCSR Code": "CCSR", "MEPS collapsed condition category": "Condition"})
    .drop(columns="CCSR Description")
)

# Merge datasets --------------------------------------------------------------

# Merge conditions file with the conditions-event link file (CLNK)
ccsr_cols = [c for c in cond_puf.columns if c.startswith("CCSR")]
cond_clink = clnk1[["DUPERSID", "CONDIDX", "EVNTIDX"]].merge(
    cond_puf[["DUPERSID", "CONDIDX"] + ccsr_cols], on=["DUPERSID", "CONDIDX"], how="outer"
)

# Convert multiple CCSRs to separate lines (wide to long; PROC TRANSPOSE)
cond_long = cond_clink.melt(
    id_vars=["DUPERSID", "CONDIDX", "EVNTIDX"], value_vars=ccsr_cols, value_name="CCSR"
).drop(columns="variable")
cond_long = cond_long[cond_long["CCSR"] != "-1"]

# Merge on collapsed condition codes
cond = cond_long.merge(condition_codes, on="CCSR", how="left")
cond = cond[cond["Condition"].notna() & (cond["Condition"] != "")]

# De-duplicate by event ID ('EVNTIDX') and collapsed code ('Condition')
cond = cond[["DUPERSID", "EVNTIDX", "Condition"]].drop_duplicates()

# Merge conditions and event files --------------------------------------------
#  - remove any observations with missing 'Condition' or negative expenditures

all_events = stacked_events.merge(cond, on=["DUPERSID", "EVNTIDX"], how="outer")
all_events = all_events[all_events["Condition"].notna() & (all_events["XP18X"] >= 0)]

# Aggregate to person-level, by Condition -------------------------------------

all_pers = (
    all_events.groupby(["DUPERSID", "VARSTR", "VARPSU", "Condition"])
    .agg(
        mean_XP=("XP18X", "mean"),
        avg_events=("n_events", "mean"),
        PERWT18F=("PERWT18F", "mean"),
        pers_XP=("XP18X", "sum"),
        n_events=("n_events", "sum"),
    )
    .reset_index()
)
all_pers["person"] = 1

# Calculate estimates using survey procedures ---------------------------------

design = dict(weight="PERWT18F", strata="VARSTR", psu="VARPSU", domain="Condition")
totals = svy_total(all_pers, ["person", "n_events", "pers_XP"], **design)
means = svy_mean(all_pers, ["pers_XP"], **design)


def show(var, label):
    tab = totals[totals["variable"] == var].rename(columns={"estimate": label})
    return tab[["Condition", label, "se"]]


# Number of people with care
print("Number of people with care")
print(show("person", "Number of people with care").to_string(index=False))

# Number of events
print("\nNumber of events")
print(show("n_events", "Number of events").to_string(index=False))

# Expenditures
print("\nTotal expenditures and mean expenditure per person with care")
exp = show("pers_XP", "Total expenditures").merge(
    means[["Condition", "estimate", "se"]].rename(
        columns={"estimate": "Mean expenditure per person with care", "se": "se_mean"}
    ),
    on="Condition",
)
print(exp.to_string(index=False))
