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
#   - C:/MEPS/h206a.sas7bdat  (2018 RX event file)
#   - C:/MEPS/h206d.sas7bdat  (2018 IP event file)
#   - C:/MEPS/h206e.sas7bdat  (2018 ER event file)
#   - C:/MEPS/h206f.sas7bdat  (2018 OP event file)
#   - C:/MEPS/h206g.sas7bdat  (2018 OB event file)
#   - C:/MEPS/h206h.sas7bdat  (2018 HH event file)
#   - C:/MEPS/h206if1.sas7bdat (2018 CLNK: Condition-event link file)
#   - C:/MEPS/h207.sas7bdat   (2018 Conditions file)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from meps_utils import meps_path, read_meps, survey_estimate

CCSR_URL = ("https://raw.githubusercontent.com/HHS-AHRQ/MEPS/master/"
            "Quick_Reference_Guides/meps_ccsr_conditions.csv")


def main(meps_dir=None):
    # Load datasets -------------------------------------------------------------

    # Load and stack event files
    # - For RX, count number of fills per event
    rx = read_meps(meps_path("h206a.sas7bdat", meps_dir))
    rx = (
        rx.groupby(["DUPERSID", "LINKIDX", "VARSTR", "VARPSU", "PERWT18F"], as_index=False)
        .agg(XPX=("RXXP18X", "sum"), n_fills=("RXXP18X", "size"))
        .rename(columns={"LINKIDX": "EVNTIDX"})
    )

    events = {"RX": rx}
    for name, file, xp in [("IP", "h206d", "IPXP18X"), ("ER", "h206e", "ERXP18X"),
                           ("OP", "h206f", "OPXP18X"), ("OB", "h206g", "OBXP18X"),
                           ("HH", "h206h", "HHXP18X")]:
        events[name] = read_meps(meps_path(f"{file}.sas7bdat", meps_dir)).rename(columns={xp: "XPX"})

    keep = ["EVNTIDX", "DUPERSID", "XPX", "VARSTR", "VARPSU", "PERWT18F", "n_fills"]
    stacked_events = pd.concat(
        [df.reindex(columns=keep).assign(data=name) for name, df in events.items()],
        ignore_index=True,
    )
    # Count events (for RX, each fill is an event)
    stacked_events["n_events"] = stacked_events["n_fills"].fillna(1).clip(lower=1)

    # Load in event-condition linking file
    clink1 = read_meps(meps_path("h206if1.sas7bdat", meps_dir), ["DUPERSID", "CONDIDX", "EVNTIDX"])

    # Load in Conditions public use file
    cond_puf = read_meps(meps_path("h207.sas7bdat", meps_dir),
                         ["DUPERSID", "CONDIDX", "CCSR1X", "CCSR2X", "CCSR3X"])

    # Load crosswalk for CCSR and collapsed conditions codes
    #  - keep the first 3 columns (the crosswalk also has a 'Category Body System' column)
    condition_codes = pd.read_csv(CCSR_URL, dtype=str).iloc[:, :3]
    condition_codes.columns = ["CCSR", "CCSR_desc", "Condition"]

    # Merge datasets ------------------------------------------------------------

    # Merge collapsed condition codes to COND PUF
    #  - convert multiple CCSRs to separate lines (wide to long; PROC TRANSPOSE -> melt)
    cond = (
        cond_puf.melt(id_vars=["DUPERSID", "CONDIDX"], var_name="CCSRnum", value_name="CCSR")
        .query("CCSR != '-1'")
        .merge(condition_codes, on="CCSR", how="left")
    )

    # Merge conditions file with the conditions-event link file (CLNK)
    cond_clink = cond.merge(clink1, on=["DUPERSID", "CONDIDX"], how="outer")

    # De-duplicate by event ID ('EVNTIDX') and collapsed code ('Condition')
    cond_clink = cond_clink.drop_duplicates(["DUPERSID", "EVNTIDX", "Condition"])

    # Merge conditions and event files ------------------------------------------
    all_events = stacked_events.merge(cond_clink, on=["DUPERSID", "EVNTIDX"], how="outer")

    # Remove observations with missing 'Condition' or negative/missing expenditures
    all_events = all_events[all_events["Condition"].notna() & (all_events["XPX"] >= 0)]

    # Aggregate to person-level, by Condition -----------------------------------
    all_pers = (
        all_events.groupby(["DUPERSID", "VARSTR", "VARPSU", "Condition"], as_index=False)
        .agg(PERWTF=("PERWT18F", "mean"),
             pers_XP=("XPX", "sum"),
             n_events=("n_events", "sum"))
        .assign(persons=1)
    )

    # Calculate estimates (domain = collapsed condition) ------------------------
    design = dict(weight="PERWTF", domain="Condition")

    # Totals (people, events, expenditures)
    totals = survey_estimate(all_pers, ["persons", "n_events", "pers_XP"], stat="total", **design)

    # Mean expenditure per person with care
    means = survey_estimate(all_pers, "pers_XP", stat="mean", **design)

    labels = {"persons": "Number of people with care",
              "n_events": "Number of events",
              "pers_XP": "Total expenditures"}
    for var, label in labels.items():
        print(f"\n{label}:")
        print(totals.loc[totals.variable == var, ["Condition", "estimate", "se"]].to_string(index=False))
    print("\nMean expenditure per person with care:")
    print(means[["Condition", "estimate", "se"]].to_string(index=False))
    return {"totals": totals, "means": means}


if __name__ == "__main__":
    main()
