# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Medical Conditions, 2015:
#  - Number of people with care
#  - Number of events
#  - Total expenditures
#  - Mean expenditure per person
#
# Note: Starting in 2016, conditions were converted from ICD-9 and CCS codes
#  to ICD-10 and CCSR codes
#
# Input files:
#   - C:\MEPS\h178a.ssp (2015 RX event file)
#   - C:\MEPS\h178d.ssp (2015 IP event file)
#   - C:\MEPS\h178e.ssp (2015 ER event file)
#   - C:\MEPS\h178f.ssp (2015 OP event file)
#   - C:\MEPS\h178g.ssp (2015 OB event file)
#   - C:\MEPS\h178h.ssp (2015 HH event file)
#   - C:\MEPS\h178if1.ssp (2015 CLNK: Condition-event link file)
#   - C:\MEPS\h180.ssp (2015 Conditions file)
#
# Python port of SAS/summary_tables_examples/cond_expenditures_2015.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import MEPS_DIR, apply_format, read_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)
pd.set_option("display.max_rows", 200)

# Define formats for CCS codes to collapsed conditions (PROC FORMAT CCCFMT) ---
# (low, high, label); values not listed are 'Other'

CCCFMT = [
    (-9, -1, ""),
    (1, 9, "Infectious diseases"),
    (11, 45, "Cancer"),
    (46, 47, "Non-malignant neoplasm"),
    (48, 48, "Thyroid disease"),
    (49, 50, "Diabetes mellitus"),
    (51, 52, "Other endocrine, nutritional & immune disorder"),
    (54, 58, "Other endocrine, nutritional & immune disorder"),
    (53, 53, "Hyperlipidemia"),
    (59, 59, "Anemia and other deficiencies"),
    (60, 64, "Hemorrhagic, coagulation, and disorders of White Blood cells"),
    (65, 75, "Mental disorders"),
    (650, 670, "Mental disorders"),
    (76, 78, "CNS infection"),
    (79, 81, "Hereditary, degenerative and other nervous system disorders"),
    (82, 82, "Paralysis"),
    (84, 84, "Headache"),
    (83, 83, "Epilepsy and convulsions"),
    (85, 85, "Coma, brain damage"),
    (86, 86, "Cataract"),
    (88, 88, "Glaucoma"),
    (87, 87, "Other eye disorders"),
    (89, 91, "Other eye disorders"),
    (92, 92, "Otitis media"),
    (93, 95, "Other CNS disorders"),
    (98, 99, "Hypertension"),
    (96, 97, "Heart disease"),
    (100, 108, "Heart disease"),
    (109, 113, "Cerebrovascular disease"),
    (114, 121, "Other circulatory conditions arteries, veins, and lymphatics"),
    (122, 122, "Pneumonia"),
    (123, 123, "Influenza"),
    (124, 124, "Tonsillitis"),
    (125, 126, "Acute Bronchitis and URI"),
    (127, 134, "COPD, asthma"),
    (135, 135, "Intestinal infection"),
    (136, 136, "Disorders of teeth and jaws"),
    (137, 137, "Disorders of mouth and esophagus"),
    (138, 141, "Disorders of the upper GI"),
    (142, 142, "Appendicitis"),
    (143, 143, "Hernias"),
    (144, 148, "Other stomach and intestinal disorders"),
    (153, 155, "Other GI"),
    (149, 152, "Gallbladder, pancreatic, and liver disease"),
    (156, 158, "Kidney Disease"),
    (160, 161, "Kidney Disease"),
    (159, 159, "Urinary tract infections"),
    (162, 163, "Other urinary"),
    (164, 166, "Male genital disorders"),
    (167, 167, "Non-malignant breast disease"),
    (168, 176, "Female genital disorders, and contraception"),
    (177, 195, "Complications of pregnancy and birth"),
    (196, 196, "Normal birth/live born"),
    (218, 218, "Normal birth/live born"),
    (197, 200, "Skin disorders"),
    (201, 204, "Osteoarthritis and other non-traumatic joint disorders"),
    (205, 205, "Back problems"),
    (206, 209, "Other bone and musculoskeletal disease"),
    (212, 212, "Other bone and musculoskeletal disease"),
    (210, 211, "Systemic lupus and connective tissues disorders"),
    (213, 217, "Congenital anomalies"),
    (219, 224, "Perinatal Conditions"),
    (225, 236, "Trauma-related disorders"),
    (239, 240, "Trauma-related disorders"),
    (244, 244, "Trauma-related disorders"),
    (237, 238, "Complications of surgery or device"),
    (241, 243, "Poisoning by medical and non-medical substances"),
    (259, 259, "Residual Codes"),
    (10, 10, "Other care and screening"),
    (254, 258, "Other care and screening"),
    (245, 252, "Symptoms"),
    (253, 253, "Allergic reactions"),
]

# Load datasets ---------------------------------------------------------------


def load(file):
    return read_meps(f"{MEPS_DIR}/{file}.ssp")


# Load and stack event files
h178a = load("h178a")  # RX
h178d = load("h178d")  # IP
h178e = load("h178e")  # ER
h178f = load("h178f")  # OP
h178g = load("h178g")  # OB
h178h = load("h178h")  # HH

# For RX events, count number of fills per event
RX = (
    h178a.groupby(["DUPERSID", "LINKIDX", "VARSTR", "VARPSU", "PERWT15F"])
    .agg(XP15X=("RXXP15X", "sum"), n_fills=("RXXP15X", "count"))
    .reset_index()
)

# Stack event files
keep = ["data", "EVNTIDX", "DUPERSID", "n_events", "XP15X", "VARSTR", "VARPSU", "PERWT15F"]
frames = [RX.rename(columns={"LINKIDX": "EVNTIDX"}).assign(data="RX")]
for name, ev in (("IP", h178d), ("ER", h178e), ("OP", h178f), ("OB", h178g), ("HH", h178h)):
    frames.append(ev.rename(columns={f"{name}XP15X": "XP15X"}).assign(data=name))
stacked_events = pd.concat(frames, ignore_index=True)
# Count events (for RX, each fill is an event)
stacked_events["n_events"] = stacked_events["n_fills"].fillna(1).clip(lower=1)
stacked_events = stacked_events[keep]

# Load in event-condition linking file
h178if1 = load("h178if1")

# Load in conditions file
h180 = load("h180")

# Merge datasets --------------------------------------------------------------
# Merge conditions data with the conditions-event link file (CLNK)
#  then de-duplicate by event ID ('EVNTIDX') and collapsed code ('Condition')

cond_clink = h178if1[["DUPERSID", "CONDIDX", "EVNTIDX"]].merge(
    h180[["DUPERSID", "CONDIDX", "CCCODEX"]], on=["DUPERSID", "CONDIDX"], how="outer"
)
cond_clink["Condition"] = apply_format(
    pd.to_numeric(cond_clink["CCCODEX"], errors="coerce"), CCCFMT, default="Other"
)
cond_clink = cond_clink.drop_duplicates(subset=["DUPERSID", "EVNTIDX", "Condition"])

# Merge events with linked conditions file and remove any observations with
#  missing 'Condition' or negative expenditures
all_events = stacked_events.merge(cond_clink, on=["DUPERSID", "EVNTIDX"], how="outer")
all_events = all_events[
    all_events["Condition"].notna() & (all_events["Condition"] != "") & (all_events["XP15X"] >= 0)
]

# Aggregate to person-level, by Condition -------------------------------------

all_pers = (
    all_events.groupby(["DUPERSID", "VARSTR", "VARPSU", "Condition"])
    .agg(
        mean_XP=("XP15X", "mean"),
        avg_events=("n_events", "mean"),
        PERWT15F=("PERWT15F", "mean"),
        pers_XP=("XP15X", "sum"),
        n_events=("n_events", "sum"),
    )
    .reset_index()
)
all_pers["person"] = 1

# Calculate estimates using survey procedures ---------------------------------

design = dict(weight="PERWT15F", strata="VARSTR", psu="VARPSU", domain="Condition")
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
