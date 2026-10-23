"""Python port of SAS/summary_tables_examples/cond_expenditures_2015.sas

Example code to replicate estimates from the MEPS-HC Data Tools summary tables

Medical Conditions, 2015:
 - Number of people with care
 - Number of events
 - Total expenditures
 - Mean expenditure per person

Note: Starting in 2016, conditions were converted from ICD-9 and CCS codes
 to ICD-10 and CCSR codes

Input files:
    - <MEPS_DIR>/h178a.ssp (2015 RX event file)
    - <MEPS_DIR>/h178d.ssp (2015 IP event file)
    - <MEPS_DIR>/h178e.ss (2015 ER event file)
    - <MEPS_DIR>/h178f.ss (2015 OP event file)
    - <MEPS_DIR>/h178g.ss (2015 OB event file)
    - <MEPS_DIR>/h178h.ss (2015 HH event file)
    - <MEPS_DIR>/h178if1.ss (2015 CLNK: Condition-event link file)
    - <MEPS_DIR>/h180.ss (2015 Conditions file)

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Define formats for CCS codes to collapsed conditions (PROC FORMAT CCCFMT) -------
# slice(a, b) = SAS range a-b; unmatched codes are 'Other'.
CCCFMT = {
    slice(-9, -1): '',
    slice(1, 9): 'Infectious diseases',
    slice(11, 45): 'Cancer',
    46: 'Non-malignant neoplasm',
    47: 'Non-malignant neoplasm',
    48: 'Thyroid disease',
    49: 'Diabetes mellitus',
    50: 'Diabetes mellitus',
    51: 'Other endocrine, nutritional & immune disorder',
    52: 'Other endocrine, nutritional & immune disorder',
    slice(54, 58): 'Other endocrine, nutritional & immune disorder',
    53: 'Hyperlipidemia',
    59: 'Anemia and other deficiencies',
    slice(60, 64): 'Hemorrhagic, coagulation, and disorders of White Blood cells',
    slice(65, 75): 'Mental disorders',
    slice(650, 670): 'Mental disorders',
    slice(76, 78): 'CNS infection',
    slice(79, 81): 'Hereditary, degenerative and other nervous system disorders',
    82: 'Paralysis',
    84: 'Headache',
    83: 'Epilepsy and convulsions',
    85: 'Coma, brain damage',
    86: 'Cataract',
    88: 'Glaucoma',
    87: 'Other eye disorders',
    slice(89, 91): 'Other eye disorders',
    92: 'Otitis media',
    slice(93, 95): 'Other CNS disorders',
    98: 'Hypertension',
    99: 'Hypertension',
    96: 'Heart disease',
    97: 'Heart disease',
    slice(100, 108): 'Heart disease',
    slice(109, 113): 'Cerebrovascular disease',
    slice(114, 121): 'Other circulatory conditions arteries, veins, and lymphatics',
    122: 'Pneumonia',
    123: 'Influenza',
    124: 'Tonsillitis',
    125: 'Acute Bronchitis and URI',
    126: 'Acute Bronchitis and URI',
    slice(127, 134): 'COPD, asthma',
    135: 'Intestinal infection',
    136: 'Disorders of teeth and jaws',
    137: 'Disorders of mouth and esophagus',
    slice(138, 141): 'Disorders of the upper GI',
    142: 'Appendicitis',
    143: 'Hernias',
    slice(144, 148): 'Other stomach and intestinal disorders',
    slice(153, 155): 'Other GI',
    slice(149, 152): 'Gallbladder, pancreatic, and liver disease',
    slice(156, 158): 'Kidney Disease',
    160: 'Kidney Disease',
    161: 'Kidney Disease',
    159: 'Urinary tract infections',
    162: 'Other urinary',
    163: 'Other urinary',
    slice(164, 166): 'Male genital disorders',
    167: 'Non-malignant breast disease',
    slice(168, 176): 'Female genital disorders, and contraception',
    slice(177, 195): 'Complications of pregnancy and birth',
    196: 'Normal birth/live born',
    218: 'Normal birth/live born',
    slice(197, 200): 'Skin disorders',
    slice(201, 204): 'Osteoarthritis and other non-traumatic joint disorders',
    205: 'Back problems',
    slice(206, 209): 'Other bone and musculoskeletal disease',
    212: 'Other bone and musculoskeletal disease',
    slice(210, 211): 'Systemic lupus and connective tissues disorders',
    slice(213, 217): 'Congenital anomalies',
    slice(219, 224): 'Perinatal Conditions',
    slice(225, 236): 'Trauma-related disorders',
    239: 'Trauma-related disorders',
    240: 'Trauma-related disorders',
    244: 'Trauma-related disorders',
    237: 'Complications of surgery or device',
    238: 'Complications of surgery or device',
    slice(241, 243): 'Poisoning by medical and non-medical substances',
    259: 'Residual Codes',
    10: 'Other care and screening',
    slice(254, 258): 'Other care and screening',
    slice(245, 252): 'Symptoms',
    253: 'Allergic reactions',
}

# Load event files (SAS: %load macro with PROC XCOPY) -----------------------------
h178a = read_meps("h178a")  # RX
h178d = read_meps("h178d")  # IP
h178e = read_meps("h178e")  # ER
h178f = read_meps("h178f")  # OP
h178g = read_meps("h178g")  # OB
h178h = read_meps("h178h")  # HH

# Aggregate RX fills to the prescribed-medicine (LINKIDX) level -------------------
design = ["VARSTR", "VARPSU", "PERWT15F"]
RX = (h178a.groupby(["DUPERSID", "LINKIDX"] + design, dropna=False)
      .agg(XP15X=("RXXP15X", "sum"), n_fills=("RXXP15X", "count")).reset_index())

# Stack events (SAS: SET ... (IN=) with RENAME) -----------------------------------
keep = ["DUPERSID", "EVNTIDX", "XP15X", "n_fills"] + design
parts = [RX.rename(columns={"LINKIDX": "EVNTIDX"}).assign(data="RX")]
for df, typ in [(h178d, "IP"), (h178e, "ER"), (h178f, "OP"), (h178g, "OB"), (h178h, "HH")]:
    parts.append(df.rename(columns={f"{typ}XP15X": "XP15X"}).reindex(columns=keep).assign(data=typ))
stacked_events = pd.concat([p.reindex(columns=keep + ["data"]) for p in parts], ignore_index=True)
stacked_events["n_events"] = stacked_events.n_fills.fillna(1).clip(lower=1)  # max(n_fills, 1)
stacked_events = stacked_events.drop(columns="n_fills")

# Load conditions file and condition-event link file ------------------------------
h178if1 = read_meps("h178if1")  # CLNK
h180 = read_meps("h180")        # Conditions

# Merge conditions with link file; collapse CCS codes ------------------------------
cond_clink = h178if1[["DUPERSID", "CONDIDX", "EVNTIDX"]].merge(
    h180[["DUPERSID", "CONDIDX", "CCCODEX"]], on=["DUPERSID", "CONDIDX"], how="outer")
cond_clink["Condition"] = sas_format(pd.to_numeric(cond_clink.CCCODEX, errors="coerce"), CCCFMT,
                                     other="Other", missing="Other")
# Remove duplicate conditions per event (SAS: PROC SORT NODUPKEY)
cond_clink = cond_clink.drop_duplicates(["DUPERSID", "EVNTIDX", "Condition"])

# Merge events with conditions; keep events with a condition and valid expenditure
all_events = stacked_events.merge(cond_clink, on=["DUPERSID", "EVNTIDX"], how="outer")
all_events = all_events[all_events.Condition.fillna("").ne("") & (all_events.XP15X >= 0)]

# Aggregate to person-condition level ----------------------------------------------
all_pers = (all_events.groupby(["DUPERSID", "VARSTR", "VARPSU", "Condition"])
            .agg(mean_XP=("XP15X", "mean"), avg_events=("n_events", "mean"), PERWT15F=("PERWT15F", "mean"),
                 pers_XP=("XP15X", "sum"), n_events=("n_events", "sum")).reset_index())
all_pers["person"] = 1

# Calculate estimates using survey procedures (DOMAIN Condition) ------------------
out = surveymeans(all_pers, ["person", "n_events", "pers_XP"], weight="PERWT15F", strata="VARSTR",
                  cluster="VARPSU", domain="Condition")
show("Number of people with care", out[out.VarName == "person"][["Condition", "Sum", "StdDev"]])
show("Number of events", out[out.VarName == "n_events"][["Condition", "Sum", "StdDev"]])
show("Total and mean expenditures", out[out.VarName == "pers_XP"][["Condition", "Sum", "StdDev", "Mean", "StdErr"]])
