# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Medical Conditions, 2015:
#  - Number of people with care
#  - Number of events
#  - Total expenditures
#  - Mean expenditure per person
#
# Note: Before 2016, conditions were coded with ICD-9 and CCS codes; CCS codes
#  are mapped to collapsed condition categories below
#
# Input files:
#   - C:/MEPS/h178a.ssp   (2015 RX event file)
#   - C:/MEPS/h178d.ssp   (2015 IP event file)
#   - C:/MEPS/h178e.ssp   (2015 ER event file)
#   - C:/MEPS/h178f.ssp   (2015 OP event file)
#   - C:/MEPS/h178g.ssp   (2015 OB event file)
#   - C:/MEPS/h178h.ssp   (2015 HH event file)
#   - C:/MEPS/h178if1.ssp (2015 CLNK: Condition-event link file)
#   - C:/MEPS/h180.ssp    (2015 Conditions file)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from meps_utils import meps_path, read_meps, survey_estimate


def _r(a, b):
    return list(range(a, b + 1))


# CCS code -> collapsed condition category (first match wins, like SAS IF/ELSE IF)
CONDITIONS = [
    (_r(1, 9), "Infectious diseases"),
    (_r(11, 45), "Cancer"),
    (_r(46, 47), "Non-malignant neoplasm"),
    ([48], "Thyroid disease"),
    (_r(49, 50), "Diabetes mellitus"),
    (_r(51, 52) + _r(54, 58), "Other endocrine, nutritional & immune disorder"),
    ([53], "Hyperlipidemia"),
    ([59], "Anemia and other deficiencies"),
    (_r(60, 64), "Hemorrhagic, coagulation, and disorders of White Blood cells"),
    (_r(65, 75) + _r(650, 670), "Mental disorders"),
    (_r(76, 78), "CNS infection"),
    (_r(79, 81), "Hereditary, degenerative and other nervous system disorders"),
    ([82], "Paralysis"),
    ([84], "Headache"),
    ([83], "Epilepsy and convulsions"),
    ([85], "Coma, brain damage"),
    ([86], "Cataract"),
    ([88], "Glaucoma"),
    ([87] + _r(89, 91), "Other eye disorders"),
    ([92], "Otitis media"),
    (_r(93, 95), "Other CNS disorders"),
    (_r(98, 99), "Hypertension"),
    (_r(96, 97) + _r(100, 108), "Heart disease"),
    (_r(109, 113), "Cerebrovascular disease"),
    (_r(114, 121), "Other circulatory conditions arteries, veins, and lymphatics"),
    ([122], "Pneumonia"),
    ([123], "Influenza"),
    ([124], "Tonsillitis"),
    (_r(125, 126), "Acute Bronchitis and URI"),
    (_r(127, 134), "COPD, asthma"),
    ([135], "Intestinal infection"),
    ([136], "Disorders of teeth and jaws"),
    ([137], "Disorders of mouth and esophagus"),
    (_r(138, 141), "Disorders of the upper GI"),
    ([142], "Appendicitis"),
    ([143], "Hernias"),
    (_r(144, 148), "Other stomach and intestinal disorders"),
    (_r(153, 155), "Other GI"),
    (_r(149, 152), "Gallbladder, pancreatic, and liver disease"),
    (_r(156, 158) + _r(160, 161), "Kidney Disease"),
    ([159], "Urinary tract infections"),
    (_r(162, 163), "Other urinary"),
    (_r(164, 166), "Male genital disorders"),
    ([167], "Non-malignant breast disease"),
    (_r(168, 176), "Female genital disorders, and contraception"),
    (_r(177, 195), "Complications of pregnancy and birth"),
    ([196, 218], "Normal birth/live born"),
    (_r(197, 200), "Skin disorders"),
    (_r(201, 204), "Osteoarthritis and other non-traumatic joint disorders"),
    ([205], "Back problems"),
    (_r(206, 209) + [212], "Other bone and musculoskeletal disease"),
    (_r(210, 211), "Systemic lupus and connective tissues disorders"),
    (_r(213, 217), "Congenital anomalies"),
    (_r(219, 224), "Perinatal Conditions"),
    (_r(225, 236) + _r(239, 240) + [244], "Trauma-related disorders"),
    (_r(237, 238), "Complications of surgery or device"),
    (_r(241, 243), "Poisoning by medical and non-medical substances"),
    ([259], "Residual Codes"),
    ([10] + _r(254, 258), "Other care and screening"),
    (_r(245, 252), "Symptoms"),
    ([253], "Allergic reactions"),
]


def collapse_ccs(ccnum: pd.Series) -> np.ndarray:
    # Negative codes -> "" (dropped later); unmatched / missing codes -> "Other" (as in the R script)
    conds = [ccnum < 0] + [ccnum.isin(codes) for codes, _ in CONDITIONS]
    labels = [""] + [label for _, label in CONDITIONS]
    return np.select(conds, labels, default="Other")


def main(meps_dir=None):
    # Load datasets -------------------------------------------------------------

    # Load and stack event files
    # - For RX, count number of fills per event
    rx = read_meps(meps_path("h178a.ssp", meps_dir))
    rx = (
        rx.groupby(["DUPERSID", "LINKIDX", "VARSTR", "VARPSU", "PERWT15F"], as_index=False)
        .agg(XPX=("RXXP15X", "sum"), n_fills=("RXXP15X", "size"))
        .rename(columns={"LINKIDX": "EVNTIDX"})
    )

    events = {"RX": rx}
    for name, file, xp in [("IP", "h178d", "IPXP15X"), ("ER", "h178e", "ERXP15X"),
                           ("OP", "h178f", "OPXP15X"), ("OB", "h178g", "OBXP15X"),
                           ("HH", "h178h", "HHXP15X")]:
        events[name] = read_meps(meps_path(f"{file}.ssp", meps_dir)).rename(columns={xp: "XPX"})

    keep = ["EVNTIDX", "DUPERSID", "XPX", "VARSTR", "VARPSU", "PERWT15F", "n_fills"]
    stacked_events = pd.concat(
        [df.reindex(columns=keep).assign(data=name) for name, df in events.items()],
        ignore_index=True,
    )
    # Count events (for RX, each fill is an event)
    stacked_events["n_events"] = stacked_events["n_fills"].fillna(1).clip(lower=1)

    # Load in event-condition linking file
    clink1 = read_meps(meps_path("h178if1.ssp", meps_dir), ["DUPERSID", "CONDIDX", "EVNTIDX"])

    # Load in Conditions public use file (CCS codes)
    conditions = read_meps(meps_path("h180.ssp", meps_dir), ["DUPERSID", "CONDIDX", "CCCODEX"])

    # Merge conditions file with the conditions-event link file (CLNK)
    cond_clink = conditions.merge(clink1, on=["DUPERSID", "CONDIDX"], how="outer")

    # Map CCS codes to collapsed condition categories
    cond_clink["Condition"] = collapse_ccs(pd.to_numeric(cond_clink["CCCODEX"], errors="coerce"))

    # De-duplicate by event ID ('EVNTIDX') and collapsed code ('Condition')
    cond_clink = cond_clink.drop_duplicates(["DUPERSID", "EVNTIDX", "Condition"])

    # Merge conditions and event files ------------------------------------------
    all_events = stacked_events.merge(cond_clink, on=["DUPERSID", "EVNTIDX"], how="outer")

    # Remove observations with missing/blank 'Condition' or negative/missing expenditures
    all_events = all_events[all_events["Condition"].notna() & (all_events["Condition"] != "")
                            & (all_events["XPX"] >= 0)]

    # Aggregate to person-level, by Condition -----------------------------------
    all_pers = (
        all_events.groupby(["DUPERSID", "VARSTR", "VARPSU", "Condition"], as_index=False)
        .agg(PERWTF=("PERWT15F", "mean"),
             pers_XP=("XPX", "sum"),
             n_events=("n_events", "sum"))
        .assign(persons=1)
    )

    # Calculate estimates (domain = collapsed condition) ------------------------
    design = {"weight": "PERWTF", "domain": "Condition"}

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
