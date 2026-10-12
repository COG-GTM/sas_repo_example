# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Prescribed drugs, 2016
#
# Purchases and expenditures by Multum therapeutic class (TC1)
#  - Number of people with purchase
#  - Total purchases
#  - Total expenditures
#
# Input file: C:/MEPS/h188a.ssp (2016 prescribed medicines file, SAS XPORT)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from meps_utils import meps_path, read_meps, survey_estimate, wide_estimates

TC1_NAMES = {
    -9: "Not_ascertained",
    -1: "Inapplicable",
    1: "Anti-infectives",
    19: "Antihyperlipidemic_agents",
    20: "Antineoplastics",
    28: "Biologicals",
    40: "Cardiovascular_agents",
    57: "Central_nervous_system_agents",
    81: "Coagulation_modifiers",
    87: "Gastrointestinal_agents",
    97: "Hormones/hormone_modifiers",
    105: "Miscellaneous_agents",
    113: "Genitourinary_tract_agents",
    115: "Nutritional_products",
    122: "Respiratory_agents",
    133: "Topical_agents",
    218: "Alternative_medicines",
    242: "Psychotherapeutic_agents",
    254: "Immunologic_agents",
    358: "Metabolic_agents",
}


def main(meps_dir=None):
    # Load RX file --------------------------------------------------------------
    rx = read_meps(meps_path("h188a.ssp", meps_dir))
    rx["TC1name"] = rx["TC1"].map(TC1_NAMES)

    # Aggregate to person-level, by therapeutic class -----------------------------
    # (PROC SORT + PROC MEANS BY ... OUTPUT -> groupby().agg())
    tc1_pers = (
        rx.groupby(["DUPERSID", "VARSTR", "VARPSU", "TC1name"], as_index=False)
        .agg(PERWT16F=("PERWT16F", "mean"),
             pers_RXXP=("RXXP16X", "sum"),
             n_purchases=("RXXP16X", "size"))
        .assign(persons=1)
    )

    # Calculate estimates -------------------------------------------------------
    totals = survey_estimate(tc1_pers, ["persons", "n_purchases", "pers_RXXP"],
                             weight="PERWT16F", stat="total", domain="TC1name")

    print(wide_estimates(totals, "TC1name").to_string(index=False))
    return {"totals": totals}


if __name__ == "__main__":
    main()
