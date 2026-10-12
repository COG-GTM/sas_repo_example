# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Accessibility and quality of care: Quality of Care, 2016
#
# Adults: Ability to schedule a routine appointment, by insurance coverage
#  - Number/percent of adults (18+) who made an appointment for routine care
#
# Input file: C:/MEPS/h192.ssp (2016 full-year consolidated, SAS XPORT)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from meps_utils import add_dummies, meps_path, read_meps, survey_estimate, wide_estimates

ROUTINE = {
    4: "Always",
    3: "Usually",
    2: "Sometimes/Never",
    1: "Sometimes/Never",
    -7: "Don't know/Non-response",
    -8: "Don't know/Non-response",
    -9: "Don't know/Non-response",
    -1: "Inapplicable",
}
INSURANCE = {
    1: "<65, Any private",
    2: "<65, Public only",
    3: "<65, Uninsured",
    4: "65+, Medicare only",
    5: "65+, Medicare and private",
    6: "65+, Medicare and other public",
    7: "65+, No medicare",
    8: "65+, No medicare",
}


def main(meps_dir=None):
    # Load FYC file -------------------------------------------------------------
    fyc = read_meps(meps_path("h192.ssp", meps_dir))

    # Define variables (PROC FORMAT -> dicts; unmapped values -> "Missing") --------
    fyc["adult_routine"] = fyc["ADRTWW42"].map(ROUTINE).fillna("Missing")
    fyc["insurance"] = fyc["INSURC16"].map(INSURANCE).fillna("Missing")

    # Calculate estimates -------------------------------------------------------
    # Subpopulation: adults who made an appointment for routine care (SAQ weight)
    subpop = (fyc["ADRTCR42"] == 1) & (fyc["AGELAST"] >= 18)
    levels = [lv for lv in dict.fromkeys(ROUTINE.values()) if (fyc["adult_routine"] == lv).any()]
    if (fyc["adult_routine"] == "Missing").any():
        levels.append("Missing")
    routine_vars = add_dummies(fyc, "adult_routine", levels)
    design = {"weight": "SAQWT16F", "domain": "insurance", "subpop": subpop}

    number = survey_estimate(fyc, routine_vars, stat="total", **design)
    percent = survey_estimate(fyc, routine_vars, stat="mean", **design)

    print("Number of adults:")
    print(wide_estimates(number, "insurance").to_string(index=False), "\n")
    print("Percent of adults:")
    print(wide_estimates(percent, "insurance").to_string(index=False))
    return {"number": number, "percent": percent}


if __name__ == "__main__":
    main()
