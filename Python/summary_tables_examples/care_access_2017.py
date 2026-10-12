# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Accessibility and quality of care: Access to Care, 2017
#
# Reasons for difficulty receiving needed care
#  - Number/percent of people
#  - By poverty status
#
# Input file: C:/MEPS/h201.sas7bdat (2017 full-year consolidated)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from meps_utils import meps_path, read_meps, survey_estimate, wide_estimates

# PROC FORMAT value labels -> dict + ordered Categorical
POVERTY = {
    1: "Negative or poor",
    2: "Near-poor",
    3: "Low income",
    4: "Middle income",
    5: "High income",
}


def main(meps_dir=None):
    # Load FYC file -------------------------------------------------------------
    fyc = read_meps(meps_path("h201.sas7bdat", meps_dir))

    # Define variables ----------------------------------------------------------
    # Reasons for difficulty receiving needed care (MD = medical, DN = dental, PM = prescribed meds)
    for t in ("MD", "DN", "PM"):
        unab, dlay = fyc[f"{t}UNAB42"], fyc[f"{t}DLAY42"]
        dlrs, unrs = fyc[f"{t}DLRS42"], fyc[f"{t}UNRS42"]
        fyc[f"delay_{t}"] = ((unab == 1) | (dlay == 1)).astype(int)
        fyc[f"afford_{t}"] = ((dlrs == 1) | (unrs == 1)).astype(int)
        fyc[f"insure_{t}"] = (dlrs.isin([2, 3]) | unrs.isin([2, 3])).astype(int)
        fyc[f"other_{t}"] = ((dlrs > 3) | (unrs > 3)).astype(int)
    for r in ("delay", "afford", "insure", "other"):
        fyc[f"{r}_ANY"] = fyc[[f"{r}_MD", f"{r}_DN", f"{r}_PM"]].max(axis=1)

    # Poverty status
    fyc["poverty"] = pd.Categorical(
        fyc["POVCAT17"].map(POVERTY), categories=POVERTY.values(), ordered=True)

    # Domain: eligible for the access to care supplement AND had difficulty receiving care.
    # Rows outside the domain are kept so the variance reflects the full design.
    fyc["domain"] = ((fyc["ACCELI42"] == 1) & (fyc["delay_ANY"] == 1)).astype(int)
    fyc.loc[(fyc["domain"] == 0) & (fyc["PERWT17F"] == 0), "PERWT17F"] = 1

    # QC new variables
    for cols in (["delay_MD", "MDUNAB42", "MDDLAY42"], ["delay_DN", "DNUNAB42", "DNDLAY42"],
                 ["delay_PM", "PMUNAB42", "PMDLAY42"],
                 ["delay_ANY", "delay_MD", "delay_DN", "delay_PM"], ["poverty", "POVCAT17"]):
        print(fyc.groupby(cols, observed=True).size().rename("n").reset_index().to_string(index=False), "\n")

    # Calculate estimates -------------------------------------------------------
    reasons = ["afford_ANY", "insure_ANY", "other_ANY"]
    design = {"weight": "PERWT17F", "domain": "poverty", "subpop": fyc["domain"] == 1}

    number = survey_estimate(fyc, reasons, stat="total", **design)
    percent = survey_estimate(fyc, reasons, stat="mean", **design)

    print("Number of people:")
    print(wide_estimates(number, "poverty").to_string(index=False), "\n")
    print("Percent of people:")
    print(wide_estimates(percent, "poverty").to_string(index=False))
    return {"number": number, "percent": percent}


if __name__ == "__main__":
    main()
