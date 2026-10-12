# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Accessibility and quality of care: Access to Care, 2019
#
# Did not receive treatment because couldn't afford it
#  - Number/percent of people
#  - By poverty status
#
# Input file: C:/MEPS/h216.sas7bdat (2019 full-year consolidated)
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
    fyc = read_meps(meps_path("h216.sas7bdat", meps_dir))

    # Define variables ----------------------------------------------------------
    # Didn't receive care because couldn't afford it
    fyc["afford_MD"] = (fyc["AFRDCA42"] == 1).astype(int)  # medical care
    fyc["afford_DN"] = (fyc["AFRDDN42"] == 1).astype(int)  # dental care
    fyc["afford_PM"] = (fyc["AFRDPM42"] == 1).astype(int)  # prescribed medicines
    fyc["afford_ANY"] = (fyc[["afford_MD", "afford_DN", "afford_PM"]].max(axis=1) == 1).astype(int)

    # Poverty status
    fyc["poverty"] = pd.Categorical(
        fyc["POVCAT19"].map(POVERTY), categories=POVERTY.values(), ordered=True)

    # Domain: persons eligible to receive the 'access to care' supplement.
    # Rows outside the domain are kept so the variance reflects the full design.
    fyc["domain"] = (fyc["ACCELI42"] == 1).astype(int)
    # SAS drops zero-weight records; giving out-of-domain ones a weight of 1 keeps their
    # PSUs in the design. Not strictly needed in Python (survey_estimate keeps every PSU),
    # and has no effect on estimates since out-of-domain records contribute 0.
    fyc.loc[(fyc["domain"] == 0) & (fyc["PERWT19F"] == 0), "PERWT19F"] = 1

    # QC new variables
    for cols in (["AFRDCA42", "afford_MD"], ["AFRDDN42", "afford_DN"], ["AFRDPM42", "afford_PM"],
                 ["afford_MD", "afford_DN", "afford_PM", "afford_ANY"], ["poverty", "POVCAT19"]):
        print(fyc.groupby(cols).size().rename("n").reset_index().to_string(index=False), "\n")

    # Calculate estimates -------------------------------------------------------
    # Did not receive treatment because couldn't afford it, by poverty status
    afford_vars = ["afford_ANY", "afford_MD", "afford_DN", "afford_PM"]
    design = dict(weight="PERWT19F", domain="poverty", subpop=fyc["domain"] == 1)

    number = survey_estimate(fyc, afford_vars, stat="total", **design)
    percent = survey_estimate(fyc, afford_vars, stat="mean", **design)

    print("Number of people:\n", wide_estimates(number, "poverty").to_string(index=False), "\n")
    print("Percent of people:\n", wide_estimates(percent, "poverty").to_string(index=False))
    return {"number": number, "percent": percent}


if __name__ == "__main__":
    pd.set_option("display.width", 200)
    main()
