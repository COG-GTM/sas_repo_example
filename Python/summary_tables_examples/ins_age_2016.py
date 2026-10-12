# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Health insurance, 2016
#
# Health insurance coverage by age group
#  - Number/percent of people
#
# Input file: C:/MEPS/h192.ssp (2016 full-year consolidated, SAS XPORT)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from meps_utils import add_dummies, meps_path, read_meps, survey_estimate, wide_estimates

AGE_LABELS = ["Under 5", "5-17", "18-44", "45-64", "65+"]
INSURANCE = {
    1: "<65, Any private",
    2: "<65, Public only",
    3: "<65, Uninsured",
    4: "65+, Medicare only",
    5: "65+, Medicare and private",
    6: "65+, Medicare and other public",
    7: "65+, No Medicare",
    8: "65+, No Medicare",
}


def main(meps_dir=None):
    # Load FYC file -------------------------------------------------------------
    fyc = read_meps(meps_path("h192.ssp", meps_dir))

    # Define variables ----------------------------------------------------------
    fyc["agegrps"] = pd.cut(fyc["AGELAST"], bins=[-1, 4.5, 17.5, 44.5, 64.5, np.inf],
                            labels=AGE_LABELS)
    fyc["insurance"] = fyc["INSURC16"].map(INSURANCE)

    # QC age groups
    print(fyc.groupby("agegrps", observed=True)["AGELAST"].agg(["min", "max", "size"]), "\n")

    # Calculate estimates -------------------------------------------------------
    ins_vars = add_dummies(fyc, "insurance", dict.fromkeys(INSURANCE.values()))
    number = survey_estimate(fyc, ins_vars, weight="PERWT16F", stat="total", domain="agegrps")
    percent = survey_estimate(fyc, ins_vars, weight="PERWT16F", stat="mean", domain="agegrps")

    print("Number of people:")
    print(wide_estimates(number, "agegrps").to_string(index=False), "\n")
    print("Percent of people:")
    print(wide_estimates(percent, "agegrps").to_string(index=False))
    return {"number": number, "percent": percent}


if __name__ == "__main__":
    main()
