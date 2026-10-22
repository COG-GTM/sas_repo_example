# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M9/M9.sas
# (validated against the SAS listing in M9.pdf)
#
# DESCRIPTION: MEAN NUMBER OF PRESCRIPTIONS, OFFICE-BASED VISITS AND ER VISITS PER PERSON,
#              2005, BY INSURANCE STATUS (UNINS05) AND BY INSURANCE COVERAGE (INSCOV05).
#
# INPUT FILE:  C:/MEPS/h97.ssp (2005 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate

YESNO = {-9: "-9 NOT ASCERTAINED", -8: "-8 DK", -7: "-7 REFUSED", -1: "-1 INAPPLICABLE", 1: "UNINSURED", 2: "INSURED"}
INSCOV = {1: "ANY PRIVATE", 2: "PUBLIC ONLY", 3: "UNINSURED"}


def main(meps_dir=None):
    puf = read_meps(meps_path("h97.ssp", meps_dir),
                    columns=["UNINS05", "INSCOV05", "RXTOT05", "OBTOTV05", "ERTOT05", "VARPSU", "VARSTR", "PERWT05F"])
    puf["UNINS05"] = puf["UNINS05"].map(YESNO)
    puf["INSCOV05"] = puf["INSCOV05"].map(INSCOV)
    w = {"weight": "PERWT05F", "stat": "mean"}
    results = {}
    for v in ["RXTOT05", "OBTOTV05", "ERTOT05"]:
        results[f"mean_{v}"] = survey_estimate(puf, v, **w)
        results[f"mean_{v}_by_unins"] = survey_estimate(puf, v, domain="UNINS05", **w)
        results[f"mean_{v}_by_inscov"] = survey_estimate(puf, v, domain="INSCOV05", **w)
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
