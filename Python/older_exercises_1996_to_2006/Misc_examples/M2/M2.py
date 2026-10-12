# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M2/M2.sas
# (validated against the SAS listing in M2.pdf)
#
# DESCRIPTION: COMPARES THE STANDARD ERROR OF TOTAL HEALTH CARE EXPENDITURES, 2005,
#              ASSUMING SIMPLE RANDOM SAMPLING (NO STRATA/CLUSTERS) WITH THE SE THAT
#              ACCOUNTS FOR THE MEPS COMPLEX SAMPLE DESIGN.
#
# INPUT FILE:  C:/MEPS/h97.ssp (2005 FULL-YEAR FILE)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate


def main(meps_dir=None):
    puf = read_meps(meps_path("h97.ssp", meps_dir), columns=["TOTEXP05", "VARPSU", "VARSTR", "PERWT05F"])
    puf = puf[puf["PERWT05F"] > 0].copy()          # SAS drops nonpositive weights
    # SRS: one stratum, each record its own PSU (SAS SURVEYMEANS without STRATA/CLUSTER)
    puf["ONE"], puf["ROWID"] = 1, range(len(puf))
    results = {
        "srs_total": survey_estimate(puf, "TOTEXP05", weight="PERWT05F", stat="total", stratum="ONE", psu="ROWID"),
        "complex_total": survey_estimate(puf, "TOTEXP05", weight="PERWT05F", stat="total"),
    }
    for name, res in results.items():
        print(f"{name}: total = ${res['estimate'].iloc[0]:,.2f}  SE = ${res['se'].iloc[0]:,.2f}")
    return results


if __name__ == "__main__":
    main()
