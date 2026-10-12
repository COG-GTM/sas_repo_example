# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E5/E5.sas
# (validated against the SAS listing in E5.pdf)
#
# DESCRIPTION: THIS EXAMPLE SHOWS HOW TO COMPUTE EVENT-LEVEL ESTIMATES: MEAN
#              EXPENSE PER HOSPITAL STAY AND PER OFFICE-BASED VISIT, 2001.
#
# INPUT FILES: C:/MEPS/h59d.ssp (2001 HOSPITAL INPATIENT STAYS)
#              C:/MEPS/h59g.ssp (2001 OFFICE-BASED VISITS)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate

# SAS drops records with nonpositive weights before forming the design; in h59d this leaves a
# stratum with a single PSU, which SAS omits from the variance (lonely_psu="remove").
DESIGN = {"stratum": "VARSTR01", "psu": "VARPSU01", "weight": "PERWT01F", "lonely_psu": "remove"}


def main(meps_dir=None):
    ip = read_meps(meps_path("h59d.ssp", meps_dir), columns=["DUPERSID", "IPFXP01X", "PERWT01F", "VARSTR01", "VARPSU01"])
    ob = read_meps(meps_path("h59g.ssp", meps_dir), columns=["DUPERSID", "OBXP01X", "PERWT01F", "VARSTR01", "VARPSU01"])
    ip, ob = ip[ip["PERWT01F"] > 0], ob[ob["PERWT01F"] > 0]
    results = {
        "mean_ip_stay": survey_estimate(ip, "IPFXP01X", stat="mean", **DESIGN),
        "mean_ob_visit": survey_estimate(ob, "OBXP01X", stat="mean", **DESIGN),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
