# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M10/M10.sas
# (M10.pdf contains only the SAS log, so there are no printed values to validate against)
#
# DESCRIPTION: MEAN EXPENSE PER HOSPITAL STAY (TOTAL, FACILITY, PHYSICIAN), 2002 AND 2003, AND
#              FOR 2003 BY WHETHER THE STAY BEGAN IN THE EMERGENCY ROOM (ER_FACEX), WITH
#              PROC SURVEYREG TESTS OF THE DIFFERENCE.
#
# INPUT FILES: C:/MEPS/h67d.ssp (2002 HOSPITAL INPATIENT STAYS; the SAS program read an internal
#              version, H67DV1X), C:/MEPS/h77d.ssp (2003 HOSPITAL INPATIENT STAYS)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate, survey_reg

# SAS drops nonpositive-weight records; strata left with one PSU are omitted (lonely_psu="remove")
SAS = {"lonely_psu": "remove"}


def main(meps_dir=None):
    s02 = read_meps(meps_path("h67d.ssp", meps_dir),
                    columns=["DUPERSID", "ERHEVIDX", "IPXP02X", "IPFXP02X", "IPDXP02X", "PERWT02F", "VARSTR", "VARPSU"])
    s03 = read_meps(meps_path("h77d.ssp", meps_dir),
                    columns=["DUPERSID", "ERHEVIDX", "IPXP03X", "IPFXP03X", "IPDXP03X", "PERWT03F", "VARSTR", "VARPSU"])
    s02, s03 = s02[s02["PERWT02F"] > 0], s03[s03["PERWT03F"] > 0].copy()
    # SAS: IF ERHEVIDX NE -1 THEN ER_FACEX = 0; ELSE ER_FACEX = 1
    s03["ER_FACEX"] = np.where(pd.to_numeric(s03["ERHEVIDX"], errors="coerce") == -1, 1, 0)

    results = {"mean_2002": survey_estimate(s02, ["IPXP02X", "IPFXP02X", "IPDXP02X"], weight="PERWT02F",
                                            stat="mean", **SAS)}
    for v in ["IPXP03X", "IPFXP03X", "IPDXP03X"]:
        results[f"mean_{v}_by_er"] = survey_estimate(s03, v, weight="PERWT03F", stat="mean", domain="ER_FACEX", **SAS)
        results[f"reg_{v}_on_er"] = survey_reg(s03, v, weight="PERWT03F", numeric=["ER_FACEX"], **SAS)
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
