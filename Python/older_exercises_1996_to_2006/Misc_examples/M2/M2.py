# --------------------------------------------------------------------
# PROGRAM:      C:\MEPS\PROG\M2.SAS
#
# DESCRIPTION:  THIS EXAMPLE SHOWS THE NEED FOR USING THE
#               STRATUM AND PSU VARIABLES WHEN ANALYZING MEPS
#               DATA FOR NATIONAL ESTIMATES.  THAT IS, TAKING
#               THE MEPS COMPLEX DESIGN PROPERTIES INTO
#               ACCOUNT.
#
# INPUT FILE:   C:\MEPS\DATA\H97.SAS7BDAT (2005 FULL-YEAR DATA FILE)
#
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M2/M2.sas
# --------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import load_meps, svy_total  # noqa: E402

# Read 2005 consolidated full year file
PUF97 = load_meps("h97", columns=["TOTEXP05", "VARPSU", "VARSTR", "PERWT05F"])

# Assume simple random sample (weights only)
srs = svy_total(PUF97, "TOTEXP05", weight="PERWT05F", strata=None, psu=None).iloc[0]
# Account for MEPS complex sample design
comp = svy_total(PUF97, "TOTEXP05", weight="PERWT05F", strata="VARSTR", psu="VARPSU").iloc[0]

print(f"SRS    TOTAL IS ${srs['estimate']:,.2f}")
print(f"SRS SE TOTAL IS ${srs['se']:,.2f}")
print(f"COMPLEX DESIGN TOTAL    IS ${comp['estimate']:,.2f}")
print(f"COMPLEX DESIGN TOTAL SE IS ${comp['se']:,.2f}")
