# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Misc_examples/M3/M3.sas
# (M3.pdf contains only the SAS log, so there are no printed values to validate against)
#
# DESCRIPTION: MERGING PERSON-LEVEL (FULL-YEAR) AND CONDITION-LEVEL FILES: A MANY-TO-ONE
#              MERGE BY DUPERSID KEEPING ALL RECORDS, AND KEEPING ONLY MATCHED RECORDS.
#
# INPUT FILES: C:/MEPS/h97.ssp (2005 FULL-YEAR FILE), C:/MEPS/h96.ssp (2005 CONDITIONS)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps

SEXF = {1: "MALE", 2: "FEMALE"}
YESNO = {-9: "-9 NOT ASCERTAINED", -1: "-1 INAPPLICABLE", 1: "1 YES", 2: "2 NO"}


def main(meps_dir=None):
    fy = read_meps(meps_path("h97.ssp", meps_dir),
                   columns=["DUID", "PID", "DUPERSID", "PANEL", "AGE05X", "SEX", "PERWT05F", "VARSTR", "VARPSU"])
    cond = read_meps(meps_path("h96.ssp", meps_dir),
                     columns=["DUPERSID", "PANEL", "CONDN", "CONDIDX", "INJURY", "ICD9CODX"])
    fy, cond = fy.sort_values("DUPERSID"), cond.sort_values(["DUPERSID", "CONDN"])
    fy["SEX"] = fy["SEX"].map(SEXF)
    cond["INJURY"] = cond["INJURY"].map(YESNO)

    condinfo = fy.merge(cond, on=["DUPERSID", "PANEL"], how="outer")        # MERGE FY2005 COND2005
    condinfo_b = fy.merge(cond, on=["DUPERSID", "PANEL"], how="inner")      # ... IF A AND B
    print(f"all records: {len(condinfo)}   matched records: {len(condinfo_b)}")
    print(condinfo_b.iloc[139:190].to_string(index=False))
    return {"condinfo": condinfo, "condinfo_b": condinfo_b}


if __name__ == "__main__":
    main()
