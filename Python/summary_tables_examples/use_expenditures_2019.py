# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Use, expenditures, and population, 2019
#
# Mean expenditure per person, by event type and source of payment
#  - Total, emergency room, and inpatient expenditures
#
# Input file: C:/MEPS/h216.sas7bdat (2019 full-year consolidated)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from meps_utils import meps_path, read_meps, survey_estimate

SOURCES = ["EXP", "SLF", "PTR", "MCR", "MCD", "OTZ"]


def main(meps_dir=None):
    # Load FYC file -------------------------------------------------------------
    fyc = read_meps(meps_path("h216.sas7bdat", meps_dir))

    # Define 'Other' source of payment (other federal, state/local, other, workers' comp, VA)
    for ev in ("TOT", "ERT", "IPT"):
        fyc[f"{ev[:2]}TOTZ"] = (fyc[f"{ev}OFD19"] + fyc[f"{ev}STL19"] + fyc[f"{ev}OSR19"]
                                + fyc[f"{ev}WCP19"] + fyc[f"{ev}VA19"])

    # Calculate estimates: mean expenditure per person, by source of payment ------
    results = {}
    for ev in ("TOT", "ERT", "IPT"):
        variables = [f"{ev}{s}19" for s in SOURCES[:-1]] + [f"{ev[:2]}TOTZ"]
        results[ev] = survey_estimate(fyc, variables, weight="PERWT19F", stat="mean")
        print(f"\n{ev}:")
        print(results[ev].to_string(index=False))
    return results


if __name__ == "__main__":
    main()
