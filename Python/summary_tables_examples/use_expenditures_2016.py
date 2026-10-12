# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Use, expenditures, and population, 2016
#
# Expenditures for office-based and outpatient visits, by source of payment
#  - Total expenditures
#  - Mean expenditure per person
#  - Mean out-of-pocket payment per person with an out-of-pocket expense
#
# Input file: C:/MEPS/h192.ssp (2016 full-year consolidated, SAS XPORT)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from meps_utils import meps_path, read_meps, survey_estimate

OTHER = ["OFD16", "STL16", "OPR16", "OPU16", "OSR16", "WCP16", "VA16"]


def main(meps_dir=None):
    # Load FYC file -------------------------------------------------------------
    fyc = read_meps(meps_path("h192.ssp", meps_dir))

    # Combine sources of payment: private (incl. TRICARE) and 'Other' -------------
    # OBV = office-based visits, OBD = office-based physician visits,
    # OPT = outpatient visits (facility + physician), OPV / OPS = OP facility / physician
    for ev in ("OBV", "OBD", "OPT", "OPV", "OPS"):
        fyc[f"{ev}PTR"] = fyc[f"{ev}PRV16"] + fyc[f"{ev}TRI16"]
        fyc[f"{ev}OTZ"] = fyc[[f"{ev}{s}" for s in OTHER]].sum(axis=1)

    # Outpatient physician visits: OP facility + OP physician payments
    fyc["OPTSLF_p"] = fyc["OPVSLF16"] + fyc["OPSSLF16"]
    fyc["OPTMCR_p"] = fyc["OPVMCR16"] + fyc["OPSMCR16"]
    fyc["OPTMCD_p"] = fyc["OPVMCD16"] + fyc["OPSMCD16"]
    fyc["OPTPTR_p"] = fyc["OPVPTR"] + fyc["OPSPTR"]
    fyc["OPTOTZ_p"] = fyc["OPVOTZ"] + fyc["OPSOTZ"]

    variables = [
        "OBVSLF16", "OBVPTR", "OBVMCR16", "OBVMCD16", "OBVOTZ",    # OB visits
        "OBDSLF16", "OBDPTR", "OBDMCR16", "OBDMCD16", "OBDOTZ",    # OB phys. visits
        "OPTSLF16", "OPTPTR", "OPTMCR16", "OPTMCD16", "OPTOTZ",    # OP visits
        "OPTSLF_p", "OPTPTR_p", "OPTMCR_p", "OPTMCD_p", "OPTOTZ_p",  # OP phys. visits
    ]

    # Calculate estimates -------------------------------------------------------
    results = {
        "totals": survey_estimate(fyc, variables, weight="PERWT16F", stat="total"),
        "means": survey_estimate(fyc, variables, weight="PERWT16F", stat="mean"),
    }

    # Mean out-of-pocket payment per person with an out-of-pocket expense
    # (subpopulation estimation; rows are not dropped)
    for var in ("OBVSLF16", "OBDSLF16", "OPTSLF16", "OPTSLF_p"):
        results[f"oop_{var}"] = survey_estimate(fyc, var, weight="PERWT16F", stat="mean",
                                                subpop=fyc[var] > 0)

    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
