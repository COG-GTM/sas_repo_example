# ----------------------------------------------------------------
# PROGRAM:      C:\MEPS\PROG\E8.SAS
#
# DESCRIPTION:  THIS PROGRAM GENERATES TWO SETS OF EXPENDITURE DATA FOR 2005
#               INPATIENT STAYS, SIMILAR TO THOSE REPORTED IN MEPS STAT BRIEF
#               #164 (FIGURES 1 AND 5). STAT BRIEF #164 USED 2004 MEPS DATA.
#               THIS PROGRAM USES 2005 MEPS DATA.
#
#               ESTIMATES COMPARABLE TO FIGURE 1 ARE BY EVENT TYPE AND BY
#               SOURCE OF PAYMENT FOR INPATIENT (IP) STAY EXPENSES.
#
#               EVENT TYPE DEFINITIONS:
#               AMBULATORY:  OFFICE-BASED + OUTPATIENT + EMERGENCY DEPARTMENT
#               HOSPITAL:    HOSPITAL INPATIENT
#               PRESCRIBED   MEDICINES: PMED (RX)
#               OTHER:       TOTAL EXPENDITURES - (AMBULATORY + HOSPITAL + PMED)
#
#               NOTES: (1) EXPENSES INCLUDE BOTH FACILITY AND PHYSICIAN EXPENSES.
#                      (2) PRIVATE INCLUDES TRICARE, BUT NOT 'OTHER PRIVATE'.
#
#               TOTAL HEALTH CARE EXP: $963,882,049,581 (SB: $963.9 BILLION)
#
#               EVENT TYPES:  2005 DATA    2004 DATA (SB#164)
#               PMED:           20.8%        19.8%
#               HOSPITAL:       30.9%        31.4%
#               AMBULATORY:     34.7%        35.6%
#               OTHER:          13.5%        13.2%
#
#               FOR HOSPITAL EXPENSES (SB #164) FIRST BULLET IN HIGHLIGHTS
#               STATES, "EXPENSES FOR HOSPITAL INPATIENT STAYS ACCOUNTED FOR
#               NEARLY ONE-THIRD OF ALL HEALTH CARE EXPENSES..." [30.9% IN 2005]
#
#               IP SOPs:       2005 DATA    2004 DATA (SB#164)
#               PRIVATE:         41.1%        43.7%
#               MEDICARE:        37.7%        38.8%
#               MEDICAID:        10.6%         9.6%
#               OUT-OF-POCKET:    3.8%         2.2%
#               OTHER:            6.9%         5.7%
#
#               FIGURE 5: IP EXPENSES PER STAY AND PER DIEM
#               PER STAY      2005 DATA     2004 DATA (SB#164)
#               W/  SURG       $15,160       $14,729
#               W/0 SURG       $ 8,439       $ 7,678
#               PER DIEM
#               W/  SURG       $ 5,226       $ 5,059
#               W/O SURG       $ 2,173       $ 1,954
#
#               NOTE: FOR THE SMALL NUMBER OF ZERO NIGHT STAYS, LENGTH
#                     OF STAY WAS CONSIDERED TO BE ONE.
#
# INPUT FILES:  (1) C:\MEPS\DATA\H97.SAS7BDAT (2005 FULL-YEAR FILE)
#               (2) C:\MEPS\DATA\H94D.SAS7BDAT (2005 HOSP. IP STAYS)
#
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E8/E8.sas
# ----------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import apply_format, load_meps, svy_mean, svy_ratio, svy_total  # noqa: E402

pd.set_option("display.width", 200)

SURGF = {1: "SURGERY"}  # OTHER = 'OTHER'
design = dict(weight="PERWT05F", strata="VARSTR", psu="VARPSU")

# Read in data from 2005 Consolidated Data File (HC-097)
cols = ["TOTEXP05", "RXEXP05", "OBVEXP05", "OPDEXP05", "OPFEXP05", "ERDEXP05", "ERFEXP05", "IPDEXP05", "IPFEXP05",
        "IPDPRV05", "IPFPRV05", "IPDTRI05", "IPFTRI05", "IPDMCR05", "IPFMCR05", "IPDMCD05", "IPFMCD05", "IPDSLF05",
        "IPFSLF05", "VARSTR", "VARPSU", "PERWT05F"]  # fmt: skip
P = load_meps("h97", columns=cols)
P["TOTAL"] = P["TOTEXP05"]
P["PRESCRIBED_MEDICINES"] = P["RXEXP05"]
P["HOSPITAL_INPATIENT"] = P["IPDEXP05"] + P["IPFEXP05"]
P["AMBULATORY_CARE"] = P[["OBVEXP05", "OPDEXP05", "OPFEXP05", "ERDEXP05", "ERFEXP05"]].sum(axis=1)
P["OTHER"] = P["TOTAL"] - (P["PRESCRIBED_MEDICINES"] + P["HOSPITAL_INPATIENT"] + P["AMBULATORY_CARE"])
P["IP_TOTAL"] = P["HOSPITAL_INPATIENT"]
P["IP_PRIVATE_INS"] = (P["IPDPRV05"] + P["IPFPRV05"]) + (P["IPDTRI05"] + P["IPFTRI05"])
P["IP_MEDICARE"] = P["IPDMCR05"] + P["IPFMCR05"]
P["IP_MEDICAID"] = P["IPDMCD05"] + P["IPFMCD05"]
P["IP_OUT_OF_POCKET"] = P["IPDSLF05"] + P["IPFSLF05"]
P["IP_OTHER"] = P["IP_TOTAL"] - P[["IP_PRIVATE_INS", "IP_MEDICARE", "IP_MEDICAID", "IP_OUT_OF_POCKET"]].sum(axis=1)


def mean_sum_ratio(variables, denominator):
    """PROC SURVEYMEANS MEAN STDERR SUM STD; VAR ... ; RATIO ... / denominator."""
    allv = variables + [denominator]
    res = svy_mean(P, allv, **design).merge(svy_total(P, allv, **design), on=["variable", "n"], suffixes=("_mean", "_sum"))
    print(res.to_string(index=False), "\n")
    print(svy_ratio(P, variables, denominator, **design).to_string(index=False), "\n")


print("EXPENDITURES BY EVENT TYPE, 2005 (FIGURE 1)")
mean_sum_ratio(["PRESCRIBED_MEDICINES", "HOSPITAL_INPATIENT", "AMBULATORY_CARE", "OTHER"], "TOTAL")
print("INPATIENT STAY EXPENSES BY SOURCE OF PAYMENT, 2005 (FIGURE 1)")
mean_sum_ratio(["IP_PRIVATE_INS", "IP_MEDICARE", "IP_MEDICAID", "IP_OUT_OF_POCKET", "IP_OTHER"], "IP_TOTAL")

# Hospital inpatient stays file (HC-094D); zero-night stays count as one night
IP2005 = load_meps("h94d", columns=["DUPERSID", "EVNTIDX", "RSNINHOS", "IPXP05X", "NUMNIGHX", "PERWT05F", "VARSTR",
                                    "VARPSU"])  # fmt: skip
IP2005["NUMNIGHX"] = IP2005["NUMNIGHX"].replace(0, 1)
# SAS ROUND() rounds half away from zero
IP2005["PERDIEM"] = np.floor(IP2005["IPXP05X"] / IP2005["NUMNIGHX"] + 0.5)
IP2005["RSNINHOS"] = apply_format(IP2005["RSNINHOS"], SURGF, default="OTHER")

print("IP EXPENSES PER STAY AND PER DIEM, BY SURGERY (FIGURE 5)")
for var in ("IPXP05X", "PERDIEM"):
    print(svy_mean(IP2005, var, domain="RSNINHOS", **design).to_string(index=False), "\n")
