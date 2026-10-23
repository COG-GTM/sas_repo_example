"""Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E8/E8.sas

/

PROGRAM:    <MEPS_DIR>/PROG/E8.SAS

DESCRIPTION:    THIS PROGRAM GENERATES TWO SETS OF EXPENDITURE DATA FOR 2005
                            INPATIENT STAYS, SIMILAR TO THOSE REPORTED IN MEPS STAT BRIEF
                            #164 (FIGURES 1 AND 5). STAT BRIEF #164 USED 2004 MEPS DATA.
                            THIS PROGRAM USES 2005 MEPS DATA.

                            ESTIMATES COMPARABLE TO FIGURE 1 ARE BY EVENT TYPE AND BY
                            SOURCE OF PAYMENT FOR INPATIENT (IP) STAY EXPENSES.

                            EVENT TYPE DEFINITIONS:

                            AMBULATORY:  OFFICE-BASED + OUTPATIENT + EMERGENCY DEPARTMENT
                            HOSPITAL:    HOSPITAL INPATIENT
                            PRESCRIBED   MEDICINES: PMED (RX)
                            OTHER:       TOTAL EXPENDITURES - (AMBULATORY + HOSPITAL + PMED)

                            NOTES: (1) EXPENSES INCLUDE BOTH FACILITY AND PHYSICIAN EXPENSES.
                                    (2) PRIVATE INCLUDES TRICARE, BUT NOT 'OTHER PRIVATE'.

            TOTAL HEALTH CARE EXP: $963,882,049,581
                       (SB: $963.9 BILLION)

             EVENT TYPES:  2005 DATA    2004 DATA (SB#164)

        PMED:               20.8%             19.8%
        HOSPITAL:     30.9%        31.4%
        AMBULATORY:   34.7%        35.6%
        OTHER:        13.5%        13.2%

        FOR HOSPITAL EXPENSES (SB #164)
        FIRST BULLET IN HIGHLIGHTS STATES,
        "EXPENSES FOR HOSPITAL INPATIENT STAYS
        ACCOUNTED FOR NEARLY ONE-THIRD OF ALL
        HEALTH CARE EXPENSES..." [30.9% IN 2005]

        IP SOPs:       2005 DATA    2004 DATA (SB#164)

             PRIVATE:       41.1%        43.7%
             MEDICARE:      37.7%        38.8%
             MEDICAID:      10.6%         9.6%
             OUT-OF-POCKET:  3.8%         2.2%
             OTHER:          6.9%         5.7%

             FIGURE 5: IP EXPENSES PER STAY AND PER DIEM

             PER STAY      2005 DATA     2004 DATA (SB#164)

             W/  SURG       $15,160       $14,729
             W/0 SURG       $ 8,439       $ 7,678

             PER DIEM

             W/  SURG       $ 5,226       $ 5,059
             W/O SURG       $ 2,173       $ 1,954

             NOTE: FOR THE SMALL NUMBER OF ZERO NIGHT STAYS, LENGTH
                   OF STAY WAS CONSIDERED TO BE ONE.

INPUT FILES:     (1) <MEPS_DIR>/DATA/H97.SAS7BDAT (2005 FULL-YEAR FILE)
              (2) <MEPS_DIR>/DATA/H94D.SAS7BDAT (2005 HOSP. IP STAYS)

/

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# 2005 full-year consolidated file (HC-097) --------------------------------------------
PUF97 = read_meps("h97", ["TOTEXP05", "RXEXP05", "OBVEXP05", "OPDEXP05", "OPFEXP05", "ERDEXP05", "ERFEXP05", "IPDEXP05",
                          "IPFEXP05", "IPDPRV05", "IPFPRV05", "IPDTRI05", "IPFTRI05", "IPDMCR05", "IPFMCR05", "IPDMCD05",
                          "IPFMCD05", "IPDSLF05", "IPFSLF05", "VARSTR", "VARPSU", "PERWT05F"])
P = PUF97
P["TOTAL"] = P.TOTEXP05
P["PRESCRIBED_MEDICINES"] = P.RXEXP05
P["HOSPITAL_INPATIENT"] = P.IPDEXP05 + P.IPFEXP05
P["AMBULATORY_CARE"] = P.OBVEXP05 + P.OPDEXP05 + P.OPFEXP05 + P.ERDEXP05 + P.ERFEXP05
P["OTHER"] = P.TOTAL - (P.PRESCRIBED_MEDICINES + P.HOSPITAL_INPATIENT + P.AMBULATORY_CARE)
P["IP_TOTAL"] = P.HOSPITAL_INPATIENT
P["IP_PRIVATE_INS"] = (P.IPDPRV05 + P.IPFPRV05) + (P.IPDTRI05 + P.IPFTRI05)
P["IP_MEDICARE"] = P.IPDMCR05 + P.IPFMCR05
P["IP_MEDICAID"] = P.IPDMCD05 + P.IPFMCD05
P["IP_OUT_OF_POCKET"] = P.IPDSLF05 + P.IPFSLF05
P["IP_OTHER"] = P.IP_TOTAL - (P.IP_PRIVATE_INS + P.IP_MEDICARE + P.IP_MEDICAID + P.IP_OUT_OF_POCKET)

design = dict(weight="PERWT05F", strata="VARSTR", cluster="VARPSU")
COLS = ["VarName", "Mean", "StdErr", "Sum", "StdDev"]
for parts, total in [(["PRESCRIBED_MEDICINES", "HOSPITAL_INPATIENT", "AMBULATORY_CARE", "OTHER"], "TOTAL"),
                     (["IP_PRIVATE_INS", "IP_MEDICARE", "IP_MEDICAID", "IP_OUT_OF_POCKET", "IP_OTHER"], "IP_TOTAL")]:
    show(f"Components of {total} (MEAN STDERR SUM STD)", surveymeans(P, parts + [total], **design)[COLS])
    show(f"RATIO components / {total}", pd.concat([surveyratio(P, v, total, **design) for v in parts], ignore_index=True))

# 2005 hospital inpatient stays (HC-094D) ------------------------------------------------
IP2005 = read_meps("h94d", ["DUPERSID", "EVNTIDX", "RSNINHOS", "IPXP05X", "NUMNIGHX", "PERWT05F", "VARSTR", "VARPSU"])
IP2005.loc[IP2005.NUMNIGHX == 0, "NUMNIGHX"] = 1
r = IP2005.IPXP05X / IP2005.NUMNIGHX
IP2005["PERDIEM"] = np.sign(r) * np.floor(np.abs(r) + 0.5)  # SAS ROUND (half away from zero)
IP2005["RSNINHOS_F"] = sas_format(IP2005.RSNINHOS, {1: "SURGERY"}, other="OTHER")
for v in ["IPXP05X", "PERDIEM"]:
    show(f"{v}", surveymeans(IP2005, v, **design)[["VarName", "Mean", "StdErr"]])
    show(f"{v} by RSNINHOS", surveymeans(IP2005, v, domain="RSNINHOS_F", **design)[["RSNINHOS_F", "VarName", "Mean", "StdErr"]])
