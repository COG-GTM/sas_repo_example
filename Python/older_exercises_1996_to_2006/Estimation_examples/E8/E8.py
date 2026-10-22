# -----------------------------------------------------------------------------
# Python port of SAS/older_exercises_1996_to_2006/Estimation_examples/E8/E8.sas
# (validated against the SAS listing in E8.pdf)
#
# DESCRIPTION: HEALTH CARE SPENDING BY TYPE OF SERVICE AND HOSPITAL INPATIENT SPENDING BY
#              SOURCE OF PAYMENT, 2005 (MEPS STAT BRIEF), AND MEAN EXPENSE AND PER DIEM
#              EXPENSE PER HOSPITAL STAY, BY REASON ENTERED HOSPITAL (SURGERY / OTHER).
#
# INPUT FILES: C:/MEPS/h97.ssp  (2005 FULL-YEAR FILE)
#              C:/MEPS/h94d.ssp (2005 HOSPITAL INPATIENT STAYS)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[3]))
from meps_utils import meps_path, read_meps, survey_estimate

TOS = ["PRESCRIBED_MEDICINES", "HOSPITAL_INPATIENT", "AMBULATORY_CARE", "OTHER"]
IP = ["IP_PRIVATE_INS", "IP_MEDICARE", "IP_MEDICAID", "IP_OUT_OF_POCKET", "IP_OTHER"]


def main(meps_dir=None):
    p = read_meps(meps_path("h97.ssp", meps_dir))
    p["TOTAL"] = p["TOTEXP05"]
    p["PRESCRIBED_MEDICINES"] = p["RXEXP05"]
    p["HOSPITAL_INPATIENT"] = p["IPDEXP05"] + p["IPFEXP05"]
    p["AMBULATORY_CARE"] = p[["OBVEXP05", "OPDEXP05", "OPFEXP05", "ERDEXP05", "ERFEXP05"]].sum(axis=1)
    p["OTHER"] = p["TOTAL"] - p[TOS[:-1]].sum(axis=1)
    p["IP_TOTAL"] = p["HOSPITAL_INPATIENT"]
    p["IP_PRIVATE_INS"] = p["IPDPRV05"] + p["IPFPRV05"] + p["IPDTRI05"] + p["IPFTRI05"]
    p["IP_MEDICARE"] = p["IPDMCR05"] + p["IPFMCR05"]
    p["IP_MEDICAID"] = p["IPDMCD05"] + p["IPFMCD05"]
    p["IP_OUT_OF_POCKET"] = p["IPDSLF05"] + p["IPFSLF05"]
    p["IP_OTHER"] = p["IP_TOTAL"] - p[IP[:-1]].sum(axis=1)

    ip = read_meps(meps_path("h94d.ssp", meps_dir),
                   columns=["DUPERSID", "EVNTIDX", "RSNINHOS", "IPXP05X", "NUMNIGHX", "PERWT05F", "VARSTR", "VARPSU"])
    # SAS drops nonpositive-weight records; strata left with one PSU are omitted (lonely_psu="remove")
    ip = ip[ip["PERWT05F"] > 0].copy()
    nights = ip["NUMNIGHX"].where(ip["NUMNIGHX"] != 0, 1)
    ip["PERDIEM"] = np.floor(ip["IPXP05X"] / nights + 0.5)        # SAS ROUND() (half away from zero)
    ip["SURGF"] = np.where(ip["RSNINHOS"] == 1, "SURGERY", "OTHER")

    w = {"weight": "PERWT05F"}
    w_ev = {"weight": "PERWT05F", "lonely_psu": "remove"}
    results = {
        "mean_tos": survey_estimate(p, TOS + ["TOTAL"], stat="mean", **w),
        "total_tos": survey_estimate(p, TOS + ["TOTAL"], stat="total", **w),
        "pct_tos": survey_estimate(p, TOS, stat="ratio", denominator="TOTAL", **w),
        "mean_ip": survey_estimate(p, IP + ["IP_TOTAL"], stat="mean", **w),
        "total_ip": survey_estimate(p, IP + ["IP_TOTAL"], stat="total", **w),
        "pct_ip": survey_estimate(p, IP, stat="ratio", denominator="IP_TOTAL", **w),
        "mean_stay_by_reason": survey_estimate(ip, "IPXP05X", stat="mean", domain="SURGF", **w_ev),
        "mean_perdiem": survey_estimate(ip, "PERDIEM", stat="mean", **w_ev),
        "mean_perdiem_by_reason": survey_estimate(ip, "PERDIEM", stat="mean", domain="SURGF", **w_ev),
    }
    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
