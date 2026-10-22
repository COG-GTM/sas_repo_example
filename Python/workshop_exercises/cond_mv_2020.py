# -----------------------------------------------------------------------------
# Example code linking MEPS-HC Medical Conditions file to the Office-based
#  medical visits file, data year 2020:
#
# Event-level estimates:
#   - Number of office-based visits for mental health
#   - Total expenditures for office-based mental health treatment
#   - Mean expenditure per office-based mental health visit
#
# Person-level estimates
#   - Number of people with office-based mental health visits
#   - Percent of people with office-based mental health visits
#   - Mean expenditure per person for office-based mental health visits
#
# Input files:
#   - h220g.sas7bdat   (2020 Office-based event file)
#   - h222.sas7bdat    (2020 Conditions file)
#   - h220if1.sas7bdat (2020 CLNK: Condition-event link file)
#   - h224.sas7bdat    (2020 Full-Year Consolidated file)
#
# Resources:
#  - CCSR codes:
#    https://github.com/HHS-AHRQ/MEPS/blob/master/Quick_Reference_Guides/meps_ccsr_conditions.csv
#  - MEPS-HC Public Use Files:
#    https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp
#  - MEPS-HC data tools:
#    https://datatools.ahrq.gov/meps-hc
#
# Python port of SAS/workshop_exercises/cond_mv_2020.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import crosstab_list, load_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

# Load datasets ---------------------------------------------------------------
ob20 = load_meps("h220g")  # Office-based medical visits
cond20 = load_meps("h222")  # Medical conditions
clnk20 = load_meps("h220if1")  # Condition-event linkage file
fyc20 = load_meps("h224", columns=["DUPERSID", "PANEL", "PERWT20F", "VARSTR", "VARPSU"])  # Person-level file

# Keep only needed variables --------------------------------------------------
ob_keep = ["PANEL", "DUPERSID", "EVNTIDX", "EVENTRN", "OBXP20X", "PERWT20F", "VARSTR", "VARPSU"]
ob_keep += [c for c in ob20.columns if c.startswith(("OBDATE", "TELE"))]
ob20x = ob20[ob_keep]
cond20x = cond20[["DUPERSID", "CONDIDX"] + [c for c in cond20.columns if c.startswith(("ICD10", "CCSR"))]].copy()
fyc20x = fyc20[["DUPERSID", "PERWT20F", "VARSTR", "VARPSU"]]

# Filter COND file to only people with Mental Disorders -----------------------
cond20x["ALL_CCSR"] = cond20x[["CCSR1X", "CCSR2X", "CCSR3X"]].fillna("").astype(str).agg("".join, axis=1)
mh_codes = ["MBD", "FAC002", "FAC007", "NVS011", "SYM008", "SYM009"]
mental_health = cond20x[cond20x["ALL_CCSR"].str.contains("|".join(mh_codes))]
print(crosstab_list(mental_health, ["ICD10CDX", "CCSR1X", "CCSR2X", "CCSR3X"]).to_string(), "\n")

# Filter CLNK file to only office-based visits (EVENTYPE = 1) ------------------
clnk_ob = clnk20[clnk20["EVENTYPE"] == 1]
print(crosstab_list(clnk_ob, ["EVENTYPE"]), "\n")

# Merge conditions file with the conditions-event link file (CLNK)
mh_clnk = mental_health.merge(clnk_ob, on=["DUPERSID", "CONDIDX"], how="inner")
print(mh_clnk[mh_clnk["CONDIDX"] == "2320109103009"].to_string(index=False))
print(mh_clnk[mh_clnk["EVNTIDX"] == "2320051101205101"].to_string(index=False), "\n")

# De-duplicate by event ID: someone can have multiple MH conditions on a visit
mh_clnk_nodup = mh_clnk[["DUPERSID", "EVNTIDX", "EVENTYPE"]].drop_duplicates()

# Merge on event files
ob_mental_health = mh_clnk_nodup.merge(ob20x, on=["DUPERSID", "EVNTIDX"], how="inner")
ob_mental_health["MH_OB_VISIT"] = 1  # indicator for all visits to help with counting later
print(ob_mental_health.head(5).to_string(index=False))
print(crosstab_list(ob_mental_health, ["EVENTYPE", "MH_OB_VISIT"]), "\n")

# DO NOT RUN: survey estimates on ob_mental_health alone would give WRONG SEs,
# because it lacks the complete strata/PSUs for the entire MEPS sample.

# Merge on FYC file for complete Strata, PSUs ---------------------------------
ob_mh_fyc = ob_mental_health.drop(columns=["PERWT20F", "VARSTR", "VARPSU"]).merge(
    fyc20x, on="DUPERSID", how="outer", indicator=True
)
ob_mh_fyc["MH_OB"] = ob_mh_fyc["_merge"].isin(["both", "left_only"]).astype(int)
ob_mh_fyc["FYC"] = ob_mh_fyc.pop("_merge").isin(["both", "right_only"]).astype(int)
print(crosstab_list(ob_mh_fyc, ["MH_OB", "MH_OB_VISIT", "FYC"]))
# Reset missing indicators to 0
ob_mh_fyc["MH_OB_VISIT"] = ob_mh_fyc["MH_OB_VISIT"].fillna(0)
print(crosstab_list(ob_mh_fyc, ["MH_OB", "MH_OB_VISIT"]), "\n")

design = dict(weight="PERWT20F", strata="VARSTR", psu="VARPSU")

# Event-level estimates -------------------------------------------------------
#  - Number of office-based visits for mental health:       343,810,085 (SE: 22,252,863)
#  - Total exp. for office-based mental health visits:  $60,209,392,314 (SE: 4,437,433,004)
#  - Mean exp. per visit:                                       $175.12 (SE: 6.46)
print("Event-level estimates (DOMAIN MH_OB so the lonely PSU is handled correctly)")
evt_vars = ["MH_OB_VISIT", "OBXP20X"]
ev = svy_total(ob_mh_fyc, evt_vars, domain="MH_OB", **design).merge(
    svy_mean(ob_mh_fyc, evt_vars, domain="MH_OB", **design), on=["MH_OB", "variable", "n"], suffixes=("_sum", "_mean")
)
print(ev.to_string(index=False), "\n")

# A note on Telehealth: TELEHEALTHFLAG = -15 for events reported before the
# telehealth questions were added in Fall 2020.
print(pd.crosstab(ob20["OBDATEMM"], ob20["TELEHEALTHFLAG"]), "\n")

# Person-level estimates ------------------------------------------------------
#  - Number of people with office visit for MH:  29,816,984 (SE: 1,192,676)
#  - Percent of people with office visit for MH:      9.08% (SE: 0.29%)
#  - Mean exp per person for office visits for MH: $2019.30 (SE: 126.16)
pers_mh = (
    ob_mh_fyc.groupby(["DUPERSID", "VARSTR", "VARPSU"])
    .agg(
        PERWT20F=("PERWT20F", "mean"),
        PERSXP=("OBXP20X", "sum"),
        PERS_NEVENTS=("MH_OB_VISIT", "sum"),
        MH_OB_VISIT_PERS=("MH_OB_VISIT", "mean"),
        MH_OB_PERS=("MH_OB", "mean"),
    )
    .reset_index()
)
print(f"pers_mh rows: {len(pers_mh)}; FYC rows: {len(fyc20)}")
print(crosstab_list(pers_mh, ["MH_OB_PERS", "MH_OB_VISIT_PERS"]))
print(crosstab_list(pers_mh[(pers_mh["MH_OB_PERS"] == 0) | (pers_mh["MH_OB_VISIT_PERS"] == 0)], ["PERS_NEVENTS"]))

print("\nPerson-level estimates (DOMAIN MH_OB_PERS)")
p_vars = ["MH_OB_VISIT_PERS", "PERSXP", "PERS_NEVENTS"]
pe = svy_total(pers_mh, p_vars, domain="MH_OB_PERS", **design).merge(
    svy_mean(pers_mh, p_vars, domain="MH_OB_PERS", **design),
    on=["MH_OB_PERS", "variable", "n"],
    suffixes=("_sum", "_mean"),
)
print(pe.to_string(index=False))
print("\nPerson-level estimates (overall: percent of people with OB MH visit)")
print(svy_mean(pers_mh, "MH_OB_VISIT_PERS", **design).to_string(index=False))
