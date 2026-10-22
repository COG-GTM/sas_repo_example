# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Use, expenditures, and population, 2016
#
# Utilization and expenditures by event type and source of payment (SOP)
#  - Total number of events
#  - Mean expenditure per event, by source of payment
#  - Mean events per person, for office-based visits
#
# Selected event types:
#  - Office-based medical visits (OBV)
#  - Office-based physician visits (OBD)
#  - Outpatient visits (OPT)
#  - Outpatient physician visits (OPV)
#
# Input files:
#   - C:\MEPS\h192.ssp (2016 full-year consolidated)
#   - C:\MEPS\h188f.ssp (2016 OP event file)
#   - C:\MEPS\h188g.ssp (2016 OB event file)
#
# Python port of SAS/summary_tables_examples/use_events_2016.sas
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import MEPS_DIR, read_meps, svy_mean, svy_total  # noqa: E402

pd.set_option("display.width", 200)

# Load datasets ---------------------------------------------------------------

h192 = read_meps(f"{MEPS_DIR}/h192.ssp")  # FYC file
h188f = read_meps(f"{MEPS_DIR}/h188f.ssp")  # OP event file
h188g = read_meps(f"{MEPS_DIR}/h188g.ssp")  # OB event file

# Aggregate payment sources for each dataset ----------------------------------
#
#  Notes:
#   - For 1996-1999: TRICARE label is CHM (changed to TRI in 2000)
#
#   - For 1996-2006: combined facility + SBD variables for hospital-type events
#      are not on PUF
#
#   - Starting in 2019, 'Other public' (OPU) and 'Other private' (OPR) are
#      dropped from the files
#
#  PR = Private (PV) + TRICARE (TR)
#
#  OZ = other federal (OF)  + State/local (SL) + other private (OR) +
#        other public (OU)  + other unclassified sources (OT) +
#        worker's comp (WC) + Veteran's (VA)


def add(df, cols):
    return df[cols].sum(axis=1, min_count=len(cols))


OB = h188g[h188g["OBXP16X"] >= 0].copy()  # Remove inapplicable events
OB["PR"] = add(OB, ["OBPV16X", "OBTR16X"])
OB["OZ"] = add(OB, ["OBOF16X", "OBSL16X", "OBVA16X", "OBOT16X", "OBOR16X", "OBOU16X", "OBWC16X"])
OB["count"] = 1  # add counter for event totals
OB["phys_count"] = (OB["SEEDOC"] == 1).astype(int)

OP = h188f[h188f["OPXP16X"] >= 0].copy()  # Remove inapplicable events
oz = ["OF", "SL", "OR", "OU", "VA", "OT", "WC"]
OP["PR_fac"] = add(OP, ["OPFPV16X", "OPFTR16X"])
OP["PR_sbd"] = add(OP, ["OPDPV16X", "OPDTR16X"])
OP["OZ_fac"] = add(OP, [f"OPF{s}16X" for s in oz])
OP["OZ_sbd"] = add(OP, [f"OPD{s}16X" for s in oz])
# Combine facility and SBD expenses for hospital-type events
OP["SF"] = OP["OPFSF16X"] + OP["OPDSF16X"]  # out-of-pocket payments
OP["MR"] = OP["OPFMR16X"] + OP["OPDMR16X"]  # Medicare
OP["MD"] = OP["OPFMD16X"] + OP["OPDMD16X"]  # Medicaid
OP["PR"] = OP["PR_fac"] + OP["PR_sbd"]  # private insurance (including TRICARE)
OP["OZ"] = OP["OZ_fac"] + OP["OZ_sbd"]  # other sources of payment
OP["count"] = 1  # add counter for event totals

# Merge with FYC to retain all PSUs -------------------------------------------

FYC = h192[["DUPERSID", "VARSTR", "VARPSU", "PERWT16F"]]
design_cols = ["VARSTR", "VARPSU", "PERWT16F"]
OB_FYC = OB.drop(columns=design_cols).merge(FYC, on="DUPERSID", how="outer")
OP_FYC = OP.drop(columns=design_cols).merge(FYC, on="DUPERSID", how="outer")

# Calculate estimates using survey procedures ---------------------------------
#
# Sources of payment (SOP) abbreviations:
#  - SF: Out-of-pocket
#  - PR: Private insurance, including TRICARE (PTR)
#  - MR: Medicare
#  - MD: Medicaid
#  - OZ: Other

design = dict(weight="PERWT16F", strata="VARSTR", psu="VARPSU")


def event_estimates(df, sop_vars, title, phys_title):
    """Total number of events and mean expenditure per event, overall and for
    physician visits (DOMAIN SEEDOC)."""
    for dom, ttl in ((None, title), ("SEEDOC", phys_title)):
        tot = svy_total(df, ["count"], domain=dom, **design)
        mean = svy_mean(df, sop_vars, domain=dom, **design)
        if dom:
            tot = tot[tot["SEEDOC"] == 1]
            mean = mean[mean["SEEDOC"] == 1]
        print(f"\n{ttl}")
        print(tot[["estimate", "se"]].rename(columns={"estimate": "Total number of events"}).to_string(index=False))
        print(
            mean[["variable", "estimate", "se"]]
            .rename(columns={"variable": "Source of Payment", "estimate": "Mean expenditure per event"})
            .to_string(index=False)
        )


# Office-based visits: Total number of events and Mean expenditure per event
event_estimates(
    OB_FYC, ["OBSF16X", "PR", "OBMR16X", "OBMD16X", "OZ"], "Office-based visits", "Office-based physician visits"
)

# Outpatient visits: Number of events and Mean expenditure per event
event_estimates(OP_FYC, ["SF", "PR", "MR", "MD", "OZ"], "Outpatient visits", "Outpatient physician visits")

# Mean events per person, office-based medical visits -------------------------

# Aggregate to person-level
pers_OB = (
    OB_FYC.groupby(["DUPERSID", "VARSTR", "VARPSU"])
    .agg(
        n_events=("count", "sum"),
        n_phys_events=("phys_count", "sum"),
        PERWT16F=("PERWT16F", "mean"),
    )
    .reset_index()
)

# Mean events per person
print("\nMean events per person, office-based medical visits")
tot = svy_total(pers_OB, ["n_events", "n_phys_events"], **design)
mean = svy_mean(pers_OB, ["n_events", "n_phys_events"], **design)
print(tot.merge(mean, on="variable", suffixes=("_sum", "_mean")).to_string(index=False))
