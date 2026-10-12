# -----------------------------------------------------------------------------
# Example code to replicate estimates from the MEPS-HC Data Tools summary tables
#
# Use, expenditures, and population, 2016
#
# Office-based and outpatient visits
#  - Number of events
#  - Mean expenditure per event, by source of payment
#  - Mean events per person (office-based visits)
#
# Input files:
#  - C:/MEPS/h192.ssp  (2016 full-year consolidated)
#  - C:/MEPS/h188f.ssp (2016 outpatient visits)
#  - C:/MEPS/h188g.ssp (2016 office-based medical visits)
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from meps_utils import meps_path, read_meps, survey_estimate


def merge_with_fyc(events, fyc):
    # Keep every FYC person (outer merge) so the full design is preserved;
    # 'domain' flags the event rows for subpopulation estimation.
    events = events.drop(columns=["VARSTR", "VARPSU", "PERWT16F"]).assign(count=1, domain=1)
    return events.merge(fyc, on="DUPERSID", how="outer")


def main(meps_dir=None):
    # Load files ----------------------------------------------------------------
    fyc = read_meps(meps_path("h192.ssp", meps_dir), columns=["DUPERSID", "PERWT16F", "VARSTR", "VARPSU"])
    op = read_meps(meps_path("h188f.ssp", meps_dir))  # outpatient visits
    ob = read_meps(meps_path("h188g.ssp", meps_dir))  # office-based medical visits

    # Define sources of payment (PR = private incl. TRICARE, OZ = other) ---------
    ob["PR"] = ob["OBPV16X"] + ob["OBTR16X"]
    ob["OZ"] = ob[["OBOF16X", "OBSL16X", "OBVA16X", "OBOT16X", "OBOR16X", "OBOU16X", "OBWC16X"]].sum(axis=1)
    ob = ob[ob["OBXP16X"] >= 0]  # remove inapplicable events

    other = ["OF", "SL", "OR", "OU", "VA", "OT", "WC"]
    for part in ("F", "D"):  # facility / separately billing doctor
        op[f"PR_{part}"] = op[f"OP{part}PV16X"] + op[f"OP{part}TR16X"]
        op[f"OZ_{part}"] = op[[f"OP{part}{s}16X" for s in other]].sum(axis=1)
    op = op[op["OPXP16X"] >= 0]  # remove inapplicable events

    # Combine facility and SBD expenses for OP events
    op["SF"] = op["OPFSF16X"] + op["OPDSF16X"]  # out-of-pocket
    op["MR"] = op["OPFMR16X"] + op["OPDMR16X"]  # Medicare
    op["MD"] = op["OPFMD16X"] + op["OPDMD16X"]  # Medicaid
    op["PR"] = op["PR_F"] + op["PR_D"]          # private insurance (incl. TRICARE)
    op["OZ"] = op["OZ_F"] + op["OZ_D"]          # other sources of payment

    ob_fyc = merge_with_fyc(ob, fyc)
    op_fyc = merge_with_fyc(op, fyc)

    # Subpopulations: all events, and physician visits only
    ob_all, op_all = ob_fyc["domain"] == 1, op_fyc["domain"] == 1
    ob_doc = ob_all & (ob_fyc["SEEDOC"] == 1)
    op_doc = op_all & (op_fyc["SEEDOC"] == 1)
    w = {"weight": "PERWT16F"}

    results = {}
    # Number of events
    results["n_OB"] = survey_estimate(ob_fyc, "count", stat="total", subpop=ob_all, **w)
    results["n_OBdoc"] = survey_estimate(ob_fyc, "count", stat="total", subpop=ob_doc, **w)
    results["n_OP"] = survey_estimate(op_fyc, "count", stat="total", subpop=op_all, **w)
    results["n_OPdoc"] = survey_estimate(op_fyc, "count", stat="total", subpop=op_doc, **w)

    # Mean expenditure per event, by source of payment
    ob_vars = ["OBSF16X", "PR", "OBMR16X", "OBMD16X", "OZ"]
    op_vars = ["SF", "PR", "MR", "MD", "OZ"]
    results["exp_OB"] = survey_estimate(ob_fyc, ob_vars, stat="mean", subpop=ob_all, **w)
    results["exp_OBdoc"] = survey_estimate(ob_fyc, ob_vars, stat="mean", subpop=ob_doc, **w)
    results["exp_OP"] = survey_estimate(op_fyc, op_vars, stat="mean", subpop=op_all, **w)
    results["exp_OPdoc"] = survey_estimate(op_fyc, op_vars, stat="mean", subpop=op_doc, **w)

    # Mean events per person (aggregate to person level first)
    ob_fyc["phys_event"] = ob_fyc["count"] * (ob_fyc["SEEDOC"] == 1)
    pers_ob = (
        ob_fyc.groupby(["DUPERSID", "VARSTR", "VARPSU"], as_index=False)
        .agg(PERWT16F=("PERWT16F", "mean"),
             n_events=("count", "sum"),
             n_phys_events=("phys_event", "sum"))
    )
    results["pers_OB"] = survey_estimate(pers_ob, "n_events", stat="mean", **w)
    results["pers_OBdoc"] = survey_estimate(pers_ob, "n_phys_events", stat="mean", **w)

    for name, res in results.items():
        print(f"\n{name}:")
        print(res.to_string(index=False))
    return results


if __name__ == "__main__":
    main()
