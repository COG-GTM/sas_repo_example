"""Compare Python estimates against reference output from the R scripts.

Usage:
    python compare_to_r.py <python_script.py> <r_csv_prefix> [--meps-dir DIR]

``r_csv_prefix`` points at the CSVs written by ``run_r_reference.R`` (one CSV per
top-level svyby/svytotal/svymean call, numbered in call order). Each Python script's
``main()`` returns tidy results; they are matched to the R CSVs in call order.
"""

import argparse
import glob
import importlib.util
import sys

import pandas as pd


def load_main(path):
    spec = importlib.util.spec_from_file_location("ported", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.main


def r_to_tidy(r: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    """Reshape an R result CSV to tidy ``[*by, variable, estimate_r, se_r]``.

    Estimate columns are discovered from the R table itself (not from the Python output),
    so a variable missing on either side shows up as unmatched.
    """
    if "Std. Error" in r.columns:  # summary(svyglm) coefficient table
        r = r.rename(columns={"Estimate": "estimate", "Std. Error": "se"})[["term", "estimate", "se"]]
    if "term" in r.columns:  # svytotal/svymean/svyratio without svyby
        return r.rename(columns={"term": "variable", "estimate": "estimate_r", "se": "se_r"})
    variables = [c for c in r.columns if c not in by and c != "se" and not c.startswith("se.")]
    rows = []
    for var in variables:
        se_col = f"se.{var}" if f"se.{var}" in r.columns else "se"
        part = r[by].copy()
        part["variable"] = var
        part["estimate_r"] = r[var]
        part["se_r"] = r[se_col]
        rows.append(part)
    out = pd.concat(rows, ignore_index=True)
    out[by] = out[by].astype(str)
    return out


def compare(py: pd.DataFrame, r: pd.DataFrame) -> pd.DataFrame:
    by = [c for c in py.columns if c not in ("variable", "estimate", "se", "t", "p")]
    r_tidy = r_to_tidy(r, by)
    py = py.copy()
    py[by] = py[by].astype(str)
    m = py.merge(r_tidy, on=by + ["variable"], how="outer", indicator=True)
    m["rel_diff_est"] = (m["estimate"] - m["estimate_r"]).abs() / m["estimate_r"].abs().clip(lower=1e-12)
    m["rel_diff_se"] = (m["se"] - m["se_r"]).abs() / m["se_r"].abs().clip(lower=1e-12)
    return m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("script")
    ap.add_argument("r_prefix")
    ap.add_argument("--meps-dir")
    ap.add_argument("--tol", type=float, default=1e-6)
    args = ap.parse_args()

    results = load_main(args.script)(args.meps_dir)
    r_files = sorted(glob.glob(f"{args.r_prefix}_[0-9][0-9]_*.csv"))
    if len(r_files) != len(results):
        print(f"FAIL: {len(results)} Python results but {len(r_files)} R reference files "
              f"matching {args.r_prefix}_NN_*.csv")
        sys.exit(1)
    ok = True
    for (name, py), r_file in zip(results.items(), r_files):
        m = compare(py, pd.read_csv(r_file, keep_default_na=False, na_values=["NA"]))
        unmatched = (m["_merge"] != "both").sum()
        both = m[m["_merge"] == "both"]
        max_est = both["rel_diff_est"].max()
        max_se = both["rel_diff_se"].max()
        passed = unmatched == 0 and max_est < args.tol and max_se < args.tol
        ok &= passed
        print(f"{name:<12} vs {r_file.split('/')[-1]:<40} rows={len(both):>5} unmatched={unmatched:>3} "
              f"max_rel_diff_est={max_est:.2e} max_rel_diff_se={max_se:.2e} {'PASS' if passed else 'FAIL'}")
        if unmatched:
            print(m.loc[m["_merge"] != "both"].head(10).to_string(index=False))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
