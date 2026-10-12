"""Compare a ported script's estimates with values printed in a SAS ``*_OUTPUT.TXT`` file.

The printed SAS values are transcribed to ``validation/sas_reference/<script>.csv`` with
columns ``result,domain,variable,estimate,se``:

* ``result``   key of the dict returned by the script's ``main()``
* ``domain``   domain level(s) joined by ``|`` (empty for overall estimates)
* ``estimate`` / ``se`` exactly as printed by SAS (``se`` may be blank)

A value passes when it agrees with the Python result to the precision SAS printed
(half a unit in the last printed digit; for E-notation, in the last significant digit).

Usage: python validation/compare_to_sas.py <script.py> <reference.csv> [--meps-dir DIR]
"""

import argparse
import csv
import importlib.util
import os
import sys
from decimal import Decimal

import pandas as pd


def half_ulp(text: str) -> float:
    exp = Decimal(text.replace(",", "")).as_tuple().exponent
    return 0.5 * 10.0 ** exp


def lookup(res: pd.DataFrame, domain: str, variable: str) -> pd.Series | None:
    dom_cols = [c for c in res.columns if c not in ("variable", "estimate", "se")]
    key = res[dom_cols].astype(str).agg("|".join, axis=1) if dom_cols else pd.Series("", index=res.index)
    hit = res[(key == domain) & (res["variable"] == variable)]
    return hit.iloc[0] if len(hit) == 1 else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("script")
    ap.add_argument("reference")
    ap.add_argument("--meps-dir", default=os.environ.get("MEPS_DIR"))
    args = ap.parse_args()

    spec = importlib.util.spec_from_file_location("ported", args.script)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    results = mod.main(args.meps_dir)

    n_fail = n = 0
    with open(args.reference, newline="") as fh:
        for row in csv.DictReader(fh):
            res = results.get(row["result"])
            hit = None if res is None else lookup(res, row["domain"], row["variable"])
            for stat in ("estimate", "se"):
                if not row[stat]:
                    continue
                n += 1
                ref = float(row[stat].replace(",", ""))
                py = float("nan") if hit is None else float(hit[stat])
                ok = abs(py - ref) <= half_ulp(row[stat]) * (1 + 1e-9)
                n_fail += not ok
                if not ok:
                    print(f"FAIL {row['result']} [{row['domain']}] {row['variable']} {stat}: "
                          f"SAS={row[stat]} Python={py!r}")
    print(f"{args.script}: {n - n_fail}/{n} printed SAS values reproduced "
          f"{'PASS' if n_fail == 0 else 'FAIL'}")
    sys.exit(1 if n_fail else 0)


if __name__ == "__main__":
    main()
