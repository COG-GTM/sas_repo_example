"""Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L2/L2.sas



<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

import os

# The NHIS person files and the NHIS-MEPS link file are not MEPS public use files.
# Put them in NHIS_DIR (default: MEPS_DIR) to run this example.
NHIS_DIR = Path(os.environ.get("NHIS_DIR", MEPS_DIR))
files = {k: NHIS_DIR / f for k, f in [("n99", "nhisper99.dat"), ("n00", "nhisper00.dat"), ("lnk", "nhmep01x.dat")]}
missing = [str(p) for p in files.values() if not p.exists()]
if missing:
    sys.exit("L2 needs the NHIS person files and the NHIS-MEPS link file: " + ", ".join(missing))

ANYLIM = {-9: "-9 Not Ascer", -1: "-1 Inapp", 1: "1 Yes", 2: "2 No"}
SEX = {1: "1 Male", 2: "2 Female"}
HSTAT = {-9: "-9 Not Ascer", -8: "-8 DK", -7: "-7 Refused", -1: "-1 Inapp", 1: "1 Excellent", 2: "2 Very Good",
         3: "3 Good", 4: "4 Fair", 5: "5 Poor", 7: "7 Refused", 8: "8 Not Ascer", 9: "9 DK"}
NHISLIM = {1: "1 Limited", 2: "2 Not Limited", 3: "3 Unknown", 7: "7 Refused", 8: "8 Not Ascer", 9: "9 DK"}
NHISCHRON = {0: "0 Not Limited", 1: "1 Lim, 1+ Chron Cond", 2: "2 Lim, Not Chron", 3: "3 Lim, Chron Unk",
             7: "7 Refused", 8: "8 Not Ascer", 9: "9 DK"}

# 2001 full-year file (HC-060): annual health status from the last non-missing round -----
meps01 = read_meps("h60", ["DUPERSID", "ANYLIM01", "RTHLTH31", "RTHLTH42", "RTHLTH53", "PERWT01F", "VARSTR01",
                           "VARPSU01"])
m = meps01
m["MEPSHSTAT"] = np.select([m.RTHLTH53 > 0, m.RTHLTH42 > 0, m.RTHLTH31 > 0], [m.RTHLTH53, m.RTHLTH42, m.RTHLTH31],
                           np.nan)
hs = ["MEPSHSTAT", "RTHLTH53", "RTHLTH42", "RTHLTH31"]
print(freq_list(m.assign(**{v: sas_format(m[v], HSTAT) for v in hs}), ["MEPSHSTAT"]).to_string(index=False), "\n")
print(freq_list(m.assign(**{v: sas_format(m[v], HSTAT) for v in hs}), hs).to_string(index=False), "\n")

# INFILE ... INPUT (column positions) -> pandas.read_fwf ---------------------------------
link = pd.read_fwf(files["lnk"], colspecs=[(0, 8), (8, 14), (14, 16), (16, 17), (18, 22)],
                   names=["DUPERSID", "HHX", "PX", "LINKFLAG", "SRVY_YR"], dtype={"DUPERSID": str, "HHX": str, "PX": str})
print(freq_list(link, ["LINKFLAG"]).to_string(index=False), "\n", freq_list(link, ["SRVY_YR"]).to_string(index=False))


def nhis(path, lim, chron, hstat):
    return pd.read_fwf(path, colspecs=[(2, 6), (6, 12), (14, 16), (17, 18), (18, 20), (lim - 1, lim),
                                       (chron - 1, chron), (hstat - 1, hstat)],
                       names=["SRVY_YR", "HHX", "PX", "SEX", "AGE", "NHISLIM", "NHISCHRON", "NHISHSTAT"],
                       dtype={"HHX": str, "PX": str})


nhis_all = pd.concat([nhis(files["n99"], 120, 563, 564), nhis(files["n00"], 123, 566, 567)])
nhis_all = nhis_all.sort_values(["HHX", "PX", "SRVY_YR"], kind="stable")

# MERGE meps01 (IN=A) link (IN=B) BY DUPERSID; IF A & B, then with NHIS BY HHX PX SRVY_YR
mepslink = meps01.drop(columns=["RTHLTH31", "RTHLTH42", "RTHLTH53"]).merge(link.drop(columns="LINKFLAG"),
                                                                           on="DUPERSID", how="inner")
total01 = mepslink.merge(nhis_all, on=["HHX", "PX", "SRVY_YR"], how="inner")
fmt = {"ANYLIM01": ANYLIM, "MEPSHSTAT": HSTAT, "SEX": SEX, "NHISLIM": NHISLIM, "NHISCHRON": NHISCHRON,
       "NHISHSTAT": HSTAT}
t = total01.assign(**{v: sas_format(total01[v], f) for v, f in fmt.items()})
for v in fmt:
    print(freq_list(t, [v]).to_string(index=False), "\n")
print(total01.AGE.agg(["count", lambda s: s.isna().sum(), "min", "max"]), "\n")

# PROC FREQ ... WEIGHT PERWT01F (weighted cross-tabulations, point estimates only)
for a, b in [("NHISLIM", "ANYLIM01"), ("NHISHSTAT", "MEPSHSTAT")]:
    f = t.groupby([a, b], dropna=False).PERWT01F.sum().rename("Frequency").reset_index()
    f["Percent"] = 100 * f.Frequency / f.Frequency.sum()
    show(f"{a} * {b} (weighted)", f, "{:,.2f}")
