# ----------------------------------------------------------------------------
# Program:      c:\meps\prog\Example_L2.sas
#
# Description:  This example shows how to:
#               (1) Create SAS files from ASCII files
#               (2) Link 2001 MEPS to 1999 and 2000 NHIS
#               (3) Compare persons' status in NHIS with their status in MEPS
#
# Input Files:  c:\meps\data\nhisper99.dat (1999 NHIS Persons, renamed from PERSONSX)
#               c:\meps\data\nhisper00.dat (2000 NHIS Persons, renamed from PERSONSX)
#               c:\meps\data\nhmep01x.dat (NHIS-MEPS Link File - read in from diskette)
#               c:\meps\data\h60.sas7bdat (2001 MEPS Persons)
#
# Python port of SAS/older_exercises_1996_to_2006/Linking_examples/L2/L2.sas
#
# NOTE: the NHIS person files and the NHIS-MEPS link file are not MEPS public
# use files and are not downloaded automatically; place them in MEPS_DIR.
# ----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import MEPS_DIR, apply_format, crosstab_list, load_meps  # noqa: E402

pd.set_option("display.width", 200)

ANYLIM = {-9: "-9 Not Ascer", -1: "-1 Inapp", 1: "1 Yes", 2: "2 No"}
SEX = {1: "1 Male", 2: "2 Female"}
HSTAT = {-9: "-9 Not Ascer", -8: "-8 DK", -7: "-7 Refused", -1: "-1 Inapp", 1: "1 Excellent", 2: "2 Very Good",
         3: "3 Good", 4: "4 Fair", 5: "5 Poor", 7: "7 Refused", 8: "8 Not Ascer", 9: "9 DK"}  # fmt: skip
NHISLIM = {1: "1 Limited", 2: "2 Not Limited", 3: "3 Unknown", 7: "7 Refused", 8: "8 Not Ascer", 9: "9 DK"}
NHISCHRON = {0: "0 Not Limited", 1: "1 Lim, 1+ Chron Cond", 2: "2 Lim, Not Chron", 3: "3 Lim, Chron Unk",
             7: "7 Refused", 8: "8 Not Ascer", 9: "9 DK"}  # fmt: skip

files = {k: Path(MEPS_DIR) / f for k, f in {"n99": "nhisper99.dat", "n00": "nhisper00.dat", "lnk": "nhmep01x.dat"}.items()}
missing = [str(p) for p in files.values() if not p.exists()]
if missing:
    sys.exit(f"Required NHIS / NHIS-MEPS link files not found: {missing}")

# MEPS 2001: annual health status from the last nonmissing round variable ----
meps01 = load_meps("h60", columns=["DUPERSID", "ANYLIM01", "RTHLTH31", "RTHLTH42", "RTHLTH53", "PERWT01F", "VARSTR01",
                                   "VARPSU01"])  # fmt: skip
meps01["MEPSHSTAT"] = (
    meps01["RTHLTH53"].where(meps01["RTHLTH53"] > 0)
    .fillna(meps01["RTHLTH42"].where(meps01["RTHLTH42"] > 0))
    .fillna(meps01["RTHLTH31"].where(meps01["RTHLTH31"] > 0))
)  # fmt: skip
print(meps01.head(60).to_string(index=False))
hs = ["MEPSHSTAT", "RTHLTH53", "RTHLTH42", "RTHLTH31"]
fmt = meps01.assign(**{c: apply_format(meps01[c], HSTAT, missing=".") for c in hs})
print(crosstab_list(fmt, ["MEPSHSTAT"]).to_string(index=False))
print(crosstab_list(fmt, hs).to_string(index=False), "\n")


def read_fixed(path, spec):
    """INFILE/INPUT with column positions: spec = {name: (start, end, is_char)} (1-based, inclusive)."""
    colspecs = [(s - 1, e) for s, e, _ in spec.values()]
    dtype = {k: str for k, (_, _, c) in spec.items() if c}
    return pd.read_fwf(path, colspecs=colspecs, names=list(spec), dtype=dtype, header=None)


link = read_fixed(files["lnk"], {"DUPERSID": (1, 8, True), "HHX": (9, 14, True), "PX": (15, 16, True),
                                 "LINKFLAG": (17, 17, False), "SRVY_YR": (19, 22, False)}).sort_values("DUPERSID")  # fmt: skip
print(link.head(60).to_string(index=False))
print(crosstab_list(link, ["LINKFLAG"]).to_string(index=False))
print(crosstab_list(link, ["SRVY_YR"]).to_string(index=False), "\n")

# NHIS persons: NHISLIM = NHIS Any Limitation; NHISCHRON = NHIS Lim'n/Chronic
# Status; NHISHSTAT = NHIS Health Status
nhis_spec = {"SRVY_YR": (3, 6, False), "HHX": (7, 12, True), "PX": (15, 16, True), "SEX": (18, 18, False),
             "AGE": (19, 20, False)}  # fmt: skip
nhis99 = read_fixed(files["n99"], nhis_spec | {"NHISLIM": (120, 120, False), "NHISCHRON": (563, 563, False),
                                               "NHISHSTAT": (564, 564, False)})  # fmt: skip
nhis00 = read_fixed(files["n00"], nhis_spec | {"NHISLIM": (123, 123, False), "NHISCHRON": (566, 566, False),
                                               "NHISHSTAT": (567, 567, False)})  # fmt: skip
key = ["HHX", "PX", "SRVY_YR"]
nhis = pd.concat([nhis99, nhis00]).sort_values(key, kind="stable")

mepslink = meps01.drop(columns=["RTHLTH31", "RTHLTH42", "RTHLTH53"]).merge(
    link.drop(columns="LINKFLAG"), on="DUPERSID", how="inner"
)
total01 = mepslink.merge(nhis, on=key, how="inner")
print(total01.head(60).to_string(index=False), "\n")

fmts = {"ANYLIM01": ANYLIM, "MEPSHSTAT": HSTAT, "SEX": SEX, "NHISLIM": NHISLIM, "NHISCHRON": NHISCHRON, "NHISHSTAT": HSTAT}
f01 = total01.assign(**{c: apply_format(total01[c], f, missing=".") for c, f in fmts.items()})
for c in fmts:
    print(crosstab_list(f01, [c]).to_string(index=False))
print(total01["AGE"].agg(["count", lambda s: s.isna().sum(), "min", "max"]).round(0), "\n")

# Weighted (PERWT01F) comparisons of NHIS vs MEPS status
for a, b in (("NHISLIM", "ANYLIM01"), ("NHISHSTAT", "MEPSHSTAT")):
    tab = f01.groupby([a, b])["PERWT01F"].sum().rename("FREQUENCY").reset_index()
    tab["PERCENT"] = 100 * tab["FREQUENCY"] / tab["FREQUENCY"].sum()
    print(tab.to_string(index=False), "\n")
