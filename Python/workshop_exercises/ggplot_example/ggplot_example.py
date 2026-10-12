# -----------------------------------------------------------------------------
# Python port of R/workshop_exercises/ggplot_example.R (no SAS counterpart)
#
# Percentage of total health care expenditures by type of service, for all people,
# people under 65, and people 65+, 2013 (MEPS Statistical Brief style figure).
# The R example draws the figure with base graphics and ggplot2; here matplotlib.
#
# Input file: C:/MEPS/h163.ssp (2013 full-year consolidated file)
# Output:     figure1.csv and figure1.png in the current directory
# -----------------------------------------------------------------------------

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))
from meps_utils import meps_path, read_meps, survey_estimate

TOS = ["IPTEXP13", "ambexp13", "RXEXP13", "DVTEXP13", "hhexp13"]
LABELS = ["Hospital IP", "Ambulatory", "RX", "Dental", "HH and Other"]


def main(meps_dir=None, out_dir="."):
    h163 = read_meps(meps_path("h163.ssp", meps_dir))
    h163["ambexp13"] = h163["OBVEXP13"] + h163["OPTEXP13"] + h163["ERTEXP13"]
    h163["hhexp13"] = h163["HHAEXP13"] + h163["HHNEXP13"] + h163["VISEXP13"] + h163["OTHEXP13"]

    def pct_tos(subpop=None):
        return survey_estimate(h163, TOS, weight="PERWT13F", stat="ratio", denominator="TOTEXP13",
                               subpop=subpop)

    results = {
        "pct_TOS": pct_tos(),
        "pct_TOS_lt65": pct_tos(h163["AGELAST"] < 65),
        "pct_TOS_ge65": pct_tos(h163["AGELAST"] >= 65),
    }
    pct_matrix = pd.DataFrame({col: res["estimate"].to_numpy() * 100 for col, res in
                               zip(["Total", "<65 years", "65+ years"], results.values())}, index=LABELS)
    print(pct_matrix)
    pct_matrix.to_csv(Path(out_dir) / "figure1.csv")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed; skipping figure")
    else:
        ax = pct_matrix.plot.bar(rot=0, color=["blue", "yellow", "magenta"], edgecolor="black")
        ax.set_ylabel("Percentage")
        ax.set_title("Percentage of total expenditures by type of service, 2013")
        plt.tight_layout()
        plt.savefig(Path(out_dir) / "figure1.png", dpi=120)
        plt.close()
    return results


if __name__ == "__main__":
    main()
