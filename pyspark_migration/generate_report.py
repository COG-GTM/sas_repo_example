"""
Generate the MEPS PySpark Migration Report (Word document).

Produces MEPS_PySpark_Migration_Report.docx with 6 sections:
1. Executive Summary
2. Migration Architecture
3. Job Inventory
4. Test Artifacts
5. Performance Benchmarking
6. Known Limitations & Risks

Usage:
    python -m pyspark_migration.generate_report [--test-results pytest_results.xml]
"""

import os
import sys
import subprocess
import xml.etree.ElementTree as ET
from docx import Document
from docx.shared import Inches, Pt, Cm
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH

from pyspark_migration.utils.benchmarking import REFERENCE_BENCHMARKS


def _add_heading(doc, text, level=1):
    doc.add_heading(text, level=level)


def _add_paragraph(doc, text, bold=False):
    p = doc.add_paragraph()
    run = p.add_run(text)
    run.bold = bold
    return p


def _add_table(doc, headers, rows, col_widths=None):
    table = doc.add_table(rows=1, cols=len(headers))
    table.style = "Light Grid Accent 1"
    table.alignment = WD_TABLE_ALIGNMENT.CENTER

    # Header row
    for i, header in enumerate(headers):
        cell = table.rows[0].cells[i]
        cell.text = header
        for paragraph in cell.paragraphs:
            for run in paragraph.runs:
                run.bold = True

    # Data rows
    for row_data in rows:
        row = table.add_row()
        for i, cell_text in enumerate(row_data):
            row.cells[i].text = str(cell_text)

    return table


# ---------------------------------------------------------------------------
# Job inventory data
# ---------------------------------------------------------------------------

JOB_INVENTORY = [
    # (Job name, Original script, Tier, Input files, Key transformations, Output path)
    ("exercise_1a", "SAS/workshop_exercises/exercise_1a/Exercise1a.sas",
     "Low", "H192 (2016 FYC)",
     "Binary flag creation (X_ANYSVCE); age categorization (AGECAT)",
     "processed/exercise_1a/"),
    ("exercise_1b", "SAS/workshop_exercises/exercise_1b/Exercise1b.sas",
     "Low", "H181 (2015 FYC)",
     "Expenditure by service type; binary flags for 6 categories",
     "processed/exercise_1b/"),
    ("exercise_1c", "SAS/workshop_exercises/exercise_1c/Exercise1c.sas",
     "Low", "H209 (2018 FYC)",
     "Expense categorization; CHAR_WITH_AN_EXPENSE variable",
     "processed/exercise_1c/"),
    ("care_access_2019", "SAS/summary_tables_examples/care_access_2019.sas",
     "Low", "h216 (2019 FYC)",
     "Affordability flags (afford_MD/DN/PM/ANY); domain adjustment",
     "processed/care_access_2019/"),
    ("ins_age_2016", "SAS/summary_tables_examples/ins_age_2016.sas",
     "Low", "h192.ssp (2016 FYC)",
     "Age group binning; insurance category labeling",
     "processed/ins_age_2016/"),
    ("use_expenditures_2016", "SAS/summary_tables_examples/use_expenditures_2016.sas",
     "Low", "h192.ssp (2016 FYC)",
     "Payment source aggregation (PTR, OTZ); facility+SBD combine",
     "processed/use_expenditures_2016/"),
    ("exercise_1a_R", "R/workshop_exercises/exercise_1a.R",
     "Low", "h192.ssp (2016 FYC)",
     "Same logic as SAS Exercise1a; has_exp flag; age_cat variable",
     "processed/exercise_1a/"),
    ("use_expenditures_2016_R", "R/summary_tables_examples/use_expenditures_2016.R",
     "Low", "h192.ssp (2016 FYC)",
     "Same logic as SAS use_expenditures_2016",
     "processed/use_expenditures_2016/"),
    ("pmed_prescribed_drug_2016", "SAS/summary_tables_examples/pmed_prescribed_drug_2016.sas",
     "Medium", "h188a (2016 PMED)",
     "Person-by-drug aggregation via groupBy; fill count + expenditure sum",
     "processed/pmed_drug_2016/"),
    ("exercise_4a", "SAS/workshop_exercises/exercise_4a/Exercise4a.sas",
     "Medium", "H181 (2015), H192 (2016)",
     "Year-specific rename; union; POOLWT = PERWT/2; SUBPOP flag",
     "processed/exercise_4a/"),
    ("exercise_5a", "SAS/workshop_exercises/exercise_5a/Exercise5a.sas",
     "Medium", "H181 (2015 FYC)",
     "Person-to-family aggregation (FAMSIZE, FAMOOP, FAMINC)",
     "processed/exercise_5a/"),
    ("exercise_5b", "SAS/workshop_exercises/exercise_5b/Exercise5b.sas",
     "Medium", "H181 (2015 FYC)",
     "Monthly insurance counting (12 months x N types); flag creation",
     "processed/exercise_5b/"),
    ("exercise_4b", "R/workshop_exercises/exercise_4b.R",
     "Medium", "h224.dta (2020 FYC)",
     "COVID delay outcome 1/2->1/0 conversion; subpop flags",
     "processed/exercise_4b/"),
    ("cond_pmed_2020", "SAS/workshop_exercises/cond_pmed_2020.sas",
     "High", "h220a, h222, h220if1, h224 (2020)",
     "4-file join chain: CCSR filter -> CLNK join -> EVNTIDX dedup -> PMED join -> person collapse -> FYC left join",
     "processed/cond_pmed_2020/"),
    ("cond_mv_2020", "Stata/workshop_exercises/cond_mv_2020.do",
     "High", "h220a, h222, h220if1, h224 (2020)",
     "Same join chain as cond_pmed_2020 (Stata equivalent)",
     "processed/cond_mv_2020/"),
    ("exercise_4d", "SAS/workshop_exercises/exercise_4d/Exercise4.sas",
     "High", "H201, H209, H216, H36U19",
     "3-year pooling; DUPERSID fix; variance file merge; SPOP + JOINT_PAIN",
     "processed/exercise_4d/"),
]


def _parse_pytest_results(xml_path):
    """Parse JUnit XML output from pytest."""
    if not os.path.exists(xml_path):
        return None, 0, 0, 0

    tree = ET.parse(xml_path)
    root = tree.getroot()

    tests = []
    total = passed = failed = 0
    for suite in root.iter("testsuite"):
        total += int(suite.get("tests", 0))
        failed += int(suite.get("failures", 0)) + int(suite.get("errors", 0))

    passed = total - failed

    for testcase in root.iter("testcase"):
        name = testcase.get("name", "unknown")
        classname = testcase.get("classname", "")
        failure = testcase.find("failure")
        status = "FAIL" if failure is not None else "PASS"
        tests.append((classname, name, status))

    return tests, total, passed, failed


def generate_report(test_xml_path=None, output_path=None):
    """Generate the MEPS PySpark Migration Report."""
    if output_path is None:
        output_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "MEPS_PySpark_Migration_Report.docx"
        )

    doc = Document()

    # Title
    title = doc.add_heading("MEPS PySpark Migration Report", level=0)
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    doc.add_paragraph("")

    # Parse test results if available
    test_data, total_tests, passed_tests, failed_tests = (None, 0, 0, 0)
    if test_xml_path:
        test_data, total_tests, passed_tests, failed_tests = _parse_pytest_results(test_xml_path)

    # -----------------------------------------------------------------------
    # Section 1: Executive Summary
    # -----------------------------------------------------------------------
    _add_heading(doc, "1. Executive Summary")

    _add_paragraph(doc,
        "This report documents the migration of MEPS (Medical Expenditure Panel Survey) "
        "ETL scripts from SAS, R, and Stata to PySpark. The migration uses a hybrid "
        "architecture where PySpark handles file ingestion, variable recoding, joins, "
        "de-duplication, and person-level aggregation, while survey-weighted estimation "
        "is delegated to Python's samplics library or R's survey package via rpy2."
    )

    doc.add_paragraph("")
    _add_paragraph(doc, "Jobs Migrated by Complexity Tier:", bold=True)

    tier_counts = {"Low": 0, "Medium": 0, "High": 0}
    for job in JOB_INVENTORY:
        tier_counts[job[2]] += 1

    _add_table(doc,
        ["Tier", "Count", "Description"],
        [
            ["Low", str(tier_counts["Low"]),
             "Single-file, simple aggregations and flag creation"],
            ["Medium", str(tier_counts["Medium"]),
             "Multi-file merges, variable construction, pooling"],
            ["High", str(tier_counts["High"]),
             "4-file join chains with de-duplication"],
            ["Total", str(sum(tier_counts.values())), ""],
        ]
    )

    if total_tests > 0:
        doc.add_paragraph("")
        _add_paragraph(doc, "Test Summary:", bold=True)
        _add_paragraph(doc,
            f"Total tests: {total_tests} | "
            f"Passed: {passed_tests} | "
            f"Failed: {failed_tests} | "
            f"Pass rate: {passed_tests/total_tests*100:.1f}%"
        )

    # -----------------------------------------------------------------------
    # Section 2: Migration Architecture
    # -----------------------------------------------------------------------
    _add_heading(doc, "2. Migration Architecture")

    _add_paragraph(doc, "Hybrid Architecture Overview:", bold=True)
    _add_paragraph(doc,
        "The migration employs a hybrid architecture because PySpark has no native "
        "equivalent to SAS PROC SURVEYMEANS, R's svydesign/svymean, or Stata's svy: prefix. "
        "Complex survey estimation requires specialized algorithms (Taylor Series "
        "Linearization) that account for the stratified, clustered design of the MEPS "
        "Household Component."
    )

    doc.add_paragraph("")
    _add_paragraph(doc, "Data Flow:", bold=True)
    flow_items = [
        "1. PySpark ETL: Read source data (SAS7BDAT, DTA, SSP/XPORT) -> Transform (recode, join, dedup, aggregate) -> Write Parquet",
        "2. Survey Estimation: Read Parquet -> Apply survey design (VARSTR, VARPSU, PERWT**F) -> Compute weighted estimates via samplics/rpy2",
        "3. Validation: Compare PySpark output estimates against golden SAS/R/Stata reference values",
    ]
    for item in flow_items:
        doc.add_paragraph(item, style="List Bullet")

    doc.add_paragraph("")
    _add_paragraph(doc, "Survey Design Variables:", bold=True)
    _add_table(doc,
        ["Variable", "Role", "Notes"],
        [
            ["VARSTR", "Stratum", "Variance estimation stratum"],
            ["VARPSU", "Cluster (PSU)", "Primary sampling unit"],
            ["PERWT**F", "Person weight", "Year-specific (e.g., PERWT16F, PERWT20F)"],
            ["POOLWT", "Pooled weight", "PERWT / N_years (for multi-year pooling)"],
            ["FAMWT**C", "Family weight", "CPS family weight (exercise_5a)"],
            ["stra9619 / psu9619", "Pooled variance", "From HC-036 file (exercise_4d)"],
        ]
    )

    _add_paragraph(doc,
        "\nCritical constraint: Every script MUST use VARSTR, VARPSU, and the "
        "year-specific weight variable together. Dropping any of these will produce "
        "biased estimates and incorrect standard errors."
    )

    # -----------------------------------------------------------------------
    # Section 3: Job Inventory
    # -----------------------------------------------------------------------
    _add_heading(doc, "3. Job Inventory")

    _add_table(doc,
        ["Job Name", "Original Script", "Tier", "Input Files",
         "Key Transformations", "Output Path"],
        [[j[0], j[1], j[2], j[3], j[4], j[5]] for j in JOB_INVENTORY]
    )

    # -----------------------------------------------------------------------
    # Section 4: Test Artifacts
    # -----------------------------------------------------------------------
    _add_heading(doc, "4. Test Artifacts")

    _add_paragraph(doc, "Test Categories:", bold=True)
    test_categories = [
        "Unit Tests: Schema validation, row count assertions, null/zero checks, de-duplication correctness",
        "Integration Tests: Intermediate dataset parity (row counts at each join checkpoint)",
        "Statistical Parity Tests: Point estimates, weighted totals/means/proportions match within tolerance",
    ]
    for cat in test_categories:
        doc.add_paragraph(cat, style="List Bullet")

    if test_data:
        doc.add_paragraph("")
        _add_paragraph(doc, "Detailed Test Results:", bold=True)

        test_rows = [(cn, tn, st) for cn, tn, st in test_data]
        _add_table(doc,
            ["Test Class", "Test Name", "Status"],
            test_rows
        )
    else:
        doc.add_paragraph("")
        _add_paragraph(doc, "Detailed Test Results:", bold=True)
        _add_paragraph(doc,
            "Test results will be populated after running: "
            "pytest pyspark_migration/tests/ --junitxml=pytest_results.xml"
        )

    doc.add_paragraph("")
    _add_paragraph(doc, "Integration Test Checkpoints (High-Complexity Jobs):", bold=True)
    _add_table(doc,
        ["Checkpoint", "What Is Asserted"],
        [
            ["After CCSR filter (hl_cond)", "Row count matches original SAS/Stata filtered count"],
            ["After CLNK join (cond_clnk)", "Row count matches post-merge count"],
            ["After EVNTIDX dedup (cond_clnk_dedup)", "Row count matches post-dedup count; no duplicate EVNTIDX"],
            ["After PMED join (hl_merged)", "Row count matches post-merge count"],
            ["After person-level collapse", "Unique DUPERSID count matches"],
            ["After FYC left join (result)", "Row count equals FYC row count (left join preserves all)"],
        ]
    )

    doc.add_paragraph("")
    _add_paragraph(doc, "Statistical Parity Tolerance:", bold=True)
    _add_table(doc,
        ["Metric", "Tolerance", "Method"],
        [
            ["Point estimates", "+/- 0.01%", "Relative error against golden SAS/R/Stata output"],
            ["Standard errors", "+/- 1%", "Relative error using Taylor Series Linearization"],
            ["Confidence intervals", "Must overlap", "95% CI from survey design"],
        ]
    )

    # -----------------------------------------------------------------------
    # Section 5: Performance Benchmarking
    # -----------------------------------------------------------------------
    _add_heading(doc, "5. Performance Benchmarking")

    _add_paragraph(doc,
        "The following table presents reference benchmarks from the original SAS/R/Stata "
        "scripts alongside PySpark performance. 'Before' metrics are derived from SAS log "
        "timestamps, R system.time(), and Stata timer output. 'After' metrics are captured "
        "using Python time.time() and Spark UI monitoring."
    )

    doc.add_paragraph("")
    bench_rows = []
    for name, bm in REFERENCE_BENCHMARKS.items():
        after_time = bm.after_wall_clock_sec or "TBD"
        after_mem = bm.after_peak_memory_mb or "TBD"
        pct_time = ""
        if bm.after_wall_clock_sec and bm.before_wall_clock_sec:
            pct = ((bm.after_wall_clock_sec - bm.before_wall_clock_sec) /
                   bm.before_wall_clock_sec * 100)
            pct_time = f"{pct:+.1f}%"

        bench_rows.append([
            bm.job_name,
            bm.complexity_tier,
            f"{bm.before_wall_clock_sec:.1f}s" if bm.before_wall_clock_sec else "N/A",
            f"{after_time}s" if isinstance(after_time, float) else after_time,
            pct_time or "TBD",
            f"{bm.before_peak_memory_mb:.0f} MB" if bm.before_peak_memory_mb else "N/A",
            f"{after_mem} MB" if isinstance(after_mem, float) else after_mem,
            f"{bm.before_input_size_mb:.0f} MB" if bm.before_input_size_mb else "N/A",
            str(bm.before_join_count),
        ])

    _add_table(doc,
        ["Job", "Tier", "Before Time", "After Time", "% Change",
         "Before Memory", "After Memory", "Input Size", "Joins"],
        bench_rows
    )

    doc.add_paragraph("")
    _add_paragraph(doc,
        "Note: PySpark 'After' metrics marked 'TBD' require execution against actual "
        "MEPS data files. Benchmark values will be updated after live data runs."
    )

    # -----------------------------------------------------------------------
    # Section 6: Known Limitations & Risks
    # -----------------------------------------------------------------------
    _add_heading(doc, "6. Known Limitations & Risks")

    limitations = [
        (
            "Year-specific variable name differences",
            "MEPS variable names change across data years (e.g., PERWT18F vs PERWT20F, "
            "INSCOV15 vs INSCOV16, JTPAIN31 vs JTPAIN31_M18). Each ETL job must use "
            "the correct year-specific suffix. The migration scripts parameterize these "
            "where possible, but adding new years requires updating variable names."
        ),
        (
            "SAS CPORT files (pre-2017 data)",
            "MEPS data files before 2017 are distributed as SAS CPORT (.ssp) transport "
            "files. These cannot be read directly into Python or PySpark. They must be "
            "pre-converted to SAS7BDAT, CSV, or Parquet format before being ingested by "
            "the PySpark pipeline. The R foreign::read.xport() function or SAS PROC XCOPY "
            "can perform this conversion."
        ),
        (
            "Pooled Linkage Variance file (HC-036)",
            "Exercise 4d pools data across the 2019 CAPI redesign boundary (2017-2019). "
            "The Pooled Linkage Variance Estimation file (HC-036, h36u19) is required to "
            "compute correct standard errors when pooling across this boundary. This file "
            "provides alternative strata (stra9619) and PSU (psu9619) variables that account "
            "for the redesign."
        ),
        (
            "Lonely PSU handling",
            "When a stratum contains only one PSU (a 'lonely PSU'), variance estimation "
            "is undefined. SAS uses PROC SURVEYMEANS default behavior, R uses "
            "options(survey.lonely.psu='adjust'), and Stata uses singleunit(centered). "
            "The samplics library handles lonely PSUs by default, but the behavior should "
            "be validated against the original outputs."
        ),
        (
            "DUPERSID format changes",
            "Before 2018, DUPERSID was 8 characters. Starting in 2018, it is 10 characters "
            "(panel number prepended). Exercise 4d handles this by concatenating the PANEL "
            "number with the old DUPERSID for 2017 data. Any new pooling jobs spanning "
            "pre/post-2018 must implement the same fix."
        ),
        (
            "De-duplication semantics",
            "The SAS 'proc sort nodupkey' and Stata 'duplicates drop, force' commands have "
            "slightly different tie-breaking behavior. PySpark's dropDuplicates() is "
            "non-deterministic in which row it keeps. For the MEPS use case, this is "
            "acceptable because the de-duplication is on EVNTIDX, and the retained row's "
            "other columns are not used after the dedup step."
        ),
        (
            "Missing data representation",
            "SAS represents missing numeric values as '.', which becomes NULL in PySpark. "
            "Stata represents missing as '.'. The PySpark ETL uses fillna() to convert "
            "these to 0 where appropriate (e.g., n_hl_fills, hl_drug_exp after FYC left "
            "join). Developers should verify null handling for any new variables."
        ),
    ]

    for title_text, desc in limitations:
        _add_paragraph(doc, title_text, bold=True)
        _add_paragraph(doc, desc)
        doc.add_paragraph("")

    # Save
    doc.save(output_path)
    return output_path


if __name__ == "__main__":
    xml_path = None
    if len(sys.argv) > 2 and sys.argv[1] == "--test-results":
        xml_path = sys.argv[2]
    path = generate_report(test_xml_path=xml_path)
    print(f"Report generated: {path}")
