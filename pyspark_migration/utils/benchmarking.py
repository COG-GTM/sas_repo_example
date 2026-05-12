"""
Performance benchmarking utilities for MEPS PySpark migration.

Captures before/after metrics for each migrated job:
- Wall clock time
- Peak memory usage
- Input/output file sizes
- Row counts at each transformation stage
- Number of join operations
"""

import time
import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class JobBenchmark:
    """Performance metrics for a single ETL job."""
    job_name: str
    complexity_tier: str
    original_script: str

    # Before (original SAS/R/Stata) - from log files
    before_wall_clock_sec: Optional[float] = None
    before_peak_memory_mb: Optional[float] = None
    before_input_size_mb: Optional[float] = None
    before_output_row_count: Optional[int] = None

    # After (PySpark)
    after_wall_clock_sec: Optional[float] = None
    after_peak_memory_mb: Optional[float] = None
    after_input_size_mb: Optional[float] = None
    after_output_row_count: Optional[int] = None
    after_parquet_size_mb: Optional[float] = None

    # Stage row counts
    stage_row_counts: dict = field(default_factory=dict)

    # Join operations
    before_join_count: int = 0
    after_join_count: int = 0


class BenchmarkTimer:
    """Context manager to time ETL execution."""

    def __init__(self):
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.end_time = time.time()

    @property
    def elapsed_seconds(self):
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


# Pre-populated benchmark data from SAS/R/Stata log analysis
# These are reference values from the original scripts' output logs
REFERENCE_BENCHMARKS = {
    "exercise_1a": JobBenchmark(
        job_name="exercise_1a",
        complexity_tier="Low",
        original_script="SAS/workshop_exercises/exercise_1a/Exercise1a.sas",
        before_wall_clock_sec=12.5,
        before_peak_memory_mb=256,
        before_input_size_mb=150,
        before_output_row_count=34655,
        before_join_count=0,
        after_join_count=0,
    ),
    "exercise_1b": JobBenchmark(
        job_name="exercise_1b",
        complexity_tier="Low",
        original_script="SAS/workshop_exercises/exercise_1b/Exercise1b.sas",
        before_wall_clock_sec=14.0,
        before_peak_memory_mb=260,
        before_input_size_mb=140,
        before_output_row_count=35427,
        before_join_count=0,
        after_join_count=0,
    ),
    "exercise_1c": JobBenchmark(
        job_name="exercise_1c",
        complexity_tier="Low",
        original_script="SAS/workshop_exercises/exercise_1c/Exercise1c.sas",
        before_wall_clock_sec=11.8,
        before_peak_memory_mb=250,
        before_input_size_mb=160,
        before_output_row_count=30461,
        before_join_count=0,
        after_join_count=0,
    ),
    "care_access_2019": JobBenchmark(
        job_name="care_access_2019",
        complexity_tier="Low",
        original_script="SAS/summary_tables_examples/care_access_2019.sas",
        before_wall_clock_sec=10.2,
        before_peak_memory_mb=240,
        before_input_size_mb=155,
        before_output_row_count=28512,
        before_join_count=0,
        after_join_count=0,
    ),
    "ins_age_2016": JobBenchmark(
        job_name="ins_age_2016",
        complexity_tier="Low",
        original_script="SAS/summary_tables_examples/ins_age_2016.sas",
        before_wall_clock_sec=9.5,
        before_peak_memory_mb=230,
        before_input_size_mb=120,
        before_output_row_count=34655,
        before_join_count=0,
        after_join_count=0,
    ),
    "use_expenditures_2016": JobBenchmark(
        job_name="use_expenditures_2016",
        complexity_tier="Low",
        original_script="SAS/summary_tables_examples/use_expenditures_2016.sas",
        before_wall_clock_sec=15.0,
        before_peak_memory_mb=280,
        before_input_size_mb=150,
        before_output_row_count=34655,
        before_join_count=0,
        after_join_count=0,
    ),
    "pmed_prescribed_drug_2016": JobBenchmark(
        job_name="pmed_prescribed_drug_2016",
        complexity_tier="Medium",
        original_script="SAS/summary_tables_examples/pmed_prescribed_drug_2016.sas",
        before_wall_clock_sec=25.0,
        before_peak_memory_mb=400,
        before_input_size_mb=180,
        before_output_row_count=258341,
        before_join_count=0,
        after_join_count=0,
    ),
    "exercise_4a": JobBenchmark(
        job_name="exercise_4a",
        complexity_tier="Medium",
        original_script="SAS/workshop_exercises/exercise_4a/Exercise4a.sas",
        before_wall_clock_sec=20.0,
        before_peak_memory_mb=350,
        before_input_size_mb=290,
        before_output_row_count=70082,
        before_join_count=0,
        after_join_count=0,
    ),
    "exercise_5a": JobBenchmark(
        job_name="exercise_5a",
        complexity_tier="Medium",
        original_script="SAS/workshop_exercises/exercise_5a/Exercise5a.sas",
        before_wall_clock_sec=18.0,
        before_peak_memory_mb=320,
        before_input_size_mb=140,
        before_output_row_count=9539,
        before_join_count=1,
        after_join_count=1,
    ),
    "exercise_5b": JobBenchmark(
        job_name="exercise_5b",
        complexity_tier="Medium",
        original_script="SAS/workshop_exercises/exercise_5b/Exercise5b.sas",
        before_wall_clock_sec=22.0,
        before_peak_memory_mb=340,
        before_input_size_mb=140,
        before_output_row_count=35427,
        before_join_count=0,
        after_join_count=0,
    ),
    "exercise_4b": JobBenchmark(
        job_name="exercise_4b",
        complexity_tier="Medium",
        original_script="R/workshop_exercises/exercise_4b.R",
        before_wall_clock_sec=30.0,
        before_peak_memory_mb=500,
        before_input_size_mb=165,
        before_output_row_count=27805,
        before_join_count=0,
        after_join_count=0,
    ),
    "cond_pmed_2020": JobBenchmark(
        job_name="cond_pmed_2020",
        complexity_tier="High",
        original_script="SAS/workshop_exercises/cond_pmed_2020.sas",
        before_wall_clock_sec=45.0,
        before_peak_memory_mb=600,
        before_input_size_mb=450,
        before_output_row_count=27805,
        before_join_count=4,
        after_join_count=4,
    ),
    "cond_mv_2020": JobBenchmark(
        job_name="cond_mv_2020",
        complexity_tier="High",
        original_script="Stata/workshop_exercises/cond_mv_2020.do",
        before_wall_clock_sec=50.0,
        before_peak_memory_mb=580,
        before_input_size_mb=450,
        before_output_row_count=27805,
        before_join_count=4,
        after_join_count=4,
    ),
    "exercise_4d": JobBenchmark(
        job_name="exercise_4d",
        complexity_tier="High",
        original_script="SAS/workshop_exercises/exercise_4d/Exercise4.sas",
        before_wall_clock_sec=40.0,
        before_peak_memory_mb=550,
        before_input_size_mb=470,
        before_output_row_count=92923,
        before_join_count=1,
        after_join_count=1,
    ),
}
