"""Performance benchmarking utilities for MEPS PySpark Migration.

Captures wall clock time, memory usage, row counts, and file sizes
for before/after comparison of SAS/R/Stata vs PySpark implementations.
"""

import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class StageMetric:
    """Metrics captured at a single transformation stage."""

    stage_name: str
    row_count: int = 0
    wall_clock_seconds: float = 0.0
    memory_mb: float = 0.0


@dataclass
class JobBenchmark:
    """Complete benchmark for a single migration job."""

    job_name: str
    complexity_tier: str
    original_script: str
    input_files: List[str] = field(default_factory=list)
    input_file_sizes_mb: Dict[str, float] = field(default_factory=dict)
    output_file_size_mb: float = 0.0
    num_join_operations: int = 0
    total_wall_clock_before: float = 0.0
    total_wall_clock_after: float = 0.0
    peak_memory_before_mb: float = 0.0
    peak_memory_after_mb: float = 0.0
    stages: List[StageMetric] = field(default_factory=list)
    output_row_count: int = 0


def get_file_size_mb(path: str) -> float:
    """Get file or directory size in MB.

    Args:
        path: Path to file or directory.

    Returns:
        Size in megabytes.
    """
    if not os.path.exists(path):
        return 0.0
    if os.path.isfile(path):
        return os.path.getsize(path) / (1024 * 1024)
    total = 0
    for dirpath, _dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total += os.path.getsize(fp)
    return total / (1024 * 1024)


def time_execution(func, *args, **kwargs):
    """Time a function execution and return (result, elapsed_seconds).

    Args:
        func: Callable to time.
        *args: Positional arguments.
        **kwargs: Keyword arguments.

    Returns:
        Tuple of (function_result, elapsed_seconds).
    """
    start = time.time()
    result = func(*args, **kwargs)
    elapsed = time.time() - start
    return result, elapsed


def create_benchmark_summary(benchmarks: List[JobBenchmark]) -> Dict:
    """Create a summary dictionary of all benchmarks.

    Args:
        benchmarks: List of JobBenchmark objects.

    Returns:
        Summary dictionary with aggregated metrics.
    """
    total_jobs = len(benchmarks)
    by_tier = {}
    for bm in benchmarks:
        tier = bm.complexity_tier
        if tier not in by_tier:
            by_tier[tier] = []
        by_tier[tier].append(bm)

    return {
        "total_jobs": total_jobs,
        "by_tier": {tier: len(jobs) for tier, jobs in by_tier.items()},
        "benchmarks": benchmarks,
    }


# Pre-defined benchmark data based on analysis of original scripts
# These represent estimated metrics from the original SAS/R/Stata implementations

ORIGINAL_BENCHMARKS: Dict[str, Dict] = {
    "exercise_1a": {
        "wall_clock_seconds": 45.0,
        "peak_memory_mb": 512.0,
        "input_size_mb": 350.0,
        "output_rows": 34655,
        "joins": 0,
    },
    "exercise_1b": {
        "wall_clock_seconds": 50.0,
        "peak_memory_mb": 520.0,
        "input_size_mb": 320.0,
        "output_rows": 35427,
        "joins": 0,
    },
    "exercise_1c": {
        "wall_clock_seconds": 48.0,
        "peak_memory_mb": 540.0,
        "input_size_mb": 380.0,
        "output_rows": 30461,
        "joins": 0,
    },
    "care_access_2019": {
        "wall_clock_seconds": 30.0,
        "peak_memory_mb": 480.0,
        "input_size_mb": 400.0,
        "output_rows": 28512,
        "joins": 0,
    },
    "ins_age_2016": {
        "wall_clock_seconds": 35.0,
        "peak_memory_mb": 450.0,
        "input_size_mb": 350.0,
        "output_rows": 34655,
        "joins": 0,
    },
    "use_expenditures_2016": {
        "wall_clock_seconds": 55.0,
        "peak_memory_mb": 560.0,
        "input_size_mb": 350.0,
        "output_rows": 34655,
        "joins": 0,
    },
    "pmed_prescribed_drug_2016": {
        "wall_clock_seconds": 120.0,
        "peak_memory_mb": 800.0,
        "input_size_mb": 180.0,
        "output_rows": 297000,
        "joins": 0,
    },
    "exercise_4a": {
        "wall_clock_seconds": 90.0,
        "peak_memory_mb": 700.0,
        "input_size_mb": 670.0,
        "output_rows": 70082,
        "joins": 0,
    },
    "exercise_5a": {
        "wall_clock_seconds": 60.0,
        "peak_memory_mb": 550.0,
        "input_size_mb": 320.0,
        "output_rows": 14500,
        "joins": 1,
    },
    "exercise_5b": {
        "wall_clock_seconds": 75.0,
        "peak_memory_mb": 650.0,
        "input_size_mb": 320.0,
        "output_rows": 35427,
        "joins": 0,
    },
    "exercise_4b_covid": {
        "wall_clock_seconds": 55.0,
        "peak_memory_mb": 580.0,
        "input_size_mb": 420.0,
        "output_rows": 27805,
        "joins": 0,
    },
    "cond_pmed_2020": {
        "wall_clock_seconds": 180.0,
        "peak_memory_mb": 1200.0,
        "input_size_mb": 850.0,
        "output_rows": 27805,
        "joins": 4,
    },
    "cond_mv_2020": {
        "wall_clock_seconds": 160.0,
        "peak_memory_mb": 1100.0,
        "input_size_mb": 850.0,
        "output_rows": 27805,
        "joins": 4,
    },
    "exercise_4d": {
        "wall_clock_seconds": 150.0,
        "peak_memory_mb": 950.0,
        "input_size_mb": 1100.0,
        "output_rows": 93000,
        "joins": 1,
    },
}
