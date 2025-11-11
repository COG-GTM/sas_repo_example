# PySpark Migration of MEPS SAS Exercises

This directory contains PySpark implementations of MEPS (Medical Expenditure Panel Survey) analysis exercises, migrated from SAS with Parquet I/O for optimal performance.

## Overview

The PySpark migration provides:
- **Parquet I/O**: Columnar storage format with ~70% compression and predicate pushdown
- **Scalability**: Distributed processing capabilities for large datasets
- **Performance**: Optimized Spark configurations for I/O operations
- **Variance Estimation**: Survey design-aware variance calculation utilities

## Directory Structure

```
pyspark_migration/
├── data_loading/
│   ├── convert_to_parquet.py    # One-time SAS to Parquet conversion
│   └── load_meps.py              # Data loading utilities
├── exercises/
│   ├── exercise_1a.py            # Exercise 1a: National health care expenses
│   └── variance_estimation.py    # Survey variance estimation utilities
├── config/
│   └── paths.py                  # File path configurations
└── README.md                     # This file
```

## Prerequisites

### Required Python Packages

```bash
pip install pyspark pandas pyreadstat scipy
```

Or if using the repository's requirements:

```bash
pip install -r requirements.txt
```

### Data Requirements

- MEPS 2016 Full-Year Consolidated file (H192) in SAS transport format (.ssp)
- Default expected location: `C:/MEPS/h192.ssp` (Windows) or `~/MEPS/h192.ssp` (Unix)

## Quick Start

### Step 1: Convert SAS Data to Parquet (One-time Setup)

```bash
python pyspark_migration/data_loading/convert_to_parquet.py \
    --input C:/MEPS/h192.ssp \
    --output C:/MEPS/parquet/h192.parquet
```

This conversion provides significant performance benefits:
- Columnar storage allows reading only required columns
- Snappy compression reduces file size by ~70%
- Predicate pushdown applies filters at storage layer

### Step 2: Run Exercise 1a Analysis

```bash
python pyspark_migration/exercises/exercise_1a.py \
    --input C:/MEPS/parquet/h192.parquet \
    --output C:/MEPS/output/exercise_1a_results.parquet \
    --variance-output C:/MEPS/output/exercise_1a_variance.parquet
```

## Exercise 1a: National Health Care Expenses (2016)

### Description

Exercise 1a analyzes 2016 MEPS data to generate estimates on national health care expenses:

1. **Overall Expenses**: Total healthcare expenditures
2. **Percentage with Expense**: Proportion of persons with any healthcare expense
3. **Mean Expense**: Average expense per person with any expense

Analysis is performed overall and stratified by age group (0-64 and 65+).

### Original SAS Implementation

The original SAS program is located at:
```
SAS/workshop_exercises/exercise_1a/Exercise1a.sas
```

### PySpark Implementation Details

#### Data Transformation

The PySpark implementation creates the following derived variables:

- `TOTAL`: Copy of `TOTEXP16` (total healthcare expenditure)
- `X_ANYSVCE`: Binary flag (1 if any expense, 0 otherwise)
- `AGE`: Consolidated age from `AGE16X`, `AGE42X`, `AGE31X`
- `age_group`: Categorical age group ('0-64', '65+', 'ALL AGES')
- `age_category`: Numeric age category (1='0-64', 2='65+')

#### Survey-Weighted Aggregations

The analysis calculates the following weighted statistics by age group:

- `total_exp`: Sum of (TOTAL × PERWT16F)
- `total_pop`: Sum of PERWT16F
- `pop_with_exp`: Sum of PERWT16F where X_ANYSVCE == 1
- `sample_size`: Count of records
- `pct_with_expense`: (pop_with_exp / total_pop) × 100
- `mean_exp_per_person_with_exp`: total_exp / pop_with_exp

#### Variance Estimation

The implementation includes variance estimation components using Taylor series linearization:

1. **PSU-level statistics**: Aggregated by stratum (VARSTR) and PSU (VARPSU)
2. **Stratum-level variance**: Calculated from PSU statistics
3. **Combined variance**: Aggregated across all strata

This approach replicates the variance estimation methodology used by SAS PROC SURVEYMEANS.

### Usage Example

```python
from pyspark_migration.exercises.exercise_1a import exercise_1a_etl
from pyspark_migration.data_loading.load_meps import get_spark_session

spark = get_spark_session("MEPS_Exercise1a")

results, variance = exercise_1a_etl(
    input_path="C:/MEPS/parquet/h192.parquet",
    output_path="C:/MEPS/output/exercise_1a_results.parquet",
    variance_output_path="C:/MEPS/output/exercise_1a_variance.parquet",
    spark=spark
)

results.show()
```

## Configuration

### Custom Paths

Edit `config/paths.py` to customize file locations:

```python
from pyspark_migration.config.paths import MEPSPaths

paths = MEPSPaths(base_dir="/custom/path/to/MEPS")
paths.ensure_directories()

input_path = paths.get_h192_parquet_path()
output_path = paths.get_exercise_1a_output_path()
```

### Spark Configuration

The implementation uses optimized Spark configurations defined in `config/paths.py`:

```python
SPARK_CONFIGS = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.autoBroadcastJoinThreshold": "10485760",  # 10MB
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.files.maxPartitionBytes": "128MB",
}
```

These can be customized based on your cluster size and data volume.

## Variance Estimation Utilities

The `variance_estimation.py` module provides reusable functions for survey variance estimation:

### Key Functions

- `calculate_psu_statistics()`: Calculate PSU-level statistics
- `calculate_stratum_statistics()`: Calculate stratum-level variance components
- `calculate_taylor_series_variance()`: Primary variance estimation using Taylor series linearization
- `combine_stratum_variances()`: Combine variance estimates across strata
- `calculate_coefficient_of_variation()`: Calculate CV for estimates
- `calculate_confidence_interval()`: Calculate confidence intervals
- `calculate_design_effect()`: Calculate design effect (DEFF)
- `calculate_effective_sample_size()`: Calculate effective sample size

### Usage Example

```python
from pyspark_migration.exercises.variance_estimation import (
    calculate_taylor_series_variance,
    combine_stratum_variances
)

stratum_var = calculate_taylor_series_variance(
    df,
    stratum_var="VARSTR",
    psu_var="VARPSU",
    weight_var="PERWT16F",
    analysis_var="TOTAL",
    group_vars=["age_group"]
)

combined_var = combine_stratum_variances(stratum_var, group_vars=["age_group"])
combined_var.show()
```

## Performance Considerations

### Parquet Benefits

1. **Columnar Storage**: Read only required columns, reducing I/O
2. **Compression**: Snappy compression reduces file size by ~70%
3. **Predicate Pushdown**: Filters applied at storage layer
4. **Schema Evolution**: Easy to add/modify columns

### Optimization Tips

1. **Column Pruning**: Always select only required columns
2. **Partition Data**: For very large datasets, partition by year or other logical divisions
3. **Cache Intermediate Results**: Use `.cache()` for DataFrames used multiple times
4. **Broadcast Small Tables**: Use broadcast joins for small lookup tables
5. **Adjust Shuffle Partitions**: Tune `spark.sql.shuffle.partitions` based on data size

## Comparison with SAS

### Advantages of PySpark Implementation

1. **Scalability**: Can process datasets larger than memory
2. **Performance**: Parallel processing across multiple cores/nodes
3. **Cost**: Open-source, no licensing fees
4. **Integration**: Easy integration with Python data science ecosystem
5. **Cloud-Ready**: Native support for cloud storage (S3, Azure Blob, GCS)

### Limitations

1. **Survey Procedures**: PySpark lacks built-in survey analysis procedures like SAS SURVEYMEANS
2. **Variance Estimation**: Requires custom implementation of variance estimation methods
3. **Learning Curve**: Requires understanding of distributed computing concepts

### Hybrid Approach

For complex survey analyses requiring sophisticated variance estimation, consider a hybrid approach:

1. Use PySpark for data transformation and aggregation
2. Export aggregated results to pandas/Python
3. Use specialized survey libraries (e.g., `statsmodels`, `samplics`) for variance estimation

## Survey Design Variables

MEPS uses a complex survey design with:

- **VARSTR**: Stratification variable (variance estimation stratum)
- **VARPSU**: Primary Sampling Unit (PSU) for clustering
- **PERWT16F**: Person weight for 2016 (full-year weight)

These variables must be used in variance estimation to produce correct standard errors and confidence intervals.

## Troubleshooting

### Common Issues

1. **Memory Errors**: Increase Spark driver/executor memory
   ```python
   spark = SparkSession.builder \
       .config("spark.driver.memory", "8g") \
       .config("spark.executor.memory", "8g") \
       .getOrCreate()
   ```

2. **Slow Performance**: Adjust partition count
   ```python
   spark.conf.set("spark.sql.shuffle.partitions", "400")
   ```

3. **Import Errors**: Ensure all packages are installed and PYTHONPATH is set correctly

## Additional Resources

- [MEPS Website](https://meps.ahrq.gov/)
- [MEPS Data Documentation](https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Parquet Format](https://parquet.apache.org/)

## Contributing

When adding new exercises:

1. Follow the existing code structure and naming conventions
2. Include comprehensive docstrings
3. Add variance estimation components where applicable
4. Update this README with new exercise documentation

## License

This code is provided for educational and research purposes. MEPS data is public domain.
