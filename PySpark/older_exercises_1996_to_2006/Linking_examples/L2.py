"""
L2: Linking MEPS to NHIS (2001 MEPS, 1999-2000 NHIS)

This example shows how to:
  (1) Create data files from ASCII source files
  (2) Link 2001 MEPS to 1999 and 2000 NHIS
  (3) Compare persons' status in NHIS with their status in MEPS

NOTE: This example requires the NHIS-MEPS Link File which is a special
restricted-access file. The PySpark version demonstrates the merge logic
assuming the link file is available.

Migrated from: SAS/older_exercises_1996_to_2006/Linking_examples/L2/L2.sas
Input files:
  nhisper99.dat (1999 NHIS Persons), nhisper00.dat (2000 NHIS Persons),
  nhmep01x.dat (NHIS-MEPS Link File), h60.sas7bdat (2001 MEPS Persons)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from utils.data_loader import load_sas7bdat, get_spark

spark = get_spark()

# ---------------------------------------------------------------------------
# Load 2001 MEPS Full-Year File
# ---------------------------------------------------------------------------
h60 = load_sas7bdat(spark, "C:/MEPS/h60.sas7bdat")

# Construct annual health status from last nonmissing round variable
meps01 = (
    h60
    .withColumn("MEPSHSTAT",
        F.when(F.col("RTHLTH53") > 0, F.col("RTHLTH53"))
         .when(F.col("RTHLTH42") > 0, F.col("RTHLTH42"))
         .when(F.col("RTHLTH31") > 0, F.col("RTHLTH31"))
         .otherwise(F.lit(None))
    )
    .select("DUPERSID", "ANYLIM01", "MEPSHSTAT", "PERWT01F",
            "VARSTR01", "VARPSU01")
)

# ---------------------------------------------------------------------------
# Load NHIS-MEPS Link File (fixed-width ASCII)
# In practice, this would be read from the .dat file
# Here we show the schema for the link file
# ---------------------------------------------------------------------------
# link = (
#     spark.read.text("C:/MEPS/nhmep01x.dat")
#     .withColumn("DUPERSID", F.trim(F.col("value").substr(1, 8)))
#     .withColumn("HHX", F.trim(F.col("value").substr(9, 6)))
#     .withColumn("PX", F.trim(F.col("value").substr(15, 2)))
#     .withColumn("LINKFLAG", F.col("value").substr(17, 1).cast("int"))
#     .withColumn("SRVY_YR", F.col("value").substr(19, 4).cast("int"))
#     .select("DUPERSID", "HHX", "PX", "LINKFLAG", "SRVY_YR")
# )

# ---------------------------------------------------------------------------
# Load NHIS Person Files (fixed-width ASCII)
# In practice, these would be read from .dat files with column positions
# ---------------------------------------------------------------------------
# nhis99 = (
#     spark.read.text("C:/MEPS/nhisper99.dat")
#     .withColumn("SRVY_YR", F.col("value").substr(3, 4).cast("int"))
#     .withColumn("HHX", F.trim(F.col("value").substr(7, 6)))
#     .withColumn("PX", F.trim(F.col("value").substr(15, 2)))
#     .withColumn("SEX", F.col("value").substr(18, 1).cast("int"))
#     .withColumn("AGE", F.col("value").substr(19, 2).cast("int"))
#     .withColumn("NHISLIM", F.col("value").substr(120, 1).cast("int"))
#     .withColumn("NHISCHRON", F.col("value").substr(563, 1).cast("int"))
#     .withColumn("NHISHSTAT", F.col("value").substr(564, 1).cast("int"))
#     .select("SRVY_YR", "HHX", "PX", "SEX", "AGE", "NHISLIM",
#             "NHISCHRON", "NHISHSTAT")
# )

# ---------------------------------------------------------------------------
# Merge Logic (demonstrated with placeholder - actual merge requires files)
# ---------------------------------------------------------------------------
# Step 1: Merge Link File with MEPS -> MEPSLINK
# mepslink = (
#     meps01
#     .drop("RTHLTH31", "RTHLTH42", "RTHLTH53")
#     .join(link.drop("LINKFLAG"), on="DUPERSID", how="inner")
# )

# Step 2: Combine 1999 & 2000 NHIS files
# nhis = nhis99.unionByName(nhis00)

# Step 3: Merge MEPSLINK with NHIS -> TOTAL01
# total01 = mepslink.join(nhis, on=["HHX", "PX", "SRVY_YR"], how="inner")

# ---------------------------------------------------------------------------
# Analysis: Compare limitation status and health status over time
# ---------------------------------------------------------------------------
# Weighted cross-tabulation of NHIS vs MEPS health status
# total01.groupBy("NHISHSTAT", "MEPSHSTAT").agg(
#     F.sum("PERWT01F").alias("Weighted_N")
# ).orderBy("NHISHSTAT", "MEPSHSTAT").show(50)

print("=== L2: NHIS-MEPS Linkage Example ===")
print("This example requires the NHIS-MEPS Link File (nhmep01x.dat)")
print("and NHIS Person Files (nhisper99.dat, nhisper00.dat).")
print("The merge logic is demonstrated above in commented code.")
print("")
print("MEPS 2001 data loaded successfully:")
meps01.show(5)

spark.stop()
