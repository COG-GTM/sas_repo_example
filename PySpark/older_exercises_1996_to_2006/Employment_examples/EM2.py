"""
EM2: Job Changes Among Employed Persons (2002 Data)

This example shows how to use the 2002 MEPS Jobs file (HC-063) to determine
how many persons working at the start of the year changed jobs. It links the
Jobs file to the Full-Year file to identify persons who had a job on or before
January 1, 2002 and who either added or stopped a job during the year.

Migrated from: SAS/older_exercises_1996_to_2006/Employment_examples/EM2/EM2.sas
Input files:
  h63.sas7bdat (2002 MEPS Jobs File)
  h62.sas7bdat (2002 MEPS Full-Year File)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from pyspark.sql import functions as F
from utils.data_loader import load_sas7bdat, get_spark

spark = get_spark()

# ---------------------------------------------------------------------------
# Load data files
# ---------------------------------------------------------------------------
h63 = load_sas7bdat(spark, "C:/MEPS/h63.sas7bdat")  # 2002 Jobs File
h62 = load_sas7bdat(spark, "C:/MEPS/h62.sas7bdat")  # 2002 Full-Year File

# ---------------------------------------------------------------------------
# Identify persons who had a job on or before January 1, 2002
# Panel 6/Round 3 or Panel 7/Round 1, subtype 1-4, job started before 2002
# ---------------------------------------------------------------------------
empstart_pop = (
    h63
    .filter(
        (((F.col("PANEL") == 6) & (F.col("RN") == 3)) |
         ((F.col("PANEL") == 7) & (F.col("RN") == 1))) &
        (F.col("SUBTYPE").isin(1, 2, 3, 4)) &
        ((F.col("JSTRTY") < 2002) |
         ((F.col("JSTRTM") == 1) & (F.col("JSTRTD") == 1) & (F.col("JSTRTY") == 2002)))
    )
    .select("DUPERSID")
    .dropDuplicates(["DUPERSID"])
)

# ---------------------------------------------------------------------------
# Identify persons who changed/added jobs during 2002
# Subtype 3,4 (former jobs) or subtype 1,2 with start date in 2002 (not Jan 1)
# ---------------------------------------------------------------------------
h63_sorted = h63.select("DUPERSID", "SUBTYPE", "JSTRTM", "JSTRTD", "JSTRTY", "JOBSIDX")

chngjob_pop = (
    h63_sorted
    .join(empstart_pop, on="DUPERSID", how="inner")
    .filter(
        (F.col("SUBTYPE").isin(3, 4)) |
        ((F.col("SUBTYPE").isin(1, 2)) &
         (F.col("JSTRTY") == 2002) &
         ~((F.col("JSTRTD") == 1) & (F.col("JSTRTM") == 1)))
    )
    .select("DUPERSID")
    .dropDuplicates(["DUPERSID"])
)

# ---------------------------------------------------------------------------
# Get person characteristics from Full-Year file (adults 18+)
# ---------------------------------------------------------------------------
hc62 = (
    h62
    .withColumn("AGE",
        F.when(F.col("AGE02X") >= 0, F.col("AGE02X"))
         .when(F.col("AGE42X") >= 0, F.col("AGE42X"))
         .when(F.col("AGE31X") >= 0, F.col("AGE31X"))
         .otherwise(-1)
    )
    .filter(F.col("AGE") >= 18)
    .select("DUPERSID", "AGE", "PERWT02P")
)

# ---------------------------------------------------------------------------
# Create combined dataset with employment flags
# ---------------------------------------------------------------------------
chnginfo = (
    hc62
    .join(empstart_pop.withColumn("EMPSTART", F.lit("YES")),
          on="DUPERSID", how="left")
    .join(chngjob_pop.withColumn("CHNGJOB", F.lit("YES")),
          on="DUPERSID", how="left")
    .fillna({"EMPSTART": "NO", "CHNGJOB": "NO"})
)

# Age category
chnginfo = chnginfo.withColumn("AGE_label",
    F.when((F.col("AGE") >= 18) & (F.col("AGE") <= 44), "18-44")
     .when((F.col("AGE") >= 45) & (F.col("AGE") <= 64), "45-64")
     .when((F.col("AGE") >= 65) & (F.col("AGE") <= 85), "65-85")
     .otherwise("Other")
)

# ---------------------------------------------------------------------------
# QC: Unweighted frequencies
# ---------------------------------------------------------------------------
print("=== Unweighted Frequencies ===")
print("\nEmployment at Start:")
chnginfo.groupBy("EMPSTART").count().show()

print("Job Change:")
chnginfo.groupBy("CHNGJOB").count().show()

print("Employment Start x Job Change:")
chnginfo.groupBy("EMPSTART", "CHNGJOB").count().orderBy(
    "EMPSTART", "CHNGJOB"
).show()

# ---------------------------------------------------------------------------
# Among persons with employment at start, percent who changed jobs
# ---------------------------------------------------------------------------
print("=== Among Employed at Start: Percent Who Changed Jobs ===")
emp_start = chnginfo.filter(F.col("EMPSTART") == "YES")

# Unweighted
print("Unweighted:")
emp_start.groupBy("CHNGJOB").count().show()

# Weighted
print("Weighted:")
emp_start.groupBy("CHNGJOB").agg(
    F.sum("PERWT02P").alias("Weighted_N")
).show()

spark.stop()
