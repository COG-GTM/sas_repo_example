"""Python port of SAS/workshop_exercises/exercise_5a/Exercise5a.sas

DESCRIPTION:  THIS PROGRAM ILLUSTRATES HOW TO CONSTRUCT FAMILY-LEVEL VARIABLES FROM PERSON-LEVEL DATA

              THERE ARE TWO DEFINITIONS OF FAMILY UNIT IN MEPS.
                 1) CPS FAMILY:  ID IS DUID + CPSFAMID.  CORRESPONDING WEIGHT IS FAMWT15C.
                 2) MEPS FAMILY: ID IS DUID + FAMIDYR.   CORRESPONDING WEIGHT IS FAMWT15F.

              THE CPS FAMILY IS USED IN THIS EXERCISE.

INPUT FILE:   <MEPS_DIR>/SAS/DATA/H181.SAS7BDAT (2015 FY PUF DATA)

;
OPTIONS LS=132 PS=79 NODATE FORMCHAR="|----|+|---+=|-//<>*" PAGENO=1;
FILENAME MYLOG "U:/MEPS/SAS/Exercise_3/Exercise3_log.TXT";
FILENAME MYPRINT "U:/MEPS/SAS/Exercise_3/Exercise3_OUTPUT.TXT";
PROC PRINTTO LOG=MYLOG PRINT=MYPRINT NEW;
RUN;
LIBNAME CDATA '<MEPS_DIR>/SAS/DATA';
LIBNAME CDATA "//programs.ahrq.local/programs/meps/AHRQ4_CY2/B_CFACT/BJ001DVK/Workshop_2017/SAS/Data";

TITLE1 '2018 AHRQ MEPS DATA USERS WORKSHOP';
TITLE2 "EXERCISE3.SAS: CALCULATE FAMILY-LEVEL ESTIMATES";

TITLE3 "SAMPLE DUMP FOR FAMILY IDS";
PROC PRINT DATA=CDATA.H181 (OBS=20);
  VAR PID AGE15X SEX CPSFAMID FAMWT15C FAMIDYR FAMWT15F;
  BY DUID;
RUN;

PROC SORT DATA=CDATA.H181 (KEEP=DUPERSID DUID CPSFAMID FAMWT15C VARSTR VARPSU TOTSLF15 TTLP15X)
              OUT=PERS;
  BY DUID CPSFAMID;
RUN;

DATA PERS2
     FAM (KEEP=DUID CPSFAMID FAMSIZE FAMOOP FAMINC);
 SET PERS;
  BY DUID CPSFAMID;

     LABEL FAMSIZE = '# OF PERSONS PER CPS FAMILY'
           FAMOOP  = 'TOTAL OUT-OF-POCKET EXP (TOTSLF15) PER CPS FAMILY'
           FAMINC  = 'TOTAL INCOME (TTLP15X) PER CPS FAMILY';

     IF FIRST.CPSFAMID THEN DO;
        FAMSIZE = 0 ;
        FAMOOP  = 0 ;
        FAMINC  = 0 ;
     END;

     FAMSIZE + 1        ;
     FAMOOP  + TOTSLF15 ;
     FAMINC  + TTLP15X  ;

     OUTPUT PERS2;
     IF LAST.CPSFAMID THEN OUTPUT FAM;
RUN;
TITLE3 "A SAMPLE DUMP TO CHECK THE CREATION OF THE FAMILY-LEVEL VARIABLES";
PROC PRINT DATA=PERS2 (OBS=20);
  BY DUID CPSFAMID;
RUN;

ADD WEIGHT, VARSTR, AND VARPSU TO THE FAMILY-LEVEL ANALYTIC DATA

<MEPS_DIR> is the folder set by the MEPS_DIR environment variable (default C:/MEPS).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(next(p for p in Path(__file__).resolve().parents if (p / "meps_utils.py").exists())))
from meps_utils import *  # noqa: E402,F401,F403

# Person records sorted by family (SAS: PROC SORT DATA=H181 OUT=PERS; BY DUID CPSFAMID)
PERS = read_meps("h181", ["DUPERSID", "DUID", "CPSFAMID", "FAMWT15C", "VARSTR", "VARPSU", "TOTSLF15", "TTLP15X"])
PERS = PERS.sort_values(["DUID", "CPSFAMID"], kind="stable")

# Family-level sums (SAS: FIRST./LAST. accumulation -> groupby().agg())
FAM = PERS.groupby(["DUID", "CPSFAMID"]).agg(FAMSIZE=("DUPERSID", "size"), FAMOOP=("TOTSLF15", "sum"),
                                             FAMINC=("TTLP15X", "sum")).reset_index()

# Family weight and design variables (NODUPKEY), then inner merge (IF AA AND BB)
FAMWT = PERS[PERS.FAMWT15C > 0].drop_duplicates(["DUID", "CPSFAMID"])[["DUID", "CPSFAMID", "FAMWT15C", "VARSTR", "VARPSU"]]
FAM2 = FAM.merge(FAMWT, on=["DUID", "CPSFAMID"], how="inner")

show("Family-level estimates (CPS families), 2015",
     surveymeans(FAM2, ["FAMSIZE", "FAMOOP", "FAMINC"], weight="FAMWT15C", strata="VARSTR", cluster="VARPSU")
     [["VarName", "N", "SumWgt", "Mean", "StdErr"]])
