"""
SAS PROC FORMAT Equivalents as Python Dictionaries

This module contains all format mappings used across the MEPS analysis files,
converted from SAS PROC FORMAT VALUE statements to Python dictionaries.

Usage:
    from utils.format_mappings import POVERTY_FORMAT, INSURANCE_FORMAT
    # Apply with PySpark:
    #   df = df.replace(POVERTY_FORMAT, subset=["POVCAT17"])
    # Or use F.when() chains for more complex mappings.
"""

# ============================================================================
# Poverty status categories (POVCAT)
# From: SAS/summary_tables_examples/care_access_2017.sas
# ============================================================================
POVERTY_FORMAT = {
    1: "1 Negative or poor",
    2: "2 Near-poor",
    3: "3 Low income",
    4: "4 Middle Income",
    5: "5 High Income",
}

# ============================================================================
# Insurance coverage categories (INSCOV)
# From: SAS/summary_tables_examples/care_access_2017.sas
# ============================================================================
INSURANCE_FORMAT = {
    1: "<65, Any private",
    2: "<65, Public only",
    3: "<65, Uninsured",
    4: "65+, Medicare only",
    5: "65+, Medicare and private",
    6: "65+, Medicare and other public",
    7: "65+, No medicare",
    8: "65+, No medicare",
}

# ============================================================================
# Insurance coverage simplified (INSCOV - 3 categories)
# From: SAS/workshop_exercises/exercise_6a/Exercise6.sas
# ============================================================================
INSCOV_3CAT_FORMAT = {
    1: "Any Private",
    2: "Public Only",
    3: "Uninsured",
}

INSCOV18_FORMAT = {
    1: "Any Private",
    2: "Public Only",
    3: "Uninsured",
}

INSCOV20_FORMAT = {
    1: "1. any private",
    2: "2. public only",
    3: "3. uninsured",
}

# ============================================================================
# Race/ethnicity categories (RACETHX)
# From: SAS/summary_tables_examples/use_race_sex_2016.sas
# ============================================================================
RACE_FORMAT = {
    1: "Hispanic",
    2: "NH White only",
    3: "NH Black only",
    4: "NH Asian only",
    5: "NH Other Race Including Multiple Races",
}

RACETHX_FORMAT = {
    1: "Hispanic",
    2: "NH White only",
    3: "NH Black only",
    4: "NH Asian only",
    5: "NH Other etc",
}

RACETHX_5B_FORMAT = {
    1: "1 HISPANIC",
    2: "2 WHITE",
    3: "3 BLACK",
    4: "4 ASIAN",
    5: "5 OTHER RACE",
}

# ============================================================================
# Race/ethnicity (older format, 4 categories)
# From: SAS/older_exercises_1996_to_2006/Misc_examples/M1/M1.sas
# ============================================================================
RACETHNB_FORMAT = {
    0: "TOTAL",
    1: "HISPANIC",
    2: "BLACK-NO OTH RACE/NOT HISPANIC",
    3: "ASIAN-NO OTH RACE/NOT HISPANIC",
    4: "OTHER/NOT HISPANIC",
}

# ============================================================================
# Sex categories
# From: SAS/summary_tables_examples/use_race_sex_2016.sas
# ============================================================================
SEX_FORMAT = {
    1: "Male",
    2: "Female",
}

SEX_6B_FORMAT = {
    1: "1. male",
    2: "2. female",
}

# ============================================================================
# Age group categories
# From: SAS/workshop_exercises/exercise_1a/Exercise1a.sas
# ============================================================================
AGE_GRP_FORMAT = {
    1: "Under 65",
    2: "65+",
}

# From: SAS/workshop_exercises/exercise_6a/Exercise6.sas
AGE_3CAT_FORMAT = {
    1: "18-34",
    2: "35-64",
    3: "65+",
}

# From: SAS/older_exercises_1996_to_2006/Estimation_examples/E1/E1.sas
AGE_E1_FORMAT = {
    1: "0-64",
    2: "65-90",
}

# From: SAS/older_exercises_1996_to_2006/Estimation_examples/E2/E2.sas
AGE_E2_FORMAT = {
    1: "AGE 0-5",
    2: "OTHER",
}

# ============================================================================
# Diabetes A1C level categories
# From: SAS/summary_tables_examples/care_diabetes_a1c_2016.sas
# ============================================================================
A1C_FORMAT = {
    1: "A1c < 7.0%",
    2: "7.0% <= A1c < 8.0%",
    3: "8.0% <= A1c < 9.0%",
    96: "A1c >= 9.0%",
}

# ============================================================================
# Quality of care - scheduling difficulty
# From: SAS/summary_tables_examples/care_quality_2016.sas
# ============================================================================
FREQ_FORMAT = {
    1: "Always",
    2: "Usually",
    3: "Sometimes/Never",
    4: "Sometimes/Never",
}

# ============================================================================
# Insurance age categories
# From: SAS/summary_tables_examples/ins_age_2016.sas
# ============================================================================
INS_AGE_FORMAT = {
    1: "<65, Any private",
    2: "<65, Public only",
    3: "<65, Uninsured",
}

# ============================================================================
# Event types
# From: SAS/summary_tables_examples/use_events_2016.sas
# ============================================================================
EVENT_TYPE_FORMAT = {
    1: "Prescribed medicines",
    2: "Dental visits",
    3: "Office-based visits",
    4: "Emergency room visits",
    5: "Outpatient visits",
    6: "Inpatient stays",
    7: "Home health",
    8: "Other",
}

# ============================================================================
# Region categories
# From: SAS/workshop_exercises/exercise_6b/exercise6.sas
# ============================================================================
REGION_FORMAT = {
    1: "1. northeast",
    2: "2. midwest",
    3: "3. south",
    4: "4. west",
}

# ============================================================================
# Flu shot format
# From: SAS/workshop_exercises/exercise_6a/Exercise6.sas
# ============================================================================
ADFLST42_FORMAT = {
    -15: "Cann't be computed",
    -1: "Inapplicable",
    1: "Yes",
    0: "No",
    2: "No",
}

# ============================================================================
# Health status
# From: SAS/older_exercises_1996_to_2006/Employment_examples/EM1/EM1.sas
# ============================================================================
HEALTH_STATUS_FORMAT = {
    1: "1 EXCELLENT",
    2: "2 VERY GOOD",
    3: "3 GOOD",
    4: "4 FAIR",
    5: "5 POOR",
}

# ============================================================================
# Quartile format
# From: SAS/older_exercises_1996_to_2006/Employment_examples/EM1/EM1.sas
# ============================================================================
QUARTILE_FORMAT = {
    1: "   1 LOWEST ",
    2: "   2        ",
    3: "   3        ",
    4: "   4 HIGHEST",
}

# ============================================================================
# Job subtype format
# From: SAS/older_exercises_1996_to_2006/Linking_examples/L1/L1.sas
# ============================================================================
JOB_SUBTYPE_FORMAT = {
    1: "1 Current Main",
    2: "2 Current Miscellaneous",
    3: "3 Former Main",
    4: "4 Former Miscellaneous",
    5: "5 Last Job Outside Rn",
    6: "6 Retirement",
}

# ============================================================================
# Family size format
# From: SAS/older_exercises_1996_to_2006/Estimation_examples/E4/E4.sas
# ============================================================================
FAMILY_SIZE_FORMAT = {
    1: "1",
    2: "2",
    3: "3",
    4: "4",
    # 5+ mapped via F.when() in code
}

# ============================================================================
# Therapeutic class (TC1) - top classes
# From: SAS/summary_tables_examples/pmed_therapeutic_class_2016.sas
# ============================================================================
TC1_TOP_CLASSES = {
    "ANTIHYPERLIPIDEMIC AGENTS": "Antihyperlipidemic agents",
    "ANALGESICS": "Analgesics",
    "ANTIDIABETIC AGENTS": "Antidiabetic agents",
    "ANTIHYPERTENSIVE AGENTS": "Antihypertensive agents",
    "BETA-ADRENERGIC BLOCKING AGENTS": "Beta-adrenergic blocking agents",
    "PROTON PUMP INHIBITORS": "Proton pump inhibitors",
    "ANTIDEPRESSANTS": "Antidepressants",
    "ANXIOLYTICS, SEDATIVES, AND HYPNOTICS": "Anxiolytics, sedatives, and hypnotics",
    "ANTICONVULSANTS": "Anticonvulsants",
    "OPHTHALMIC PREPARATIONS": "Ophthalmic preparations",
}
