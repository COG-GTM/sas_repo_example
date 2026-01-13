"""
MEPS Workshop Exercise 3b: Expenditures for All Events Associated with Diabetes, 2015

This program analyzes expenditures for healthcare events specifically linked
to diabetes conditions using the condition-event link file.

Input files:
  - C:/MEPS/h181.ssp (2015 Full-Year Consolidated file)
  - C:/MEPS/h180.ssp (2015 Medical Conditions file)
  - C:/MEPS/h178if1.ssp (2015 Condition-Event Link file)
  - Event files (Office-based, Outpatient, ER, Inpatient, Rx)

This is the Python equivalent of the SAS program Exercise3b.sas
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.meps_loader import load_meps_data
from utils.survey_design import MEPSSurveyDesign


def main():
    # Read in data files
    fyc = load_meps_data("C:/MEPS/h181.ssp")  # 2015 FYC
    cond = load_meps_data("C:/MEPS/h180.ssp")  # 2015 Conditions
    clnk = load_meps_data("C:/MEPS/h178if1.ssp")  # 2015 CLNK
    
    # Read event files
    ob = load_meps_data("C:/MEPS/h178g.ssp")  # Office-based
    op = load_meps_data("C:/MEPS/h178f.ssp")  # Outpatient
    er = load_meps_data("C:/MEPS/h178e.ssp")  # Emergency room
    ip = load_meps_data("C:/MEPS/h178d.ssp")  # Inpatient
    rx = load_meps_data("C:/MEPS/h178a.ssp")  # Prescribed medicines
    
    # CCS codes for diabetes
    DIABETES_CCS = ['049', '050']
    
    print("=" * 60)
    print("EXPENDITURES FOR EVENTS ASSOCIATED WITH DIABETES, 2015")
    print("=" * 60)
    
    # Filter conditions to diabetes
    diabetes_cond = cond[cond['CCCODEX'].isin(DIABETES_CCS)].copy()
    print(f"\nNumber of diabetes condition records: {len(diabetes_cond):,}")
    
    # Merge conditions with CLNK to get event IDs
    diabetes_events = diabetes_cond.merge(
        clnk[['DUPERSID', 'CONDIDX', 'EVNTIDX', 'EVENTYPE']],
        on=['DUPERSID', 'CONDIDX'],
        how='inner'
    )
    
    # De-duplicate by event ID to avoid double counting
    diabetes_events = diabetes_events.drop_duplicates(subset=['DUPERSID', 'EVNTIDX'])
    
    print(f"Number of events linked to diabetes: {len(diabetes_events):,}")
    
    # Event type codes:
    # 1 = Office-based, 2 = Outpatient, 3 = ER, 4 = Inpatient, 8 = Rx
    
    # Function to calculate event expenditures
    def calc_event_exp(events_df, event_file, event_type, exp_var, event_name):
        type_events = events_df[events_df['EVENTYPE'] == event_type]
        if len(type_events) == 0:
            return 0, 0
        
        merged = type_events.merge(
            event_file[['DUPERSID', 'EVNTIDX', exp_var, 'VARSTR', 'VARPSU', 'PERWT15F']],
            on=['DUPERSID', 'EVNTIDX'],
            how='inner'
        )
        
        if len(merged) == 0:
            return 0, 0
        
        # Aggregate to person level
        person_exp = merged.groupby(
            ['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT15F']
        )[exp_var].sum().reset_index()
        person_exp.columns = ['DUPERSID', 'VARSTR', 'VARPSU', 'PERWT15F', 'exp']
        
        design = MEPSSurveyDesign(
            data=person_exp,
            strata='VARSTR',
            cluster='VARPSU',
            weights='PERWT15F'
        )
        
        result = design.total('exp')
        return result['total'].values[0], result['se'].values[0]
    
    print("\n" + "-" * 60)
    print("TOTAL EXPENDITURES BY EVENT TYPE (Events linked to diabetes)")
    print("-" * 60)
    
    # Calculate expenditures by event type
    event_types = [
        (1, ob, 'OBXP15X', 'Office-Based'),
        (2, op, 'OPXP15X', 'Outpatient'),
        (3, er, 'ERXP15X', 'Emergency Room'),
        (4, ip, 'IPXP15X', 'Inpatient'),
        (8, rx, 'RXXP15X', 'Prescribed Medicines')
    ]
    
    total_diabetes_exp = 0
    for event_type, event_file, exp_var, event_name in event_types:
        total, se = calc_event_exp(diabetes_events, event_file, event_type, exp_var, event_name)
        total_diabetes_exp += total
        print(f"{event_name:25s}: ${total:>15,.0f}  (SE: ${se:>12,.0f})")
    
    print("-" * 60)
    print(f"{'Total':25s}: ${total_diabetes_exp:>15,.0f}")


if __name__ == "__main__":
    main()
