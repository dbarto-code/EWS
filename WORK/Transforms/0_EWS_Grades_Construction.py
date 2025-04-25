###############################################################################
#  Grades Cleaning (SY24-25)
#  Peter Li - tiange.li@ped.nm.gov, 
#  Created: 4/16/2025; Last Update: 4/21/2025
###############################################################################

###############################################################################
### Contents
# This script extracts course and grade data from the NOVA Grades and Section 
# Verification. It also adds course title and graduation requirement status
# 
#
# Req. files: 
# Student Grades and Section Verification.csv - from NOVA Report
# State Courses By Course ID.csv - from PED
# gradreq_coursecode.csv - from PED
#
#
# Output: 
# '5. CLEAN/Grades_Long.csv'
# '5. CLEAN/Grades_Long_sub.csv' Just 9-12 Grades
###############################################################################
### Setup 

import os
import re
import pandas as pd

### WD 
wd = 'C:/Users/Tiange.Li/OneDrive - State of New Mexico/Desktop/EWS/Workspace/'
os.chdir(wd)

dat = pd.read_csv('1. RAW/Course/Student Grades and Section Verification.csv')
course_id = pd.read_csv('1. RAW/Course/State Courses By Course ID.csv')
req_courses = pd.read_csv('1. RAW/Course/gradreq_coursecode.csv') 
grades = [9, 10, 11, 12]

###############################################################################
### Extract Grades from String

courses = course_id[['Course_ID', 'Full_Course_Name', 'Course_Subject_Area']].rename(
    columns={'Course_ID': 'StateCourseCode'}
)
courses.StateCourseCode = pd.to_numeric(courses.StateCourseCode, errors = 'coerce')


sub = dat[['SchoolId', 'StudentUniqueId1', 'Grade_Level', 'StateCourseCode', 'Grades',
           'DistrictCode', 'DistrictName', 'LocationCode']]

### Patterns
re_label = re.compile(r'^\d+:(.*?)(?:\s*\()')
re_grade = re.compile(r'[a-zA-Z]:\s*["“]?([^"/]+)["”]?\s*/\s*["“]?([^"/]+)["”]?')

def grades_func(row):
    grades_string = row['Grades']
    if pd.isna(grades_string) or not grades_string.strip():
        return []

    splits = grades_string.split(';')
    seen = set()
    records = []

    for entry in splits:
        entry = entry.strip()

        label_match = re_label.search(entry)
        grade_match = re_grade.search(entry)
        
        # # Checker for failed parses
        # if not label_match or not grade_match:
        #     print("Skipped:", entry)

        if label_match and grade_match:
            period = label_match.group(1).strip()
            if period in seen:
                continue
            seen.add(period)
            grade_value = f'{grade_match.group(1)}/{grade_match.group(2)}'
            
            # Checker for successful parse
            # print('Row parsed:', {
            #     'StudentUniqueId1': row['StudentUniqueId1'],
            #     'StateCourseCode': row['StateCourseCode'],
            #     'Period': period,
            #     'Grade': grade_value
            # })
            
            records.append({
                'StudentID': row['StudentUniqueId1'],
                'StateCourseCode': row['StateCourseCode'],
                'SchoolID': row['SchoolId'],
                'DistrictCode': row['DistrictCode'],
                'DistrictName': row['DistrictName'],
                'LocationCode': row['LocationCode'],
                'StudentGrade': row['Grade_Level'],
                'Period': period,
                'Grade': grade_value
            })
            
    return records

long_records = []

for _, row in sub.iterrows():
    long_records.extend(grades_func(row))

grades_long_df = pd.DataFrame(long_records)
# print('Rows before:', len(sub))
# print('Parsed records:', len(grades_long_df))

### split into GradesL, GradesN
grades_long_df[['Letter', 'Score']] = grades_long_df['Grade'].str.split('/', expand=True)


###############################################################################
### Add Vars

### Course Names and Requirement Status 
grades_names = grades_long_df.merge(
    courses, on = 'StateCourseCode', how = 'left'
    ).merge(
    req_courses, on = 'StateCourseCode', how = 'left'
    )
        
grades_names['Requirement'] = grades_names['Requirement'].fillna(0)    

### Location Info
longit = pd.read_excel(
    '1. RAW/Other/LongitID Crosswalk SY 2023-24 20241021 SH.xlsx'
    )[['SchNumb', 'School', 'Accountability']]

longit['SchNumb'] = longit['SchNumb'].apply(str)

grades_names['SchNumb'] = grades_names['SchoolID'].apply(str).str[2:]

grades_final = grades_names.merge(
    longit, on = 'SchNumb', how = 'left'
    ).drop(
        columns='SchoolID'
    ).rename(
        columns={
            'Full_Course_Name': 'CourseName', 
            'Course_Subject_Area': 'Subject',
            'School': 'LocationName',
            'District': 'DistrictName',
            'DistCode': 'DistrictCode',
            'SchNumb': 'SchoolCode'
            }
    )
        
### Clean student grades (year)
def convert_student_grade(StudentGrade):
    if pd.isna(StudentGrade):
        return None
    if StudentGrade == 'PK':
        return -1
    if StudentGrade == 'KF':
        return 0
    try:
        return int(StudentGrade.lstrip('0'))  
    except:
        return None  

grades_final['StudentGrade_clean'] = grades_final['StudentGrade'].astype(str).str.strip()
grades_final['StudentGradeNumeric'] = grades_final['StudentGrade_clean'].apply(convert_student_grade)        


###############################################################################
### Save Full Long File 
grades_final = grades_final[['StudentID', 'StudentGradeNumeric', 'CourseName', 'Period', 
                             'Grade', 'Letter', 'Score', 'Subject', 'StateCourseCode',
                             'DistrictName', 'LocationName', 'DistrictCode', 'LocationCode',
                             'SchoolCode', 'Accountability', 'Requirement']]

grades_final.to_csv('5. CLEAN/Grades_Long.csv', index=False)  


### Subset on EWS-target grades and classes
# keep 9-12 
grades_final_sub = grades_final[grades_final['StudentGradeNumeric'].isin(grades)]

grades_final_sub.to_csv('5. CLEAN/Grades_Long_sub.csv', index=False)  
