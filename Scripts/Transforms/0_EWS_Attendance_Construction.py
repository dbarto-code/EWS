###############################################################################
#  Attendance Cleaning (SY24-25)
#  Peter Li - tiange.li@ped.nm.gov, 
#  Created: 4/21/2025; Last Update: 4/22/2025
###############################################################################

###############################################################################
#### Contents
# This script extracts attendance data from NOVA for the EWS. Warning codes 
# 1, 2, and 8 are filtered from the dataset. Grade level OS is recoded as -3,
# 2U as -2, PK as -1, and KF as 0.
# 
#
#### 
# Error Notes from NOVA:
# Warning 1: Days Enrolled is less than or equal to zero or Null. 
# (Derived from the StudentSchoolAssociation Resource based on Entry Date and Exit Date)
# Check the CalendarDates to make sure dates have been identified as Instructional Days
# Also check Warning 6 as it might impact this warning.
#
# Warning 2: Days Present is less than or equal to zero. (Derived from the Days 
# Enrolled - (Days Absent Unexcused + Days Absent Excused))
#
# Warning 3: Days Present and/or Days Enrolled may be higher than expected 
# for the reporting period — Moderate is > 55 days and Very High is > 65 days.
#
# Warning 4: Days Present is higher than Days Enrolled.
#
# Warning 5: Days Absent Unexcused is higher than Derived Total Days Absent 
# [Days Absent Unexcused + Days Absent Excused].
#
# Warning 6: The StudentSchoolAssociation record is missing a calendarCode. 
# CalendarCode should match the calendar of the school assigned to.
#
# Warning 8: Days Enrolled is Null. (Derived from the StudentSchoolAssociation 
# Resource based on Entry Date and Exit Date)
#
# Warning 9: Current Grade Level (StudentSchoolAssociation Resource) is out of School Grade Range.
#
# Warning 15: Day Attenance Rate = 100% (If a stundent has not absents this data
# would be correct and warning can be ignored.)
#
####
# Req. files: 
# Student Grades and Section Verification.csv - from NOVA Report
# State Courses By Course ID.csv - from PED
# gradreq_coursecode.csv - from PED
#
####
# Output: 
# '5. CLEAN/SY24-25_Attendance.csv'
# '5. CLEAN/SY24-25_Attendance_sub.csv' Just 9-12 Grades
###############################################################################
### Setup

import os
import pandas as pd

### WD 
wd = 'C:/Users/Tiange.Li/OneDrive - State of New Mexico/Desktop/EWS/Workspace/'
os.chdir(wd)

attend40 = pd.read_csv('1. RAW/Attend/40D_SY24-25_Daily_Attendance_Data_Review_Report.csv').assign(Snap="40D")
attend80 = pd.read_csv('1. RAW/Attend/80D_SY24-25_Daily_Attendance_Data_Review_Report.csv').assign(Snap="80D")
attend120 = pd.read_csv('1. RAW/Attend/120D_SY24-25_Daily_Attendance_Data_Review_Report.csv').assign(Snap="120D") 
# attendEOY = pd.read_csv('1. RAW/Attend/EOY_SY24-25_Daily_Attendance_Data_Review_Report.csv').assign(Snap="EOY") 

grades = [9, 10, 11, 12]

###############################################################################
### Append and Clean Selected Vars

attendance = pd.concat([attend40, attend80, attend120], ignore_index = True).drop(
    columns = ['EducationOrganizationId_District1', 'EducationOrganizationId_School1',
               'GradeRange', 'CalendarCode', 'Date_Enrolled', 'Date_Withdrew',
               'Day_Attendance_Rate']) # add EOY when available

# ### Some value checks 
# check_active = attendance['ActiveOnSnapshot'].value_counts()
# check_record = attendance['Show_Record'].value_counts()
# check_grades = attendance['CurrentGradeLvl'].value_counts()
# check_errors = attendance['Warnings_General_Attendance'].value_counts()
# check_errors2 = attendance['Warnings_Daily_Attendance'].value_counts()

### Fix Grades
def convert_student_grade(CurrentGradeLvl):
    if pd.isna(CurrentGradeLvl):
        return None
    if CurrentGradeLvl == 'OS':
        return -3
    if CurrentGradeLvl == '2U':
        return -2
    if CurrentGradeLvl == 'PK':
        return -1
    if CurrentGradeLvl == 'KF':
        return 0
    try:
        return int(CurrentGradeLvl.lstrip('0'))  
    except:
        return None  

attendance['Grades_temp'] = attendance['CurrentGradeLvl'].astype(str).str.strip()
attendance['StudentGradeNumeric'] = attendance['Grades_temp'].apply(convert_student_grade) 

### Create SchoolCode
attendance['SchoolCode'] = (attendance['DistrictCode'] * 1000) + attendance['LocationCode']

### Filter errors 
# Removing Warnings 1, 2, 8

attendance['Warnings_General_Attendance'] = attendance['Warnings_General_Attendance'].astype(str).str.strip().str.lower()
attendance = attendance[~attendance["Warnings_General_Attendance"].str.startswith(('*8'))]
attendance['Warnings_Daily_Attendance'] = attendance['Warnings_Daily_Attendance'].astype(str).str.strip().str.lower()
attendance = attendance[~attendance['Warnings_Daily_Attendance'].str.startswith(('*1', '*2'))]

### Rename and Drop Temp Vars
att = attendance.drop(
    columns={
        'Warnings_General_Attendance', 'Warnings_Daily_Attendance',
        'Grades_temp', 'ReportingPeriodSchoolInstructionalDays', 'Show_Record',
        'ActiveOnSnapshot'
        }
    ).rename(
        columns={
            'StudentUniqueId': 'StudentId', 
            'CurrentGradeLvl': 'StudentGradeRaw',
            'School_Name': 'LocationName',
            'District_Name': 'DistrictName',
            
            'Textbox6': 'DaysAbsentSnap', # this is exc + unexc days summed
            'Days_Present': 'DaysPresentSnap',
            'Days_Enrolled': 'DaysEnrollSnap',
            'Days_Absent_Unexcused': 'DaysUnexcusedSnap',
            'Days_Absent_Excused': 'DaysExcusedSnap'
            }
    )

###############################################################################
### Calculate Attendance Rates 
att['SnapsAccounted'] = att.groupby('StudentId')['StudentId'].transform('count')
# check_counts = attendance['SnapsAccounted'].value_counts()

### Days Attended/Absent on Year
att['DaysPresentYear'] = att.groupby('StudentId')['DaysPresentSnap'].transform('sum')
att['DaysEnrollYear'] = att.groupby('StudentId')['DaysEnrollSnap'].transform('sum')
att['DaysAbsYear'] = att.groupby('StudentId')['DaysAbsentSnap'].transform('sum')
att['DaysExcYear'] = att.groupby('StudentId')['DaysExcusedSnap'].transform('sum')
att['DaysUnexcYear'] = att.groupby('StudentId')['DaysUnexcusedSnap'].transform('sum')

att['AttRateYear'] = att['DaysPresentYear']/att['DaysEnrollYear']



###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################



