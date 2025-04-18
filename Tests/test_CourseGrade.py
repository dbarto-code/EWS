# -*- coding: utf-8 -*-
"""
Created on Thu Apr 17 15:11:06 2025

@author: Daniel.Barto
"""


#Transform NOTES:
#This is explortary. Calibrate on one course then generalize to all
    
#Evaluate Final Period
#Evaluate Pass/Fail    
#DistrictCode + LocationCode
#Rename - LocationCode- SchoolCode


import pandas as pd
import os

#%% Dir load
def load_directory(local_dir, remote_dir):
    return local_dir if os.path.exists(local_dir) else remote_dir

local_directory = 'C:\\Users\\Daniel.Barto\\OneDrive - State of New Mexico\\PED-Accountability Data Storage - Data\\'
remote_directory = 'OTHER'

main_dir = load_directory(local_directory, remote_directory)

#Data Load
datadir = main_dir + '5. CLEAN\\'
datafile = 'Grades_Long_sub.csv'

df = pd.read_csv(datadir+datafile)

#%% Filter to Unq Student/Course

targetcourse = 'Algebra II'
df_course = df[df['CourseName'] == targetcourse]

#Drop to unique students (keeping last for now)
df_course_dedup = df_course.drop_duplicates(subset='StudentID',keep='last')

#Grade Distribution 
## TODO: Clean up grade eval in transform script
allgrades = df_course_dedup['Letter'].value_counts()
df_course_dedup['Result'] = df_course_dedup['Letter'].apply(lambda x: 'Fail' if x == 'F' else 'Pass')
passfail  =   df_course_dedup['Result'].value_counts()


#%% District Coverage Test
## TODO : Clean up District code index

df_course_dedup['DistrictFudge'] = df_course_dedup['LocationCode'].astype(str).apply(
    lambda x: x[:1] if len(x) == 4 else x[:2] if len(x) == 5 else x[:3] if len(x) == 6 else None
)


# Step 1–2: Create DistrictFudge and convert to int
df_course_dedup['DistrictFudge'] = df_course_dedup['LocationCode'].astype(str).apply(
    lambda x: int(x[:1]) if len(x) == 4 else
              int(x[:2]) if len(x) == 5 else
              int(x[:3]) if len(x) == 6 else None
)

# Step 3–4: Value count + sort descending numerically
district_counts = df_course_dedup['DistrictFudge'].value_counts().sort_index(ascending=False)


freq_dist = df_course_dedup['DistrictFudge'].value_counts()


