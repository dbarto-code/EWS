# -*- coding: utf-8 -*-
"""
Created on Thu Apr 17 15:11:06 2025

@author: Daniel.Barto
"""


#Transform NOTES:

    
#Evaluate Final Period
#Evaluate Pass/Fail    



import pandas as pd
import os

#%% Dir load
def load_directory(local_dir, remote_dir):
    return local_dir if os.path.exists(local_dir) else remote_dir

local_directory = 'C:\\Users\\Daniel.Barto\\Documents\\GitHub\EWS\\'
remote_directory = 'OTHER'

main_dir = load_directory(local_directory, remote_directory)

#Data Load
datadir = main_dir + 'Data\\OUT\\'
datafile = 'Grades_Long_sub 1.csv'

df = pd.read_csv(datadir+datafile)

#%%
df_alg1 = df[df['CourseName'] == 'Algebra I']
df_alg2 = df[df['CourseName'] == 'Algebra II']
