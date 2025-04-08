#GRAD Rate Aggregation and Calculations 8.5.2021
#daniel.barto2@state.nm.us


import pandas as pd
import numpy as np
from datetime import datetime


#Select year here!!!!!
yr = "Four_Year"
#yr = "Five_Year"
#yr = "Six_Year"


#path = "C:\\Users\\Daniel.Barto\\Desktop\\GRAD_Folder\\postjune_14\\postjune25\\Final\\"
#rosterpath = "C:\\Users\\Daniel.Barto\\Desktop\\2023 Cohort\\Data\\Data Review Revision Versions\\4_Final_Rosters\\"



path = 'C:\\Users\\Daniel.Barto\\Desktop\\Grad_process\\data\out\\'
#file = 'zONErow_4year_1.22.25_UNIUQE.csv'  #prior to appeals

#file = 'zONErow_4year_4.1.25_UNIUQE_MANUALCORRECTIONS.csv'
file = "zONErow_4year_4.1.25_UNIUQE_MANUALCORRECTIONS_ELFIX.csv" #Debug

dfUS_path = 'C:\\Users\\Daniel.Barto\\OneDrive - State of New Mexico\\2023-24 Accountability Cycle\\2023-24 AMD and DSRC\\Base File, Entities List\\'
usfileNAME = 'LongitID Crosswalk SY 2023-24 20241021 SH.xlsx'

cohortgradYEAR = 'CohortGrad_4YR'
cohortYEAR = 'Cohort_4YR'


#MAKE CHANGES TO THAT FILE!!!!!

#DO 5th and 6th year

#DOUBLE CHECK FILE VERSION
if yr == "Four_Year":
    #filename = path + "4_Year_Outcomes_PostCorrections_23_SY24_withdemos,V.2024.06.5b.xlsx"
    filename = path + file
    outfileName = path+"\\outRATE\\" + yr + "cohort2024_SY24-25_embargo_4.3.25_db"

# if yr == "Five_Year":
#     filename = rosterpath + "5_Year_Outcomes_PostCorrections_22_SY24,V.11_1_24db.csv"
#     outfileName = rosterpath+"\\out\\" + yr + "cohort2022_SY2024_11.01.24_DB"

# if yr == "Six_Year":
#     filename = rosterpath + "6_Year_Outcomes_PostCorrections_21_SY24,V.09_30_24db.csv"
#     outfileName = rosterpath+"\\out\\" + yr + "cohort2021_SY2024_9.30.24_DB"

dfORG = pd.read_csv(filename, header=0)
print("Reading in DataFile:        " + filename)
print ("Based on filename the cohort year is set to:      ", yr)

#Cottonwood fix ughhhh
dfORG.loc[(dfORG['SchoolName'] == 'COTTONWOOD CLASSICAL PREP') &  (dfORG['DistrictCode'] == 502) & 
    (dfORG['SchoolCode'] == 1) & (dfORG['LocationCode'] == 769), 'LocationCode'] = 1

#VISTA Grande FIXX UGHGHG
dfORG.loc[(dfORG['SchoolName'] == 'VISTA GRANDE HIGH SCHOOL') &  (dfORG['DistrictCode'] == 585) & 
    (dfORG['SchoolCode'] == 1) & (dfORG['LocationCode'] == 12), 'LocationCode'] = 1




## join up our freshman academies. Note "SchNumb" will be old id, "schnumb" is used for aggregation
dfORG.loc[dfORG["SchNumb"] == 12056, ["SchNumb", "DistrictCode", "LocationCode"]] = [12036, 12, 36]
dfORG.loc[dfORG["SchNumb"] == 22187, ["SchNumb", "DistrictCode", "LocationCode"]] = [22014, 22, 14]
dfORG.loc[dfORG["SchNumb"] == 31083, ["SchNumb", "DistrictCode", "LocationCode"]] = [31081, 31, 81]
dfORG.loc[dfORG["SchNumb"] == 33055, ["SchNumb", "DistrictCode", "LocationCode"]] = [33058, 33, 58]
dfORG.loc[dfORG["SchNumb"] == 41079, ["SchNumb", "DistrictCode", "LocationCode"]] = [41080, 41, 80]

          

print("Reading in Schools File:      " + dfUS_path + usfileNAME)
df_us = pd.read_excel(dfUS_path+usfileNAME,header=0)
#Drop any school with blank AIGID number
#df_us = df_us[df_us['AGAID'].notnull()]


## Merge to obtain school name and district names ##
#Prep input file with schnumber
dfORG['schnumb'] = (dfORG['DistrictCode'] * 1000) + dfORG['LocationCode']
#Keep Relevant Columns for universial schools
#msClean = df_us[['SY', 'AGAID','SchNumb','DistName','SchName','DistCode','HS','HiGrade','Feeder','Level','SchType']]
#msClean = df_us[['SY', 'LongitID','SchNumb2022','District','School','DistCode','HS','Type']]
msClean = df_us[['SY', 'LongitID','SchNumb2024','District','School','DistCode','HS','Type']]



msClean = msClean.rename(columns={'LongitID':"AGAID", "SchNumb2024":"SchNumb", "District":"DistName","School":"SchName"})




#Drop central office workaround that distrups AGAID
indexCO = msClean[ msClean['Type'] == 'Central Office' ].index
msClean.drop(indexCO , inplace=True)

#Drop Private Schools ugh
indexPR = msClean[ msClean['Type'] == 'Private' ].index
msClean.drop(indexPR , inplace=True)
#Remove duplicates in Universial Schools only keeping last entry
msClean.drop_duplicates(subset="SchNumb",inplace=True, keep='last')
#Sort by year, this ensures that when final file write we use most current AGID-school number
msClean.sort_values(by=['SY'],ascending=False, inplace=True)
#Merge on School Number
df = dfORG.merge(msClean, how="inner", left_on=['schnumb'], right_on=['SchNumb'])


## Define Schools to be Removed from Dataset ##
#schDEL=[1040, 1048, 4001,12030,17019,17035,17038,19006,31001,33059,65003,65008,68003,68025,71009]
schDEL=[]








#Now we will define schools that have closed, will then drop them. Note this evalulation is on Schnumber
#We will also remove those ghost entries of students assigned to old schools that have changed numbers; will retain newest number
#if yr=="Four_Year":
    #schDEL=[1040,1592,42010,89195,91008,94015,513001,517001,1007,1992,42003,91004,91006,
            #579001, 522001, 502001, 553001, 561001, 514001, 43097, 506001, 91010,1611,76001, 71009, 71026, 71164,]
    #schDEL=[]
#if yr=="Five_Year":
 #schDEL = [1592,1974,1992,65003,91001,91002,91004,91007,91011,562001,572001,1611,20002,45173,80100,91003,578001,27136,64003,91006,91010,
           #52001, 502001, 1707, 553001, 508001, 506001, 561001, 522001]
 #schDEL = []
#if yr=="Six_Year":
 #schDEL = [1592,1769,27136,62990,80100,91004,91008,91010,1781,12081,42002,68025,91001,91005,91011,517001,1707,74002,99005,508001, 1749, 1752, 578001, 1708, 1753]
 #schDEL = []

#Here is where we define our special codes
idx_DistOffice = df[df['LocationCode'] == 0].index #district office code  #Disable?
#idx_Hospit = df[df['LocationCode'] == 993].index  #Hospitalized
idx_Private = df[df['LocationCode'] == 997].index  #Private code
#idx_Homebound = df[df['LocationCode'] == 998].index  #Homebound
idx_Homeschool = df[df['LocationCode'] == 999].index  #Home School
idx_VictoryChristan = df[df['LocationCode'] == 188].index  #Location id for VC
idxSch = df[df['schnumb'].isin(schDEL)].index

# Now we drop schools defined above. Comment out here to refrain from dropping
df.drop(idx_DistOffice, inplace=True)
#df.drop(idx_Hospit, inplace=True)
df.drop(idx_Private, inplace=True)
#df.drop(idx_Homebound, inplace=True)
df.drop(idx_Homeschool, inplace=True)
df.drop(idx_VictoryChristan, inplace=True)
df.drop(idxSch, inplace=True)

print ("Total Number of Records Removed Prior to Calculation: ", len(dfORG) - len(df) )
print ("Reduction of ", round(100-len(df)/len(dfORG)*100, 3),'%')
print ("See SUPPLEMENTAL file for for more info")

#Here is our check to ascertain amount of records removed based on school code drops
#note some students such as those with 997 (aka Misc-Private) location code were dropped with universal school merge)
merged = df.merge(dfORG, indicator=True, how='outer')
df_studsDropped = merged[merged['_merge'] == 'right_only'] #Will write this out to supplemental when done




## Calculate Total Outcome Codes ##
count = df['Outcome'].value_counts() #Save this to out on sheet for state
#Set our N/A outcomes to a different file for further review
na_free = df.dropna(subset=['Outcome'])
df_studsUnknown = df[~df.index.isin(na_free.index)]
df=na_free #Reset our N/A free dataframe assignment for continuity


#Drop NAN districsts
df = df.dropna(subset=['DistrictCode'])


### Graduation Calculation ###

#Graduated code set CHANGE WC Due to COVID ###
#From outcome files: \\172.30.5.107\Assessment and Accountability\Accountability\Graduation\Graduation Outcome Codes_Descriptions.xlsx
gradYES = ['WG','G']
gradNO  = ['WC','E1','E2','E3','R1','R2','R3','W1','W2','W3','W4','W5','W7','W9','W11','W12','W13','W14','W15','W16','W17','W18','W21','W23','W24', 'WDO'] #note eval actually is just for WG
gradEXCL= ['D1', 'D2','D3','W6','W8','W10','W81','WD','FX','']

#Graduation Status based on Code
df['Excluded']=np.where(df["Outcome"].isin(gradEXCL), 1, 0)
df['Graduated']=np.where(df["Outcome"].isin(gradYES), 1, 0)
#df['Graduated']=np.where(df["Outcome"].isin(gradNO), 0, "Other") #don't need this will just assume not WG or WC = not graduated, and set our exlcuded students aside

#Record our Excluded students for further review
excluded = df[df['Excluded'] ==1]
#Remove them from our graduated dataframe
df.drop(excluded.index , inplace=True) #Changed this to keep them in dataset and eval on gradyes / gradyes + gradno #Later we will add them back into our state roster

#Calculate school contribution
df['SchoolWT'] = (df['NumSnapshots']/df['TotalSnapshots'])
#Calculate Graduation Contribution
df['SchNUMER'] = ((df['Graduated'] * df['SchoolWT' ]))
# Calculate School Contribution of Grads+NonGra
df['SchDENOM'] = (df['SchoolWT'])


### Aggretation and Slicing ###

##We first will get unique values
unqDist = np.unique(df['DistrictCode'])
unqSCH  = np.unique(df['schnumb'])

#quick clean up for unformmated values and last second add of multirace demographic
df['Economically_Disadvantage_Status'] = df['Economically_Disadvantage_Status'].replace('', 'N/S').fillna('N/S')
#df['AnyBlack'] = df.apply(lambda row: 'Y' if 'B' in (row['Race1'], row['Race1'],row['Race3'], row['Race5']) else 'N', axis=1)
#df['AnyNative'] = df.apply(lambda row: 'Y' if 'I' in (row['Race1'], row['Race1'],row['Race3'],row['Race5']) else 'N', axis=1)


##Now we create method to scan for demographics
def groupscan(df_Subset):
    # Get all Students first
    idx_ALL = df_Subset.index.values
    # Index by group membership
    try: idx_Male = df_Subset.groupby(df['Student_Gender']).groups['Male']
    except KeyError: idx_Male = []  # Catch if no group memebers present

    try: idx_Fema = df_Subset.groupby(df['Student_Gender']).groups['Female']
    except KeyError: idx_Fema = []

    try: idx_Black = df_Subset.groupby(df['Ethnicity']).groups['Black or African American']
    except KeyError: idx_Black = []

    try: idx_White = df_Subset.groupby(df['Ethnicity']).groups['Caucasian']
    except KeyError: idx_White = []

    try: idx_Asian = df_Subset.groupby(df['Ethnicity']).groups['Asian']
    except KeyError: idx_Asian = []

    try: idx_Hispan = df_Subset.groupby(df['Ethnicity']).groups['Hispanic']
    except KeyError: idx_Hispan = []

    try: idx_Native = df_Subset.groupby(df['Ethnicity']).groups['American Indian/Alaskan Native']
    except KeyError: idx_Native = []

    try: idx_ELL = df_Subset.groupby(df['ELL_Program_Eligibility_Code']).groups['Y'] #for grad cohor tportal
 #   try: idx_ELL = df_Subset.groupby(df['ELL']).groups['Y'] #for soap
    except KeyError: idx_ELL = []

    try: idx_FRL = df_Subset.groupby(df['FRL']).groups['Y']
    except KeyError: idx_FRL = []

    try: idx_IEP = df_Subset.groupby(df['Special_Ed_Status']).groups['Y'] #for grad cohort portal
    #try: idx_IEP = df_Subset.groupby(df['SWD']).groups['Y'] #For SOAP
    except KeyError: idx_IEP = []

    try: idx_Homeless = df_Subset.groupby(df['Homeless_Status_Code']).groups['Y']
    except KeyError: idx_Homeless = []

    #try: idx_Fostercare = df_Subset.groupby(df['FC']).groups['Y'] #for SOAP
    try: idx_Fostercare = df_Subset.groupby(df['Fostercare']).groups['Y'] #for grad cohort portal
    except KeyError: idx_Fostercare = []

    try: idx_Migrant = df_Subset.groupby(df['Migrant_Status_Code']).groups['Y']
    except KeyError: idx_Migrant = []

    try: idx_EcoDis = df_Subset.groupby(df['Economically_Disadvantage_Status']).groups['Y']
    except KeyError: idx_EcoDis = []

    try: idx_Miltary = df_Subset.groupby(df['MILITARY_FAMILY_CD1']).groups['Y']
    except KeyError: idx_Miltary = []

    try: idx_Gifted = df_Subset.groupby(df['Gifted_Participation_Code']).groups['Y']
    except KeyError: idx_Gifted = []
    
    

#not frl
    try: idx_notFRL = df_Subset.groupby(df['FRL']).groups['N']
    except KeyError: idx_notFRL = []
#not direct
    try: idx_notDirect = df_Subset.groupby(df['Economically_Disadvantage_Status']).groups['N/S']
    except KeyError: idx_notDirect = []
#not SWD
    try: idx_notSWD = df_Subset.groupby(df['EverIEP']).groups['N']
    except KeyError: idx_notSWD = []
#not EL
    try:
        idx_notELL = df_Subset.groupby(df['EverELL']).groups['N']  # for grad cohor tportal
    except KeyError:
        idx_notELL = []
#multi
    try:
        idx_Multi = df_Subset.groupby(df['Multi']).groups[1]  # for grad cohor tportal
    except KeyError:
        idx_Multi = []
#any native
    try:
        idx_AnyNative = df_Subset.groupby(df['AnyNative']).groups['Y']  # for grad cohor tportal
    except KeyError:
        idx_AnyNative = []
#any black
    try:
        idx_anyBlack = df_Subset.groupby(df['AnyBlack']).groups['Y']  # for grad cohor tportal
    except KeyError:
        idx_anyBlack = []
        
    try: idx_MISSING = df_Subset.groupby(df['Ethnicity']).groups['MISSING']
    except KeyError: 
        idx_MISSING = []


    # create vector of indicies #note not including multirace; append in same way
    idx_VECTOR = [idx_ALL, idx_Male, idx_Fema, idx_Black, idx_White, idx_Asian, idx_Hispan, idx_Native, idx_ELL, idx_FRL, idx_IEP,idx_Homeless, idx_Fostercare,idx_Migrant,idx_EcoDis,idx_Miltary,idx_Gifted,idx_notFRL,idx_notDirect,idx_notSWD,idx_notELL,idx_Multi,idx_AnyNative,idx_anyBlack,idx_MISSING]
    idx_NAME =   ['All', 'Male', 'Female', 'African American', 'White', 'Asian', 'Hispanic', 'Native American', 'EverELL','FRL', 'IEPever', 'Homeless_Status_Code','Foster','Migrant_Status_Code','Economically_Disadvantage_Status','MILITARY_FAMILY_CD1','Gifted_Participation_Code','NotFRL','NotDirectCert','NotSwD','NotEL','Multirace','Any_NativeAmerican','Any_AfricanAmerican','DemographicDataMissing']  # Names to write out Must match idx_vector!!
    return idx_VECTOR, idx_NAME

#Method we'll use to aggregate (Note StateWide calc below is different)
def aggUP (df_Subset, v):
    with np.errstate(invalid='ignore'): #supress numpy warning of dividing by 0
        numERATOR = round(df_Subset.loc[v, 'SchNUMER'].sum(),2)
        demONATOR = round(df_Subset.loc[v, 'SchDENOM'].sum(),2)
        gradRATE = round(df_Subset.loc[v, 'SchNUMER'].sum() / df_Subset.loc[v, 'SchDENOM'].sum()*100,2)
        studN = df_Subset.loc[v, 'SchDENOM'].count()
        return numERATOR, demONATOR, gradRATE, studN

def aggUP_state (df_Subset, v):
    with np.errstate(invalid='ignore'): #supress numpy warning of dividing by 0
        #Here we divide graduates / total students (duplicates deleted in method below)
        studN = df_Subset.loc[v, 'SchDENOM'].count()
        demONATOR = studN
        numERATOR = round(df_Subset.loc[v, 'Graduated'].sum(),2)
        gradRATE = round(((numERATOR / demONATOR)*100),2)
        return numERATOR, demONATOR, gradRATE, studN

## Now We will perform our calculations to be outputted in master aggregation files
print("District Calculation...")
#Districts
dfDist_OUT = pd.DataFrame(columns=['AGAID','schnumb', 'DistrictCode', 'DistrictName', 'SchoolCode', 'SchoolName', 'Group', 'Numerator', 'Denominator', 'Total_Records', 'GradRate','SortCode']) #Master file we will appened to
for d in unqDist:
        #Extract subset of data that matches district
        df_Subset=df.loc[df['DistrictCode'] == d]
        #Generate indexes by group membership
        [idx_VECTOR, idx_NAME] = groupscan(df_Subset)
        idx=0 #Counter to move through subgroups
        #Calculate graduation rate by subgroup
        for v in idx_VECTOR:
            [numERATOR, demONATOR, gradRATE, studN] = aggUP(df_Subset, v) #Aggrgate DEBUG
            #[numERATOR, demONATOR, gradRATE, studN] = aggUP_state(df_Subset, v)  # For counts
            #Build row of values
            agg_Vector = [idx_NAME[idx], numERATOR, demONATOR, studN, gradRATE, idx]
            #Build nominal labels
            agg_Name = ['0',df_Subset['DistrictCode'].iloc[0]*1000 , df_Subset['DistrictCode'].iloc[0],  df_Subset['DistName'].iloc[0], '0' , 'DistrictWide']
            outWrite = agg_Name + agg_Vector
            #append info to the last row of our final district df
            dfDist_OUT.loc[len(dfDist_OUT)] = outWrite
            idx=idx+1 #Move to next subgroup within District

print ("School Level Calculation...")
dfSchools_OUT = pd.DataFrame(columns=['AGAID','schnumb', 'DistrictCode', 'DistrictName', 'SchoolCode', 'SchoolName', 'Group', 'Numerator', 'Denominator', 'Total_Records', 'GradRate','SortCode']) #Master file we will appened to
for s in unqSCH:
        #Extract subset of data that matches district
        #df_Subset=df.loc[df['AGAID'] == s] AIGI
        df_Subset=df.loc[df['schnumb'] == s]

        #Generate indexes by group membership
        [idx_VECTOR, idx_NAME] = groupscan(df_Subset)
        idx=0 #Counter to move through subgroups
        #Calculate for all subgroups
        for v in idx_VECTOR:
            [numERATOR, demONATOR, gradRATE, studN] = aggUP(df_Subset, v) #Aggrgate
            #[numERATOR, demONATOR, gradRATE, studN] = aggUP_state(df_Subset, v)  # For counts
            #Build row of values
            agg_Vector = [idx_NAME[idx], numERATOR, demONATOR, studN, gradRATE, idx]
            #Build nominal labels
            agg_Name = [df_Subset['AGAID'].iloc[0], df_Subset['schnumb'].iloc[0], df_Subset['DistrictCode'].iloc[0],  df_Subset['DistName'].iloc[0], df_Subset['LocationCode'].iloc[0] , df_Subset['SchName'].iloc[0]]
            outWrite = agg_Name + agg_Vector
            #append info to the last row of our final district df
            dfSchools_OUT.loc[len(dfSchools_OUT)] = outWrite
            idx=idx+1 #Move to next subgroup within School

#Final Work around to ensure AGAID corresponds to most recent year School number
#Note we are just doing this for schools because district and state don't have AGAID numbers
#get max value year for ms cleann
currYear = msClean['SY'].max()
#Drop all other years
msCurrent = msClean[msClean['SY'] ==(currYear)]
#Drop any school with blank AIGID number
msCurrent.dropna(subset = ["AGAID"], inplace=True)

#Drop duplicates, retaining only last year record of the school
msCurrent.drop_duplicates(subset="SchNumb",inplace=True, keep='last')
#merge values onto output dataframe, overriding school number
dfSchools_TEMP = dfSchools_OUT.merge(msCurrent, how="inner", left_on=['AGAID'], right_on=['AGAID'])
#Overwrite original columns with corresponding value in univerisal schools
dfSchools_TEMP["schnumb"] = dfSchools_TEMP['SchNumb']
dfSchools_TEMP["DistCode"] = dfSchools_TEMP['DistCode']
dfSchools_TEMP["DistrictName"] = dfSchools_TEMP['DistName']
dfSchools_TEMP["SchoolName"] = dfSchools_TEMP['SchName']
dfSchools_OUT = dfSchools_TEMP.drop(['SY','SchNumb','DistName', 'SchName','DistrictCode','HS'], axis=1) #Swap out just for variable naming consistency


#Add Excluded back in and Write out full roster with numertor and demo at student level before deduplication
#df_rosterOUT = df.append(excluded)
df_rosterOUT = pd.concat([df, excluded])


print("State Level Calculation...")
#State Wide
dfState_OUT = pd.DataFrame(columns=['AGAID','schnumb', 'DistrictCode', 'DistrictName', 'SchoolCode', 'SchoolName', 'Group', 'Numerator', 'Denominator', 'Total_Records', 'GradRate','SortCode']) #Dataframe we will appened to
state= [1]

# Extract all data for state
df_State = df
for i in state:
        #df_State.drop(df_State[df_State['LastLocation']=='N'].index, inplace=True)
        dupesWRITES = df_State.loc[df_State['Student_ID'].duplicated(), :]
        df_State.drop_duplicates(subset=["Student_ID"], keep="first", inplace=True, )  # actually delete them
        #df_State['LastName'] = (df_State['LastName'].str[0]) + '.'  # De-identify student names
        #df_State['FirstName'] = (df_State['FirstName'].str[0]) + '.'  ##DEBUG
        df_State.reset_index(drop=True, inplace=True)  # drop that python index
        #txtOUT=yr+"_state_roster_3.17.2020ALLFINAL.csv"
        #df_State.to_csv(txtOUT)
        #Generate indexes by group membership
        [idx_VECTOR, idx_NAME] = groupscan(df_State)
        idx=0 #Counter to move through subgroups
        #Calculate for all subgroups
        for v in idx_VECTOR:
            [numERATOR, demONATOR, gradRATE, studN] = aggUP_state(df_State, v) #Aggrgate
            #Build row of values
            agg_Vector = [idx_NAME[idx], numERATOR, demONATOR, studN, gradRATE, idx]
            #Build nominal labels
            agg_Name = ['0','999999' , '999',  'New Mexico', '999' , 'StateWide']
            outWrite = agg_Name + agg_Vector
            #append info to the last row of our final district df
            dfState_OUT.loc[len(dfState_OUT)] = outWrite
            idx=idx+1 #Move to next subgroup within State

#Combine all
df_FINAtemp = pd.concat([dfState_OUT, dfDist_OUT])
df_FINALOUT = pd.concat([df_FINAtemp,dfSchools_OUT] )

#TODO Final Checks for Duplicate School Entries:?

#Clean up Column nmaes alittle bit from the back-end
df_FINALOUT.replace(['FRL', 'IEPever', 'EverELL', 'Multi','White'],['FRL','Students with Disabilities','English Language Learner','MultiRacial','Caucasian'],inplace=True)

#Sort out rosters for easy comparison
df_State.sort_values(by="Student_ID", inplace=True)
df_rosterOUT.sort_values(by="Student_ID", inplace=True)
df_rosterOUT['Cohort'] = cohortYEAR


#Merge against lost IDS
lostpath = 'C:\\Users\\Daniel.Barto\\Desktop\\Grad_process\\data\\'
file = 'IDSlost.csv'

df_lost = pd.read_csv(lostpath + file)
df_lost['STID_in_Portal'] = 'N'
df_lost = df_lost[['StudentID','STID_in_Portal']]


df_State2 = pd.merge(df_State,df_lost, left_on = 'Student_ID', right_on='StudentID', how='left')
df_rosterOUT2 = pd.merge(df_rosterOUT,df_lost, left_on = 'Student_ID', right_on='StudentID', how='left')

df_State =  df_State
df_rosterOUT = df_rosterOUT2

#Clean up RosterFile
columns_to_drop = ['Unnamed: 0', 'SchNumb_x', 'DiplomaLastModifiedDate', 'schnumb','AGAID','DistCode', 'HS',	'Type','StudentID'
]
df_rosterOUT = df_rosterOUT.drop(columns=columns_to_drop)

remap_dict = {
    "Outcome_Portal": "Grad Portal",
    "Exit_Outcome_LC": "SOAP_Upload_LC",
    "Corrected_Outcome_APS": "SOAP_Upload_APS",
    "Outcome_Corrected_ACE": "SOAP_Upload_ACE",
    "Outcome_Corrected_EM": "SOAP_Upload_EM",
    "FlatFileImport": "SOAP_Upload",
    "Dropout_Portal": "Dropout_Portal",
    "AccountabilitySchoolEnrollmentValidation_STARS": "AccountabilitySchoolEnrollmentValidation_STARS",
    "ExitCode_FlatFileLV": "SOAP_Upload_LV",
    "EnrollmentDataValidation_NOVA": "EnrollmentDataValidation_NOVA",
    "Email": "Email"
}

# Apply the remap to the entire DataFrame
df_rosterOUT = df_rosterOUT.replace(remap_dict)
df_rosterOUT['STID_in_Portal'] = df_rosterOUT['STID_in_Portal'].fillna('Y')
df_rosterOUT[['SchoolWT', 'SchNUMER', 'SchDENOM']] = df_rosterOUT[['SchoolWT', 'SchNUMER', 'SchDENOM']].round(2)


column_order = [
    "DistrictCode", "SchoolCode", "Student_ID", 
    "FirstName", "LastSurname", "BirthDate", "Snapshot", "Grade_Level_AtSnapshot", 
    "NumSnapshots", "TotalSnapshots", "LastLocation", "Outcome", "OutcomeYear", "Outcome_Summary", 
    "ExitSource", "Excluded", "Graduated", "SchoolWT", 
    "SchNUMER", "SchDENOM", "Ethnicity", "Student_Gender", 
    "Homeless_Status_Code", "ELL_Program_Eligibility_Code", "Special_Ed_Status", 
    "MILITARY_FAMILY_CD1", "Economically_Disadvantage_Status", "STID_in_Portal", "Appealed"
]

# Reorder the columns in df_rosterOUT
df_rosterOUT = df_rosterOUT[column_order]

#REMAP to old column names:
column_mapping = {
    "DistrictCode": "DistrictCode",
    "SchoolCode": "SchoolCode",
    "DistName": "DistName",
    "SchoolName": "SchoolName",
    "Student_ID": "StudentID",
    "FirstName": "FirstName",
    "LastSurname": "LastName",
    "BirthDate": "DOB",
    "Snapshot": "Outcome_SchoolYear",
    "NumSnapshots": "SchoolSnaps",
    "TotalSnapshots": "StateSnaps",
    "LastLocation": "LastLocation",
    "Outcome": "Outcome",
    "Outcome_Summary": "Outcome_Desc",
    "Excluded": "Excluded",
    "Graduated": "Graduated",
    "SchoolWT": "SchoolFraction",
    "SchNUMER": "SchoolNumerator",
    "SchDENOM": "SchoolDenominator",
    "Ethnicity": "Ethnicity",
    "Student_Gender": "Gender",
    "Homeless_Status_Code": "Homeless",
    "ELL_Program_Eligibility_Code": "ELL",
    "Special_Ed_Status": "IEP",
    "MILITARY_FAMILY_CD1": "Military_Family_Code",
    "Economically_Disadvantage_Status": "Economically_Disadvantage_Status"
}

# Remap the column names in df_rosterOUT
df_rosterOUT = df_rosterOUT.rename(columns=column_mapping)    


# List of new columns to create with blank (empty string) values
blank_cols = [
    "Fostercare", "Migrant", "SPED_ReasonCode", "SPED_Reason", "Outcome_Unknown",
    "Entry9thGrade", "Entry10thGrade", "Entry11thGrade", "Entry12thGrade", "Gifted_Participation_Code",
    "Economic_Code",'FRL'
]

# Create the new columns in df_rosterOUT and set all values to blank (empty string)
for col in blank_cols:
    df_rosterOUT[col] = ""



#df_rosterOUT = df_rosterOUT[new_columns]

#df_rosterOUT = df_rosterOUT.drop(columns=["Cohort", "DiplomaTypeDescription"])









#round rate file to 1 digit:
df_FINALOUT['GradRate'] = df_FINALOUT['GradRate'].round(2)

df_FINALOUT['Cohort'] = cohortgradYEAR
#df_rosterOUT['Cohort'] = cohortYEAR

# # Fill NaN values in 'DistCode' with the corresponding value from 'DistrictCode'
# df_rosterOUT['DistCode'] = df_rosterOUT['DistCode'].fillna(df_rosterOUT['DistrictCode'])
# # Fill NaN values in 'DistrictCode' with the corresponding value from 'DistCode'
# df_rosterOUT['DistrictCode'] = df_rosterOUT['DistrictCode'].fillna(df_rosterOUT['DistCode'])
# Fill NaN values in 'DistCode' with the corresponding value from 'DistrictCode'
df_FINALOUT['DistCode'] = df_FINALOUT['DistCode'].fillna(df_FINALOUT['DistrictCode'])
# Fill NaN values in 'DistrictCode' with the corresponding value from 'DistCode'
df_FINALOUT['DistrictCode'] = df_FINALOUT['DistrictCode'].fillna(df_FINALOUT['DistCode'])



#DROP Demographics not there 

remove_list = [
    "FRL", "Foster", "Migrant_Status_Code", "Gifted_Participation_Code", 
    "NotFRL", "NotSwD", "NotEL", "Multirace", "Any_NativeAmerican", "Any_AfricanAmerican"
]

# Filter the dataframe by excluding rows where 'Demographic' is in the remove_list
df_FINALOUT = df_FINALOUT[~df_FINALOUT['Group'].isin(remove_list)]
columns_to_drop = ['DistCode', 'Type', 'AGAID', 'schnumb','SortCode']
df_FINALOUT = df_FINALOUT.drop(columns=columns_to_drop)





print ("Writing out Rates File...")


#Quick fix for Reni to align schema
columns_to_drop = ['Cohort']
df_FINALOUT = df_FINALOUT.drop(columns=columns_to_drop)
df_FINALOUT = df_FINALOUT.rename(columns={'GradRate':"Rate"})


#Write out rates to file (path defined at beginning of script)
pathOUT = outfileName + 'gradRATES_v11_ELLFIX.xlsx'
writerALL = pd.ExcelWriter(pathOUT, engine='xlsxwriter')
df_FINALOUT.to_excel(writerALL, sheet_name="State_District_School_Wide", index=False)
#count.to_excel(writerALL, sheet_name="Outcome Code Frequencies")
writerALL.close()

print("Writing out State Roster")
pathOUT = outfileName + "_SA_Roster_v11_ELLFIX" + '.xlsx'
writerALL = pd.ExcelWriter(pathOUT, engine='xlsxwriter')
#df_State.to_excel(writerALL, sheet_name="State.Roster(DeDuped)", index=False)
df_rosterOUT.to_excel(writerALL, sheet_name="SharedAcc.Roster(Unduped)", index=False)
writerALL.close()


print("Process Complete!!!!!!!!~~~~~~~~~~~~~~~")
print("File Written to ", pathOUT)









