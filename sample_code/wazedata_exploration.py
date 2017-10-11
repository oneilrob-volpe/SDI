# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 14:30:42 2017

@author: Matthew.Goodwin
"""

import pandas as pd
from pandas.io.json import json_normalize
import json
import os 
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pytz



"""
This file converts and flattens the WAZE json files contained in a speciifc directory into pandas dataframes.
These dataframes can then be converted into csv, xlsx, or other filetypes. Summary statistics are also
calculated for the datasets that are pulled in and printed to the console
"""

#This function goes through a specified json file, flattens the information held in the "report"
# and creates a dataframe where each attirbute from a report shows up as its own column
def df_creation_file(filename):
    #pull in json from file
    with open(filename) as json_data:
        d=json.load(json_data)
    #print (d)
   
    #Normalise json so that alert information shows up in column. Turn into data frame
    df_normalized = json_normalize(d,'alerts',['endTime','endTimeMillis','startTime','startTimeMillis'])  
    df_normalized['Lat']=df_normalized['location'].apply(lambda row: row.get('x'))
    df_normalized['Lon']=df_normalized['location'].apply(lambda row: row.get('y'))
    
    return df_normalized
 

#test for df_creation_file
#test_file = df_creation_file(filename)
    
#Uncomment to save to a csv file
#fileout= "H:/SDC/jpo-data-access-tools/AWS/MA/test.xlsx"
#df_normalized.to_csv(fileout)



#Convert all .json in state level folder into a pandas dataframed

#Set State level directory location
directory = "C:/Users/Matthew.Goodwin/Documents/SDC/jpo-data-access-tools/AWS/MA"


#This function goes through the subdirectories under the state folder (e.g. MA)
# and appends their json files into one, tidy dataframe.

def df_creation(dir):
    working_df = pd.DataFrame()
    for root, dirs, files in os.walk(dir):
        for name in files:
#            print(os.path.join(root, name))
            with open(os.path.join(root, name)) as json_data:
                data=json.load(json_data)
#                print(data)
                df_nrm = json_normalize(data,'alerts',['endTime','endTimeMillis','startTime','startTimeMillis'])
                #print(df_nrm)
                df_nrm['Lon']=df_nrm['location'].apply(lambda row: row.get('x')) #pull out Longitude from 'location' object'
                df_nrm['Lat']=df_nrm['location'].apply(lambda row: row.get('y')) #pull out Latitude from 'locations' objects
                working_df=working_df.append(df_nrm)
    return working_df

#Run function to created dataframe
df_State = df_creation(directory)

#Debug to see if dataframe columns/first two rows appear as expected
#test = df_State.head(2)
##################################################################################################

#The following functions recode or transform exisiting variables for easier vizulaization/summary

#Convert pubMillis to seconds from Epoch
def timeconvert(time):
    newtime= datetime.datetime.fromtimestamp(time/1000.0,pytz.utc)
    return newtime

#pubMillis are in UTC. Need to convert to local timezone

to_zone = pytz.timezone('America/New_York')

#Convert time to a readable datetime
df_State['date_time']=df_State['pubMillis'].apply(lambda x: timeconvert(x))
#set as UTC
#df_State['date_time2']=df_State['date_time'].apply(lambda x: x.replace(tzinfo=from_zone))
#convert to Eastern Time Zone
df_State['date_time']=df_State['date_time'].apply(lambda x: x.astimezone(to_zone))

#Create column for hour of day report was published
df_State['hour']=df_State['date_time'].apply(lambda x:x.hour+(x.minute/60)+(x.second/3600))

#Create column for day of week
df_State['weekday']=df_State['date_time'].apply(lambda x: x.weekday())

#Create column for date only
df_State['date'] = df_State['date_time'].apply(lambda x: x.date())


#create dictionary of road types to convert numeric to categorical(from API docs)
road_types = dict([(1,'Streets'),(2,'Primary Street'),(3,'Freeways'),(4,'Ramps'),
                   (5,'Trails'),(6,'Primary'),(7,'Secondary'),(8,'4x4 Trails'),
                   (14,'4x4 Trails'),(9,'Walkway'),(15,'Ferry Crossing'),(10,'Pedestrian'),
                   (11,'Exit'),(16,'Stairway'),(17,'Private Road'),(18,'Railroads'),
                   (19,'Runway/Taxiway'),(20,'Parking Lot Road'),(21,'Service Road'),(np.nan,'N/A')])

#Add column for road type categories
df_State['RoadTypeCat'] = df_State['roadType'].apply(lambda x:road_types.get(x))

#Filter on date
df_State= df_State[(df_State['date_time']>'2017-07-01') & (df_State['date_time']<'2017-07-29')]

#Optional: Filter on City
#df_State= df_State[(df_State['city']=="Boston, MA")]
# Uncomment lines below to export the State dataframe to a csv file in the fileout_csv location
#Note: Do not place in same directory as original .json files.
# =============================================================================
# 
fileout_csv = "C:/Users/Matthew.Goodwin/Desktop/SDC Artifacts/timeeval.csv"
df_State.to_csv(fileout_csv)

# =============================================================================

###############################################################################
#The following section computes various summary statistics for the data in the
#dataframe

#Mean "report rating"
reportRating_mean = df_State.reportRating.mean()
#Summary for "Report Rating" grouped by report "type" and "subtype"
reportRating_description = df_State['reportRating'].groupby(df_State['type']).describe()
reportRating_subtype_description = df_State['reportRating'].groupby(df_State['subtype']).describe()
print("Report Rating by type")
print (reportRating_description)
print("Report Rating by subtype")
print(reportRating_subtype_description)


#Mean "Reliability"
reliability_mean = df_State.reliability.mean()
#Summary for "Reliability" grouped by "type" and "subtype"
reliability_description = df_State['reliability'].groupby(df_State['type']).describe()
reliability__subtype_description = df_State['reliability'].groupby(df_State['subtype']).describe()
print("Report Reliability by type")
print(reliability_description)
print("Report Reliability by subtype")
print(reliability__subtype_description)

#Summary for "confidence" grouped by "type" and "subtype" 
confidence__type_description = df_State['confidence'].groupby(df_State['type']).describe()
confidence__subtype_description = df_State['confidence'].groupby(df_State['subtype']).describe()
print("Report Confidence by type")
print(confidence__type_description)
print("Report Confidence by subtype")
print(confidence__subtype_description)

