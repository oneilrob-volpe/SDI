

import os, sys, pyodbc, math, datetime, glob, shutil
import arcpy
from geopy import distance
from geopy.point import Point


# CONFIG
# -----------------------------------------------------------------------------------------------

# connStr = 'DRIVER={SQL Server Native Client 10.0};SERVER=.\SQLEXPRESS;DATABASE=temp;UID=me;PWD=pass;Trusted_Connection=yes'
# connStr = 'DRIVER={SQL Server Native Client 11.0};SERVER=.\SQLEXPRESS;DATABASE=COD_2015_AIRPORT;UID=me;PWD=pass;Trusted_Connection=yes'
connStr = 'DRIVER={SQL Server Native Client 11.0};SERVER=.\SQLEXPRESS;DATABASE=temp_data;UID=me;PWD=pass;Trusted_Connection=yes'

#outputDir = r'D:\Tasks\2015\2015_03_09_close_apts\output'
outputDir = r'D:\mcm_runs\results_2017_08_14'

#minDistNm = .00001

maxDistNm = 3


# -----------------------------------------------------------------------------------------------



dict1 = {}
dict2 = {}

cnxn1 = pyodbc.connect(connStr)
cursor1 = cnxn1.cursor()
#for row in cursor1.execute("select apt_id, lat, lon from list1"):
#for row in cursor1.execute("select apt_id, lat, lon from APT_MAIN"):
for row in cursor1.execute("select apt_id, lat, lon from temp_location_list"):
	dict1[row.apt_id] = Point(row.lat, row.lon)

cnxn2 = pyodbc.connect(connStr)
cursor2 = cnxn2.cursor()
#for row in cursor2.execute("select apt_id, lat, lon from list2"):
#for row in cursor2.execute("select apt_id, lat, lon from APT_MAIN"):
for row in cursor2.execute("select apt_id, lat, lon from temp_location_list"):
	dict2[row.apt_id] = Point(row.lat, row.lon)


print len(dict1)
print len(dict2)


print 'done loading data'

# DETERMINE CLOSENESS.
# creates something that looks like:
# AKA _RCO []
# ABRC_RCO [[u'ABRB_RCO', 2.465426216317404], [u'ABRA_RCO', 0.0], [u'ABR _RCO', 0.0]]
# -----------------------------------------------------------------------------------------------

proximDict = {}


for primKey in dict1:

	proximDict[primKey] = []

	primaryLoc = dict1[primKey]

	for secKey in dict2:

		if not primKey == secKey:

			secondaryLoc = dict2[secKey]

			try :
				dist = distance.distance(primaryLoc, secondaryLoc)

				if ( dist.nm < maxDistNm):
					hit = [secKey, dist.nm]
					proximDict[primKey].append(hit)

			except ValueError:
				pass


print 'Done calculating distances, dumping'

# -----------------------------------------------------------------------------------------------

outputFile = os.path.join(outputDir, "result.txt")
wf = open(outputFile, 'w')


for k, v in proximDict.iteritems():

	if len(v) > 0:
		wf.write('{} -> {}\n'.format(k, v))

wf.close()
