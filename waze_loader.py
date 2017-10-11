# -*- coding: utf-8 -*-
'''
#===================================================================================================
#
# Name:       waze_load.py
#
# Purpose:    load waze data into a database
#
# Author:     Rob O'Neil
#
# Version:    1.0 - 27 Sep 2017
#
# ==================================================================================================
'''
from __future__ import print_function

import os
import sys
import csv
from datetime import datetime
from shutil import copy, rmtree
import re
import logging
import json
import collections
import utilities as utils
import pypyodbc
import pytz

ALERT_TYPES = {'ACCIDENT': 1,
               'JAM': 2,
               'WEATHERHAZARD': 3,
               'HAZARD': 4,
               'MISC': 5,
               'CONSTRUCTION': 6,
               'ROAD_CLOSED': 7
              }
# end ALERT_TYPES

ALERT_SUBTYPES = {'ACCIDENT_MINOR': 1,
                  'ACCIDENT_MAJOR': 2,
                  'JAM_MODERATE_TRAFFIC': 3,
                  'JAM_HEAVY_TRAFFIC': 4,
                  'JAM_STAND_STILL_TRAFFIC': 5,
                  'JAM_LIGHT_TRAFFIC': 6,
                  'HAZARD_ON_SHOULDER': 8,
                  'HAZARD_ON_SHOULDER_CAR_STOPPED': 13,
                  'HAZARD_ON_SHOULDER_ANIMALS': 14,
                  'HAZARD_ON_SHOULDER_MISSING_SIGN': 15,
                  'HAZARD_WEATHER': 9,
                  'HAZARD_WEATHER_FOG': 16,
                  'HAZARD_WEATHER_HAIL': 17,
                  'HAZARD_WEATHER_HEAVY_RAIN': 18,
                  'HAZARD_WEATHER_HEAVY_SNOW': 19,
                  'HAZARD_WEATHER_FLOOD': 20,
                  'HAZARD_WEATHER_MONSOON': 21,
                  'HAZARD_WEATHER_TORNADO': 22,
                  'HAZARD_WEATHER_HEAT_WAVE': 23,
                  'HAZARD_WEATHER_HURRICANE': 24,
                  'HAZARD_WEATHER_FREEZING_RAIN': 25,
                  'HAZARD_ON_ROAD': 7,
                  'HAZARD_ON_ROAD_OBJECT': 10,
                  'HAZARD_ON_ROAD_POT_HOLE': 11,
                  'HAZARD_ON_ROAD_ROAD_KILL': 12,
                  'HAZARD_ON_ROAD_ICE': 28, 
                  'HAZARD_ON_ROAD_OIL': 27,
                  'HAZARD_ON_ROAD_CAR_STOPPED': 30, # 30 -> 127 -> 28
                  'HAZARD_ON_ROAD_LANE_CLOSED': 26, # 26 -> 128 -> 29
                  'HAZARD_ON_ROAD_CONSTRUCTION': 29,# 29 -> 129 -> 30
                  'ROAD_CLOSED_HAZARD': 31,
                  'ROAD_CLOSED_CONSTRUCTION': 32,
                  'ROAD_CLOSED_EVENT': 33
                 }
# end ALERT_SUBTYPES

WAZE_FIELD_SPEC = """
    [uuid]|[uniqueidentifier]|NOT NULL
    [city]|[varchar](64)|NULL
    [report_rating]|[smallint]|NULL
    [confidence]|[smallint]|NULL
    [reliability]|[smallint]|NULL
    [alert_type]|[smallint]|NOT NULL
    [alert_subtype]|[smallint]|NOT NULL
    [road_type]|[smallint]|NOT NULL
    [magvar]|[smallint]|NOT NULL
    [street]|[varchar](128)|NULL
    [pub_millis]|bigint|NOT NULL
    [report_time_utc]|[datetime2](0)|NOT NULL
    [latitude]|[decimal](9, 6)|NOT NULL
    [longitude]|[decimal](9, 6)|NOT NULL
"""

class WazeAlert(object):
    ''' represents a single report (one line of a waze data capture) '''
    def __init__(self, uuid, city, report_rating, confidence, reliability,
                 alert_type, alert_subtype, road_type, magvar, street, pub_millis,
                 longitude, latitude):
        self.uuid = uuid
        self.city = city
        self.report_rating = report_rating
        self.confidence = confidence
        self.reliability = reliability
        self.alert_type = alert_type
        self.alert_subtype = alert_subtype
        self.road_type = road_type
        self.magvar = magvar
        self.street = street
        self.pub_millis = pub_millis
        self.report_time_utc = datetime.fromtimestamp(pub_millis / 1000.0, pytz.utc)
        self.latitude = latitude
        self.longitude = longitude
    # end __init__

    def get_values(self, delimiter='!'):
        ''' creates a list of values associated with this object '''

        return [self.uuid, self.city, self.report_rating, self.confidence, self.reliability,
                self.alert_type, self.alert_subtype, self.road_type, self.magvar, self.street,
                self.pub_millis, self.report_time_utc, self.latitude, self.longitude
               ]
    # end get_values

    @staticmethod
    def get_fieldnames():
        '''  creates a list of field names associated with this object '''

        return ['uuid', 'city', 'report_rating', 'confidence', 'reliability',
                'alert_type', 'alert_subtype', 'road_type', 'magvar', 'street',
                'pub_millis', 'report_time_utc', 'latitude', 'longitude']
    # end get_fieldnames
# end WazeAlert

# ==================================================================================================
# ENTRY POINT
# ==================================================================================================
def main(parameter_list):
    ''' main entry point '''
    start_time = datetime.now()

    config = utils.get_config(parameter_list)

    utils.setup_logging(
        config['LOG_DIRECTORY'],
        config['PROGRAM_NAME'],
        config['FILE_LOGGING_LEVEL'],
        config['CONSOLE_LOGGING_LEVEL']
    )

    utils.setup_output(config['OUTPUT_FOLDER'])

    for key, value in config.items():
        logging.debug('cfg param %s : %s', key, value)

    logging.info('Start time %s', start_time.strftime("%Y-%m-%d %H:%M:%S"))

    data_folder = config['DATA_FOLDER']
    logging.info('Processing files in %s', data_folder)
        
    utz_zone = pytz.timezone('UTC')
    first_epoch = long(config['FIRST_EPOCH'])
    last_epoch = long(config['LAST_EPOCH'])
    filter_expression = config['FILE_FILTER_REGEX']

    # cut out _files_ based on date
    logging.info('Starting to filter records in %s by dates %s - %s', 
                 data_folder, first_epoch, last_epoch)
    files_examined, matching_files = extract_filtered_file_list(data_folder, first_epoch, last_epoch, filter_expression)
    logging.info('Filtered %s files to list of %s', files_examined, len(matching_files))
    logging.info('Current time %s', datetime.now().strftime('%H:%M:%S'))

    subset_folder = config['SUBSET_FOLDER']
    if os.path.exists(subset_folder):
        shutil.rmtree(subset_folder)
    os.mkdir(output_dir)

    logging.info('Creating subset of files: %s', subset_folder)
    for file_name in matching_files:
        copy(file_name, subset_folder)

    logging.info('Processing files into records')
    logging.info('Current time %s', datetime.now().strftime('%H:%M:%S'))
    files_processed, total_lines, jams_skipped, single_file_duplicates, combined_lines = extract_lines(subset_folder)
    logging.info('Processed files: %s; total lines: %s; files with duplicate lines: %s; unique lines: %s',
                 files_processed, total_lines, len(single_file_duplicates), len(combined_lines))
    if jams_skipped > 0:
        logging.error('Found lines containing %s "jams" records that were skipped', jams_skipped)

    logging.info('Current time %s', datetime.now().strftime('%H:%M:%S'))

    # cut out _records_ based on date
    filter_expression = config['OBJECT_FILTER_REGEX']
    study_set = extract_waze_objects(combined_lines, first_epoch, last_epoch, filter_expression) 
    logging.info('Processed %s lines, created study set of size: %s',
                 len(combined_lines), len(study_set))

    logging.info('Building study file at %s', datetime.now().strftime("%H:%M:%S"))

    study_file = os.path.join(config['OUTPUT_FOLDER'], config['STUDY_OUTPUT_FILE'])
    build_study_output(study_set, study_file)

    utils.report_runtime(start_time)
    print('\n')
    return

    # duplicate_file = os.path.join(config['OUTPUT_FOLDER'], config['DUPLICATE_FILE'])
    # build_file_duplicates_output(single_file_duplicates, duplicate_file)

    # connection_string = utils.make_connection_string(
    #     config['DB_DRIVER'],
    #     config['DB_SERVER'],
    #     config['DB_NAME'],
    #     config['DB_USER'],
    #     config['DB_PASS'],
    #     config['DB_TRUSTED']
    # )

    # import_table_name = config['WAZE_IMPORT_TABLENAME']
    # waze_field_specs = utils.get_field_spec(WAZE_FIELD_SPEC)
    # sql = utils.make_create_table_sql(waze_field_specs, import_table_name)
    # utils.create_table(connection_string, import_table_name, sql, True)
    # utils.bulk_insert_csv_file_to_db(connection_string, import_table_name, study_file, 1, '|')
    
# end main

def waze_decoder(line):
    ''' decodes a line of text into a WazeAlert. Required for export

    Args:
        line (string): json encoded waze alert data

    Returns:
        WazeAlert (class)
    '''
    # creat an anonymous object from the line of text
    anon = json.loads(line)

    uuid = anon.get('uuid')
    city = anon.get('city')
    report_rating = anon.get('reportRating')
    confidence = int(anon.get('confidence', 0))
    reliability = int(anon.get('reliability', 0))

    # convert types to int
    type_text = str(anon.get('type')).upper()
    alert_type = ALERT_TYPES.get(type_text, 0)
    subtype_text = str(anon.get('subtype')).upper()
    alert_subtype = ALERT_SUBTYPES.get(subtype_text, 0)

    road_type = anon.get('roadType') # this comes 'pre-encoded' from waze
    magvar = anon.get('magvar')
    street = anon.get('street')

    pub_millis = (anon['pubMillis']) # this gets decodes as a long; e.g. 1492064278378L
    latitude = float(anon['location']['y']) # Note from docs (X Y Longlat)
    longitude = float(anon['location']['x'])

    return WazeAlert(uuid, city, report_rating, confidence, reliability, alert_type, alert_subtype,
                     road_type, magvar, street, pub_millis, longitude, latitude)
# end waze_decoder

def extract_filtered_file_list(data_folder, first_epoch, last_epoch, filter_expression):
    ''' Creates a list of files that contain a record between supplied dates of valid subtype

    Parameters:
    Returns:
    - matching_files (list(string)) - full paths to files that match filter
    '''
    #target_subtypes = re.compile(r'(?P<type>ACCIDENT)+(.*)(?P<pubMillis>\d{13})}$')
    target_subtypes = re.compile(filter_expression)
    matching_files = []
    files_examined = 0

    for file_name in os.listdir(data_folder):
        file_path = os.path.join(data_folder, file_name)
        with open(file_path, "r") as data_file:
            for line in data_file:
                if 'jams' in line:
                    logging.info('Cannot process jam: %s', line)
                    continue
                
                line = line.strip()
                match = target_subtypes.search(line)
                if match:
                    match_millis = match.group('pubMillis')
                    match_type = match.group('type')
                    pub_seconds = long(match_millis) // 1000
                    if first_epoch <= pub_seconds < last_epoch:
                        matching_files.append(file_path)
                        break
                    # else:
                    #     logging.debug('matched on subtype: %s; rejected by date %s', match_type, pub_seconds)
                
        files_examined += 1
        if files_examined % 1000 == 0:
            logging.info('Examined %s files, found %s', files_examined, len(matching_files))
    return files_examined, matching_files
# end extract_filtered_file_list    

def extract_lines(data_folder):
    ''' Reads json formatted waze alert records from specified directory (including subs)

    Parameters:
    - data_folder (string) - full path to root directory

    Returns:
    - files_processed (int)
    - total_lines (int) - should = size of combined_lines - jams_skipped
    - jams_skipped (int)
    - single_file_duplicates (dic(line -> list(file)))
    - duplicate_lines_skipped (int)
    - combined_lines (list(string)) - all unique lines within data_folder (skipping jams)
    '''
    total_lines = 0
    files_processed = 0
    jams_skipped = 0

    single_file_duplicates = collections.defaultdict(list) # track which line shows up in which file multiple times

    combined_lines = []
    for file_name in os.listdir(data_folder):
        file_path = os.path.join(data_folder, file_name)

        try:
            file_lines = []
            with open(file_path, "r") as data_file:
                # first make a set of unique lines (and record any line duplicates)
                for line in data_file:
                    total_lines += 1
                    if 'jams' in line:
                        logging.info('Cannot process jam: %s', line)
                        jams_skipped += 1
                        continue
                    if line in file_lines:
                        single_file_duplicates[line].append(file_name)
                    
                    combined_lines.append(line)

        except: #pylint: disable=w0702
            logging.exception('Could not process: %s', file_name)
        finally:
            files_processed += 1

        if files_processed % 1000 == 0:
            logging.info('Processed %s files', files_processed)

    return files_processed, total_lines, jams_skipped, single_file_duplicates, combined_lines
# end extract_lines

def extract_waze_objects(lines, first_epoch, last_epoch, filter_expression):
    '''Creates a set of waze alert records between first_epoch and last_epoch
     - does not check for duplicates  

    Parameters:
    - lines (set(string)) - lines to process

    Returns:
    - study_set dict(id -> list(WazeAlert))
    '''
    # target_subtypes = re.compile(r'(.*)(?P<pubMillis>\d{13})}$')
    target_subtypes = re.compile(filter_expression)

    study_set = collections.defaultdict(list)
    for index, line in enumerate(lines):
        if index > 0 and index % 10000 == 0:
            logging.info('Processing line %s', index)

        line = line.strip()
        match = target_subtypes.search(line)
        if match:
            match_millis = match.group('pubMillis')
            pub_seconds = long(match_millis) // 1000
            if first_epoch <= pub_seconds < last_epoch:
                record = waze_decoder(line)
                study_set[record.uuid].append(record)

    return study_set
# end extract_waze_objects

def build_study_output(study_set, study_file):
    ''' Creates a text file using data from study_set

    Parameters:
    - study_set dict(id -> list(WazeAlert)) - data to write
    - study_file (string) - full path to where to write output (will be created/truncated)

    Returns:
        None (creates file at specified location)
    '''
    logging.info('writing csv file to: %s', study_file)
    header = WazeAlert.get_fieldnames()
    logging.debug('csv header row: %s', header)
    objects_processed = 0
    with open(study_file, 'wb') as delimited_file:
        writer = csv.writer(delimited_file, delimiter='|', quoting=csv.QUOTE_NONE)
        writer.writerow(header)
        for uuid, group in study_set.iteritems():
            writer.writerows(record.get_values() for record in group)
            objects_processed += 1
            if objects_processed % 1000 == 0:
                logging.info('Processed %s objects', objects_processed)

    logging.info('Processed %s objects', objects_processed)
# end build_study_output

def build_file_duplicates_output(single_file_duplicates, duplicate_file):
    ''' Creates a text file listing location of duplicate records within the same file
    Parameters:
    - single_file_duplicates (dic(line -> list(file)))
    - duplicate_file  - full path to where to write output (will be created/truncated)

    Returns:
        None (creates file at specified location)
    '''
    with open(duplicate_file, 'wb') as output:
        fieldnames = ['line', 'file_name']
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for line, file_name in single_file_duplicates.iteritems():
            writer.writerow({'line': line, 'file_name': file_name})
    
# end build_file_duplicates_output

if __name__ == "__main__":
    main(sys.argv)
