from __future__ import print_function

import os
from datetime import datetime
import re
import json

def extract_filtered_json(data_folder):
    ''' Creates a list of files that contain a record between supplied dates of valid subtype

    Parameters:

    Returns:
    - matching_files (list(string)) - full paths to files that match filter
    '''
    matching_files = []
    files_examined = 0
    valid_subtypes = ('ACCIDENT_MINOR', 'ACCIDENT_MAJOR', 'HAZARD_ON_SHOULDER_CAR_STOPPED')
    for root, dirs, file_names in os.walk(data_folder):
        for file_name in file_names:
            file_path = os.path.join(data_folder, file_name)
            with open(file_path, "r") as data_file:
                for line in data_file:
                    if 'jams' in line:
                        continue

                    anon = json.loads(line)
                    pubSecs = anon['pubMillis'] / 1000.0
                    subtype_text = str(anon.get('subtype'))
                    if subtype_text in valid_subtypes and 1496880000 <= pubSecs < 1496966399:
                        matching_files.append(file_path)
                        break

            if files_examined % 100 == 0:
                print('Proessed {} files, found {} files'.format(files_examined, len(matching_files)))
            files_examined += 1
    return files_examined, matching_files
# end extract_filtered_json

def extract_filter_regex_json(data_folder):
    ''' Creates a list of files that contain a record between supplied dates of valid subtype

    Parameters:

    Returns:
    - matching_files (list(string)) - full paths to files that match filter
    '''
    matching_files = []
    files_examined = 0
    target_subtypes = '(ACCIDENT_MINOR|ACCIDENT_MAJOR|HAZARD_ON_SHOULDER_CAR_STOPPED)'
    for root, dirs, file_names in os.walk(data_folder):
        for file_name in file_names:
            file_path = os.path.join(data_folder, file_name)
            with open(file_path, "r") as data_file:
                for line in data_file:
                    if 'jams' in line:
                        continue
                    
                    match = re.search(target_subtypes, line)
                    if match:
                        anon = json.loads(line)
                        pubSecs = anon['pubMillis'] / 1000.0
                        if 1496880000 <= pubSecs < 1496966399:
                            matching_files.append(file_path)
                            break

            if files_examined % 100 == 0:
                print('Proessed {} files, found {} files'.format(files_examined, len(matching_files)))
            files_examined += 1
    return files_examined, matching_files
# end extract_filter_regex_json

def extract_filter_regex(data_folder):
    ''' Creates a list of files that contain a record between supplied dates of valid subtype
    Parameters:
    Returns:
    - matching_files (list(string)) - full paths to files that match filter
    '''
    matching_files = []
    files_examined = 0
    # https://regex101.com/r/FLlzXn/3
    target_subtypes = re.compile(r'(?P<subtype>HAZARD_ON_SHOULDER_CAR_STOPPED|ACCIDENT_MINOR|ACCIDENT_MAJOR)+(.*)(?P<pubMillis>\d{13})}$')
    for root, dirs, file_names in os.walk(data_folder):
        for file_name in file_names:
            file_path = os.path.join(data_folder, file_name)
            with open(file_path, "r") as data_file:
                for line in data_file:
                    if 'jams' in line:
                        continue
                    line = line.strip()
                    match = target_subtypes.search(line)
                    if match:
                        match_millis = match.group('pubMillis')
                        match_subtype = match.group('subtype')
                        pubSecs = match_millis // 1000
                        if 1496880000 <= pubSecs < 1496966399:
                            matching_files.append(file_path)
                            break

            if files_examined % 100 == 0:
                print('Proessed {} files, found {} files'.format(files_examined, len(matching_files)))
            files_examined += 1
    return files_examined, matching_files
# end extract_filter_regex

def report_runtime(start_time):
    ''' logs runtime at info level '''
    total_run_time = datetime.now() - start_time
    hours = total_run_time.seconds / 3600
    mins = (total_run_time.seconds % 3600) / 60
    secs = (total_run_time.seconds % 3600) % 60

    print(
        "Now is {}.  Elapsed run time (HH:MM:SS) = {:02d}:{:02d}:{:02d}".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), hours, mins, secs)
        )
# end report_runtime

if __name__ == '__main__':
    INPUT_DIR = '/Users/roboneil/OneDrive/projects/waze/sample'

    start_time = datetime.now() 

    files_examined, matching_files = extract_filter_regex(INPUT_DIR)
    print('extract_filter_regex complete; processed {} files, found {} files'.format(files_examined, len(matching_files)))

    report_runtime(start_time) 
    ####################################
    start_time = datetime.now() 

    files_examined, matching_files = extract_filtered_json(INPUT_DIR)
    print('extract_filtered_json complete; processed {} files, found {} files'.format(files_examined, len(matching_files)))

    report_runtime(start_time)
    #####################################
    start_time = datetime.now() 

    files_examined, matching_files = extract_filter_regex_json(INPUT_DIR)
    print('extract_filter_regex_json complete; processed {} files, found {} files'.format(files_examined, len(matching_files)))

    report_runtime(start_time)
