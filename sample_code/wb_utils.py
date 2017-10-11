
#===================================================================================================
#
# Name:       wb_utils
#
# Purpose:    waybill utilities
#
# Author:     Gary Baker
#
# Version:    1.0 - 10/28/2016
# Version:    1.1 - 03/08/2017 - updated to run off of gdb instead of shapefiles
# Version:    1.2 - 03/30/2017 - further layer name standardization, spatialref standardization, 
#                                and network connectivity checks
# Version:    1.3 - 05/01/2017 - added flow rights and additional startup checks
# Version:    1.4 - 06/23/2017 - distance checks and net checks for all RRs
#
# ==================================================================================================
# IMPORT REQUIRED MODULES
# ==================================================================================================

from __future__ import print_function

import os, sys
import ConfigParser
import logging
from datetime import datetime, timedelta
import struct
from decimal import *
import math
import pypyodbc
import csv

# ==================================================================================================
# UNPACK FIXED FORMAT RECORDS.  USED BY BOTH WB_LOAD AND STN_LOAD
# ==================================================================================================

def report_runtime(start_time):

    total_run_time = datetime.now() - start_time

    # TODO don't want output that looks like: Total run time (H:M:S) = 0:2:6
    # should be 0:02:06
    ## Fixed? SZC 5/25/2017

    hours = total_run_time.seconds / 3600
    mins  = (total_run_time.seconds % 3600) / 60
    secs  = (total_run_time.seconds % 3600) % 60

    logging.info(
        "Done at {}.  Total run time (HH:MM:SS) = {:02d}:{:02d}:{:02d}".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), hours, mins, secs)
        )

# ==================================================================================================
# UNPACK FIXED FORMAT RECORDS.  USED BY BOTH WB_LOAD AND STN_LOAD
# ==================================================================================================

def build_record_unpacker(field_specs, subset_fields):

    fmt_string = ""

    for field_spec in field_specs:

        start = field_spec.fr_to_cols[0] - 1
        end   = start + field_spec.num_positions

        if subset_fields:
            if field_spec.key_field:
                fmt_string += str(end - start) + "s "
            else:
                fmt_string += str(end - start) + "x "
        else:
            fmt_string += str(end - start) + "s "

    field_struct = struct.Struct(fmt_string.strip())
    parse        = field_struct.unpack_from

    # internal debug
    #print('fmtstring: |{}|'.format(fmt_string))

    return parse


# ==================================================================================================
# MAKE CONNECTION STRING
# ==================================================================================================

def make_connection_string(driver, db_host, db_name, db_user, db_password, db_trusted):

    connection_string = 'Driver={' + driver + '};Server=' + db_host + ';Database=' + \
            db_name + ';UID=' + db_user + ';PWD=' + db_password + ';' + db_trusted + ';'

    logging.debug('connection string = {}'.format(connection_string))

    # TODO test connection string here

    return connection_string


# ==================================================================================================
# MAKE CREATE TABLE SQL
# ==================================================================================================

def make_create_table_sql(field_specs, table_name, uniq_id_field_name):

    create_table_sql = 'create table {} (\n'.format(table_name)

    if len(uniq_id_field_name) > 0:
        create_table_sql += uniq_id_field_name + ' int not null,\n'

    for field_spec in field_specs:
        create_table_sql += '{} {} {},\n'.format(
                field_spec.field_name, field_spec.sql_type, field_spec.sql_nullable)

    create_table_sql = create_table_sql.rstrip(',') + ')'

    logging.debug('create table sql = {}'.format(create_table_sql))

    return create_table_sql


# ==================================================================================================
# DROP TABLE IF EXISTS
# ==================================================================================================

def drop_table_if_exists(connection, table_name):

    cursor = connection.cursor()

    if cursor.tables(table=table_name).fetchone():
        cursor.execute("DROP TABLE {}".format(table_name))
        cursor.commit()
        logging.debug('Dropped {} table'.format(table_name))

    connection.commit()


# ==================================================================================================
# CREATE TABLE
# ==================================================================================================

def create_table(connection_string, table_name, create_table_sql):

    connection = pypyodbc.connect(connection_string)

    cursor = connection.cursor()

    if cursor.tables(table=table_name).fetchone():
        cursor.execute("DROP TABLE {}".format(table_name))
        cursor.commit()
        logging.info('Dropped existing {} table'.format(table_name))

    logging.info('Creating table {} ...'.format(table_name))

    cursor.execute(create_table_sql)
    connection.commit()
    connection.close()

    #logging.info('Create table complete')

# ==================================================================================================
# BULK INSERT TEXT FILE TO DATABASE
# ==================================================================================================

# TODO elegant failure for misformatted data

def bulk_insert_text_file_to_db(connection_string, table_name, input_file):

    logging.info('Bulk inserting file {} to table {}...'.format(input_file, table_name))

    insert_command = "bulk insert {} from '{}' with (FIRSTROW = 2, FIELDTERMINATOR = '\\t',ROWTERMINATOR = '\\n')".format(
            table_name, input_file)

    logging.debug('bulk insert command is {}'.format(insert_command))

    connection = pypyodbc.connect(connection_string)

    cursor = connection.cursor()

    cursor.execute(insert_command)
    cursor.commit()
    cursor.close()

    logging.info('Bulk insert complete')


# ==================================================================================================
# STRING TO BOOL
# ==================================================================================================

def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")

    
# ==================================================================================================
# GET DATA FROM CONFIGURATION FILE HELPER
# ==================================================================================================

def read_config_file_helper(config, section, key):

    if not config.has_option(section, key):
        raise Exception("CONFIG FILE ERROR: Can't find {} in section {}".format(key, section))
    else:
        val = config.get(section, key).strip().strip("'").strip('"')
        return val

# ==================================================================================================
# GET DATA FROM CONFIGURATION FILE
# TODO: do some checking on these values, eg raw_waybill_data must exist, dir for reformatted waybill
# must exist
# TODO - make a more generic handler for true false values
# ==================================================================================================

def read_config_file(cfg_file):

    print('\nReading configuration file ...')

    cfg_dict = {}  # return value

    if not os.path.exists(cfg_file):
        raise Exception("CONFIG FILE ERROR: {} could not be found".format(cfg_file))

    cfg = ConfigParser.RawConfigParser()
    cfg.read(cfg_file)


    # RUN DIRECTORY
    cfg_dict['RUN_DIRECTORY'] = read_config_file_helper(cfg, 'common', 'RUN_DIRECTORY')

    # LOG TO FILE LEVEL
    cfg_dict['FILE_LOGGING_LEVEL']   = read_config_file_helper(
            cfg, 'common', 'FILE_LOGGING_LEVEL').lower()
    # TODO check for valid log level

    # LOG TO CONSOLE LEVEL
    cfg_dict['CONSOLE_LOGGING_LEVEL']   = read_config_file_helper(
            cfg, 'common', 'CONSOLE_LOGGING_LEVEL').lower()
    # TODO check for valid log level

    # DATABASE
    cfg_dict['DB_SERVER']   = read_config_file_helper(cfg, 'common', 'DB_SERVER')
    cfg_dict['DB_NAME']     = read_config_file_helper(cfg, 'common', 'DB_NAME')
    cfg_dict['DB_USER']     = read_config_file_helper(cfg, 'common', 'DB_USER')
    cfg_dict['DB_PASS']     = read_config_file_helper(cfg, 'common', 'DB_PASS')
    cfg_dict['DB_TRUSTED']  = read_config_file_helper(cfg, 'common', 'DB_TRUSTED')
    cfg_dict['DB_DRIVER']   = read_config_file_helper(cfg, 'common', 'DB_DRIVER')


    # WAYBILL SECTION
    # ------------------------------------------------------

    # TODO check that the file exists
    cfg_dict['RAW_WAYBILL_DATA'] = read_config_file_helper(cfg, 'common', 'RAW_WAYBILL_DATA')


    # CENTRALIZED STATION MASTER TABLE
    # ------------------------------------------------------

    # TODO check that the file exists
    cfg_dict['RAW_CSM_DATA']     = read_config_file_helper(cfg, 'common', 'RAW_CSM_DATA')


    # RAIL NETWORK SECTION
    # ------------------------------------------------------

    cfg_dict['NARN_GDB']   = read_config_file_helper(cfg, 'common', 'NARN_GDB')

    if not os.path.exists(cfg_dict['NARN_GDB']):
        raise Exception("{} doesn't exists".format(cfg_dict['NARN_GDB']))


    # FLOWING AND FLOW MAPS SECTION
    # ------------------------------------------------------

    cfg_dict['WAYBILL_FIELD_TO_SUM_FOR_FLOW'] = read_config_file_helper(
            cfg, 'common', 'WAYBILL_FIELD_TO_SUM_FOR_FLOW').strip()

    cfg_dict['WAYBILL_WHERE_CLAUSE_FOR_FLOW'] = read_config_file_helper(
            cfg, 'common', 'WAYBILL_WHERE_CLAUSE_FOR_FLOW').strip()


    # NETWORK CHECKS
    # -----------------------------------------------------
    cfg_dict['NET_CHECK_NON_WAYBILL_RAILROADS'] = str2bool(
            read_config_file_helper(cfg, 'common', 'NET_CHECK_NON_WAYBILL_RAILROADS').strip()
    )
    
    # OPTIONAL RAILROAD SELECTION FOR FLOW
    # -----------------------------------------------------
    cfg_dict['RAILROADS_TO_FLOW'] = str(read_config_file_helper(cfg, 'common', 'RAILROADS_TO_FLOW').strip())
    
    cfg_dict['DISTANCE_TOLERANCE'] = float(read_config_file_helper(cfg, 'common', 'DISTANCE_TOLERANCE'))
    cfg_dict['MAPBOOK_NUMBER_TO_DO'] = int(read_config_file_helper(cfg, 'common', 'MAPBOOK_NUMBER_TO_DO'))

    return cfg_dict

# ==================================================================================================
# LOG LEVEL HELPER
# ==================================================================================================

def log_level_helper(level):

    if level == 'debug':
        return logging.DEBUG
    elif level == 'info':
        return logging.INFO
    elif level == 'warning':
        return logging.WARNING
    elif level == 'error':
        return logging.ERROR
    elif level == 'critical':
        return logging.CRITICAL
    else:
        raise Exception("Invalid log level specified.  Valid values include debug, info, warning, error, and critical")

# ==================================================================================================
# SETUP LOGGING
# ==================================================================================================

def setup_logging(run_dir, program_name, file_logging_level, console_logging_level):

    if not os.path.exists(run_dir):
        print('ERROR: run directory {} can''t be found!'.format(run_dir))
        sys.exit()

    log_dir = os.path.join(run_dir, 'logs')

    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    full_path_to_log_file = os.path.join(
            log_dir,
            program_name + "_" + datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + ".log"
            )

    # SET UP LOGGING TO FILE
    # ----------------------
    logging.basicConfig(
            level=log_level_helper(file_logging_level),
            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
            datefmt='%m-%d %H:%M',
            filename=full_path_to_log_file,
            filemode='a'
            )

    # SET UP LOGGING TO CONSOLE
    # -------------------------
    console = logging.StreamHandler()
    console.setLevel(log_level_helper(console_logging_level))
    console_format = logging.Formatter('%(levelname)-8s %(message)s')
    console.setFormatter(console_format)
    logging.getLogger('').addHandler(console)


# ==================================================================================================
# GREAT CIRCLE DISTANCTE IN MILES
# ==================================================================================================
    
def great_circle_dist_miles(lon1, lat1, lon2, lat2):

    # SET DECIMAL PRECISION TO AVOID ROUNDING ERRORS
    getcontext().prec = 10

    # CONVERT DEGREES TO RADIANS
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    dy = lat2 - lat1
    dx = lon2 - lon1

    # EXECUTE HAVERSINE FORMULA
    a = math.sin(dy/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dx/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # MULTIPLY RADIAN RESULT BY RADIUS OF THE EARTH IN NAUTICAL MILES FOR RESULT
    nmi = 3440.2769 * c

    # CONVERT TO MILES
    miles = nmi * 1.15078
    return miles


# ==================================================================================================
# REPORT CSV WRITING - NEW HEADERS FOR CLEANER APPEARANCE
# ==================================================================================================

def write_out_results_reports(overview_data, flagged_data, full_path_to_flow_overview_report_csv, full_path_to_flow_flagged_report_csv):
    
    # New headers for cleaner reports
    overview_header1 = [
            'Statistics',
            'Overall',
            '',
            'Did not flow',
            '',
            '',
            '',
            'Flow did not match Waybill',
            '',
            '',
            '',
            ]

    overview_header2 = [
            'Railroad',
            'Number of Legs',
            'Total frequency',
            'Number of legs',
            'Percent of legs',
            'Total frequency',
            'Percent of frequency',
            'Number of legs',
            'Percent of legs',
            'Total frequency',
            'Percent of frequency',
            ]
    
    flagged_header = [
            'Railroad',
            'Leg Origin',
            'Leg Destination',
            'Frequency',
            'Waybill Distance',
            'Flowed Distance',
            ]

    # write out data
    with open(full_path_to_flow_overview_report_csv,'wb') as csvfile_overview:
        wf_overview = csv.writer(csvfile_overview, delimiter=',')
        wf_overview.writerow(overview_header1)
        wf_overview.writerow(overview_header2)
        for row in overview_data:
            wf_overview.writerow(row)
        
    with open(full_path_to_flow_flagged_report_csv,'wb') as csvfile_flagged:
        wf_flagged = csv.writer(csvfile_flagged, delimiter=',')
        wf_flagged.writerow(flagged_header)
        for row in flagged_data:
            wf_flagged.writerow(row)

    return


# ==================================================================================================
# REPORT ON RESULTS OF FLOWING RAILROADS
# ==================================================================================================

def generate_flow_results_report(distance_tolerance, rrs_to_process, sql_db_connection_string, full_path_to_flow_overview_report_csv, full_path_to_flow_flagged_report_csv ):
    
    # connect to db
    connection = pypyodbc.connect(sql_db_connection_string)
    cursor = connection.cursor()
    
    # wb dist should equal flow dist (i.e. x = y, slope = 1).  Based on plotting out the wb dist vs the 
    # flow dist for now the real outliers seem to be + or - 750 around x = y


    overview_data = []
    flagged_data = []

    # Cycle through looking for issues
    for rr_to_process in rrs_to_process:

        this_rr, freq = rr_to_process
        this_rr = str(this_rr)
         
        # Compile results of the flow operation
        sql = '''
        SELECT railroad, orig_splc_or_rule260, dest_splc_or_rule260, freq, wb_median_miles, flow_dist_miles
        FROM waybill_legs
        where railroad = '{}'
        order by railroad
        '''.format(this_rr)
        #logging.debug(sql)
    
        cursor.execute(sql)
        data = cursor.fetchall()

        if len(data) == 0:
            # No data returned - report empty set for railroad
            #logging.debug('Report on railroad: {} ... No records in waybill')  
            overview_data.append([this_rr,0,0,0,0,0,0,0,0,0,0])
            # TODO would this ever happen?
            continue

        total_legs = 0
        total_freq = 0

        no_flow_legs = 0
        no_flow_freq = 0

        large_flow_diff_legs = 0
        large_flow_diff_freq = 0

        # examine each leg of data
        for row in data:
            railroad, orig_splc_or_rule260, dest_splc_or_rule260, freq, wb_median_miles, flow_dist_miles = row

            # overall stats
            total_legs += 1
            total_freq += freq
            
            # failed to flow
            if flow_dist_miles == 0:
                no_flow_legs += 1
                no_flow_freq += freq
                
                flagged_data.append(row)
            else:
                # Flowed... but was distance okay?
                min_acceptable_flow_dist = wb_median_miles - distance_tolerance
                max_acceptable_flow_dist = wb_median_miles + distance_tolerance

                if wb_median_miles != 0 and ((flow_dist_miles < min_acceptable_flow_dist) or (flow_dist_miles > max_acceptable_flow_dist)):
                    # bad flow distance
                    large_flow_diff_legs += 1
                    large_flow_diff_freq += freq
                    
                    flagged_data.append(row)

        # CREATE PERCENTAGES FOR REPORTING

        pct_no_flow_legs = (100.0 * no_flow_legs) / total_legs
        pct_no_flow_freq  = (100.0 * no_flow_freq)  / total_freq

        pct_lg_flow_diff_legs = (100.0 * large_flow_diff_legs) / total_legs
        pct_lg_flow_diff_freq = (100.0 * large_flow_diff_freq)  / total_freq
        
        overview_data.append([this_rr, 
                              total_legs, total_freq, 
                              no_flow_legs, pct_no_flow_legs, 
                              no_flow_freq, pct_no_flow_freq,
                              large_flow_diff_legs, pct_lg_flow_diff_legs,
                              large_flow_diff_freq, pct_lg_flow_diff_freq])
        
        write_out_results_reports(overview_data, flagged_data, full_path_to_flow_overview_report_csv, full_path_to_flow_flagged_report_csv)
        
        # Possibly make mapbooks of the worst offenders here?
        
        
    return
