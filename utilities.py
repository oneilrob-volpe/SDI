# -*- coding: utf-8 -*-
#===================================================================================================
#
# Name:       utilities.py
#
# Purpose:    OTS-P data fusion utilities
#
# Author:     Rob O'Neil
#
# Version:    1.0 - 21 Sep 2017
#
# ==================================================================================================
# IMPORT REQUIRED MODULES
# ==================================================================================================
from __future__ import print_function

import os
import errno
import logging
from datetime import datetime

import ConfigParser
import pypyodbc

class DataField(object):

    def __init__(self, field_name, sql_type, sql_nullable):

        self.field_name = field_name
        self.sql_type = sql_type
        self.sql_nullable = sql_nullable
# end DataField

def get_config(parameter_list):
    '''Parses config file and sets up logging

    Parameters:
    - parameter_list (list): should pass sys.arv
    - parameter_list[0] - program name
    - parameter_list[1] - config file

    Returns:
    - cfg (dictionary)
    '''
    program_full_path = parameter_list[0]
    file_name = os.path.basename(program_full_path)
    root, ext = os.path.splitext(program_full_path) # returns full path up to last '.' and extension (discarded)
    program_name, ext = os.path.splitext(file_name) # get just the program name (extension discarded)


    if len(parameter_list) == 2:
        config_file = parameter_list[1]
        cfg = read_config_file(config_file)
    else:
        # attempt to get config by convention
        config_file = root + '.cfg'
        cfg = read_config_file(config_file)

    cfg['PROGRAM_NAME'] = program_name

    return cfg
# end get_config

def read_config_file(cfg_file):
    ''' GET DATA FROM CONFIGURATION FILE '''

    if not os.path.exists(cfg_file):
        raise Exception(
            "CONFIG FILE ERROR: {} could not be found".format(cfg_file))

    print('\nReading configuration file ...')

    result = {}

    config = ConfigParser.ConfigParser()
    config.read(cfg_file)

    for section in config.sections():
        for key, val in config.items(section):
            result[key.upper()] = val.strip().strip("'").strip('"')

    return result
# end read_config_file

def make_connection_string(driver, db_host, db_name, db_user, db_password, db_trusted):
    ''' MAKE CONNECTION STRING '''

    connection_string = 'Driver={{{}}};Server={};Database={};UID={};PWD={};{};'.format(
        driver, db_host, db_name, db_user, db_password, db_trusted)

    logging.debug('connection string = %s', connection_string)

    return connection_string
# end make_connection_string

def make_create_table_sql(field_specs, table_name):
    ''' MAKE CREATE TABLE SQL '''

    create_table_sql = 'create table {} (\n'.format(table_name)

    for field_spec in field_specs:
        create_table_sql += '{} {} {},\n'.format(
            field_spec.field_name, field_spec.sql_type, field_spec.sql_nullable)

    # strip off last ',\n'
    create_table_sql = create_table_sql[:-2]
    create_table_sql = create_table_sql + ')'

    return create_table_sql
# end make_create_table_sql

def create_table(connection_string, table_name, create_table_sql, drop_existing):
    ''' CREATE TABLE '''

    connection = pypyodbc.connect(connection_string)
    cursor = connection.cursor()

    if drop_existing and cursor.tables(table=table_name).fetchone():
        cursor.execute("DROP TABLE {}".format(table_name))
        cursor.commit()
        logging.info('Dropped existing %s table', table_name)

    logging.info('Creating table %s ...', table_name)
    logging.debug('Executing sql:\n%s', create_table_sql)

    cursor.execute(create_table_sql)
    connection.commit()
    connection.close()

    logging.info('Create table complete')
# end create_table

def bulk_insert_csv_file_to_db(connection_string, table_name, input_file, first_row, field_terminator=','):
    ''' BULK INSERT TEXT FILE TO DATABASE '''

    logging.info('Bulk inserting file %s to table %s...', input_file, table_name)

    if not os.path.isfile(input_file):
        raise IOError(errno.ENOENT, 'File {} does not exist'.format(input_file))

    insert_command = "bulk insert {} from '{}' with (FIRSTROW={}, FIELDTERMINATOR='{}', ROWTERMINATOR='0x0a')".format(
        table_name, input_file, first_row, field_terminator)

    logging.debug('bulk insert command is %s', insert_command)

    connection = pypyodbc.connect(connection_string)

    cursor = connection.cursor()

    cursor.execute(insert_command)
    cursor.commit()
    cursor.close()

    logging.info('Bulk insert complete')
# end bulk_insert_csv_file_to_db

def log_level_helper(level):
    ''' LOG LEVEL HELPER '''

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
        raise Exception(
            "Invalid log level specified.  Valid values include debug, info, warning, error, and critical")
# end log_level_helper

def setup_logging(log_dir, program_name, file_logging_level, console_logging_level):
    ''' SETUP LOGGING '''

    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    full_path_to_log_file = os.path.join(
        log_dir,
        "run_" + program_name + "_" + datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + ".log"
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
# end setup_logging

def setup_output(output_dir):
    ''' Verifies and creates output directory '''
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

# end setup_output

def report_runtime(start_time):
    ''' logs runtime at info level '''
    total_run_time = datetime.now() - start_time
    hours = total_run_time.seconds / 3600
    mins = (total_run_time.seconds % 3600) / 60
    secs = (total_run_time.seconds % 3600) % 60

    logging.info(
        "Done at {}.  Total run time (HH:MM:SS) = {:02d}:{:02d}:{:02d}".format(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), hours, mins, secs)
        )
# end report_runtime

def get_field_spec(spec):
    '''Creates a list of field specifications

    Args:
        spec (string): a new line delimited string with each line describing one field.
        Each line should have a '!' delimited string with the following fields
        field_name, sql_type, sql_nullable, description, key_field

    Returns:
        List
    '''
    result = []
    lines = spec.split('\n')

    for line in lines:
        if len(line) > 0:
            fields = line.strip().split('|')
            field_name = fields[0].strip()
            sql_type = fields[1].strip()
            sql_nullable = fields[2].strip()

            field = DataField(field_name, sql_type, sql_nullable)
            result.append(field)

    return result
# end get_field_spec
