# -*- coding: utf-8 -*-
#===================================================================================================
#
# Name:       data_loader
#
# Purpose:    loads the raw FARS Data into db
#
# Author:     Rob O'Neil
#
# Version:    1.0 - 21 Sep 2017
#
# ==================================================================================================
from __future__ import print_function

import os
import sys
from datetime import datetime
import logging

import utilities as utils
import pypyodbc

FARS_FIELD_SPEC = """
    [STATE]|[tinyint]|NOT NULL
    [ST_CASE]|[int]|NOT NULL
    [VE_TOTAL]|[smallint]|NOT NULL
    [VE_FORMS]|[smallint]|NOT NULL
    [PVH_INVL]|[smallint]|NOT NULL
    [PEDS]|[tinyint]|NOT NULL
    [PERNOTMVIT]|[tinyint]|NOT NULL
    [PERMVIT]|[smallint]|NOT NULL
    [PERSONS]|[smallint]|NOT NULL
    [COUNTY]|[smallint]|NOT NULL
    [CITY]|[smallint]|NOT NULL
    [DAY]|[tinyint]|NOT NULL
    [MONTH]|[tinyint]|NOT NULL
    [YEAR]|[smallint]|NOT NULL
    [DAY_WEEK]|[tinyint]|NOT NULL
    [HOUR]|[tinyint]|NOT NULL
    [MINUTE]|[tinyint]|NOT NULL
    [NHS]|[tinyint]|NOT NULL
    [RUR_URB]|[tinyint]|NOT NULL
    [FUNC_SYS]|[tinyint]|NOT NULL
    [RD_OWNER]|[tinyint]|NOT NULL
    [ROUTE]|[tinyint]|NOT NULL
    [TWAY_ID]|[nvarchar](30)|NOT NULL
    [TWAY_ID2]|[nvarchar](30)|NOT NULL
    [MILEPT]|[int]|NOT NULL
    [LATITUDE]|[decimal](9,6)|NOT NULL
    [LONGITUD]|[decimal](9,6)|NOT NULL
    [SP_JUR]|[tinyint]|NOT NULL
    [HARM_EV]|[tinyint]|NOT NULL
    [MAN_COLL]|[tinyint]|NOT NULL
    [RELJCT1]|[tinyint]|NOT NULL
    [RELJCT2]|[tinyint]|NOT NULL
    [TYP_INT]|[tinyint]|NOT NULL
    [WRK_ZONE]|[tinyint]|NOT NULL
    [REL_ROAD]|[tinyint]|NOT NULL
    [LGT_COND]|[tinyint]|NOT NULL
    [WEATHER1]|[tinyint]|NOT NULL
    [WEATHER2]|[tinyint]|NOT NULL
    [WEATHER]|[tinyint]|NOT NULL
    [SCH_BUS]|[bit]|NOT NULL
    [RAIL]|[varchar](7)|NOT NULL
    [NOT_HOUR]|[tinyint]|NOT NULL
    [NOT_MIN]|[tinyint]|NOT NULL
    [ARR_HOUR]|[tinyint]|NOT NULL
    [ARR_MIN]|[tinyint]|NOT NULL
    [HOSP_HR]|[tinyint]|NOT NULL
    [HOSP_MN]|[tinyint]|NOT NULL
    [CF1]|[tinyint]|NOT NULL
    [CF2]|[tinyint]|NOT NULL
    [CF3]|[tinyint]|NOT NULL
    [FATALS]|[tinyint]|NOT NULL
    [DRUNK_DR]|[tinyint]|NOT NULL
"""

STATES_FIELD_SPEC = """
    [code]|[int]|NOT NULL
    [statename]|[varchar](20)|NOT NULL
"""

def add_crash_datetime(connection_string, table_name):
    '''Alters existing FARS accident table by adding CRASH_DATETIME column
    Args:
        connection_string (string): database connection string
        table_name (string): table to modify
    '''
    logging.info('Adding CRASH_DATETIME to existing %s table', table_name)

    connection = pypyodbc.connect(connection_string)
    cursor = connection.cursor()

    sql = 'ALTER TABLE {} ADD CRASH_DATETIME datetime2(0) NULL'.format(table_name)
    logging.debug('Executing sql:\n%s', sql)

    cursor.execute(sql)
    connection.commit()
    connection.close()

    logging.info('Adding CRASH_DATETIME complete')
# end add_crash_datetime

def update_crash_datetime(connection_string, table_name):
    '''Updates existing table, populating CRASH_DATETIME from existing data

    Args:
        connection_string (string): database connection string
        table_name (string): table to modify

    Returns:
        None
    '''
    logging.info('Updating CRASH_DATETIME in %s table', table_name)

    connection = pypyodbc.connect(connection_string)
    cursor = connection.cursor()

    sql = ('UPDATE {} '
           'SET CRASH_DATETIME = DATETIME2FROMPARTS([year], [month], [day], [hour], [minute], 0, 0, 0) '
           'where [year] <= 2015 and [month] <= 12 and [day] <= 31 and [hour] <= 24 and [minute] <=60'
          ).format(table_name)
    logging.debug('Executing sql:\n%s', sql)

    cursor.execute(sql)
    connection.commit()
    connection.close()

    logging.info('Updating CRASH_DATETIME complete')
# end update_crash_datetime

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

    # debug print config file params
    for key, value in config.items():
        logging.debug('cfg param %s : %s', key, value)

    logging.info("Start time %s", start_time.strftime("%Y-%m-%d %H:%M:%S"))

    connection_string = utils.make_connection_string(
        config['DB_DRIVER'],
        config['DB_SERVER'],
        config['DB_NAME'],
        config['DB_USER'],
        config['DB_PASS'],
        config['DB_TRUSTED']
    )

    # split here
    import_table_name = config['ACCIDENT_IMPORT_TABLENAME']
    fars_field_specs = utils.get_field_spec(FARS_FIELD_SPEC)
    sql = utils.make_create_table_sql(fars_field_specs, import_table_name)
    utils.create_table(connection_string, import_table_name, sql, True)
    utils.bulk_insert_csv_file_to_db(connection_string, import_table_name, config['ACCIDENT_DATAFILE'], 2)
    add_crash_datetime(connection_string, import_table_name)
    update_crash_datetime(connection_string, import_table_name)

    # split here
    import_table_name = config['STATE_CODE_IMPORT_TABLENAME']
    fars_field_specs = utils.get_field_spec(STATES_FIELD_SPEC)
    sql = utils.make_create_table_sql(fars_field_specs, import_table_name)
    utils.create_table(connection_string, import_table_name, sql, True)
    utils.bulk_insert_csv_file_to_db(connection_string, import_table_name, config['STATE_CODE_DATAFILE'], 1)

    # split here
    utils.report_runtime(start_time)

    print('\n')  # blank line after run is done
    return

if __name__ == "__main__":
    main(sys.argv)
