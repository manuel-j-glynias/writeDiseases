import mysql.connector
import datetime
import pandas
import csv
from sql_utils import load_table, create_table, does_table_exist, \
    get_local_db_connection, maybe_create_and_select_database, \
    drop_table_if_exists

###################################################
#  WRITE SQL
###################################################
# Creates and writes sql table
# Input: dictionary with column names
# Output: sql table is created
def write_sql(db_dict, table_name):
    my_db = None
    my_cursor = None
    try:
        my_db = get_local_db_connection()
        my_cursor = my_db.cursor(buffered=True)
        for db_name in sorted(db_dict.keys()):
            maybe_create_and_select_database(my_cursor, db_name)
            #for table_name in sorted(db_dict[db_name].keys()):
            drop_table_if_exists(my_cursor, table_name)
            create_table(my_cursor, table_name, db_name, db_dict)
            load_table(my_cursor, table_name,
                       db_dict[db_name][table_name]['col_order'])
            my_db.commit()
    except mysql.connector.Error as error:
        print("Failed in MySQL: {}".format(error))
    finally:
        if (my_db.is_connected()):
            my_cursor.close()
