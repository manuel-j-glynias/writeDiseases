"""
This script extracts data from omnidisease  table  and converts
data  to SQL tables:
1. extract_file - extracts data from csv into  a dataframe
2. parse_omnidisease - parses and cleans dataframe
3. write_load_files - writes dataframe to csv file
4. get_schema - gets the schema for SQL table
5. write_sql - writes sql table

6/4/2020
by IK
"""
import mysql.connector
import datetime
import pandas
import csv
from sql_utils import load_table, create_table, does_table_exist, \
    get_local_db_connection, maybe_create_and_select_database, \
    drop_table_if_exists

def main():
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    path = '../data/tblOS_GLOBAL_GLOBAL_Ref_OmniDiseases.csv'

    #Create dataframe
    df=extract_file(path)

    omni_disease_df = parse_omnidisease(df)
    path = '../load_files/omni_diseases.csv'
    write_load_files(omni_disease_df, path)

    db_dict = get_schema('omni_diseases')

    write_sql(db_dict)


# Creates omnidisease  dataframe
# Input: dataframe
# Output: new  dataframe with required fields
def parse_omnidisease(df):
    omnidisease_list = []

    # Parse dataframe
    for index, row in df.iterrows():
        code = row['OmniDiseaseID']
        graph_id = 'omnidisease_' + code.split('_')[1]
        df.at[index, 'graph_id'] = graph_id
    return df


###################################################
#  EXTRACT FILE
###################################################
# Extracts data from the file to a dataframe
# Removes inactive entries
# Input: file path
# Output: dataframe:
def extract_file(path):
    df = pandas.read_csv(path)
    return df

###################################################
#  WRITE CSV
###################################################
# Converts dataframe to csv file
# Input: dataframe and file path
# Output: csv file is written
def write_load_files (df, path):
    try:
        df.to_csv(path, encoding='utf-8', index=False)
    except IOError:
        print("I/O error csv file")


###################################################
#  GET SCHEMA TO CREATE SQL TABLES
###################################################
# Extracts sql table schema from a provided config file
# Input: path to config file
# Ouput: dictionary with
def get_schema(resource):
    db_dict = {}  # {db_name:{table_name:{col:[type,key,allow_null,
    # ref_col_list],'col_order':[cols in order]}}}
    init_file = open('../config/table_descriptions.csv', 'r')
    reader = csv.reader(init_file, quotechar='\"')
    for line in reader:
        # print(line)
        db_name = line[0]
        if (db_name == 'Database'):
            continue
        if (db_name not in db_dict.keys()):
            db_dict[db_name] = {}
        table_name = line[1]

        if (resource not in table_name):
            continue
        col = line[2]
        col_type = line[3]
        col_key = line[4]
        allow_null = line[5]
        auto_incr = line[6]
        ref_col_list = line[7].split('|')  # we will ignore this for now during development
        try:
            ref_col_list.remove('')
        except:
            pass
        try:
            db_dict[db_name][table_name][col] = [col_type, col_key, allow_null, auto_incr, ref_col_list]
            db_dict[db_name][table_name]['col_order'].append(col)
        except:
            db_dict[db_name][table_name] = {col: [col_type, col_key, allow_null, auto_incr, ref_col_list]}
            db_dict[db_name][table_name] = {col: [col_type, col_key, allow_null, auto_incr, ref_col_list],
                                            'col_order': [col]}
    init_file.close()
    return db_dict

###################################################
#  WRITE SQL
###################################################
# Creates and writes sql table
# Input: dictionary with column names
# Output: sql table is created
def write_sql(db_dict):
    my_db = None
    my_cursor = None
    try:
        my_db = get_local_db_connection()
        my_cursor = my_db.cursor(buffered=True)
        for db_name in sorted(db_dict.keys()):
            maybe_create_and_select_database(my_cursor, db_name)
            for table_name in sorted(db_dict[db_name].keys()):
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


if __name__ == "__main__":
    main()