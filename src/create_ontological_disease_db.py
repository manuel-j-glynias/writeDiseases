import mysql.connector
import datetime
import pandas
import extract_ontological
from pandas.io.json import json_normalize
import csv
from sql_utils import load_table, create_table, does_table_exist, \
    get_local_db_connection, maybe_create_and_select_database, \
    drop_table_if_exists
import json
import write_sql
import get_schema
import create_id
import create_EditableDiseaseList
load_directory = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/'
loader_id = '007'
editable_statement_list = ['name', 'description']
table_name = 'ontological_diseases'
import create_editable_statement
id_class = create_id.ID('', '')
import extract_ontological


def main(load_directory):
    ontological_df = extract_ontological.parse_go()
    ontological_df_with_statements = ontological_df = create_editable_statement.assign_editable_statement(ontological_df,
                                                                             editable_statement_list, loader_id,
                                                                             load_directory, table_name, id_class)
    ontological_df_with_disease_listst = assign_disease_lists()
    return ontological_df

def assign_disease_lists():
    print()
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


if __name__ == "__main__":
    main(load_directory)