"""
This script extracts data from tcode table  and converts
data  to SQL tables:
1. extract_file - extracts data from csv into  a dataframe
2. parse_tcode_main - parses and cleans dataframe
3. write_load_files - writes dataframe to csv file
4. get_schema - gets the schema for SQL table
5. write_sql - writes sql table

6/4/2020
by IK
"""
import mysql.connector
import datetime
import pandas
import write_load_files
import csv
import get_schema
import write_sql
import create_id

load_directory = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/'
loader_id = '007'
editable_statement_list = ['tissuePath']
table_name = 'tcode_diseases'
import create_editable_statement
id_class = create_id.ID('', '', 0, 0, 0, 0, 0, 0)

from sql_utils import load_table, create_table, does_table_exist, \
    get_local_db_connection, maybe_create_and_select_database, \
    drop_table_if_exists

def main(load_directory):
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    path = '../data/tblOS_GLOBAL_GLOBAL_Ref_TCodes.csv'

    #Create dataframe
    df=extract_file(path)

    tcode_disease_df = parse_tcode_main(df)
    path = load_directory + 'tcode_diseases.csv'
    write_load_files.main(tcode_disease_df, path)

    tcode_parents_df = parse_tcode_parents(df)
    path_parents  =load_directory + 'tcode_parents.csv'
    write_load_files.main(tcode_parents_df, path_parents)

    tcode_children_df = parse_tcode_children(df)
    path_children = load_directory + 'tcode_children.csv'
    write_load_files.main(tcode_children_df, path_children)

    db_dict = get_schema.get_schema('tcode_diseases')
    db_parents_dict = get_schema.get_schema('tcode_parents')
    db_children_dict = get_schema.get_schema('tcode_children')

    write_sql.write_sql(db_dict, 'tcode_diseases')
    write_sql.write_sql(db_parents_dict, 'tcode_parents')
    write_sql.write_sql(db_children_dict, 'tcode_children')

# Creates  tcode dataframe
# Input: dataframe
# Output: new  dataframe with required fields
def parse_tcode_main(df):
    tcode_list = []

    # Parse dataframe
    for index, row in df.iterrows():
        new_dict = {}
        code = row['TCode']
        new_dict['tcode'] = code
        new_dict['tissuePath'] = row['TissuePath']
        graph_id = 'Tcode_' + code
        new_dict['graph_id'] = graph_id
        df.at[index, 'graph_id'] = graph_id
        tcode_list.append(new_dict)
    tcode_df = pandas.DataFrame(tcode_list)
    tcode_with_editable = create_editable_statement.assign_editable_statement(tcode_df,
                                                                              editable_statement_list, loader_id,
                                                                              load_directory, table_name, id_class)
    return tcode_with_editable

# Creates  tcode dataframe
# Input: dataframe
# Output: new  dataframe with required fields
def parse_tcode_parents(df):
    tcode_parent_dict_list = []
    # Parse dataframe
    for index, row in df.iterrows():
        parent = row['ParentTCode']
        if  pandas.isnull(parent):
            pass
        else:
            new_dict = {}
            new_dict['graph_id'] = row['graph_id']
            new_dict['parent'] =  'Tcode_' + row['ParentTCode']
            tcode_parent_dict_list.append(new_dict)
    tcode_df = pandas.DataFrame(tcode_parent_dict_list)
    return tcode_df

# Creates  tcode dataframe
# Input: dataframe
# Output: new  dataframe with required fields
def parse_tcode_children(df):
    tcode_child_dict_list = []
    # Create tcode_parent dictionary
    for index, row in df.iterrows():
        parent = row['ParentTCode']
        if  pandas.isnull(parent):
            pass
        else:
            new_dict = {}
            new_dict['graph_id'] = 'Tcode_' + row['ParentTCode']
            new_dict['child'] = row['graph_id']
            # Add dictionary to a list
            tcode_child_dict_list.append(new_dict)
    #Convert tcode_child_dict list to a dataframe
    tcode_df = pandas.DataFrame(tcode_child_dict_list)
    return tcode_df

###################################################
#  EXTRACT FILE
###################################################
# Extracts data from the file to a dataframe
# Removes inactive entries
# Input: file path
# Output: dataframe:
def extract_file(path):
    unparsed_df = pandas.read_csv(path)
    df =unparsed_df[unparsed_df.Active_Flag != 0]
    return df


if __name__ == "__main__":
    main(load_directory)