"""
This script extracts data from mcode table  and converts
data  to SQL tables:
1. extract_file - extracts data from csv into  a dataframe
2. parse_mcode_main - parses and cleans dataframe
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
import write_sql
import get_schema
import write_load_files
import create_id
#load_directory = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/'
#loader_id = '007'
editable_statement_list = ['diseasePath']
table_name = 'mcode_diseases'
import create_editable_statement
#id_class = create_id.ID('', '', 0, 0, 0, 0, 0, 0)


def main(load_directory, loader_id, id_class):
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    path = '../data/tblOS_GLOBAL_GLOBAL_Ref_MCodes.csv'

    #Create dataframe
    df=extract_file(path)

    mcode_disease_df = parse_mcode_main(df, load_directory, loader_id, id_class)
    path = load_directory + 'mcode_diseases.csv'
    write_load_files.main(mcode_disease_df, path)

    mcode_parents_df = parse_mcode_parents(df)
    mcode_parents_df = combine_parents_and_children(mcode_parents_df, 'parent')
    path_parents  = load_directory + 'mcode_parents.csv'
    write_load_files.main(mcode_parents_df, path_parents)

    mcode_children_df = parse_mcode_children(df)
    mcode_children_df = combine_parents_and_children(mcode_children_df, 'child')
    path_children = load_directory + 'mcode_children.csv'
    write_load_files.main(mcode_children_df, path_children)


    db_dict = get_schema.get_schema('mcode_diseases')
    db_parents_dict = get_schema.get_schema('mcode_parents')
    db_children_dict = get_schema.get_schema('mcode_children')

    write_sql.write_sql(db_dict, table_name)
    write_sql.write_sql(db_parents_dict, 'mcode_parents')
    write_sql.write_sql(db_children_dict, 'mcode_children')


# Creates  mcode dataframe
# Input: dataframe
# Output: new  dataframe with required fields
def parse_mcode_main(df, load_directory, loader_id, id_class):
    mcode_list = []

    # Parse dataframe
    for index, row in df.iterrows():
        new_dict = {}
        code = row['Mcode']
        new_dict['mcode'] = code
        new_dict['diseasePath'] = row['DiseasePath']
        new_dict['omniDisease'] =  row['OmniDisease_ID']
        graph_id = 'Mcode_' + code
        new_dict['graph_id'] = graph_id
        df.at[index, 'graph_id'] = graph_id
        mcode_list.append(new_dict)
    mcode_df = pandas.DataFrame(mcode_list)
    mcode_with_editable = create_editable_statement.assign_editable_statement(mcode_df,
                                                                      editable_statement_list, loader_id,
                                                                      load_directory, table_name, id_class)
    return mcode_with_editable

# Creates  mcode dataframe
# Input: dataframe
# Output: new  dataframe with required fields
def parse_mcode_parents(df):
    mcode_parent_dict_list = []
    # Parse dataframe
    for index, row in df.iterrows():
        parent = row['ParentMCode']
        if  pandas.isnull(parent):
            pass
        else:
            new_dict = {}
            new_dict['graph_id'] = row['graph_id']
            new_dict['parent'] =  'Mcode_' + row['ParentMCode']
            mcode_parent_dict_list.append(new_dict)
    mcode_df = pandas.DataFrame(mcode_parent_dict_list)
    return mcode_df

# Creates  mcode dataframe
# Input: dataframe
# Output: new  dataframe with required fields
def parse_mcode_children(df):
    mcode_child_dict_list = []
    # Create mcode_parent dictionary
    for index, row in df.iterrows():
        parent = row['ParentMCode']
        if  pandas.isnull(parent):
            pass
        else:
            new_dict = {}
            new_dict['graph_id'] = 'Mcode_' + row['ParentMCode']
            new_dict['child'] = row['graph_id']
            # Add dictionary to a list
            mcode_child_dict_list.append(new_dict)
    #Convert mcode_child_dict list to a dataframe
    mcode_df = pandas.DataFrame(mcode_child_dict_list)
    return mcode_df

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

def combine_parents_and_children(df, column):
    input_dict = {}
    input_list = []
    for index, row in df.iterrows():
        disease_id = row['graph_id']
        parent_or_child = row[column]
        if disease_id in input_dict:
            disease_list = input_dict[disease_id]
            disease_list.append(parent_or_child)
            input_dict[disease_id] = disease_list
        else:
            input_dict[disease_id] = [parent_or_child]
    for entry in input_dict:
        temp_dict = {}
        parent_or_child_list = input_dict[entry]
        pipe_strings = '|'.join(parent_or_child_list)
        temp_dict['graph_id'] = entry
        temp_dict[column] = pipe_strings
        input_list.append(temp_dict)
    df = pandas.DataFrame(input_list)
    return df

#if __name__ == "__main__":
    #main(load_directory, loader_id, id_class)