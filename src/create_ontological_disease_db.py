"""
This script extracts diseases from GO_diseases files and creates an ontological_diseases table
that contains ontological diseases and corresponding diseases from other sources (jax, do, oncotree, go):
It includes the following steps:
1. parse_go: Using combine_dataframe methods
2. create_EditableDiseaseList - creates editable disease lists and assigns id for each list
3. create_editable_statement - assigns editable statements to appropriate columns
7/4/2020
IK
"""
import mysql.connector
import datetime
import pandas
from pandas.io.json import json_normalize
import json


import create_id
import create_EditableDiseaseList
import create_editable_statement
import create_EditableXrefsList

# Variables
files = ['../data/GO_diseases/diseases_pg1.json',
         '../data/GO_diseases/diseases_pg2.json',
         '../data/GO_diseases/diseases_pg3.json',
         '../data/GO_diseases/diseases_pg4.json',
         '../data/GO_diseases/diseases_pg5.json',
         '../data/GO_diseases/diseases_pg6.json']

# Required columns in ontological_disease table
cols = ['name', 'definition',  'graph_id', 'jaxDiseases', 'doDiseases', 'goDiseases', 'oncoTreeDiseases']

# Names for fields for disease elements tables
names = ['jax_disease_', 'do_disease_', 'go_disease_', 'oncotree_disease_', 'ontological_disease']

# Columns that contain editable statemens in ontological_disease table
editable_statement_list = ['name', 'description']

table_name = 'ontological_diseases'

# How corresponding diseases appear in go data file
jax_code = 'JAX-CKB=JAX'
do_code = 'DOID='
oncotree_code_column = 'oncotree_code'
codes = 'codes'
results = 'results'
id = 'id'
definition = 'definition'
description = 'description'
#load_directory = '../load_files/'
#loader_id = '007'
temporary_xref_path_go = '../load_files/go_xrefs.csv'
#id_class = create_id.ID('', '',  0, 0, 0, 0, 0, 0)



def main(load_directory, loader_id, id_class):
    # Create a dataframe
    df = parse_go()
    xrefs_temporary_df = create_xrefs(temporary_xref_path_go, df)
    xref_editable = create_EditableXrefsList.assign_editable_xrefs_lists(xrefs_temporary_df, loader_id, load_directory, id_class)
    # Create editable diseases lists and add it to a dataframe
    ontological_df = create_EditableDiseaseList.assign_editable_disease_lists(df, loader_id, load_directory, id_class)

    # Add editable statements to a dataframe
    ontological_df_with_statements = create_editable_statement.assign_editable_statement(ontological_df,
                                                                             editable_statement_list, loader_id,
                                                                             load_directory, table_name, id_class)
    ontological_df_with_statements = add_column_to_dataframe(ontological_df_with_statements, xref_editable, 'xrefs')

    ontological_df_with_statements.to_csv('../load_files/ontological_diseases.csv')
    return ontological_df_with_statements


def parse_go():
    #  Create a list of dataframes to combine
    dataframe_list = list_of_dfs_to_combine(files)
    # Converts files to dataframes and combines dataframes
    go_df = combine_dataframes(dataframe_list)
    return go_df


# Add diseases extracted from GO files to a ontological disease dataframe
def add_disease_to_df(code_list, df_to_edit, index, substring, id_string, column_name):
    diseases = [l for l in code_list if substring in l]
    if diseases:
        final_disease_list = []
        for disease in diseases:
            disease_id = id_string + disease.replace(substring, '')
            final_disease_list.append(disease_id)
        df_to_edit.at[index, column_name] = final_disease_list
    else:
        df_to_edit.at[index, column_name] = diseases
    return df_to_edit


#  Create a list of dataframes to combine
def list_of_dfs_to_combine(files):
    dataframe_list = []
    # Create a dataframe for each file
    for file in files:
        # Convert json to dataframe
        data = json.load(open(file))
        df = json_normalize(data, results)
        dataframe_list.append(df)
    return dataframe_list

# Combines dataframes
def combine_dataframes(dataframe_list):
    # Clean dataframes and add graph_id column
    dfs_to_combine = []
    counter = 1
    for df in dataframe_list:
        # Add new columns and create parent-child and id dictionaries
        for index, row in df.iterrows():
            # Create graph_id column
            df.at[index, cols[2]] = ''
            df.at[index, cols[3]] = ''
            df.at[index,cols[4]] = ''
            df.at[index, cols[5]] = ''
            df.at[index, cols[6]] = ''

            # Get diseases from 'codes' column
            code_list = row[codes]
            if type(code_list) is list:
                if code_list:
                    df = add_disease_to_df(code_list, df, index, jax_code, names[0], cols[3])
                    df = add_disease_to_df(code_list, df, index, do_code, names[1], cols[4])
            else:
                temp_list = []
                df.at[index, cols[3]] = temp_list
                df.at[index, cols[4]] = temp_list

            # Assign ontological_disease id
            df.at[index, cols[2]] = names[4] + str(counter)
            counter += 1

            # Get info rom oncotree column
            oncotree_code = row[oncotree_code_column]
            if pandas.isnull(oncotree_code):
                onco_list = []
                df.at[index, cols[6]] = onco_list
            else:
                if type(oncotree_code) is list:
                    if oncotree_code:
                        df = add_disease_to_df(oncotree_code, df, index, '', names[3], cols[6])
                else:
                    onco_list = []
                    onco_id = names[3] + oncotree_code
                    onco_list.append(onco_id)
                    df.at[index, cols[6]] = onco_list

            # Assign go_disease id (using go diseases as data for ontological diseases table)
            go_id = names[2] + row[id]
            go_list = []
            go_list.append(go_id)
            df.at[index, cols[5]]  = go_list

        # Clean dataframes
        df1 = df[df.graph_id != 'go_0000000-0000-0000-0000-000000000000']
        df2 = df1[cols].copy(deep=True)
        df2 = df2.rename(columns={definition: description})
        dfs_to_combine.append(df2)

    # Combine dataframes
    combined_df = pandas.concat(dfs_to_combine, ignore_index=True)

    return combined_df

def create_xrefs(temporary_xref_path_go, df):
    temp_list = []
    xrefs_df = pandas.read_csv(temporary_xref_path_go)
    temp_dict = {}
    for index, row  in df.iterrows():
        graph_id = row['graph_id']
        go_id = row['goDiseases'][0]
        temp_dict[go_id] = graph_id

    for index, row in xrefs_df.iterrows():
        graph_id = row['graph_id']
        if graph_id in temp_dict:
            onto_id = temp_dict[graph_id]
            xrefs_df.at[ index, 'graph_id'] = onto_id
        else:
            pass
    return xrefs_df

def add_column_to_dataframe(df_in_need, column_dict, column):
   #Put esl value back to the main dataframe
   for index, row in df_in_need.iterrows():
       df_in_need.at[index, column] = ""
       disease_id = row['graph_id']
       if disease_id in column_dict:
           df_in_need.at[index, column] = column_dict[disease_id]
   return df_in_need


#if __name__ == "__main__":
    #main(load_directory)