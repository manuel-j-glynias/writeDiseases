"""
This script extracts data from GO_diseases files and converts
the files to SQL tables:
1. combine_files - extracts data from json  files to a dataframe
2. parse_go_main - parses and cleans dataframe
3. write_load_files - writes dataframe to csv file
4. get_schema - gets the schema for SQL table
5. write_sql - writes sql table

6/3/2020
by IK
"""
import mysql.connector
import create_id
import datetime
import pandas
from pandas.io.json import json_normalize
import write_load_files
import create_editable_statement
import create_EditableStringList
import write_sql
import create_EditableXrefsList

editable_statement_list = ['name', 'definition']
editable_synonyms_list = ['synonyms']
loader_id = '007'
load_directory = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/'
table_name = 'GoDiseases'
id_class = create_id.ID('', '')


import csv
from sql_utils import load_table, create_table, does_table_exist, \
    get_local_db_connection, maybe_create_and_select_database, \
    drop_table_if_exists
import json
import get_schema

def parse_go():
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    files = ['../data/GO_diseases/diseases_pg1.json',
             '../data/GO_diseases/diseases_pg2.json',
             '../data/GO_diseases/diseases_pg3.json',
             '../data/GO_diseases/diseases_pg4.json',
             '../data/GO_diseases/diseases_pg5.json',
             '../data/GO_diseases/diseases_pg6.json']

    #Create dataframe
    df=create_go_dataframe(files)
    return df

def create_go_dataframe( files):

    dataframe_list = list_of_dfs_to_combine(files)
    go_df = combine_dataframes(dataframe_list)
    return go_df

#  Create a list of dataframes to combine
def list_of_dfs_to_combine(files):
    dataframe_list = []
    # Create a dataframe for each file
    for file in files:
        # Convert json to dataframe
        data = json.load(open(file))
        df = json_normalize(data, 'results')
        dataframe_list.append(df)
    return dataframe_list

def combine_dataframes(dataframe_list):
    # Clean dataframes and add graph_id column
    dfs_to_combine = []
    counter = 1
    for df in dataframe_list:
        # Add new columns and create parent-child and id dictionaries
        for index, row in df.iterrows():
            # Create graph_id column
            df.at[index, 'graph_id'] = ''
            df.at[index, 'jaxDiseases'] = ''
            df.at[index, 'doDiseases'] = ''
            df.at[index, 'goDiseases'] = ''
            df.at[index, 'oncoTreeDiseases'] = ''

            code_list = row[ 'codes']
            if type(code_list) is list:
                if code_list:
                    jax = [j for j in code_list if 'JAX-CKB=' in j]
                    if jax:
                        jax_list = []
                        for jax_disease in jax:
                            jax_id = 'jax_disease_' +  jax_disease.replace('JAX-CKB=JAX', '')
                            jax_list.append(jax_id)
                        df.at[index, 'jaxDiseases'] = jax_list
                    else:
                        df.at[index, 'jaxDiseases'] = jax

                do = [d for d in code_list if 'DOID=' in d]
                if do:
                    do_list = []
                    for do_disease in do:
                        do_id = 'do_disease_' + do_disease.replace('DOID=', '')
                        do_list.append(do_id)
                    df.at[index, 'doDiseases'] = do_list
                else:
                    df.at[index, 'doDiseases'] = do

            else:
                temp_list = []
                df.at[index, 'jaxDiseases'] = temp_list
                df.at[index, 'doDiseases'] = temp_list

            df.at[index, 'graph_id'] = 'ontological_disease_' + str(counter)
            counter += 1

            oncotree_code = row['oncotree_code']
            if pandas.isnull(oncotree_code):
                onco_list = []
                df.at[index, 'oncoTreeDiseases'] = onco_list
            else:
                if type(oncotree_code) is list:
                    if oncotree_code:
                        onco_list = []
                        for onco_disease  in oncotree_code:
                            onco_id = 'oncotree_disease_' + onco_disease
                            onco_list.append(onco_id)
                            df.at[index, 'oncoTreeDiseases'] = onco_list
                else:
                    onco_list = []
                    onco_id = 'oncotree_disease_' + oncotree_code
                    onco_list.append(onco_id)
                    df.at[index, 'oncoTreeDiseases'] = onco_list

            go_id = 'go_disease_' + row[ 'id']
            go_list = []
            go_list.append(go_id)
            df.at[index, 'goDiseases']  = go_list

    # Clean dataframes
    df1 = df[df.graph_id != 'go_0000000-0000-0000-0000-000000000000']
    df2 = df1[['name', 'definition',  'graph_id', 'jaxDiseases', 'doDiseases', 'goDiseases', 'oncoTreeDiseases']].copy(deep=True)

    dfs_to_combine.append(df2)

    # Combine dataframes
    combined_df = pandas.concat(dfs_to_combine, ignore_index=True)
    return combined_df

if __name__ == "__main__":
    parse_go()