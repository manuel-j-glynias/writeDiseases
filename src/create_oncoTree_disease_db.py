import mysql.connector
import datetime
import pandas
import write_sql
import get_schema
import write_load_files

import create_id
load_directory = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/'
loader_id = '007'
editable_statement_list = ['name', 'mainType', 'tissue']
table_name = 'oncotree_diseases'
import create_editable_statement
id_class = create_id.ID('', '')

import csv
from sql_helpers import get_one_jax_gene, preflight_ref, insert_editable_statement, insert_es_ref, get_loader_user_id
#from sql_utils import get_local_db_connection, maybe_create_and_select_database, does_table_exist, drop_table_if_exists
from sql_utils import load_table, create_table, does_table_exist, get_local_db_connection, maybe_create_and_select_database, drop_table_if_exists
import json
import sys
import os

def main(load_directory):
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    file = ('../data/onctoree.json')

    #Convert json to dataframe
    df = pandas.read_json(file)

    #Parse dataframes
    oncotree_df= parse_oncotree_main(df)
    path = load_directory +  'oncotree_diseases.csv'
    write_load_files.main(oncotree_df, path)

    oncotree_df_refs = parse_oncotree_refs(df)
    path_refs = load_directory +  'oncotree_xrefs.csv'
    write_load_files.main(oncotree_df_refs, path_refs)

    oncotree_df_parent = parse_oncotree_parents(df)
    path_parents =load_directory +  'oncotree_parents.csv'
    write_load_files.main(oncotree_df_parent, path_parents)

    oncotree_df_children = parse_oncotree_children(df)
    path_children = load_directory +  'oncotree_children.csv'
    write_load_files.main(oncotree_df_children, path_children)


    # Write sql tables
    db_dict = get_schema.get_schema('oncotree_diseases')
    db_dict_refs = get_schema.get_schema( 'oncotree_xrefs')
    db_dict_parents = get_schema.get_schema('oncotree_parents')
    db_dict_children = get_schema.get_schema('oncotree_children')

    write_sql.write_sql(db_dict, 'oncotree_diseases')
    write_sql.write_sql(db_dict_refs, 'oncotree_xrefs')
    write_sql.write_sql(db_dict_parents, 'oncotree_parents')
    write_sql.write_sql(db_dict_children, 'oncotree_children')

    print(datetime.datetime.now().strftime("%H:%M:%S"))


###################################################
#  PARSE
###################################################

# Transforms dataframe by adding and filling the columns
# Input: dataframe
# Output: transformed dataframe
def parse_oncotree_main(df):

    id_dict = {}
    parent_dict = {}

    # Add new columns and create parent-child and id dictionaries
    for index, row in df.iterrows():
        # Add children column to dataframe
        df.at[index, 'children'] = []
        df.at[index, 'graph_id'] = ""
        graph_id = 'oncotree_disease_' + df.at[index, 'code']
        df.at[index, 'graph_id'] = graph_id
        # Create parent child dict
        parent_dict[graph_id] = row['parent']
        id_dict[row['code']] = graph_id


    for index, row in df.iterrows():
        parent_entry = df.at[index, 'parent']

        #Change parent value from code to id
        if parent_entry:
            parent_entry_id  =id_dict[parent_entry]
            df.at[index, 'parent'] = parent_entry_id

        # Assign children to appropriate parents
        for entry in parent_dict:
            parent = parent_dict[entry]
            child = entry
            if parent == row['code']:
                children = df.at[index, 'children']
                children.append(child)
                df.at[index, 'children'] = children

    # Copy and change dataframe to write csv
    column_to_delete  = ['color', 'externalReferences', 'history', 'level', 'revocations', 'precursors', 'parent', 'children']
    df1 = df.copy(deep = True)
    for column in column_to_delete:
        del df1[column]
    onco_with_editable = create_editable_statement.assign_editable_statement(df1,
                                                                             editable_statement_list, loader_id,
                                                                             load_directory, table_name, id_class)
    return onco_with_editable

# Creates dataframe with references
# Input: dataframe
# Output: new  dataframe with xrefs
def parse_oncotree_refs(df):
    parent_dict = {}
    refs_list = []
    # Get refs
    for index, row in df.iterrows():
        # Dictionary for references
        refs_dict = {}
        refs_dict = row['externalReferences']
        if not refs_dict:
            pass
        else:
            for entry in refs_dict:
                new_dict = {}
                new_dict['graph_id'] = row['graph_id']
                new_dict['source'] = entry
                new_dict['xrefld'] =  refs_dict[entry][0]
                refs_list.append(new_dict)
    refs_df = pandas.DataFrame(refs_list)
    return refs_df

# Creates dataframe with parents
# Input: dataframe
# Output: new  dataframe with parents
def parse_oncotree_parents(df):
    columns = ['graph_id', 'parent']
    df2 = pandas.DataFrame(columns=columns)
    df2['graph_id'] = df['graph_id']
    df2['parent'] = df['parent']
    return df2

# Creates dataframe with children
# Input: dataframe
# Output: new  dataframe with children
def parse_oncotree_children(df):
    children_list = []
    for index, row in df.iterrows():
        children = df.at[index, 'children']
        for entry in children:
            children_dict = {}
            children_dict['graph_id'] = row['graph_id']
            children_dict['child'] = entry
            children_list.append(children_dict)
    df_children = pandas.DataFrame(children_list)
    return df_children


if __name__ == "__main__":
    main(load_directory)