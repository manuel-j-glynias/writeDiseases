import mysql.connector
import datetime
import pandas

import create_EditableXrefsList
import write_sql
import get_schema
import write_load_files

import create_id
#load_directory = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/'
#loader_id = '007'
editable_statement_list = ['name', 'mainType', 'tissue']
table_name = 'oncotree_diseases'
import create_editable_statement
#id_class = create_id.ID('', '', 0, 0, 0, 0, 0, 0)

import csv
from sql_helpers import get_one_jax_gene, preflight_ref, insert_editable_statement, insert_es_ref, get_loader_user_id
#from sql_utils import get_local_db_connection, maybe_create_and_select_database, does_table_exist, drop_table_if_exists
from sql_utils import load_table, create_table, does_table_exist, get_local_db_connection, maybe_create_and_select_database, drop_table_if_exists
import json
import sys
import os

def main(load_directory, loader_id, id_class):
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    file = ('../data/onctoree.json')

    #Convert json to dataframe
    df = pandas.read_json(file)

    #Parse dataframes
    oncotree_df= parse_oncotree_main(df, load_directory, loader_id, id_class)
    xrefs_editable = create_EditableXrefsList.assign_editable_xrefs_lists(oncotree_df, loader_id, load_directory,
                                                                          id_class)
    oncotree_disease_with_xrefs = add_column_to_dataframe(oncotree_df, xrefs_editable, 'xrefs')
    oncotree_df = oncotree_disease_with_xrefs[
        ['code', 'name', 'mainType', 'tissue',  'xrefs', 'graph_id']]

    path = load_directory + 'oncotree_diseases.csv'
    write_load_files.main(oncotree_df, path)

    oncotree_df_parent = parse_oncotree_parents(df)
    oncotree_df_parent = combine_parents_and_children(oncotree_df_parent, 'parent')
    path_parents =load_directory +  'oncotree_parents.csv'
    write_load_files.main(oncotree_df_parent, path_parents)

    oncotree_df_children = parse_oncotree_children(df)
    oncotree_df_children = combine_parents_and_children(oncotree_df_children, 'child')
    path_children = load_directory +  'oncotree_children.csv'
    write_load_files.main(oncotree_df_children, path_children)


    # Write sql tables
    db_dict = get_schema.get_schema('oncotree_diseases')
    db_dict_parents = get_schema.get_schema('oncotree_parents')
    db_dict_children = get_schema.get_schema('oncotree_children')

    write_sql.write_sql(db_dict, 'oncotree_diseases')
    write_sql.write_sql(db_dict_parents, 'oncotree_parents')
    write_sql.write_sql(db_dict_children, 'oncotree_children')

    print(datetime.datetime.now().strftime("%H:%M:%S"))


###################################################
#  PARSE
###################################################

# Transforms dataframe by adding and filling the columns
# Input: dataframe
# Output: transformed dataframe
def parse_oncotree_main(df, load_directory, loader_id, id_class):

    id_dict = {}
    parent_dict = {}

    # Add new columns and create parent-child and id dictionaries
    for index, row in df.iterrows():
        # Add children column to dataframe
        graph_id = 'oncotree_disease_' + df.at[index, 'code']
        df.at[index, 'graph_id'] = graph_id
        refs = row['externalReferences']
        if refs == {}:
            df.at[index, 'externalReferences'] = ""
        else:
            refs_list = []
            for entry in refs:
                temp_dict = {}
                temp_dict[entry] =refs[entry][0]
                refs_list.append(temp_dict)
            df.at[index, 'externalReferences'] = refs_list

    onco_with_editable = create_editable_statement.assign_editable_statement(df,
                                                                             editable_statement_list, loader_id,
                                                                             load_directory, table_name, id_class)
    onco_with_editable = onco_with_editable.rename(columns={'externalReferences': 'xrefs'})
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
                new_dict['xrefId'] =  refs_dict[entry][0]
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

def add_column_to_dataframe(df_in_need, column_dict, column):
   #Put esl value back to the main dataframe
   for index, row in df_in_need.iterrows():
       df_in_need.at[index, column] = ""
       disease_id = row['graph_id']
       if disease_id in column_dict:
           df_in_need.at[index, column] = column_dict[disease_id]
   return df_in_need

def combine_parents_and_children(df, column):
    input_dict = {}
    input_list = []
    for index, row in df.iterrows():
        disease_id = row['graph_id']
        parent_or_child = row[column]
        if pandas.isnull(parent_or_child):
            pass
        else:
            if disease_id in input_dict:
                disease_list = input_dict[disease_id]
                disease_list.append(parent_or_child)
                input_dict[disease_id] = disease_list
            else:
                input_dict[disease_id] = [parent_or_child]
    for entry in input_dict:
        temp_dict = {}
        parent_or_child_list = input_dict[entry]
        if parent_or_child_list:
            pipe_strings = '|'.join(parent_or_child_list)
            temp_dict['graph_id'] = entry
            temp_dict[column] = pipe_strings
            input_list.append(temp_dict)
    df = pandas.DataFrame(input_list)
    return df

#if __name__ == "__main__":
    #main(load_directory, load_id, id_class)