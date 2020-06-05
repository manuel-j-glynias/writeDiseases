import mysql.connector
import datetime
import pandas

import csv
from sql_helpers import get_one_jax_gene, preflight_ref, insert_editable_statement, insert_es_ref, get_loader_user_id
#from sql_utils import get_local_db_connection, maybe_create_and_select_database, does_table_exist, drop_table_if_exists
from sql_utils import load_table, create_table, does_table_exist, get_local_db_connection, maybe_create_and_select_database, drop_table_if_exists
import json
import sys
import os

def main():
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    file = ('../data/onctoree.json')

    #Convert json to dataframe
    df = pandas.read_json(file)

    #Parse dataframes
    oncotree_df= parse_oncotree_main(df)
    path = '../load_files/oncotree_diseases.csv'
    write_load_files_oncotree(oncotree_df, path)

    oncotree_df_refs = parse_oncotree_refs(df)
    path_refs =  '../load_files/oncotree_xrefs.csv'
    write_load_files_oncotree(oncotree_df_refs, path_refs)

    oncotree_df_parent = parse_oncotree_parents(df)
    path_parents = '../load_files/oncotree_parents.csv'
    write_load_files_oncotree(oncotree_df_parent, path_parents)

    oncotree_df_children = parse_oncotree_children(df)
    path_children = '../load_files/oncotree_children.csv'
    write_load_files_oncotree(oncotree_df_children, path_children)

    # Write sql tables
    db_dict = get_schema( 'oncotree_diseases')
    db_dict_refs = get_schema( 'oncotree_xrefs')
    db_dict_parents = get_schema('oncotree_parents')
    db_dict_children = get_schema('oncotree_children')

    write_sql(db_dict)
    write_sql(db_dict_refs)
    write_sql(db_dict_parents)
    write_sql(db_dict_children)

    print(datetime.datetime.now().strftime("%H:%M:%S"))


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
                load_table(my_cursor, table_name, db_dict[db_name][table_name]['col_order'])
                my_db.commit()
    except mysql.connector.Error as error:
        print("Failed in MySQL: {}".format(error))
    finally:
        if (my_db.is_connected()):
            my_cursor.close()

###################################################
#  WRITE CSV
###################################################

# Converts dataframe to csv file
# Input: dataframe and file path
# Output: csv file is written
def write_load_files_oncotree (df, path):
    try:
        df.to_csv(path, encoding='utf-8', index=False)
    except IOError:
        print("I/O error csv file")

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
    return df1

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


# Extracts sql table schema from a provided config file
# Input: path to config file
# Ouput: dictionary with
def get_schema(resource):
    db_dict = {}  # {db_name:{table_name:{col:[type,key,allow_null,ref_col_list],'col_order':[cols in order]}}}
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
    # print(db_dict)
    return db_dict

if __name__ == "__main__":
    main()