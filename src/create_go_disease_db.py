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
import create_editable_synonyms
import write_sql

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

def main(load_directory):
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    files = ['../data/GO_diseases/diseases_pg1.json',
             '../data/GO_diseases/diseases_pg2.json',
             '../data/GO_diseases/diseases_pg3.json',
             '../data/GO_diseases/diseases_pg4.json',
             '../data/GO_diseases/diseases_pg5.json',
             '../data/GO_diseases/diseases_pg6.json']

    #Create dataframe
    df=combine_files(files)

    #Parse dataframes
    go_disease_df= parse_go_main(df)
    go_parents_df = parse_go_parents(df)
    go_children_df = parse_go_children(df)
    go_xrefs = parse_go_refs(df)
    go_disease_df = parse_go_synonyms(df, go_disease_df)

    path = load_directory + 'go_diseases.csv'
    write_load_files.main(go_disease_df, path)

    path_parents = load_directory + 'go_parents.csv'
    write_load_files.main(go_parents_df, path_parents)

    path_children = load_directory + 'go_children.csv'
    write_load_files.main(go_children_df, path_children)

    path_xrefs = load_directory + 'go_xrefs.csv'
    write_load_files.main(go_xrefs, path_xrefs)

    # path_synonyms = load_directory +  'go_synonyms.csv'
    # write_load_files.main(go_synonyms, path_synonyms)

    # Write sql tables
    db_dict = get_schema.get_schema('go_diseases')
    db_parents_dict = get_schema.get_schema('go_parents')
    db_children_dict = get_schema.get_schema('go_children')
    db_xrefs_dict = get_schema.get_schema('go_xrefs')
    #db_synonyms_dict = get_schema.get_schema('go_synonyms')
    editable_statement_dict = get_schema.get_schema('EditableStatement')
    editable_synonyms_list_dict = get_schema.get_schema('EditableSynonymsList')
    synonym_dict = get_schema.get_schema('Synonym')

    write_sql.write_sql(db_dict, 'go_diseases')
    write_sql.write_sql(db_parents_dict, 'go_parents')
    write_sql.write_sql(db_children_dict, 'go_children')
    write_sql.write_sql(db_xrefs_dict, 'go_xrefs')
    #write_sql.write_sql(db_synonyms_dict, 'go_synonyms')
    write_sql.write_sql(editable_statement_dict, 'EditableStatement')
    write_sql.write_sql(editable_synonyms_list_dict, 'EditableSynonymsList')
    write_sql.write_sql(synonym_dict, 'Synonym')


###################################################
#  EXTRACT DATA FROM FILES TO DATAFRAME
###################################################
# Combines files into a single dataframe
def combine_files( files):
    dataframe_list = []

    # Create a dataframe for each file
    for file in files:
        # Convert json to dataframe
        data = json.load(open(file))
        df = json_normalize(data, 'results')
        dataframe_list.append(df)

    # Clean dataframes and add graph_id column
    dfs_to_combine = []
    for df in dataframe_list:
        # Add new columns and create parent-child and id dictionaries
        for index, row in df.iterrows():
            # Create graph_id column
            df.at[index, 'graph_id'] = ""
            oncotree_code = df.at[index, 'oncotree_code']
            if pandas.isnull(oncotree_code):
                pass
            else:
                codes = df.at[index, 'codes']
                if isinstance(codes, list):
                    codes.append('oncotree=' +  oncotree_code)
                    df.at[index, 'codes'] = codes

            graph_id = 'go_disease_' + df.at[index, 'id']
            df.at[index, 'graph_id'] = graph_id

        #Clean dataframes
        df1 = df[df.graph_id != 'go_0000000-0000-0000-0000-000000000000']
        df2 = df1[['id', 'name', 'definition', 'codes', 'parents', 'children',
                   'synonyms', 'graph_id']].copy(deep=True)
        dfs_to_combine.append(df2)

    #Combine dataframes
    combined_df = pandas.concat(dfs_to_combine, ignore_index=True)
    return combined_df

###################################################
#  PARSE
###################################################
# Transforms dataframe by adding and selecting  the columns
# Input: dataframe
# Output: transformed dataframe
def parse_go_main(df):

    df1 = df[['id', 'name', 'definition', 'graph_id']].copy(deep=True)
    df_editable = create_editable_statement.assign_editable_statement(df1, editable_statement_list, loader_id, load_directory, table_name,id_class)
    return df_editable

# Creates dataframe with parents
# Input: dataframe
# Output: new  dataframe with one parent for each graph_id
def parse_go_parents(df):
    parent_dict_list = []
    name_dict = {}

    #Isolate parents in the list of dicts
    for index, row in df.iterrows():
        parents = row['parents']
        if isinstance(parents, list):
            graph_id  = row['graph_id']
            name =  row['name']
            # Create graph-id - name dict
            name_dict[name] = graph_id
            for parent in parents:
                parent_dict = {}
                parent_dict[graph_id] = parent
                parent_dict_list.append(parent_dict)
    # Create a list of dicts (graph_id vs parent)
    new_parent_dict_list = []
    for dict in parent_dict_list:
        for entry in name_dict:
            for input in dict:
                if dict[input] == entry:
                    new_dict = {}
                    new_dict['graph_id'] = input
                    new_dict['parent' ] = name_dict[entry]
                    new_parent_dict_list.append(new_dict)

     # Create new  parent dataframe
    parent_df = pandas.DataFrame(new_parent_dict_list)
    return parent_df

# Creates dataframe with children
# Input: dataframe
# Output: new  dataframe with children
def parse_go_children(df):
    children_dict_list = []
    name_dict = {}

    #Isolate children in the list of dicts
    for index, row in df.iterrows():
        children = row['children']
        if isinstance(children, list):
            graph_id  = row['graph_id']
            name =  row['name']
            # Create graph-id - name dict
            name_dict[name] = graph_id

            for child in children:
                children_dict = {}
                children_dict[graph_id] = child
                children_dict_list.append(children_dict)
    # Create a list of dicts (graph_id vs child)
    new_children_dict_list = []
    for dict in children_dict_list:
        for entry in name_dict:
            for input in dict:
                if dict[input] == entry:
                    new_dict = {}
                    new_dict['graph_id'] = input
                    new_dict['child' ] = name_dict[entry]
                    new_children_dict_list.append(new_dict)

     # Create new  parent dataframe
    parent_df = pandas.DataFrame(new_children_dict_list)
    return parent_df

# Creates dataframe with references
# Input: dataframe
# Output: new  dataframe with xrefs
def parse_go_refs(df):
    parent_dict = {}
    refs_list = []
    # Get refs
    for index, row in df.iterrows():
        refs = row['codes']
        if isinstance(refs, list):
            for entry in refs:
                new_dict = {}
                new_dict['graph_id'] = row['graph_id']
                new_dict['source'] = entry.split('=')[0]
                new_dict['xrefld'] = entry.split('=')[1]
                refs_list.append(new_dict)
    refs_df = pandas.DataFrame(refs_list)
    return refs_df

# Creates a dataframe with graph_id and corresponding synonym
# Input: original dataframe
#Output: dataframe with graph_id and synonym
def parse_go_synonyms(df, go_disease_df):
    synonyms_list = []
    # Get synonyms
    for index, row in df.iterrows():
        refs = row['synonyms']
        if isinstance(refs, list):
            for entry in refs:
                new_dict = {}
                new_dict['graph_id'] = row['graph_id']
                new_dict['synonym'] = entry
                synonyms_list.append(new_dict)
    synonyms_df = pandas.DataFrame(synonyms_list)
    syn_esl_dict = create_editable_synonyms.assign_editable_synonyms(synonyms_df, loader_id, load_directory, table_name, id_class)

    # Put esl value back to the main dataframe
    for index, row in go_disease_df.iterrows():
        go_disease_df.at[index, 'synonyms'] = ""
        disease_id = row['graph_id']
        if disease_id in syn_esl_dict:
            go_disease_df.at[index, 'synonyms'] = syn_esl_dict[disease_id]
    go_disease_df = go_disease_df[['id', 'name', 'definition', 'synonyms', 'graph_id']]
    return go_disease_df

if __name__ == "__main__":
    main(load_directory)