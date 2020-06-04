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
import datetime
import pandas
from pandas.io.json import json_normalize
import csv
from sql_utils import load_table, create_table, does_table_exist, \
    get_local_db_connection, maybe_create_and_select_database, \
    drop_table_if_exists
import json

def main():
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
    path = '../load_files/go_diseases.csv'
    write_load_files(go_disease_df, path)

    go_parents_df = parse_go_parents(df)
    path_parents = '../load_files/go_parents.csv'
    write_load_files(go_parents_df, path_parents)

    go_children_df = parse_go_children(df)
    path_children = '../load_files/go_children.csv'
    write_load_files(go_children_df, path_children)

    go_xrefs = parse_go_refs(df)
    path_xrefs = '../load_files/go_xrefs.csv'
    write_load_files(go_xrefs, path_xrefs)

    go_synonyms = parse_go_synonyms(df)
    path_synonyms = '../load_files/go_synonyms.csv'
    write_load_files(go_synonyms, path_synonyms)

    # Write sql tables
    db_dict = get_schema('go_diseases')
    db_parents_dict = get_schema('go_parents')
    db_children_dict = get_schema('go_children')
    db_xrefs_dict = get_schema('go_xrefs')
    db_synonyms_dict = get_schema('go_synonyms')

    write_sql(db_dict)
    write_sql(db_parents_dict)
    write_sql(db_children_dict)
    write_sql(db_xrefs_dict)
    write_sql(db_synonyms_dict)

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
            graph_id = 'go_disease_' + df.at[index, 'id']
            df.at[index, 'graph_id'] = graph_id

        #Clean dataframes
        df1 = df[df.graph_id != 'go_0000000-0000-0000-0000-000000000000']
        df2 = df1[['id', 'name', 'definition', 'codes', 'parents', 'children',
                   'synonyms', 'graph_id']].copy(deep=True)
        dfs_to_combine.append(df2)

    #Combine dataframes
    combined_df = pandas.concat(dfs_to_combine)
    return combined_df

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
#  PARSE
###################################################
# Transforms dataframe by adding and selecting  the columns
# Input: dataframe
# Output: transformed dataframe
def parse_go_main(df):
    new_df = df[['id', 'name', 'definition', 'graph_id']].copy(deep=True)
    return new_df

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
def parse_go_synonyms(df):
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
    return synonyms_df

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

if __name__ == "__main__":
    main()