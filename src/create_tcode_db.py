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
import csv
from sql_utils import load_table, create_table, does_table_exist, \
    get_local_db_connection, maybe_create_and_select_database, \
    drop_table_if_exists

def main():
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    path = '../data/tblOS_GLOBAL_GLOBAL_Ref_TCodes.csv'

    #Create dataframe
    df=extract_file(path)

    tcode_disease_df = parse_tcode_main(df)
    path = '../load_files/tcode_diseases.csv'
    write_load_files(tcode_disease_df, path)

    tcode_parents_df = parse_tcode_parents(df)
    path_parents  = '../load_files/tcode_parents.csv'
    write_load_files(tcode_parents_df, path_parents)

    tcode_children_df = parse_tcode_children(df)
    path_children = '../load_files/tcode_children.csv'
    write_load_files(tcode_children_df, path_children)

    db_dict = get_schema('tcode_diseases')
    db_parents_dict = get_schema('tcode_parents')
    db_children_dict = get_schema('tcode_children')

    write_sql(db_dict)
    write_sql(db_parents_dict)
    write_sql(db_children_dict)


# Creates  tcode dataframe
# Input: dataframe
# Output: new  dataframe with required fields
def parse_tcode_main(df):
    tcode_list = []

    # Parse dataframe
    for index, row in df.iterrows():
        new_dict = {}
        code = row['TCode']
        new_dict['code'] = code
        new_dict['tissuePath'] = row['TissuePath']
        graph_id = 'Tcode_' + code
        new_dict['graph_id'] = graph_id
        df.at[index, 'graph_id'] = graph_id
        tcode_list.append(new_dict)
    tcode_df = pandas.DataFrame(tcode_list)
    return tcode_df

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


if __name__ == "__main__":
    main()