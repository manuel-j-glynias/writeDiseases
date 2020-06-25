import mysql.connector
import datetime
import csv
from sql_helpers import get_one_jax_gene, preflight_ref, insert_editable_statement, insert_es_ref, get_loader_user_id
#from sql_utils import get_local_db_connection, maybe_create_and_select_database, does_table_exist, drop_table_if_exists
from sql_utils import load_table, create_table, does_table_exist, get_local_db_connection, maybe_create_and_select_database, drop_table_if_exists
import json
import sys
import os

import pandas
from sql_utils import load_table, create_table, does_table_exist, get_local_db_connection, maybe_create_and_select_database, drop_table_if_exists
import create_id
import create_editable_statement
import create_editable_synonyms
import write_sql
import get_schema


editable_statement_list = ['name', 'definition']
editable_synonyms_list = ['synonyms']
loader_id = '007'
load_directory = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/'
do_table_name = 'DoDiseases'
import write_load_files
import get_schema

id_class = create_id.ID('', '')


###################################################
#  WRITE CSV
###################################################

def write_load_files (disease_list, load_directory):
    csv_columns = ["jaxId", "name", "source", "definition", "currentPreferredTerm", "lastUpdateDateFromDO",
                   "termId", "graph_id"]
    csv_file =  load_directory + 'jax_diseases.csv'
    try:
        with open(csv_file, "w", encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns, lineterminator="\n")
            writer.writeheader()
            for data in disease_list:
                writer.writerow(data)
    except IOError:
        print("I/O error csv file")

###################################################
#  GET FILES
###################################################
def parse_jax():
    json_files = get_list_of_files('../data/indications')
    disease_list = []
    print("num diseases=",len(json_files))
    #loader_id = get_loader_user_id(my_cursor)
    #counter = 0
    for json_file in json_files:
        disease: dict = read_one_disease_json(json_file)
        if disease is not None:
            disease_list.append(disease)
    return(disease_list)

def get_list_of_files(path: str) -> list:
    json_files = []
    for entry in os.scandir(path):
        if entry.is_file():
            json_files.append(entry.path)
    return json_files


###################################################
#  CREATE DICTIONARY
###################################################

def read_one_disease_json(path:str)->dict:
    with open(path, 'r') as afile:
        disease_data = json.loads(afile.read())
        id = str(disease_data['id'])
        name = disease_data['name']
        source = disease_data['source']
        definition = disease_data['definition']
        if definition is not None:
            definition = definition.split(' [', 1)[0]
        currentPreferredTerm = disease_data['currentPreferredTerm']
        lastUpdateDateFromDO= disease_data['lastUpdateDateFromDO']
        termId = disease_data['termId']
        if termId is not None:
            graph_id = termId.replace(':', '_').lower()
            graph_id = 'jax_disease_' + graph_id.split('_')[1]
        else:
            graph_id = None

        disease = {
            'jaxId': id,
            'name': name,
            'source': source,
            'definition': definition,
            'currentPreferredTerm': currentPreferredTerm,
            'lastUpdateDateFromDO': lastUpdateDateFromDO,
            'termId': termId,
            'graph_id': graph_id
        }
        return disease


def get_schema():
    db_dict = {}  # {db_name:{table_name:{col:[type,key,allow_null,ref_col_list],'col_order':[cols in order]}}}

    init_file = open('../config/table_descriptions.csv', 'r')
    reader = csv.reader(init_file, quotechar='\"')
    for line in reader:
        #print(line)
        db_name = line[0]
        if (db_name == 'Database'):
            continue
        if (db_name not in db_dict.keys()):
            db_dict[db_name] = {}
        table_name = line[1]
        resource = 'jax'
        if (resource + '_' not in table_name):
            continue
        col = line[2]
        col_type = line[3]
        col_key = line[4]
        allow_null = line[5]
        auto_incr = line[6]
        ref_col_list = line[7].split('|') # we will ignore this for now during development
        try:
            ref_col_list.remove('')
        except:
            pass

        try:
            db_dict[db_name][table_name][col] = [col_type, col_key, allow_null, auto_incr, ref_col_list]
            db_dict[db_name][table_name]['col_order'].append(col)
        except:
            db_dict[db_name][table_name] = {col: [col_type, col_key, allow_null, auto_incr, ref_col_list]}
            db_dict[db_name][table_name] = {col: [col_type, col_key, allow_null, auto_incr, ref_col_list], 'col_order': [col]}
    init_file.close()
    #print(db_dict)
    return db_dict

def main(load_directory):
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    my_db = None
    my_cursor = None

    print('WARNING: skipping JAX download during development')
    #download_jax()
    disease_list = parse_jax()
    write_load_files(disease_list, load_directory)

    db_dict = get_schema()
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
    print(datetime.datetime.now().strftime("%H:%M:%S"))


if __name__ == "__main__":
    main()