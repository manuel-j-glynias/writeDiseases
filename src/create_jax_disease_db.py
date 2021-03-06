import mysql.connector
import datetime
import csv
import json
import os
import config

editable_statement_list = ['name', 'definition']


import pandas
from sql_utils import load_table, create_table, does_table_exist, get_local_db_connection, maybe_create_and_select_database, drop_table_if_exists
import create_id
import create_editable_statement

editable_statement_list = ['name', 'definition']
config_directory ='C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/config/table_descriptions.csv'
do_table_name = 'DoDiseases'
import write_load_files
#load_directory = '../load_files/'
#loader_id = 'user_20200422163431232329'
#id_class = create_id.ID('', '', 0, 0, 0, 0, 0, 0, 0, True, [])



###################################################
#  WRITE CSV
###################################################

def write_load_files (disease_list, load_directory):
    csv_columns = ["jaxId", "name", "source", "definition",
                   "termId", "graph_id"]
    csv_file =  load_directory + 'jax_diseases.csv'
    try:
        with open(csv_file, "w", encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns, lineterminator="\n", quoting=csv.QUOTE_ALL)
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

def add_editables(disease_list, load_directory, loader_id, id_class):
    df_to_edit = pandas.DataFrame(disease_list)
    table_name = 'jax_diseases'
    df_editable = create_editable_statement.assign_editable_statement(df_to_edit,
                                                                      editable_statement_list, loader_id, load_directory, table_name,id_class)
    del df_editable['currentPreferredTerm']
    del df_editable['lastUpdateDateFromDO']
    edited_disease_list = df_editable.T.to_dict().values()
    return edited_disease_list

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
            graph_id = 'jax_disease_' + id
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

def main(load_directory, loader_id, id_class):
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    my_db = None
    my_cursor = None
    print('WARNING: skipping JAX download during development')
    #download_jax()
    table_dict = {}
    database_dict = {}
    disease_list = parse_jax()
    disease_list_with_editables = add_editables(disease_list, load_directory, loader_id, id_class)
    write_load_files(disease_list_with_editables, load_directory)
    table_name = 'jax_diseases'
    db_name = 'OmniSeqKnowledgebase2'
    table_descriptions =  config.extract_file(config_directory)
    table_descr = table_descriptions['jax_diseases']
    table_dict[table_name] = table_descr
    database_dict[db_name]= table_dict
    db_dict = database_dict
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


#if __name__ == "__main__":
    #main(load_directory, loader_id, id_class)