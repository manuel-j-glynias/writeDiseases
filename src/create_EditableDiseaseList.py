"""
This script creates diseases lists and disease list elements tables corresponding to an ontological disease
7/4/2020
IK
"""
import datetime
import csv
import os
import get_schema
import pandas

# Variables
disease_lists = ['EditableJaxDiseaseList', 'EditableDoDiseaseList', 'EditableGoDiseaseList',
                        'EditableOncoTreeDiseaseList', 'EditableMCodeList']
disease_elements_lists = [ 'EditableJaxDiseaseListElements',
                               'EditableDoDiseaseListElements', 'EditableGoDiseaseListElements',
                        'EditableOncoTreeDiseaseListElements', 'EditableMCodeListElements']
ids = ['ejdl', 'eddl', 'egdl', 'eotdl', 'emcdl']
columns = ['jaxDiseases', 'doDiseases', 'goDiseases', 'oncoTreeDiseases', 'MCodes']
ontological_disease = 'ontological_disease_'
name = 'name'


def assign_editable_disease_lists(df, loader_id, load_dir,  id_class):

    disease_writers= create_writer_objects(disease_lists, load_dir)
    element_writers = create_writer_objects(disease_elements_lists, load_dir)

    counter = 1
    for index, row in df.iterrows():
        lists_from_dataframe = [row[columns[0]], row[columns[1]], row[columns[2]], row[columns[3]], row[columns[4]]]
        for i  in range(len(lists_from_dataframe)):

            disease_name = row[name]
            if pandas.isnull(disease_name):
                pass
            else:
                field = ontological_disease  + disease_name.replace(' ', '_').lower()
                # Assigns id to each element
                esl = id_class.assign_id().replace('es_', ids[i] + '_')
                df.at[index, columns[i]] = esl

            # Write editable disease list into csv
            write_editable_disease_list(field, disease_writers[i], loader_id, esl)

            # Write editable disease  elements list into csv
            write_elements(lists_from_dataframe[i], esl, element_writers[i], i, id_class)
    return df

# Writes editable disease list elements table
def write_elements(elements_list, esl, element_writer, i, id_class):
    if elements_list:
        element_id = 0
        pipe_strings = '|'.join(elements_list)
        if i == 0:
            element_id = id_class.get_jax_id()
        elif i == 1:
            element_id = id_class.get_do_id()
        elif i == 2:
            element_id = id_class.get_go_id()
        elif i == 3:
            element_id = id_class.get_onco_id()
        element_writer.writerow([element_id, pipe_strings, esl])

# Creates writer object for each needed csv file
def create_writer_objects(lists_to_create, load_dir):
    list_of_writer_objects = []
    for entry in lists_to_create:
        schema = get_schema.get_schema(entry)
        loaded_files = create_load_files_dict(schema, load_dir)
        writer = loaded_files[entry]['writer']
        list_of_writer_objects.append(writer)
    return list_of_writer_objects

def write_editable_disease_list(graph_id, editable_string_writer, loader_id, esl):
    now = datetime.datetime.now()
    editable_string_writer.writerow([graph_id, now.strftime("%Y-%m-%d-%H-%M-%S"), loader_id, esl])

# Creates a writer object for editable statement
def create_load_files_dict(db_dict, load_dir):
    load_files_dict = {}
    for database in db_dict:
        entry = db_dict[database]
        for table_name in entry:
            out_file = open(load_dir + table_name + '.csv', 'a+', encoding='utf-8')
            writer = csv.writer(out_file, lineterminator='\n', quoting=csv.QUOTE_ALL)
            if is_empty(out_file):
                header = db_dict[database][table_name]['col_order']
                writer.writerow(header)
            load_files_dict[table_name] = {'writer':writer, 'file':out_file}
    return load_files_dict

def is_empty(out_file):
    out_file.seek(0, os.SEEK_END)
    return out_file.tell()==0