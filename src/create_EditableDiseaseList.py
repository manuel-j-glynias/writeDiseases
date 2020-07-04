import datetime
import csv
import os
import get_schema
import pandas


def assign_editable_disease_lists(df, loader_id, load_dir,  id_class):
    disease_lists = ['EditableJaxDiseaseList', 'EditableDoDiseaseList', 'EditableGoDiseaseList',
                        'EditableOncoTreeDiseaseList']
    disease_elements_lists = [ 'EditableJaxDiseaseListElements',
                               'EditableDoDiseaseListElements', 'EditableGoDiseaseListElements',
                        'EditableOncoTreeDiseaseListElements']
    disease_writers= create_writer_objects(disease_lists, load_dir)
    element_writers = create_writer_objects(disease_elements_lists, load_dir)
    ids = ['ejdl', 'eddl', 'egdl', 'eodl' ]
    columns = [ 'jaxDiseases', 'doDiseases', 'goDiseases', 'oncoTreeDiseases']
    counter = 1
    for index, row in df.iterrows():
        lists_from_dataframe = [ row['jaxDiseases'], row['doDiseases'], row['goDiseases'], row['oncoTreeDiseases']]
        for i  in range(len(lists_from_dataframe)):

            disease_name = row['name']
            if pandas.isnull(disease_name):
                print (row['graph_id'])
                pass
            else:
                field = 'ontological_disease_'  + disease_name.replace(' ', '_').lower()
                esl = id_class.assign_id().replace('es_', ids[i] + '_')
                df.at[index, columns[i]] = esl

            # Write editable disease list into csv
            write_editable_disease_list(field, disease_writers[i], loader_id, esl)
            counter = write_elements(lists_from_dataframe[i], esl, element_writers[i], counter)
    return df

# Writes editable disease list elements table
def write_elements(elements_list, esl, element_writer, counter):
    for entry in elements_list:
        element_writer.writerow([counter, entry, esl])
        counter += 1
    return counter

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
            writer = csv.writer(out_file, lineterminator='\n')
            if is_empty(out_file):
                header = db_dict[database][table_name]['col_order']
                writer.writerow(header)
            load_files_dict[table_name] = {'writer':writer, 'file':out_file}
    return load_files_dict

def is_empty(out_file):
    out_file.seek(0, os.SEEK_END)
    return out_file.tell()==0