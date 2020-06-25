import datetime
import csv
import os
import get_schema


def assign_editable_synonyms(df, loader_id, load_dir , table_name, id_class):

    # Gets the info on what columns should be created
    db_dict_ed_syn_list = get_schema.get_schema('EditableSynonymsList')
    db_dict_syn = get_schema.get_schema('Synonym')

    # Creates writer objects for EditableSynonymsList and Synonym
    load_files_ed_syn_dict = create_load_files_dict(db_dict_ed_syn_list, load_dir)
    editable_synonyms_writer = load_files_ed_syn_dict['EditableSynonymsList']['writer']

    load_files_syn_dict = create_load_files_dict(db_dict_syn, load_dir)
    synonym_writer = load_files_syn_dict['Synonym']['writer']

    # create a list of synonyms for each graph id
    synonyms_dict = {}
    for index, row in df.iterrows():
        graph_id = row['graph_id']
        if graph_id in synonyms_dict:
            synonyms = synonyms_dict[graph_id]
            synonyms.append( row['synonym'])
            synonyms_dict[graph_id] = synonyms
        else:
            synonyms = []
            synonyms.append( row['synonym'])
            synonyms_dict[graph_id] = synonyms
    synonym_esl_dict = {}
    counter = 0
    for entry in synonyms_dict:
        synonyms = synonyms_dict[entry]
        counter += 1
        # Create an id for group of synonyms

        #EditableSynonymsList_graph_id:
        esl  = id_class.assign_id().replace('es_', 'esl_')

        synonym_esl_dict[entry] = esl
        graph_id = 'synonyms_ ' + entry

        # Write editable synonyms list  csv file entry
        write_editable_synonyms(graph_id, editable_synonyms_writer, loader_id, esl)

        for synonym in synonyms:
            id = counter
            name = synonym
            EditableSynonymsList_graph_id = esl

            # Write synonym fcsv file entry
            write_synonym(id, synonym_writer, name, EditableSynonymsList_graph_id)
    return synonym_esl_dict

def write_editable_synonyms(graph_id, editable_synonyms_writer, loader_id, esl):
    now = datetime.datetime.now()
    editable_synonyms_writer.writerow([graph_id, now.strftime("%Y-%m-%d-%H-%M-%S"), loader_id, esl])

def write_synonym(id, synonym_writer, name, EditableSynonymsList_graph_id):
    synonym_writer.writerow([id, name, EditableSynonymsList_graph_id])

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