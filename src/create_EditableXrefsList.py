import datetime
import csv
import os
import get_schema


def assign_editable_xrefs_lists(df, loader_id, load_dir,  id_class):

    # Gets the info on what columns should be created
    db_dict_ed_xrefs_list = get_schema.get_schema('EditableXrefsList')
    db_dict_ed_xrefs_el= get_schema.get_schema('EditableXrefsListElements')

    # Creates writer objects for EditableXrefsList and EditableXrefsListElements
    load_files_ed_xrefs_list_dict = create_load_files_dict(db_dict_ed_xrefs_list, load_dir)
    editable_list_writer = load_files_ed_xrefs_list_dict['EditableXrefsList']['writer']

    load_ed_el_syn_dict = create_load_files_dict(db_dict_ed_xrefs_el, load_dir)
    element_writer = load_ed_el_syn_dict['EditableXrefsListElements']['writer']

    # create a list of xrefs for each graph id
    strings_dict = {}
    for index, row in df.iterrows():
        graph_id = row['graph_id']
        if graph_id in strings_dict:
            strings = strings_dict[graph_id]
            source_xrefId = {}
            source_xrefId[row['source']] = row['xrefId']
            strings.append(source_xrefId)
            strings_dict[graph_id] = strings
        else:
            strings = []
            source_xrefId = {}
            source_xrefId[row['source']] = row['xrefId']
            strings.append(source_xrefId)
            strings_dict[graph_id] = strings

    esl_dict = {}
    counter = 0
    for entry in strings_dict:
        entries = strings_dict[entry]
        counter += 1
        # Create an id for group of synonyms

        #EditableSynonymsList_graph_id:
        esl  = id_class.assign_id().replace('es_', 'xref_')

        esl_dict[entry] = esl
        graph_id =  entry

        # Write editable synonyms list  csv file entry
        write_editable_xrefs_list(graph_id, editable_list_writer, loader_id, esl)

        for el  in entries:
            for input in el:
               source = input
               sourceId = el[input]
               # Write elements  csv file entry
               write_editable_xrefs_elements(esl, element_writer, source, sourceId)
    return esl_dict

def write_editable_xrefs_list(graph_id, editable_string_writer, loader_id, esl):
    now = datetime.datetime.now()
    editable_string_writer.writerow([graph_id, now.strftime("%Y-%m-%d-%H-%M-%S"), loader_id, esl])

def write_editable_xrefs_elements(id, editable_elements_writer, source, sourceId):
    editable_elements_writer.writerow([id, source, sourceId])

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