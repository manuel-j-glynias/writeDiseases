import datetime
import csv
import os
import get_schema


def assign_editable_xrefs_lists(df, loader_id, load_dir,  id_class):
    # Gets the info on what columns should be created
    db_dict_ed_xrefs_list = get_schema.get_schema('EditableXrefsList')
    db_dict_ed_xrefs_el= get_schema.get_schema('EditableXrefsListElements')
    db_dict_xref = get_schema.get_schema('Xref')

    # Creates writer objects for EditableXrefsList and EditableXrefsListElements
    load_files_ed_xrefs_list_dict = create_load_files_dict(db_dict_ed_xrefs_list, load_dir)
    editable_list_writer = load_files_ed_xrefs_list_dict['EditableXrefsList']['writer']

    load_ed_el_syn_dict = create_load_files_dict(db_dict_ed_xrefs_el, load_dir)
    element_writer = load_ed_el_syn_dict['EditableXrefsListElements']['writer']

    load_xref = create_load_files_dict(db_dict_xref, load_dir)
    xref_writer = load_xref['Xref']['writer']

    # create a list of xrefs for each graph id
    strings_dict = create_dict_of_xrefs(df)

    esl_dict = {}
    counter = 0
    for entry in strings_dict:
        entries = strings_dict[entry]
        counter += 1

        #EditableXrefs_graph_id:
        esl  = id_class.assign_id().replace('es_', 'exl_')
        esl_dict[entry] = esl
        graph_id =  'xrefs_' + entry

        # Write editable synonyms list  csv file entry
        write_editable_xrefs_list(graph_id, editable_list_writer, loader_id, esl)
        write_xref_and_elements(entries, esl, element_writer, xref_writer, counter)
    return esl_dict

# Writes xref elements and xref tables
def write_xref_and_elements(entries, esl, element_writer, xref_writer, counter):
    for el in entries:
        for input in el:
            source = input
            sourceId = el[input]
            XRef_graph_id = 'xref_' + source.lower() + '_' + sourceId.lower()
            EditableXRefList_graph_id = esl
            # Write elements  csv file entry
            write_editable_xrefs_elements(counter, element_writer, XRef_graph_id, EditableXRefList_graph_id)
            write_xref(source, sourceId, xref_writer, XRef_graph_id)

def write_editable_xrefs_list(graph_id, editable_string_writer, loader_id, esl):
    now = datetime.datetime.now()
    editable_string_writer.writerow([graph_id, now.strftime("%Y-%m-%d-%H-%M-%S"), loader_id, esl])

def write_editable_xrefs_elements(counter, editable_elements_writer, XRef_graph_id, EditableXRefList_graph_id):
    editable_elements_writer.writerow([counter, XRef_graph_id, EditableXRefList_graph_id])

def write_xref(source, source_id, xref_writer, graph_id):
    xref_writer.writerow([source, source_id, graph_id])

# create a list of xrefs for each graph id
def create_dict_of_xrefs(df):
    strings_dict = {}
    for index, row in df.iterrows():
        source_xrefId = {}
        source_xrefId[row['source']] = row['xrefId']
        graph_id = row['graph_id']
        # if xrefs for this graph id are already there
        if graph_id in strings_dict:
            strings = strings_dict[graph_id]
        else:
            strings = []
        strings.append(source_xrefId)
        strings_dict[graph_id] = strings
    return strings_dict

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