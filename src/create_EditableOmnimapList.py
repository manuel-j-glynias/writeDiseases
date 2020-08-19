import datetime
import csv
import os
import get_schema
import pandas

path = '../config/table_descriptions.csv'

def extract_file(path):
    unparsed_df = pandas.read_csv(path)
    table_database = {}
    col_order = []
    previous = ''
    for index, row in unparsed_df.iterrows():
        field_list = [row['DataType'], row['Key'], row['NullValues'],
                      row['AutoIncrement'], row ['ReferenceFields']]
        if  row['Table'] in table_database:
            field_dict = table_database[row['Table'] ]
            field_dict[ row['Field']] = field_list
        else:
            if previous != '':
                previous_dict = table_database[previous]
                previous_dict['col_order'] = col_order
            col_order = []
            field_dict = {}
            field_dict[ row['Field']] = field_list
            table_database[row['Table'] ] = field_dict
        col_order.append( row['Field'])
        previous = row['Table']

    return table_database
def assign_editable_xrefs_lists(df, jax_dict,  loader_id, load_dir,  id_class):
    # Gets the info on what columns should be created
    db_dict = extract_file(path)
    db_dict_ed_om_list = {'OmniseqKnowledgeDatabase2' : {'EditableOmniMapList': db_dict['EditableOmniMapList']}}
    db_dict_ed_om_el = {'OmniseqKnowledgeDatabase2' : {'EditableOmniMapListElements': db_dict['EditableOmniMapListElements']}}
    db_dict['Omni_map']['col_order'] = ['omniDisease_graph_id', 'mCode_graph_id', 'graph_id']
    db_dict_om = { 'OmniseqKnowledgeDatabase2': {'Omni_map': db_dict['Omni_map']}}

    # Creates writer objects for EditableXrefsList and EditableXrefsListElements
    load_files_ed_om_list_dict = create_load_files_dict(db_dict_ed_om_list, load_dir)
    editable_list_writer = load_files_ed_om_list_dict['EditableOmniMapList']['writer']

    load_ed_el_om_dict = create_load_files_dict(db_dict_ed_om_el, load_dir)
    element_writer = load_ed_el_om_dict['EditableOmniMapListElements']['writer']

    load_om = create_load_files_dict(db_dict_om, load_dir)
    om_writer = load_om['Omni_map']['writer']

    # create a list of xrefs for each graph id
    #strings_dict = create_dict_of_xrefs(df)

    esl_dict = {}
    counter = 0
    for index, row in df.iterrows():
        entry = row['graph_id']
        #EditableOmnimap_graph_id:
        esl  = id_class.assign_id().replace('es_', 'eoml_')
        esl_dict[entry] = esl
        #graph_id =  'xrefs_' + entry
        jax_diseases = row['jaxDiseases']
        omnimap_list = []
        if jax_diseases:
            for jax in jax_diseases:
                if jax in jax_dict:
                    omnimap = jax_dict[jax]
                    for omnidisease in omnimap:
                        mcode_list = omnimap[omnidisease]
                        for mcode in mcode_list:
                            map = {}
                            map[omnidisease]= mcode
                            omnimap_list.append(map)

        if not omnimap_list:
            omnimap_list = [{"" : ""}]


        # Write editable synonyms list  csv file entry
        omnimap_list = [dict(t) for t in {tuple(d.items()) for d in omnimap_list}]
        write_editable_omni_list(entry, editable_list_writer, loader_id, esl)
        write_omni_and_elements(omnimap_list, esl, element_writer, om_writer, counter, id_class)
    return esl_dict

# Writes xref elements and xref tables
def write_omni_and_elements(entries, esl, element_writer, om_writer, counter, id_class):
    element_list = []
    EditableMapList_graph_id =""
    for el in entries:
        for input in el:
            element_id = id_class.get_om_id()
            id_class.set_om_id()
            omniDisease_graph_id = input
            mCode_graph_id = el[input]
            if (omniDisease_graph_id == "" and  mCode_graph_id == ""):
                omni_id = 'omnimap_' + str( element_id)
                EditableMapList_graph_id = esl
                # Write elements  csv file entry
                write_omni(omniDisease_graph_id, mCode_graph_id, om_writer, omni_id)
                element_list.append(omni_id)
            else:
                omni_id = 'omnimap_' + str( element_id)
                EditableMapList_graph_id = esl
                if  '_' in omniDisease_graph_id :
                    omniDisease_graph_id = 'omnidisease_' + omniDisease_graph_id.split('_')[1]
                if '-' in mCode_graph_id:
                    mCode_graph_id = 'Mcode_' + mCode_graph_id
                # Write elements  csv file entry
                write_omni(omniDisease_graph_id, mCode_graph_id, om_writer, omni_id)
                element_list.append(omni_id)

    pipe_strings = '|'.join(element_list)
    write_editable_omni_elements(element_writer, pipe_strings, EditableMapList_graph_id)
    return counter

def write_editable_omni_list(field, editable_string_writer, loader_id, esl):
    now = datetime.datetime.now()
    editable_string_writer.writerow([field, now.strftime("%Y-%m-%d-%H-%M-%S"), loader_id, esl])

def write_editable_omni_elements(editable_elements_writer, pipe_strings, EditableMapList_graph_id):
    editable_elements_writer.writerow([pipe_strings, EditableMapList_graph_id])

def write_omni(source, source_id, xref_writer, graph_id):
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
            writer = csv.writer(out_file, lineterminator='\n', quoting=csv.QUOTE_ALL)
            if is_empty(out_file):
                header = db_dict[database][table_name]['col_order']
                writer.writerow(header)
            load_files_dict[table_name] = {'writer':writer, 'file':out_file}
    return load_files_dict

def is_empty(out_file):
    out_file.seek(0, os.SEEK_END)
    return out_file.tell()==0
