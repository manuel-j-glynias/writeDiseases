import datetime
import csv
import os
import get_schema


def assign_editable_disease_lists(disease_dict, loader_id, load_dir,  id_class, disease, onto_dict):

    # Creates writer objects
    jax_list_schema = get_schema.get_schema('EditableJaxDiseaseList')
    load_files_jax = create_load_files_dict(jax_list_schema, load_dir)
    jax_list_writer = load_files_jax['EditableJaxDiseaseList']['writer']

    do_list_schema = get_schema.get_schema('EditableDoDiseaseList')
    load_files_do = create_load_files_dict(do_list_schema, load_dir)
    do_list_writer = load_files_do['EditableDoDiseaseList']['writer']

    go_list_schema = get_schema.get_schema('EditableGoDiseaseList')
    load_files_go = create_load_files_dict(go_list_schema, load_dir)
    go_list_writer = load_files_go['EditableGoDiseaseList']['writer']

    onco_list_schema = get_schema.get_schema('EditableOncoTreeDiseaseList')
    load_files_onco = create_load_files_dict(onco_list_schema, load_dir)
    onco_list_writer = load_files_onco['EditableOncoTreeDiseaseList']['writer']

    jax_list_el_schema = get_schema.get_schema('EditableJaxDiseaseElementsList')
    load_files_jax_el = create_load_files_dict(jax_list_el_schema, load_dir)
    jax_el_list_writer = load_files_jax_el['EditableJaxDiseaseElementsList']['writer']

    do_list_el_schema = get_schema.get_schema('EditableDoDiseaseElementsList')
    load_files_do_el = create_load_files_dict(do_list_el_schema, load_dir)
    do_el_list_writer = load_files_do_el['EditableDoDiseaseElementsList']['writer']

    go_list_el_schema = get_schema.get_schema('EditableGoDiseaseElementsList')
    load_files_go_el = create_load_files_dict(go_list_el_schema, load_dir)
    go_el_list_writer = load_files_go_el['EditableGoDiseaseElementsList']['writer']

    onco_list_el_schema = get_schema.get_schema('EditableOncoTreeDiseaseElementsList')
    load_files_onco_el = create_load_files_dict(onco_list_el_schema, load_dir)
    onco_el_list_writer = load_files_onco_el['EditableOncoTreeDiseaseElementsList']['writer']


    db_dict_ed_el= get_schema.get_schema('EditableDiseaseListElements')

    editable_list_writer = load_files_ed_disease_list_dict['EditableDiseaseList']['writer']
    load_ed_el_dict = create_load_files_dict(db_dict_ed_el, load_dir)
    element_writer = load_ed_el_dict['EditableDiseaseListElements']['writer']

    strings_dict = disease_dict
    esl_dict = {}
    counter = 0
    for ontological_disease in strings_dict:
        corresponding_disease = strings_dict[ontological_disease]
        description = onto_dict[ontological_disease]
        field = description[0]

       # Creates time stampe id using id class
        esl  = id_class.assign_id().replace('es_', disease)
        esl_dict[ontological_disease] = esl
        graph_id =  'ontological_disease_' + field

        # Write editable disease list into csv
        write_editable_disease_list(graph_id, editable_list_writer, loader_id, esl)
        counter = write_elements(corresponding_disease, esl, element_writer, counter)
    return esl_dict

# Writes editable disease list elements table
def write_elements(ontological_disease, esl, element_writer, counter):
    counter += 1
    # Write elements  csv file entry
    element_writer.writerow([counter, ontological_disease, esl])
    return counter

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