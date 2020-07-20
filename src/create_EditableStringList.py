import datetime
import csv
import os
import get_schema


def assign_editable_lists(df, loader_id, load_dir,  id_class, element):

    # Gets the info on what columns should be created
    db_dict_ed_str_list = get_schema.get_schema('EditableStringList')
    db_dict_ed_str_el= get_schema.get_schema('EditableStringListElements')

    # Creates writer objects for EditableSynonymsList and Synonym
    load_files_ed_str_list_dict = create_load_files_dict(db_dict_ed_str_list, load_dir)
    editable_string_writer = load_files_ed_str_list_dict['EditableStringList']['writer']

    load_ed_el_syn_dict = create_load_files_dict(db_dict_ed_str_el, load_dir)
    element_writer = load_ed_el_syn_dict['EditableStringListElements']['writer']

    # create a list of synonyms for each graph id
    strings_dict = {}
    for index, row in df.iterrows():
        graph_id = row['graph_id']
        if graph_id in strings_dict:
            strings = strings_dict[graph_id]
            if type(row[element]) is list:
                for syn in row[element]:
                    strings.append(syn)
            else:
                strings.append( row[element])
            strings_dict[graph_id] = strings
        else:
            strings = []
            if type(row[element]) is list:
                for syn in row[element]:
                    strings.append(syn)
            else:
                strings.append( row[element])
            strings_dict[graph_id] = strings

    esl_dict = {}
    counter = 0
    for entry in strings_dict:
        entries = strings_dict[entry]

        #EditableSynonymsList_graph_id:
        esl  = id_class.assign_id().replace('es_', 'esl_')

        esl_dict[entry] = esl
        graph_id = element + '_ ' + entry

        # Write editable synonyms list  csv file entry
        write_editable_string_list(graph_id, editable_string_writer, loader_id, esl)

        pipe_strings = '|'.join(entries)

            # Write synonym  csv file entry
        write_editable_list_elements( element_writer, pipe_strings, esl)
    return esl_dict

def write_editable_string_list(graph_id, editable_string_writer, loader_id, esl):
    now = datetime.datetime.now()
    editable_string_writer.writerow([graph_id, now.strftime("%Y-%m-%d-%H-%M-%S"), loader_id, esl])

def write_editable_list_elements(editable_elements_writer, pipe_strings, esl):
    editable_elements_writer.writerow([pipe_strings, esl])

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