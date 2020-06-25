# -*- coding: utf-8 -*-
"""
Created on Tue May 26 10:38:13 2020

Purpose: download and parse Disease Ontology

@author: Paul.DePietro

Changed on 6/2/2020:

"""

import os
import datetime
import requests
import csv
import mysql.connector
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

def download_do():
    os.chdir('../data/')

    #download_link = 'http://www.berkeleybop.org/ontologies/doid.obo'
    #download_link = 'https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/master/src/ontology/subsets/DO_cancer_slim.obo'
    download_link = 'http://sourceforge.net/p/diseaseontology/code/HEAD/tree/trunk/HumanDO.obo'
    print('Downloading',download_link.split('/')[-1])
    response = requests.get(download_link)
    with open(download_link.split('/')[-1], 'wb') as f:
        f.write(response.content)

    os.chdir('../src/')

def parse_do():
    # in_file = open('../data/doid.obo', 'r')
    # in_file = open('../data/DO_cancer_slim.obo', 'r')
    in_file = open('../data/HumanDO.obo', 'r')
    doid_dict = {}  # {do_id:{'name':name, 'alt_id':(alt_ids), 'def':[(urls),definition], 'subset':(subsets), 'synonym':{syn_type:(synonyms)}, 'xref':{xref_src:(src_ids)}, 'is_a':(do_ids)}

    doid_child_dict = {}  # {do_id:(direct children terms)}

    term_flag = False
    for line in in_file:
        if (line.strip() == '[Term]'):
            term_flag = True
            obsolete_flag = False
            # temp_dict = {'name':None, 'alt_id':None, 'def':None, 'subset':None, 'synonym':None, 'xref':None, 'is_a':None}
            temp_dict = {}  #
            continue
        elif (term_flag == False):
            continue

        if (line.strip() == ''):
            if (obsolete_flag == False):
                doid_dict[do_id] = temp_dict
        elif (line.strip() == '[Typedef]'):
            break

        cur_label = line.split(':')[0]
        cur_data = ':'.join(line.split(':')[1:]).strip()

        if (cur_label == 'id'):
            do_id = cur_data

        elif (cur_label == 'is_obsolete'):
            if (cur_data == 'true'):
                obsolete_flag = True

        elif (cur_label == 'name'):
            temp_dict[cur_label] = cur_data

        # NEED TO WORK ON CAPTURING ALL INFO IN DEFINITION (CAN HAVE PUBMED IDS, MULTIPLE URLS, ETC.)
        elif (cur_label == 'def'):
            definition = cur_data.split('\" [')[0].strip('\"')
            try:
                url_raw = cur_data.split('\" [')[1].split(']')[0]
            except:
                continue
            for item in url_raw.split(', '):
                if ('http' not in item):
                    continue
                def_url = 'http' + ''.join(item.strip().replace('url:','').replace('URL:','').split('http')[1])
                try:
                    temp_dict[cur_label][0].add(def_url)
                except:
                    temp_dict[cur_label] = [set([def_url]), definition]

        elif (cur_label == 'xref'):
            xref_src = line.split(':')[1].strip()
            xref_src_id = str(line.split(':')[-1].strip())
            try:
                temp_dict[cur_label][xref_src].add(xref_src_id)
            except:
                try:
                    temp_dict[cur_label][xref_src] = set([xref_src_id])
                except:
                    temp_dict[cur_label] = {xref_src: set([xref_src_id])}

        elif (cur_label == 'synonym'):
            synonym = line.split('\"')[1].split('\"')[0].strip()
            syn_type = line.split('\"')[-1].split('[')[0].strip()
            try:
                temp_dict[cur_label][syn_type].add(synonym)
            except:
                try:
                    temp_dict[cur_label][syn_type] = set([synonym])
                except:
                    temp_dict[cur_label] = {syn_type: set([synonym])}

        else:
            if (cur_label == 'alt_id'):
                alt_id = ':'.join(line.split(':')[1:]).strip()
                try:
                    temp_dict[cur_label].add(alt_id)
                except:
                    temp_dict[cur_label] = set([alt_id])
            elif (cur_label == 'subset'):
                subset = line.split(':')[-1].strip()
                try:
                    temp_dict[cur_label].add(subset)
                except:
                    temp_dict[cur_label] = set([subset])
            elif (cur_label == 'is_a'):
                isa_id_raw = ':'.join(line.split(':')[1:]).split('!')[0].strip()
                # seems HumanDO has no is_inferred flags
                if ('is_inferred') in isa_id_raw:
                    isa_id = isa_id_raw.split()[0]
                    if ('true' in isa_id_raw):
                        inferred_flag = '-1'
                    else:
                        inferred_flag = '0'
                else:
                    isa_id = isa_id_raw
                    inferred_flag = '0'

                try:
                    temp_dict[cur_label].add(isa_id)
                    #temp_dict[cur_label][isa_id] = inferred_flag
                except:
                    temp_dict[cur_label] = set([isa_id])
                    #temp_dict[cur_label] = {isa_id: inferred_flag}
                try:
                    doid_child_dict[isa_id].add(do_id)
                except:
                    doid_child_dict[isa_id] = set([do_id])

    in_file.close()

    # Change do_ids to graph_ids
    graph_id_child_dic = {}

    # Create new dictionary with graph_id
    for entry in doid_child_dict:
        new_key = entry.replace('ID:', '_disease_').lower()
        new_value_set = set()
        value_dict = doid_child_dict[entry]
        for value in value_dict:
            value =  value.replace('ID:', '_disease_').lower()
            new_value_set.add(value)
        graph_id_child_dic[new_key] = new_value_set

    # Set doid_child_dict to new graph_id_child_dic
    doid_child_dict = graph_id_child_dic
    print('Found', len(doid_dict.keys()), 'Disease Ontology terms that are not obsolete')
    return(doid_dict, doid_child_dict)

def get_schema_original():
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
        resource = 'do'
        #if ('do_' not in table_name):
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
    #pprint(db_dict)
    return db_dict


def write_load_files_original(db_dict, doid_dict, doid_child_dict, load_directory):
    list_of_lists = []
    do_disease_list = []
    do_url_list = []
    do_syn_list = []
    do_xrefs_list = []
    do_subsets_list = []
    do_parents_list = []
    do_children_list = []
    for db_name in sorted(db_dict.keys()):
        for table_name in sorted(db_dict[db_name].keys()):
            if (table_name == 'do_diseases'):
                length_now = len(doid_dict.keys())
                for do_id in sorted(doid_dict.keys()):
                    dx_name = doid_dict[do_id]['name']
                    try:
                        definition = doid_dict[do_id]['def'][1]
                    except:
                        definition = ''
                    graph_id = do_id.replace('ID:', '_disease_').lower()
                    altered = definition.replace('\\"', '\'')
                    altered = altered.replace('\\n', ' ')
                    new_dict = {'doId': do_id, 'name': dx_name, 'definition': altered, 'graph_id':graph_id}
                    do_disease_list.append(new_dict)
            if ('definition_urls' in table_name):
                for do_id in sorted(doid_dict.keys()):
                    try:
                        url_set = doid_dict[do_id]['def'][0]
                    except:
                        continue
                    for url in sorted(url_set):
                        cur_data = [do_id.replace('ID:', '_disease_').lower(), url]
                        new_dict = {'graph_id': do_id.replace('ID:', '_disease_').lower(), 'url': url}
                        do_url_list.append(new_dict)
            elif ('synonyms' in table_name):
                for do_id in sorted(doid_dict.keys()):
                    if ('synonym' in doid_dict[do_id].keys()):
                        for syn_type in sorted(doid_dict[do_id]['synonym'].keys()):
                            for synonym in sorted(doid_dict[do_id]['synonym'][syn_type]):
                                new_dict = {'graph_id': do_id.replace('ID:', '_disease_').lower(), 'synonymType': syn_type, 'synonym': synonym}
                                do_syn_list.append(new_dict)
            elif ('xrefs' in table_name):
                for do_id in sorted(doid_dict.keys()):
                    if ('xref' in doid_dict[do_id].keys()):
                        for source in sorted(doid_dict[do_id]['xref'].keys()):
                            for src_id in sorted(doid_dict[do_id]['xref'][source]):
                                new_dict = {'graph_id': do_id.replace('ID:', '_disease_').lower(),
                                            'source': source, 'xrefId': src_id}
                                do_xrefs_list.append(new_dict)
            elif ('subsets' in table_name):
                for do_id in sorted(doid_dict.keys()):
                    if ('subset' in doid_dict[do_id].keys()):
                        for subset in sorted(doid_dict[do_id]['subset']):
                            new_dict = {'graph_id': do_id.replace('ID:', '_disease_').lower(),
                                        'subset': subset}
                            do_subsets_list.append(new_dict)
            elif ('parents' in table_name):
                for do_id in sorted(doid_dict.keys()):
                    if ('is_a' in doid_dict[do_id].keys()):
                        for parent in sorted(doid_dict[do_id]['is_a']):
                            new_dict = {'graph_id': do_id.replace('ID:', '_disease_').lower(),
                                        'parent': parent.replace('ID:', '_disease_').lower()}
                            do_parents_list.append(new_dict)
            elif ('children' in table_name):
                for do_id in sorted(doid_child_dict.keys()):
                    for child in sorted(doid_child_dict[do_id]):
                        new_dict = {'graph_id': do_id.replace('ID:', '_disease_').lower(),
                                    'child': child}
                        do_children_list.append(new_dict)

    list_of_lists.append(do_disease_list)
    print ('length: ' + str(len(do_disease_list)))
    list_of_lists.append(do_url_list)
    list_of_lists.append(do_syn_list)
    list_of_lists.append(do_xrefs_list)
    list_of_lists.append(do_subsets_list)
    list_of_lists.append(do_parents_list)
    list_of_lists.append(do_children_list)
    return list_of_lists

def main(load_directory):
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    my_db = None
    my_cursor = None

    print('WARNING: skipping DiseaseOntology download during development')
    #download_do()
    doid_dict,doid_child_dict = parse_do()

    db_dict = get_schema_original()
    do_disease_list_of_listst = write_load_files_original(db_dict, doid_dict, doid_child_dict, load_directory)

    do_disease_df = pandas.DataFrame(do_disease_list_of_listst[0])
    df_editable = create_editable_statement.assign_editable_statement(do_disease_df,
                                                                      editable_statement_list, loader_id, load_directory, do_table_name, id_class)

    df_synonyms = pandas.DataFrame(do_disease_list_of_listst[2])
    df_synonyms_exact = split_synonyms('EXACT', df_synonyms)
    df_synonyms_related = split_synonyms('RELATED', df_synonyms)
    syn_dict_exact = create_editable_synonyms.assign_editable_synonyms(df_synonyms_exact, loader_id, load_directory , do_table_name, id_class)
    syn_dict_related = create_editable_synonyms.assign_editable_synonyms(df_synonyms_related, loader_id, load_directory , do_table_name, id_class)

    df_editable = add_column_to_dataframe(df_editable, syn_dict_exact, 'exact_synonyms')
    df_editable = add_column_to_dataframe(df_editable, syn_dict_related, 'related_synonyms')
  ############################
    do_disease_df = df_editable[['doId', 'name', 'definition', 'exact_synonyms', 'related_synonyms', 'graph_id']]

    path = load_directory + 'do_diseases.csv'
    write_load_files.main(do_disease_df, path)

    do_parents_df = pandas.DataFrame(do_disease_list_of_listst[5])
    path_parents = load_directory + 'do_parents.csv'
    write_load_files.main(do_parents_df, path_parents)

    do_children_df = pandas.DataFrame(do_disease_list_of_listst[6])
    path_children = load_directory + 'do_children.csv'
    write_load_files.main(do_children_df, path_children)

    do_xrefs_df = pandas.DataFrame(do_disease_list_of_listst[3])
    path_xrefs = load_directory + 'do_xrefs.csv'
    write_load_files.main(do_xrefs_df, path_xrefs)

    do_url_df = pandas.DataFrame(do_disease_list_of_listst[1])
    path_urls = load_directory + 'do_definition_urls.csv'
    write_load_files.main(do_url_df, path_urls)

    do_subsets_df = pandas.DataFrame(do_disease_list_of_listst[4])
    path_subsets = load_directory + 'do_subsets.csv'
    write_load_files.main(do_subsets_df, path_subsets)

    # Write sql tables
    db_dict = get_schema.get_schema('do_diseases')
    db_parents_dict = get_schema.get_schema('do_parents')
    db_children_dict = get_schema.get_schema('do_children')
    db_xrefs_dict= get_schema.get_schema('do_xrefs')
    db_urls_dict = get_schema.get_schema('do_definition_urls')
    db_subsets_dict = get_schema.get_schema('do_subsets')
    editable_statement_dict = get_schema.get_schema('EditableStatement')
    editable_synonyms_list_dict = get_schema.get_schema('EditableSynonymsList')
    synonym_dict = get_schema.get_schema('Synonym')

    write_sql.write_sql(db_dict, 'do_diseases')
    write_sql.write_sql(db_parents_dict, 'do_parents')
    write_sql.write_sql(db_children_dict, 'do_children')
    write_sql.write_sql(db_xrefs_dict, 'do_xrefs')
    write_sql.write_sql(db_urls_dict, 'do_definition_urls')
    write_sql.write_sql(db_subsets_dict, 'do_subsets')
    write_sql.write_sql(editable_statement_dict, 'EditableStatement')
    write_sql.write_sql(editable_synonyms_list_dict, 'EditableSynonymsList')
    write_sql.write_sql(synonym_dict, 'Synonym')

    print(datetime.datetime.now().strftime("%H:%M:%S"))

def split_synonyms(synonymType, df_synonyms):
    df_splitted = df_synonyms[df_synonyms['synonymType'] ==  synonymType]

    return df_splitted

def add_column_to_dataframe(df_in_need, synonym_dict, column):
   #Put esl value back to the main dataframe
   for index, row in df_in_need.iterrows():
       df_in_need.at[index, column] = ""
       disease_id = row['graph_id']
       if disease_id == 'do_disease_0050025':
           print()
       if disease_id in synonym_dict:
           df_in_need.at[index, column] = synonym_dict[disease_id]
   return df_in_need

if __name__ == "__main__":
    main(load_directory)