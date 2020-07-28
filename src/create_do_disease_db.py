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
import create_EditableStringList
import create_references
import write_sql
import numpy as np

editable_statement_list = ['name', 'definition']
editable_synonyms_list = ['synonyms']
#loader_id = '007'
#load_directory = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/'
do_table_name = 'DoDiseases'
import write_load_files
import get_schema
import create_EditableXrefsList
load_directory = '../load_files/'
loader_id = 'user_20200422163431232329'
id_class = create_id.ID('', '', 0, 0, 0, 0, 0, 0, 0)


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

def main(load_directory, loader_id, id_class):
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    my_db = None
    my_cursor = None

    print('WARNING: skipping DiseaseOntology download during development')

    #download_do()
    doid_dict,doid_child_dict = parse_do()
    #db_dict = get_schema_original()
    # creates a dataframe from doid_dict
    do_df = create_dataframe(doid_dict)

    # replaces values with editable statements
    df_editable = create_editable_statement.assign_editable_statement(do_df,
                                                                      editable_statement_list, loader_id,
                                                                      load_directory, do_table_name, id_class)
    # replaces list values with editable lists
    exact_dict = create_EditableStringList.assign_editable_lists(df_editable, loader_id, load_directory,
                                                                     id_class, 'exact_synonyms')
    df_exact = add_dict(df_editable, exact_dict, 'exact_synonyms')

    related_dict = create_EditableStringList.assign_editable_lists(df_editable, loader_id, load_directory,
                                                                       id_class, 'related_synonyms')
    df_related = add_dict(df_exact, related_dict, 'related_synonyms')

    narrow_dict = create_EditableStringList.assign_editable_lists(df_related, loader_id, load_directory,
                                                                      id_class, 'narrow_synonyms')
    df_narrow = add_dict(df_related, narrow_dict, 'narrow_synonyms')

    subset_dict = create_EditableStringList.assign_editable_lists(df_narrow, loader_id, load_directory, id_class,
                                                               'subset')
    df_subset = add_dict(df_narrow, subset_dict, 'subset')

    xref_dict = create_EditableXrefsList.assign_editable_xrefs_lists(df_subset, loader_id, load_directory,
                                                                          id_class)
    df_xref = add_dict(df_subset, xref_dict, 'xrefs')

    del df_xref['reference']
    df_xref.to_csv(load_directory+ 'do_diseases.csv')

    children_df = add_children(df_xref, doid_child_dict)
    children_df.to_csv(load_directory + 'do_children.csv')

    parents_df = add_parents(df_xref, doid_child_dict)
    parents_df.to_csv(load_directory + 'do_parents.csv')

    create_references.main(do_df)
    print('do diseases are extracted')

# Creates a dataframe given a dictionary of do_diseases extracted from OBO files
def create_dataframe(doid_dict):
    list_of_entries = []
    for entry in doid_dict:
        entry_dict = {}
        name, definition, reference, xref = "", "", "", ""
        exact_synonyms , related_synonyms, narrow_synonyms, subset =[], [], [], []
        temp_dict = doid_dict[entry]
        if 'name' in temp_dict:
            name = temp_dict['name']
        if 'def' in temp_dict:
            definition =  temp_dict['def'][1]
            reference =  temp_dict['def'][0]
        if 'synonym' in temp_dict:
            syn_list = sort_synonyms(temp_dict['synonym'])
            exact_synonyms = syn_list[0]
            related_synonyms = syn_list[1]
            narrow_synonyms = syn_list[2]
        if 'subset' in temp_dict:
            subset_list = []
            subset_set = temp_dict['subset']
            for sub in subset_set:
                subset_list.append(sub)
            subset = subset_list
        if 'xref' in temp_dict:
            xref_list = []
            xref_set = temp_dict['xref']
            for ref in xref_set:
                set_entries =  xref_set[ref]
                for subset in set_entries:
                    ref_dict = {}
                    ref_dict[ref] =subset
                    xref_list.append(ref_dict)
            xref = xref_list

        entry_dict['doId'] = entry
        entry_dict['name'] = name
        entry_dict['definition'] = definition
        entry_dict['exact_synonyms'] = exact_synonyms
        entry_dict['related_synonyms'] = related_synonyms
        entry_dict['narrow_synonyms'] = narrow_synonyms
        entry_dict['subset'] = subset
        entry_dict['xrefs'] = xref
        entry_dict['reference'] = reference
        entry_dict['graph_id'] = entry.replace('ID:', '_disease_').lower()

        list_of_entries.append(entry_dict)
    return pandas.DataFrame(list_of_entries)

# Sorts synonyms from the original files into 3 columns: exact, related, and narrow
def sort_synonyms(input):
    synonym_dict = input
    syn_list = [[], [], []]
    for key in synonym_dict:
        if key == 'EXACT':
            exact_set = synonym_dict[key]
            for synonym in exact_set:
                syn_list[0].append(synonym)
        if key == 'RELATED':
            related_set = synonym_dict[key]
            for synonym in related_set:
                syn_list[1].append(synonym)
        if key == 'NARROW':
            narrow_set = synonym_dict[key]
            for synonym in narrow_set:
                syn_list[2].append(synonym)
    return syn_list

# Adds dictionary to a dataframe
def add_dict(df, dict, column_name):
    for index, row in df.iterrows():
        graph_id = row['graph_id']
        list_id = dict [graph_id]
        df.at[index, column_name] = list_id
    return df

# Creates children dataframe
def add_children(df, dict):
    for index, row in df.iterrows():
        graph_id = df.at[index, 'graph_id']
        if graph_id in dict:
            children = list(dict[graph_id])
            df.at[index, 'child'] = '|'.join(children)
        else:
            df.at[index, 'child'] = ""
    df_children = df[['graph_id', 'child']]
    return df_children

# Creates parents dataframe
def add_parents(df, dict):
    # Reverses parents-children
    parent_dict = {}
    for entry in dict:
        dict_list = list(dict[entry])
        for disease in dict_list:
            if disease in parent_dict:
                parents = parent_dict[disease]
                parents.append(entry)
            else:
                parent_dict[disease] = [entry]
    # Adds parents to a dataframe
    for index, row in df.iterrows():
        graph_id = df.at[index, 'graph_id']
        if graph_id in parent_dict:
            parent = parent_dict[graph_id]
            df.at[index, 'parent'] = '|'.join(parent)
        else:
            df.at[index, 'parent'] = ""
    df_parents = df[['graph_id', 'parent']]
    return df_parents


#if __name__ == "__main__":
    #main(load_directory, id_class, loader_id)


