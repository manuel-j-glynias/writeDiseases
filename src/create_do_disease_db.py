# -*- coding: utf-8 -*-
"""
Created on Tue May 26 10:38:13 2020

Purpose: download and parse Disease Ontology

@author: Paul.DePietro
"""

import os
#import sys
from pprint import pprint
import datetime
import requests
import pymysql
import csv
import mysql.connector
from sql_utils import load_table, create_table, does_table_exist, get_local_db_connection, maybe_create_and_select_database, drop_table_if_exists

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
    doid_dict = {}  # {do_id:{'name':name, 'alt_id':(alt_ids), 'def':[url,definition], 'subset':(subsets), 'synonym':{syn_type:(synonyms)}, 'xref':{xref_src:(src_ids)}, 'is_a':(do_ids)}
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
            definition = cur_data.split(' [')[0].strip('\"')
            def_url = cur_data.split('[')[1].split(']')[0]
            temp_dict[cur_label] = [def_url, definition]

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
                '''
                try:
                    # doid_isa_dict[do_id].add(isa_id)
                    doid_isa_dict[do_id][isa_id] = inferred_flag
                except:
                    # doid_isa_dict[do_id] = set([isa_id])
                    doid_isa_dict[do_id] = {isa_id: inferred_flag}
                '''
                try:
                    doid_child_dict[isa_id].add(do_id)
                except:
                    doid_child_dict[isa_id] = set([do_id])

    in_file.close()

    print('Found', len(doid_dict.keys()), 'Disease Ontology terms that are not obsolete')
    return(doid_dict, doid_child_dict)

def get_schema():
    db_dict = {}  # {db_name:{table_name:{col:[type,key,allow_null,ref_col_list],'col_order':[cols in order]}}}

    init_file = open('../config/DO_table_descriptions.csv', 'r')
    reader = csv.reader(init_file, quotechar='\"')
    for line in reader:
        #print(line)
        db_name = line[0]
        if (db_name == 'Database'):
            continue
        if (db_name not in db_dict.keys()):
            db_dict[db_name] = {}
        table_name = line[1]
        if ('do_' not in table_name):
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


def write_load_files(db_dict, doid_dict, doid_child_dict):
    #{db_name: {table_name: {col: [type, key, allow_null,ref_col_list], 'col_order': [cols in order]}}}
    #doid_dict = {}  # {do_id:{'name':name, 'alt_id':(alt_ids), 'def':[url,definition], 'subset':(subsets), 'synonym':{syn_type:(synonyms)}, 'xref':{xref_src:(src_ids)}, 'is_a':(is_a)}}
    for db_name in sorted(db_dict.keys()):
        for table_name in sorted(db_dict[db_name].keys()):
            out_file = open('../load_files/' + table_name + '.csv', 'w', encoding='utf-8')
            header = db_dict[db_name][table_name]['col_order']
            writer = csv.writer(out_file, lineterminator='\n')
            writer.writerow(header)

            if ('diseases' in table_name):
                for do_id in sorted(doid_dict.keys()):
                    dx_name = doid_dict[do_id]['name']
                    try:
                        url = doid_dict[do_id]['def'][0]
                        definition = doid_dict[do_id]['def'][1]
                    except:
                        url = ''
                        definition = ''
                    graph_id = do_id.replace(':','_').lower()
                    cur_data = [do_id,dx_name,definition,url,graph_id]
                    writer.writerow(cur_data)
            elif ('synonyms' in table_name):
                for do_id in sorted(doid_dict.keys()):
                    if ('synonym' in doid_dict[do_id].keys()):
                        for syn_type in sorted(doid_dict[do_id]['synonym'].keys()):
                            for synonym in sorted(doid_dict[do_id]['synonym'][syn_type]):
                                cur_data = [do_id, syn_type, synonym]
                                writer.writerow(cur_data)
            elif ('xrefs' in table_name):
                for do_id in sorted(doid_dict.keys()):
                    if ('xref' in doid_dict[do_id].keys()):
                        for source in sorted(doid_dict[do_id]['xref'].keys()):
                            for src_id in sorted(doid_dict[do_id]['xref'][source]):
                                cur_data = [do_id, source, src_id]
                                writer.writerow(cur_data)
            elif ('subsets' in table_name):
                for do_id in sorted(doid_dict.keys()):
                    if ('subset' in doid_dict[do_id].keys()):
                        for subset in sorted(doid_dict[do_id]['subset']):
                            cur_data = [do_id, subset]
                            writer.writerow(cur_data)
            elif ('parents' in table_name):
                for do_id in sorted(doid_dict.keys()):
                    if ('is_a' in doid_dict[do_id].keys()):
                        for parent in sorted(doid_dict[do_id]['is_a']):
                            #inferred_flag = doid_dict[do_id]['is_a'][parent]
                            cur_data = [do_id, parent]
                            #cur_data = [do_id, parent, inferred_flag]
                            writer.writerow(cur_data)
            elif ('children' in table_name):
                for do_id in sorted(doid_child_dict.keys()):
                    for child in sorted(doid_child_dict[do_id]):
                        cur_data = [do_id,child]
                        writer.writerow(cur_data)
            out_file.close()

def main():
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    my_db = None
    my_cursor = None

    print('WARNING: skipping DiseaseOntology download during development')
    #download_do()
    doid_dict,doid_child_dict = parse_do()
    db_dict = get_schema()
    write_load_files(db_dict, doid_dict, doid_child_dict)
    try:
        my_db = get_local_db_connection()
        my_cursor = my_db.cursor(buffered=True)
        for db_name in sorted(db_dict.keys()):
            maybe_create_and_select_database(my_cursor, db_name)
            for table_name in sorted(db_dict[db_name].keys()):
                drop_table_if_exists(my_cursor, table_name)
                create_table(my_cursor, table_name, db_name, db_dict)
                load_table(my_cursor, table_name, db_dict[db_name][table_name]['col_order'])
    except mysql.connector.Error as error:
        print("Failed in MySQL: {}".format(error))
    finally:
        if (my_db.is_connected()):
            my_cursor.close()
    print(datetime.datetime.now().strftime("%H:%M:%S"))


if __name__ == "__main__":
    main()