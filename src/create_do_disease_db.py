# -*- coding: utf-8 -*-
"""
Created on Tue May 26 10:38:13 2020

Purpose: download and parse Disease Ontology

@author: Paul.DePietro
"""

import os
import sys
import time
import requests
import pymysql
import csv

print('Overriding DiseaseOntology download during development...')
'''
############################################################
# Download Disease Ontology
############################################################
os.chdir('../data/')

#download_link = 'http://www.berkeleybop.org/ontologies/doid.obo'
#download_link = 'https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/master/src/ontology/subsets/DO_cancer_slim.obo'
download_link = 'http://sourceforge.net/p/diseaseontology/code/HEAD/tree/trunk/HumanDO.obo'
print('Downloading',download_link.split('/')[-1])
response = requests.get(download_link)
with open(download_link.split('/')[-1], 'wb') as f:
    f.write(response.content)

os.chdir('../src/')
'''

############################################################
# Parse Disease Ontology file
############################################################

# in_file = open('../data/doid.obo', 'r')
# in_file = open('../data/DO_cancer_slim.obo', 'r')
in_file = open('../data/HumanDO.obo', 'r')
doid_dict = {}  # {do_id:{'name':name, 'alt_id':(alt_ids), 'def':[url,definition], 'subset':(subsets), 'synonym':{syn_type:(synonyms)}, 'xref':{xref_src:(src_ids)}, 'is_a':(is_a)}}
#doid_alt = {}  # {do_id:}
doid_isa_dict = {}  # {do_id:{is_a:inferred_flag}}
doid_child_dict = {}  # {do_id:(direct children terms)}
doid_mesh = {}  # {do_id:(mesh_ids)}
doid_icd10 = {}  # {do_id:(icd10_ids)}

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

        if (xref_src == 'MSH'):
            try:
                doid_mesh[do_id].add(xref_src_id)
            except:
                doid_mesh[do_id] = set([xref_src_id])
        if (xref_src == 'ICD10CM'):
            try:
                doid_icd10[do_id].add(xref_src_id)
            except:
                doid_icd10[do_id] = set([xref_src_id])

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
                # temp_dict[cur_label].add(isa_id)
                temp_dict[cur_label][isa_id] = inferred_flag
            except:
                # temp_dict[cur_label] = set([isa_id])
                temp_dict[cur_label] = {isa_id: inferred_flag}
            try:
                # doid_isa_dict[do_id].add(isa_id)
                doid_isa_dict[do_id][isa_id] = inferred_flag
            except:
                # doid_isa_dict[do_id] = set([isa_id])
                doid_isa_dict[do_id] = {isa_id: inferred_flag}
            try:
                doid_child_dict[isa_id].add(do_id)
            except:
                doid_child_dict[isa_id] = set([do_id])

in_file.close()

print('Found', len(doid_dict.keys()), 'Disease Ontology terms that are not obsolete')

############################################################
# Write output files
############################################################

label_list = ['name', 'alt_id', 'def', 'subset', 'synonym', 'xref', 'is_a']
out_files = {}
for label in label_list:
    out_files[label] = open('../load_files/doid_' + label + '.txt', 'w', encoding='utf-8')
    #out_files[label] = open('../load_files/doid_' + label + '.csv', 'w', encoding='utf-8')
    if (label == 'synonym'):
        out_files[label].write('DO_ID\tSynType\tSynonym\n')
    elif (label == 'xref'):
        out_files[label].write('DO_ID\tXrefSource\tXref_ID\n')
    elif (label == 'def'):
        out_files[label].write('DO_ID\tURL\tDefinition\n')
    elif (label == 'is_a'):
        out_files[label].write('DO_ID\tIs_a\tInferred_Flag\n')
    else:
        out_files[label].write('DO_ID\t' + label.capitalize() + '\n')

for do_id in sorted(doid_dict.keys()):
    for label in sorted(doid_dict[do_id].keys()):
        if (label == 'name'):
            value = doid_dict[do_id][label]
            out_files[label].write(do_id + '\t' + value + '\n')
        elif (label == 'def'):
            def_url = doid_dict[do_id][label][0]
            definition = doid_dict[do_id][label][1]
            out_files[label].write(do_id + '\t' + def_url + '\t' + definition + '\n')
        elif (label == 'is_a'):
            for is_a in sorted(doid_dict[do_id][label].keys()):
                inferred_flag = doid_dict[do_id][label][is_a]
                out_files[label].write(do_id + '\t' + is_a + '\t' + inferred_flag + '\n')
        elif ((label == 'synonym') | (label == 'xref')):
            for sub_key in sorted(doid_dict[do_id][label].keys()):
                for sub_value in sorted(doid_dict[do_id][label][sub_key]):
                    out_files[label].write(do_id + '\t' + sub_key + '\t' + sub_value + '\n')
        else:
            for value in sorted(doid_dict[do_id][label]):
                out_files[label].write(do_id + '\t' + value + '\n')

for label in label_list:
    out_files[label].close()
