import csv
import datetime
import os
import shutil
import json
import mysql.connector
import csv

from src.graphql_utils import replace_characters, get_reference_from_pmid_by_metapub, fix_author_id, get_authors_names, ref_name_from_authors_pmid_and_year
from src.sql_utils import get_local_db_connection, maybe_create_and_select_database, drop_table_if_exists, create_table, \
    get_schema, write_load_files_using_func, load_table, get_load_dir

def main():

    # FIND DIRECTORIES
    load_files_directory = get_load_dir()
    load_dir = load_files_directory  + '/load_files_disease/'
    extract_dir = get_load_dir() + 'extracted_disease/'
    descriptions_csv_path = '../config/table_descriptions_03_01.csv'

    # CREATE DICTIONARY FROM SCHEMA
    db_dict = get_schema(descriptions_csv_path)['OmniSeqKnowledgebase2']

    # LOAD FILES
    load_files_dict = create_load_files_dict(db_dict, load_dir)

    user_id = get_loader_user_id(extract_dir)

    data_dict = {'loader_id': user_id}
    data_dict['do_disease_dict'] = get_do_disease_dict(extract_dir)
    data_dict['do_disease_pmid'] = get_definition_urls_dict(extract_dir)
    data_dict['do_parents'] = get_do_disease_parents_dict(extract_dir)
    data_dict['ref_by_pmid'] = get_literature_reference_dict(load_files_directory)
    # [graph_id] = [name(of journal)]
    data_dict['journal_dict'] = get_journal_dict(load_files_directory)
    # [graph_id] = {[first initial], [last initial]}
    data_dict['author_dict'] = get_author_dict(load_files_directory)

    do_disease_loader(load_files_dict, data_dict)

    print()

# [PMID] : [graph_id}
def get_literature_reference_dict(extract_dir)->dict:
    literature_reference_dict = {}
    firstline = True
    with open(extract_dir + 'LiteratureReference.csv', encoding="utf8") as csvfile:
        refs = csv.reader(csvfile)
        for row in refs:
            if firstline:
                firstline = False
            else:
                literature_reference_dict[row[0]] = row[3]
    return literature_reference_dict

def get_loader_user_id(extract_dir):
    user_id = None
    firstline = True
    directory = extract_dir + 'User.csv'
    with open(directory) as csvfile:
        users = csv.reader(csvfile)
        for row in users:
            if firstline:
                firstline = False
            else:
                if row[0]=='loader':
                    user_id = row[3]
                    break
    return user_id

# Given file location creates doid-graph_id dictionary
def get_do_disease_dict(extract_dir)->dict:
    do_disease_dict = {}
    firstline = True
    with open(extract_dir + 'do_diseases.csv') as csvfile:
        do_diseases = csv.reader(csvfile)
        for row in do_diseases:
            if firstline:
                firstline = False
            else:
                do_disease_dict[row[0]] = row[3]
    return do_disease_dict

####################EDITED###################################
# [PMID] : [graph_id}
def get_definition_urls_dict(extract_dir)->dict:
    reference_dict = {}
    firstline = True
    with open(extract_dir + 'do_definition_urls.csv') as csvfile:
        refs = csv.reader(csvfile)
        for row in refs:
            if firstline:
                firstline = False
            else:
                reference = row[1]
                if '/pubmed/' in reference:
                    pubmed = reference.split('/pubmed/')[1]
                    reference_dict[row[0]] = pubmed
    return reference_dict

def get_do_disease_parents_dict(extract_dir):
    do_parents_list = []
    with open('../load_files/extracted/do_parents.csv') as csvfile:
        do_parents_list = [{k: str(v) for k, v in row.items()}
                           for row in csv.DictReader(csvfile, skipinitialspace=True)]
    return do_parents_list

def do_disease_loader(load_files_dict,data_dict):
    print ('in disease')
    # get writer object from load_files_dict
    editable_statement_writer = load_files_dict['EditableStatement_DoDisease']['writer']
    do_disease_writer = load_files_dict['DoDiseases']['writer']
    es_lr_writer = load_files_dict['EditableStatement_LiteratureReference']['writer']

    loader_id = data_dict['loader_id']
    do_disease_dict = data_dict['do_disease_dict']
    counter = 0

    ###################################################################################
    do_disease_list = []
    with open('../load_files/extracted/do_diseases.csv') as csvfile:
        do_disease_list = [{k: str(v) for k, v in row.items()}
             for row in csv.DictReader(csvfile, skipinitialspace=True)]
    ###################################################################################
    for disease in do_disease_list:
        counter += 1
        if (counter % 100 == 0):
            print(counter)
        if disease is not None:
            # print(variant)
            graph_id = disease['graph_id']

            des_field: str = str(graph_id)
            es_des_id = write_editable_statement(des_field, editable_statement_writer, loader_id, disease['name'])

            do_disease_writer.writerow([disease['doId'], disease['name'], disease['definition'], es_des_id, disease['graph_id']])
            ref_dict = data_dict['do_disease_pmid']


            if graph_id  in ref_dict:
                pmid = ref_dict[graph_id]
                if pmid != None:
                    ref_id =preflight_ref(pmid, load_files_dict, data_dict)
                    if ref_id != None:
                        es_lr_writer.writerow([None, es_des_id, ref_id])




#Input:
def preflight_ref(pmid, load_files_dict, data_dict):
    graph_id = None
    ref_by_pmid = data_dict['ref_by_pmid']
    journal_dict = data_dict['journal_dict']
    author_dict = data_dict['author_dict']
    if not pmid in ref_by_pmid:
        reference = get_reference_from_pmid_by_metapub(pmid)
        if reference != None:
            graph_id = 'ref_' + str(pmid)
            journal = reference['journal']
            journal_id = 'journal_' + fix_author_id(journal)
            if not journal_id in journal_dict:
                journal_writer = load_files_dict['Journal']['writer']
                journal_writer.writerow([journal,journal_id])
                journal_dict[journal_id] = journal
            literatureReference_writer = load_files_dict['LiteratureReference']['writer']
            short_ref = ref_name_from_authors_pmid_and_year(reference['authors'], reference['pmid'], reference['year'])
            literatureReference_writer.writerow([reference['pmid'],reference['doi'],reference['title'],journal_id,reference['volume'],reference['first_page'],
                             reference['last_page'],reference['year'],short_ref,reference['abstract'],graph_id])
            author_writer = load_files_dict['Author']['writer']
            literatureReference_author_writer = load_files_dict['LiteratureReference_Author']['writer']
            for author in reference['authors']:
                first, surname = get_authors_names(author)
                author_id = fix_author_id('author_' + surname + '_' + first)
                if not author_id in author_dict:
                    author_writer.writerow([surname, first, author_id])
                    author_dict[author_id] = {'surname':surname, 'firstInitial':first}
                literatureReference_author_writer.writerow([None,author_id,graph_id])
            ref_by_pmid[pmid] = graph_id
    else:
        graph_id = ref_by_pmid[pmid]
    return graph_id

def write_editable_statement(field, editable_statement_writer, loader_id, statement):
    now = datetime.datetime.now()
    es_des_id: str = 'es_' + now.strftime("%Y%m%d%H%M%S%f")
    editable_statement_writer.writerow([field, statement, now.strftime("%Y-%m-%d-%H-%M-%S-%f"), loader_id, es_des_id])
    return es_des_id

def is_empty(out_file):
    out_file.seek(0, os.SEEK_END)
    return out_file.tell()==0


def create_load_files_dict(db_dict, load_dir):
    load_files_dict = {}
    load_files_list = ['Author', 'DoDefinitionUrls', 'DoDiseases', 'DoSubsets', 'DoSynonyms', 'DoXrefs',
                       'EditableStatement_LiteratureReference', 'EditableStatement_DoDisease']
    for table_name in load_files_list:
        out_file = open(load_dir + table_name + '.csv', 'a+', encoding='utf-8')
        writer = csv.writer(out_file, lineterminator='\n')
        if is_empty(out_file):
            header = db_dict[table_name]['col_order']
            writer.writerow(header)
        load_files_dict[table_name] = {'writer':writer, 'file':out_file}
    return load_files_dict

# [graph_id] = [name(of journal)]
def get_journal_dict(extract_dir):
    journal_dict = {}
    firstline = True
    with open(extract_dir + 'Journal.csv',  encoding='utf-8') as csvfile:
        journals = csv.reader(csvfile)
        for row in journals:
            if firstline:
                firstline = False
            else:
                journal_dict[row[1]] = row[0]

    return journal_dict
# [graph_id] = {[first initial], [last initial]}
def get_author_dict(extract_dir):
    author_dict = {}
    firstline = True
    with open(extract_dir + 'Author.csv',  encoding='utf-8') as csvfile:
        authors = csv.reader(csvfile)
        for row in authors:
            if firstline:
                firstline = False
            else:
                author_dict[row[2]] = {'surname':row[0], 'firstInitial':row[1]}

    return author_dict
if __name__ == "__main__":
    main()