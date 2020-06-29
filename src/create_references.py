import csv
import datetime
import os
import shutil
import json
import mysql.connector
import csv
import os.path
from os import path
import config
import sql_utils
import graphql_utils
import get_schema

loader_id = '007'
extract_dir = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/'

def extract_list(file_to_extract):
    extracted_list = []
    if (path.exists(file_to_extract)):
        with open(file_to_extract) as f:
            extracted_list = [{k: str(v) for k, v in row.items()}
                               for row in csv.DictReader(f, skipinitialspace=True)]
    return extracted_list


# Given file location creates doid-graph_id dictionary
def get_do_disease_dict(extract_dir)->dict:
    do_disease_list = extract_list(extract_dir + 'do_diseases.csv')
    new_dict = {item['graph_id']: item for item in do_disease_list}
    return new_dict

def get_literature_reference_dict(extract_dir):
    pmid_list = extract_list(extract_dir + 'LiteratureReference.csv')
    new_dict = {}
    for entry in pmid_list:
        new_dict[entry['PMID']] = entry['graph_id']
    return new_dict

def get_journal_dict(extract_dir):
    journal_list = extract_list(extract_dir + 'Journal.csv')
    new_dict = {item['graph_id']: item for item in journal_list}
    return new_dict

def get_author_dict(extract_dir):
    author_list = extract_list(extract_dir + 'Author.csv')
    new_dict = {item['graph_id']: item for item in author_list}
    return new_dict

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

def create_load_files_dict(db_dict, load_dir):
    load_files_dict = {}
    load_files_list = ['Author', 'do_definition_urls', 'do_diseases', 'do_xrefs',
                       'EditableStatement_LiteratureReference', 'Journal', 'LiteratureReference',  'LiteratureReference_Author']
    for table_name in load_files_list:
        out_file = open(load_dir + table_name + '.csv', 'a+', encoding='utf-8')
        writer = csv.writer(out_file, lineterminator='\n')
        if is_empty(out_file):
            header = db_dict[table_name]['col_order']
            writer.writerow(header)
        load_files_dict[table_name] = {'writer':writer, 'file':out_file}
    return load_files_dict

def is_empty(out_file):
    out_file.seek(0, os.SEEK_END)
    return out_file.tell()==0


def preflight_ref(pmid, load_files_dict, data_dict):
    graph_id = None
    ref_by_pmid = data_dict['ref_by_pmid']

    journal_dict = data_dict['journal_dict']
    author_dict = data_dict['author_dict']
    if not pmid in ref_by_pmid:
        reference = graphql_utils.get_reference_from_pmid_by_metapub(pmid)
        if reference != None:
            graph_id = 'ref_' + str(pmid)
            journal = reference['journal']
            journal_id = 'journal_' + graphql_utils.fix_author_id(journal)
            if not journal_id in journal_dict:
                journal_writer = load_files_dict['Journal']['writer']
                journal_writer.writerow([journal,journal_id])
                journal_dict[journal_id] = journal
            literatureReference_writer = load_files_dict['LiteratureReference']['writer']
            short_ref = graphql_utils.ref_name_from_authors_pmid_and_year(reference['authors'], reference['pmid'], reference['year'])
            literatureReference_writer.writerow([reference['pmid'],reference['doi'],reference['title'],journal_id,reference['volume'],reference['first_page'],
                             reference['last_page'],reference['year'],short_ref,reference['abstract'],graph_id])
            author_writer = load_files_dict['Author']['writer']
            literatureReference_author_writer = load_files_dict['LiteratureReference_Author']['writer']
            for author in reference['authors']:
                first, surname = graphql_utils.get_authors_names(author)
                author_id = graphql_utils.fix_author_id('author_' + surname + '_' + first)
                if not author_id in author_dict:
                    author_writer.writerow([surname, first, author_id])
                    author_dict[author_id] = {'surname':surname, 'firstInitial':first}
                literatureReference_author_writer.writerow([None,author_id,graph_id])
            ref_by_pmid[pmid] = graph_id
    else:
        graph_id = ref_by_pmid[pmid]
    return graph_id
def write_reference():
    print()


data_dict = {'loader_id': loader_id}
data_dict['do_disease_dict'] = get_do_disease_dict(extract_dir)
#data_dict['do_disease_pmid'] = get_definition_urls_dict(extract_dir)
#data_dict['do_parents'] = get_do_disease_parents_dict(extract_dir)
data_dict['ref_by_pmid'] = get_literature_reference_dict(extract_dir)
# [graph_id] = [name(of journal)]
data_dict['journal_dict'] = get_journal_dict(extract_dir)
# [graph_id] = {[first initial], [last initial]}
data_dict['author_dict'] = get_author_dict(extract_dir)

pmid_dict = get_definition_urls_dict(extract_dir)
definition_dict = data_dict['do_disease_dict']
literature_reference = data_dict['ref_by_pmid']

descriptions_csv_path = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/config/table_descriptions.csv'
db_dict = config.extract_file(descriptions_csv_path)
load_files_dict = create_load_files_dict(db_dict, extract_dir)

for entry in pmid_dict:
    input = pmid_dict[entry]
    ref_id =  preflight_ref(input, load_files_dict, data_dict)
    print(ref_id)

print()
