import csv
import datetime
import os
import shutil
import json
import mysql.connector
import csv
import os.path
from os import path

loader_id = '007'
extract_dir = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/'

def extract_list(file_to_extract):
    extracted_list = []
    if (path.exists(file_to_extract)):
        with open(file_to_extract) as f:
            extracted_list = [{k: int(v) for k, v in row.items()}
                               for row in csv.DictReader(f, skipinitialspace=True)]
    return extracted_list


# Given file location creates doid-graph_id dictionary
def get_do_disease_dict(extract_dir)->dict:
    do_disease_list = extract_list(extract_dir + 'do_diseases.csv')
    new_dict = {item['graph_id']: item for item in do_disease_list}
    return new_dict

def get_definition_urls_dict(extract_dir):
    definition_url_list = extract_list(extract_dir + 'do_definition_urls.csv')
    new_dict = {item['graph_id']: item for item in definition_url_list}
    return new_dict

def get_literature_reference_dict(extract_dir):
    pmid_list = extract_list(extract_dir + 'LiteratureReference.csv')
    new_dict = {item['graph_id']: item for item in pmid_list}
    return new_dict

def get_journal_dict(extract_dir):
    journal_list = extract_list(extract_dir + 'Journal.csv')
    new_dict = {item['graph_id']: item for item in journal_list}
    return new_dict

def get_author_dict(extract_dir):
    author_list = extract_list(extract_dir + 'Author.csv')
    new_dict = {item['graph_id']: item for item in author_list}
    return new_dict

data_dict = {'loader_id': loader_id}
data_dict['do_disease_dict'] = get_do_disease_dict(extract_dir)
data_dict['do_disease_pmid'] = get_definition_urls_dict(extract_dir)
#data_dict['do_parents'] = get_do_disease_parents_dict(extract_dir)
data_dict['ref_by_pmid'] = get_literature_reference_dict(extract_dir)
# [graph_id] = [name(of journal)]
data_dict['journal_dict'] = get_journal_dict(extract_dir)
# [graph_id] = {[first initial], [last initial]}
data_dict['author_dict'] = get_author_dict(extract_dir)
