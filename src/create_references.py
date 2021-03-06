import os
import csv
import os.path
import re
from os import path
import config
import sql_utils
import graphql_utils
import create_id
import pandas

load_directory = '../load_files/'
extract_dir =  '../load_files/'
loader_id = 'user_20200422163431232329'
#id_class = create_id.ID('', '', 0, 0, 0, 0, 0, 0, 0)

def extract_list(file_to_extract):
    unparsed_df = pandas.read_csv(file_to_extract)
    unparsed_df = unparsed_df.applymap(str)
    if 'LiteratureReference.csv' in file_to_extract:
        for index, row in unparsed_df.iterrows():
            pmid = row['PMID']
            if '=' in pmid:
                print (pmid)
    extracted_list = unparsed_df.to_dict('records')
    return extracted_list


# Given file location creates doid-graph_id dictionary
def get_do_disease_dict(extract_dir)->dict:
    do_disease_list = extract_list(extract_dir + 'do_diseases.csv')
    new_dict = {item['graph_id']: item for item in do_disease_list}
    return new_dict

def get_literature_reference_dict(extract_dir):
    if path.exists(extract_dir + 'LiteratureReference.csv'):
       pass
    else:
        ref_df = pandas.DataFrame( columns=['PMID' ,'DOI', 'title', 'Journal_graph_id', 'volume',
                                            'firstPage', 'lastPage', 'publicationYear', 'shortReference', 'abstract', 'graph_id'])
        ref_df.to_csv(extract_dir + 'LiteratureReference.csv', index=False)
    pmid_list = extract_list(extract_dir + 'LiteratureReference.csv')
    new_dict = {}
    for entry in pmid_list:
        new_dict[entry['PMID']] = entry['graph_id']
    return new_dict

def get_journal_dict(extract_dir):
    if path.exists(extract_dir + 'Journal.csv'):
        pass
    else:
        ref_df = pandas.DataFrame(columns=['name' , 'graph_id'])
        ref_df.to_csv(extract_dir + 'Journal.csv', index=False)
    journal_list = extract_list(extract_dir + 'Journal.csv')
    new_dict = {item['graph_id']: item for item in journal_list}
    return new_dict

def get_author_dict(extract_dir):
    if path.exists(extract_dir + 'Author.csv'):
        pass
    else:
        ref_df = pandas.DataFrame(columns=['surname', 'firstInitial', 'graph_id'])
        ref_df.to_csv(extract_dir + 'Author.csv', index=False)
    author_list = extract_list(extract_dir + 'Author.csv')
    new_dict = {item['graph_id']: item for item in author_list}
    return new_dict

# [PMID] : [graph_id}
def get_definition_urls_dict(extract_dir)->dict:
    reference_dict = {}
    df = pandas.read_csv(extract_dir + 'do_definition_urls.csv')
    for index, row in df.iterrows():
        graph_id = row['graph_id']
        reference = row['reference']
        if pandas.isnull(reference):
            pass
        else:
            if '/pubmed/' in reference:
                pubmed = reference.split('/pubmed/')[1]
                pubmed = pubmed.split('\'')[0]
                reference_dict[graph_id] = pubmed
    return reference_dict

def create_load_files_dict(db_dict, load_dir):
    load_files_dict = {}
    load_files_list = ['Author', 'do_definition_urls', 'do_diseases',
                       'EditableStatement_LiteratureReference', 'Journal', 'LiteratureReference',  'LiteratureReference_Author']
    for table_name in load_files_list:
        out_file = open(load_dir + table_name + '.csv', 'a+', encoding='utf-8')
        writer = csv.writer(out_file, lineterminator='\n', quoting=csv.QUOTE_ALL)
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
    if pmid in ref_by_pmid:
        graph_id = ref_by_pmid[pmid]
    else:
        #print(pmid)
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

    return graph_id

def write_reference():
    print()

def main(df):
    url_df  = df[['graph_id','reference']]
    url_df.to_csv(load_directory + 'do_definition_urls.csv')
    data_dict = {'loader_id': loader_id}
    data_dict['do_disease_dict'] = get_do_disease_dict(extract_dir)
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

    disease_dict = data_dict['do_disease_dict']
    for entry in pmid_dict:
        input = pmid_dict[entry]
        input = re.sub("[^0-9]", "", input)
        ref_id =  preflight_ref(input, load_files_dict, data_dict)
        disease = disease_dict[entry]
        definition = disease['definition']
        EditableStatement_LiteratureReference_writer = load_files_dict['EditableStatement_LiteratureReference']['writer']
        EditableStatement_LiteratureReference_writer.writerow(['', definition, ref_id ])
    print('All done!')


if __name__ == "__main__":
    main()