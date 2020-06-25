import mysql.connector
import datetime
import pandas
from pandas.io.json import json_normalize
import csv
from sql_utils import load_table, create_table, does_table_exist, \
    get_local_db_connection, maybe_create_and_select_database, \
    drop_table_if_exists
import json
import write_sql
import get_schema


def main(load_directory):
    print(datetime.datetime.now().strftime("%H:%M:%S"))

    # Data
    go_data =  load_directory + 'go_diseases.csv'
    go_xrefs_data =  load_directory + 'go_xrefs.csv'
    jax_diseases_data = load_directory + 'jax_diseases.csv'
    omnimap_data = '../data/tblOS_GLOBAL_JAX_DL_OmniMap.csv'


    # Create dataframes from data

    go_df = pandas.read_csv(go_data)
    go_xrefs_df= pandas.read_csv(go_xrefs_data)
    jax_diseases_df = pandas.read_csv(jax_diseases_data)
    omnimap_df = pandas.read_csv(omnimap_data)

    #Main ontological dataframe
    ontological_df = parse_ontological(go_df)
    ontological_path =  load_directory + 'ontological_diseases.csv'
    write_load_files(ontological_df, ontological_path)


    #Connecting tables
    ontological_jax_df = parse_jax(go_xrefs_df, ontological_df)
    ontological_jax_path =  load_directory + 'ontological_jax_diseases.csv'
    write_load_files(ontological_jax_df, ontological_jax_path)

    ontological_go_df = parse_go(ontological_df)
    ontological_go_path =  load_directory + 'ontological_go_diseases.csv'
    write_load_files(ontological_go_df, ontological_go_path)

    # Create onto-go and onto-jax dictionaries
    ontological_go_dict = create_onto_go_dict(ontological_go_df)
    ontological_jax_dict = create_onto_jax_dict(ontological_jax_df)

    ontological_do_df = parse_do(jax_diseases_df, ontological_jax_dict)
    ontological_do_path =  load_directory + 'ontological_do_diseases.csv'
    write_load_files(ontological_do_df, ontological_do_path)

    ontological_oncotree_df = parse_oncotree(go_xrefs_df, ontological_go_dict)
    ontological_oncotree_path =  load_directory + 'ontological_oncotree_diseases.csv'
    write_load_files(ontological_oncotree_df, ontological_oncotree_path)

    ontological_omni_df = parse_omnidisease(omnimap_df, ontological_jax_dict)
    ontological_omni_path =  load_directory + 'ontological_omnidiseases.csv'
    write_load_files(ontological_omni_df, ontological_omni_path)

    jax_mcode_df = parse_mcode(omnimap_df, ontological_jax_dict)
    jax_mcode_path =  load_directory + 'ontological_mcode_diseases.csv'
    write_load_files(jax_mcode_df, jax_mcode_path)


    ontological_diseases_dict= get_schema.get_schema('ontological_diseases')
    ontological_jax_diseases_dict = get_schema.get_schema('ontological_jax_diseases')
    ontological_do_diseases_dict = get_schema.get_schema('ontological_do_diseases')
    ontological_oncotree_diseases_dict = get_schema.get_schema('ontological_oncotree_diseases')
    ontological_go_diseases_dict = get_schema.get_schema('ontological_go_diseases')
    ontological_omnidiseases_dict = get_schema.get_schema('ontological_omnidiseases')
    ontological_mcode_diseases = get_schema.get_schema('ontological_mcode_diseases')


    write_sql.write_sql(ontological_diseases_dict)
    write_sql.write_sql(ontological_jax_diseases_dict)
    write_sql.write_sql(ontological_do_diseases_dict)
    write_sql.write_sql(ontological_oncotree_diseases_dict)
    write_sql.write_sql(ontological_go_diseases_dict)
    write_sql.write_sql(ontological_omnidiseases_dict)
    write_sql.write_sql(ontological_mcode_diseases)

# Create dictionaries

def create_onto_go_dict(ontological_go_df):
    ontological_go_dict = {}
    for index, row in ontological_go_df.iterrows():
        # new_dict['go_disease_id'] = go_df.at[index, 'definition']
        ontological_go_dict[ontological_go_df.at[index, 'go_disease_id']] = ontological_go_df.at[index, 'graph_id']
    return ontological_go_dict

def create_onto_jax_dict(ontological_jax_df):
    ontological_jax_dict = {}
    for index, row in ontological_jax_df.iterrows():
        jax_id = ontological_jax_df.at[index, 'jax_id']
        ontological_jax_dict[jax_id] = ontological_jax_df.at[index, 'graph_id']
    return ontological_jax_dict

# Create ontological disease df
def parse_ontological(go_df):
    ontological_list = []
    counter = 1
    for index, row in go_df.iterrows():
        new_dict = {}
        new_dict['graph_id'] = 'ontological_disease_' + str(counter)
        new_dict['name'] = go_df.at[index, 'name']
        new_dict['definition'] = go_df.at[index, 'definition']
        new_dict['go_disease_id'] = go_df.at[index, 'graph_id']
        ontological_list.append(new_dict)
        counter += 1
    ontological_df = pandas.DataFrame(ontological_list)
    return ontological_df


def parse_go(ontological_df):
    ontological_go_list = []
    for index, row in ontological_df.iterrows():
        new_dict = {}
        new_dict['graph_id']= ontological_df.at[index, 'graph_id']
        new_dict['go_disease_id'] = ontological_df.at[index, 'go_disease_id']
        ontological_go_list.append(new_dict)
    ontological_go_df = pandas.DataFrame(ontological_go_list)
    return ontological_go_df

def parse_jax(go_xrefs_df, ontological_go_df):
    ontological_go_dict = {}
    for index, row in ontological_go_df.iterrows():
    #new_dict['go_disease_id'] = go_df.at[index, 'definition']
        ontological_go_dict [ontological_go_df.at[index, 'go_disease_id']] = ontological_go_df.at[index, 'graph_id']
    go_jax_list = []
    for index, row in go_xrefs_df.iterrows():
        source = go_xrefs_df.at[index, 'source']
        if source == 'JAX-CKB':
            for entry in ontological_go_dict:
                reference_ontological = ontological_go_dict[entry]
                reference_go = entry
                current_go = go_xrefs_df.at[index, 'graph_id']
                if current_go == reference_go:
                    new_dict = {}
                    new_dict['graph_id']= reference_ontological
                    new_dict['jax_id'] = (go_xrefs_df.at[index, 'xrefld']).replace('JAX', 'jax_disease_')
                    go_jax_list.append(new_dict)
    ontological_jax_df = pandas.DataFrame(go_jax_list)
    return ontological_jax_df

def parse_do(jax_diseases_df, ontological_jax_dict):
    jax_do_list = []
    for index, row in jax_diseases_df.iterrows():
        if (jax_diseases_df.at[index, 'source'] == 'DOID'):
            for entry in ontological_jax_dict:
                reference_jax_id = entry
                current_jax_id = jax_diseases_df.at[index, 'graph_id']
                if reference_jax_id == current_jax_id:
                    new_dict = {}
                    new_dict['graph_id'] = ontological_jax_dict[entry]
                    do_id =  (jax_diseases_df.at[index, 'termId']).replace('DOID:', 'do_disease_')
                    new_dict['do_disease_id'] =do_id
                    jax_do_list.append(new_dict)
    ontological_do_df = pandas.DataFrame(jax_do_list)
    return ontological_do_df

def parse_oncotree(go_xrefs_df, ontological_go_dict):
    go_oncotree_list = []
    for index, row in go_xrefs_df.iterrows():
        source = go_xrefs_df.at[index, 'source']
        if source == 'oncotree':
            for entry in ontological_go_dict:
                reference_go_id = entry
                reference_ontological_id = ontological_go_dict[entry]
                current_go_id = go_xrefs_df.at[index, 'graph_id']
                if reference_go_id == current_go_id:
                    new_dict = {}
                    new_dict['graph_id'] = reference_ontological_id
                    new_dict['oncotree_disease_id'] = 'oncotree_disease_'  + (go_xrefs_df.at[index, 'xrefld'])
                    go_oncotree_list.append(new_dict)
    go_oncotree_df = pandas.DataFrame(go_oncotree_list)
    return go_oncotree_df

def parse_omnidisease(omnimap_df, ontological_jax_dict):
    ontological_omnidisease_list = []
    for index, row in omnimap_df.iterrows():
        omnidisease = omnimap_df.at[index, 'OmniDisease_ID']
        jax_id = omnimap_df.at[index, 'ResourceDiseaseID']
        jax_id = 'jax_disease_' + str(jax_id)
        current_jax_id = jax_id
        if pandas.isnull(omnidisease):
            pass
        else:
            for entry in ontological_jax_dict:
                reference_jax_id = entry

                if reference_jax_id == current_jax_id:
                    new_dict = {}
                    omnidisease_id = omnimap_df.at[index, 'OmniDisease_ID']
                    omnidisease_id = omnidisease_id.replace('OmniDx_', 'omnidisease_')

                    new_dict['graph_id'] = ontological_jax_dict[entry]
                    new_dict['omnidisease_id'] = omnidisease_id
                    ontological_omnidisease_list.append(new_dict)
    ontological_omnidisease_df = pandas.DataFrame(ontological_omnidisease_list)
    return ontological_omnidisease_df

def parse_mcode(omnimap_df, ontological_jax_dict):
    ontological_mcode_list = []
    for index, row in omnimap_df.iterrows():
        mcode = omnimap_df.at[index, 'MCode']
        jax_id = omnimap_df.at[index, 'ResourceDiseaseID']
        jax_id = 'jax_disease_' + str(jax_id)
        current_jax_id = jax_id
        if pandas.isnull(mcode):
            pass
        else:
            for entry in ontological_jax_dict:
                reference_jax_id = entry
                if reference_jax_id == current_jax_id:
                    new_dict = {}
                    mcode_id = 'Mcode_' + str(mcode)
                    new_dict['graph_id'] = ontological_jax_dict[entry]
                    new_dict['mcode_id'] = mcode_id
                    ontological_mcode_list.append(new_dict)
    ontological_mcode_df = pandas.DataFrame(ontological_mcode_list)
    return ontological_mcode_df

###################################################
#  WRITE CSV
###################################################
# Converts dataframe to csv file
# Input: dataframe and file path
# Output: csv file is written
def write_load_files (df, path):
    try:
        df.to_csv(path, encoding='utf-8', index=False)
    except IOError:
        print("I/O error csv file")


if __name__ == "__main__":
    main()