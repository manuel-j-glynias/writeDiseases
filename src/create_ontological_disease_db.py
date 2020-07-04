import mysql.connector
import datetime
import pandas
import extract_ontological
from pandas.io.json import json_normalize
import csv
from sql_utils import load_table, create_table, does_table_exist, \
    get_local_db_connection, maybe_create_and_select_database, \
    drop_table_if_exists
import json
import write_sql
import get_schema
import create_id
import create_EditableDiseaseList
load_directory = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/'
loader_id = '007'
editable_statement_list = ['name', 'description']
table_name = 'ontological_diseases'
import create_editable_statement
id_class = create_id.ID('', '')
import extract_ontological


def main(load_directory):
    # Create a dataframe
    ontological_df = parse_go()

    # Create editable diseases lists and add it to a dataframe
    ontological_df = create_EditableDiseaseList.assign_editable_disease_lists(ontological_df, loader_id, load_directory, id_class)

    # Add editable statements to a dataframe
    ontological_df_with_statements = create_editable_statement.assign_editable_statement(ontological_df,
                                                                             editable_statement_list, loader_id,
                                                                             load_directory, table_name, id_class)

    # Temporary file
    path = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/ontological_diseases_table.csv'
    ontological_df_with_statements.to_csv(path, encoding='utf-8', index=False)


    return ontological_df


def parse_go():
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    files = ['../data/GO_diseases/diseases_pg1.json',
             '../data/GO_diseases/diseases_pg2.json',
             '../data/GO_diseases/diseases_pg3.json',
             '../data/GO_diseases/diseases_pg4.json',
             '../data/GO_diseases/diseases_pg5.json',
             '../data/GO_diseases/diseases_pg6.json']

    #Create dataframe
    df=create_go_dataframe(files)
    return df

# Add diseases extracted from GO files to a ontological disease dataframe
def add_disease_to_df(code_list, df_to_edit, index, substring, id_string, column_name):
    diseases = [l for l in code_list if substring in l]
    if diseases:
        final_disease_list = []
        for disease in diseases:
            disease_id = id_string + disease.replace(substring, '')
            final_disease_list.append(disease_id)
        df_to_edit.at[index, column_name] = final_disease_list
    else:
        df_to_edit.at[index, column_name] = diseases
    return df_to_edit

# Converts files to dataframes and combines dataframes
def create_go_dataframe( files):
    dataframe_list = list_of_dfs_to_combine(files)
    go_df = combine_dataframes(dataframe_list)
    return go_df

#  Create a list of dataframes to combine
def list_of_dfs_to_combine(files):
    dataframe_list = []
    # Create a dataframe for each file
    for file in files:
        # Convert json to dataframe
        data = json.load(open(file))
        df = json_normalize(data, 'results')
        dataframe_list.append(df)
    return dataframe_list

# Combines dataframes
def combine_dataframes(dataframe_list):
    # Clean dataframes and add graph_id column
    dfs_to_combine = []
    counter = 1
    for df in dataframe_list:
        # Add new columns and create parent-child and id dictionaries
        for index, row in df.iterrows():
            # Create graph_id column
            df.at[index, 'graph_id'] = ''
            df.at[index, 'jaxDiseases'] = ''
            df.at[index, 'doDiseases'] = ''
            df.at[index, 'goDiseases'] = ''
            df.at[index, 'oncoTreeDiseases'] = ''

            # Get diseases from 'codes' column
            code_list = row[ 'codes']
            if type(code_list) is list:
                if code_list:
                    df = add_disease_to_df(code_list, df, index, 'JAX-CKB=JAX', 'jax_disease_', 'jaxDiseases')
                    df = add_disease_to_df(code_list, df, index, 'DOID=', 'do_disease_', 'doDiseases')
            else:
                temp_list = []
                df.at[index, 'jaxDiseases'] = temp_list
                df.at[index, 'doDiseases'] = temp_list

            # Assign ontological_disease id
            df.at[index, 'graph_id'] = 'ontological_disease_' + str(counter)
            counter += 1

            # Get info rom oncotree column
            oncotree_code = row['oncotree_code']
            if pandas.isnull(oncotree_code):
                onco_list = []
                df.at[index, 'oncoTreeDiseases'] = onco_list
            else:
                if type(oncotree_code) is list:
                    if oncotree_code:
                        df = add_disease_to_df(oncotree_code, df, index, '', 'oncotree_disease_', 'oncoTreeDiseases')
                else:
                    onco_list = []
                    onco_id = 'oncotree_disease_' + oncotree_code
                    onco_list.append(onco_id)
                    df.at[index, 'oncoTreeDiseases'] = onco_list

            # Assign go_disease id (using go diseases as data for ontological diseases table)
            go_id = 'go_disease_' + row[ 'id']
            go_list = []
            go_list.append(go_id)
            df.at[index, 'goDiseases']  = go_list

        # Clean dataframes
        df1 = df[df.graph_id != 'go_0000000-0000-0000-0000-000000000000']
        df2 = df1[['name', 'definition',  'graph_id', 'jaxDiseases', 'doDiseases', 'goDiseases', 'oncoTreeDiseases']].copy(deep=True)
        df2 = df2.rename(columns={'definition': 'description'})
        dfs_to_combine.append(df2)

    # Combine dataframes
    combined_df = pandas.concat(dfs_to_combine, ignore_index=True)

    # Temporary csv file
    path = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/combined_df.csv'
    combined_df.to_csv(path, encoding='utf-8', index=False)

    return combined_df

if __name__ == "__main__":
    main(load_directory)