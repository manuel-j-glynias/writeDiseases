"""
This script extracts data from omnidisease  table  and converts
data  to SQL tables:
1. extract_file - extracts data from csv into  a dataframe
2. parse_omnidisease - parses and cleans dataframe
3. write_load_files - writes dataframe to csv file
4. get_schema - gets the schema for SQL table
5. write_sql - writes sql table

6/4/2020
by IK
"""
import datetime
import pandas
import get_schema
import write_sql
import write_load_files

import create_id
#load_directory = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/'
#loader_id = '007'
editable_statement_list = ['name']
table_name = 'omni_diseases'
import create_editable_statement
#id_class = create_id.ID('', '', 0, 0, 0, 0, 0, 0, 0)

def main(load_directory, loader_id, id_class):
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    path = '../data/tblOS_GLOBAL_GLOBAL_Ref_OmniDiseases.csv'

    #Create dataframe
    df=extract_file(path)

    omni_disease_df = parse_omnidisease(df, load_directory, loader_id, id_class)
    path = load_directory + 'omni_diseases.csv'
    write_load_files.main(omni_disease_df, path)

    db_dict = get_schema.get_schema('omni_diseases')
    write_sql.write_sql(db_dict, 'omni_diseases')


# Creates omnidisease  dataframe
# Input: dataframe
# Output: new  dataframe with required fields
def parse_omnidisease(df, load_directory, loader_id, id_class):
    # Parse dataframe
    for index, row in df.iterrows():
        code = row['OmniDiseaseID']
        graph_id = 'omnidisease_' + code.split('_')[1]
        df.at[index, 'graph_id'] = graph_id
        df.at[index, 'omniDiseaseId'] = code
        df.at[index, 'name'] = row['OmniDiseaseName']
        df.at[index, 'omniDiseaseType'] = (row['OmniDiseaseType']).replace(" ","")
    df = df[['omniDiseaseId', 'name', 'omniDiseaseType', 'graph_id']]
    omni_with_editable = create_editable_statement.assign_editable_statement(df,
                                                                              editable_statement_list, loader_id,
                                                                              load_directory, table_name, id_class)
    return omni_with_editable


###################################################
#  EXTRACT FILE
###################################################
# Extracts data from the file to a dataframe
# Removes inactive entries
# Input: file path
# Output: dataframe:
def extract_file(path):
    df = pandas.read_csv(path)
    return df


#if __name__ == "__main__":
    #main(load_directory, loader_id, id_class)