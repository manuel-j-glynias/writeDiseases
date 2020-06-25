import datetime
import csv
import os
import get_schema
import pandas
# This script creates editable statements and writes it to the editable statement file
#  6/23/2020 by IK

# Creates editable statement id and writes each statement into a csv file
def assign_editable_statement(df, editable_statement_list,loader_id, load_dir , table_name, id_class):

    # Gets the info on what columns should be created
    db_dict = get_schema.get_schema('EditableStatement')

    # Creates writer object
    load_files_dict = create_load_files_dict(db_dict, load_dir)
    editable_statement_writer = load_files_dict['EditableStatement']['writer']

    column_list = list(df)
    data = df.T.to_dict().values()
    # Go over data
    for input in data:
        for entry in column_list:
            #  Check if  the column value contains editable statement
            if entry in editable_statement_list:
                graph_id = input['graph_id']
                field = entry
                # Create field in editable statement table
                field =  table_name + field.capitalize()  +  '_' + graph_id
                statement = input[entry]
                # Assign unique value to editable statement and write it
                es_des_id =write_editable_statement(field, editable_statement_writer, loader_id, statement,id_class)
                # Put this value in the original table
                input[entry]= es_des_id
    # Create new dataframe with editable statement ids
    df2 = pandas.DataFrame(data)
    return df2

# Helper function that writes editable statement csv
def write_editable_statement(field, editable_statement_writer, loader_id, statement,id_class):
    now = datetime.datetime.now()
    time = now.strftime("%Y%m%d%H%M%S")
    # Uses id class to assigne the unique id
    es_des_id = id_class.assign_id()
    editable_statement_writer.writerow([str(field), str(statement), now.strftime("%Y-%m-%d-%H-%M-%S"), loader_id, es_des_id])
    return es_des_id

# Creates a writer object for editable statement
def create_load_files_dict(db_dict, load_dir):
    load_files_dict = {}
    for database in db_dict:
        entry = db_dict[database]
        for table_name in entry:
            out_file = open(load_dir + table_name + '.csv', 'a+', encoding='utf-8')
            writer = csv.writer(out_file, lineterminator='\n')
            if is_empty(out_file):
                header = db_dict[database][table_name]['col_order']
                writer.writerow(header)
            load_files_dict[table_name] = {'writer':writer, 'file':out_file}
    return load_files_dict

# Checks if file already exists so no duplicate header is created
def is_empty(out_file):
    out_file.seek(0, os.SEEK_END)
    return out_file.tell()==0