import csv
import pandas

def extract_file(path):
    unparsed_df = pandas.read_csv(path)
    table_database = {}
    col_order = []
    previous = ''
    for index, row in unparsed_df.iterrows():
        field_list = [row['DataType'], row['Key'], row['NullValues'],
                      row['AutoIncrement'], row ['ReferenceFields']]
        if  row['Table'] in table_database:
            field_dict = table_database[row['Table'] ]
            field_dict[ row['Field']] = field_list
        else:
            if previous != '':
                previous_dict = table_database[previous]
                previous_dict['col_order'] = col_order
            col_order = []
            field_dict = {}
            field_dict[ row['Field']] = field_list
            table_database[row['Table'] ] = field_dict
        col_order.append( row['Field'])
        previous = row['Table']

    return table_database
