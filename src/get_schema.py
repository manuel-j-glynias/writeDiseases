import csv

###################################################
#  GET SCHEMA TO CREATE SQL TABLES
###################################################
# Extracts sql table schema from a provided config file
# Input: path to config file
# Ouput: dictionary with
def get_schema(resource):
    db_dict = {}  # {db_name:{table_name:{col:[type,key,allow_null,
    # ref_col_list],'col_order':[cols in order]}}}
    init_file = open('../config/table_descriptions.csv', 'r')
    reader = csv.reader(init_file, quotechar='\"')
    for line in reader:
        # print(line)
        db_name = line[0]
        if (db_name == 'Database'):
            continue
        if (db_name not in db_dict.keys()):
            db_dict[db_name] = {}
        table_name = line[1]

        if (resource not in table_name):
            continue
        col = line[2]
        col_type = line[3]
        col_key = line[4]
        allow_null = line[5]
        auto_incr = line[6]
        ref_col_list = line[7].split('|')  # we will ignore this for now during development
        try:
            ref_col_list.remove('')
        except:
            pass
        try:
            db_dict[db_name][table_name][col] = [col_type, col_key, allow_null, auto_incr, ref_col_list]
            db_dict[db_name][table_name]['col_order'].append(col)
        except:
            db_dict[db_name][table_name] = {col: [col_type, col_key, allow_null, auto_incr, ref_col_list]}
            db_dict[db_name][table_name] = {col: [col_type, col_key, allow_null, auto_incr, ref_col_list],
                                            'col_order': [col]}
    init_file.close()
    return db_dict
