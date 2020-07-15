###################################################
#  WRITE CSV
###################################################
# Converts dataframe to csv file
# Input: dataframe and file path
# Output: csv file is written
import csv
def main (df, path):
    try:
        df.to_csv(path, encoding='utf-8', index=False, quoting=csv.QUOTE_ALL)
    except IOError:
        print("I/O error csv file")