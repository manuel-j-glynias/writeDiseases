###################################################
#  WRITE CSV
###################################################
# Converts dataframe to csv file
# Input: dataframe and file path
# Output: csv file is written
def main (df, path):
    try:
        df.to_csv(path, encoding='utf-8', index=False)
    except IOError:
        print("I/O error csv file")