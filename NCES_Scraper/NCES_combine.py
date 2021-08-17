import os
import glob
import pandas as pd

#combine csv files
os.chdir("/schoolWeb_csv")
extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
combined_csv.to_csv( "combined_csv.csv", index=False, encoding='utf-8-sig')

#combine xls files
os.chdir("/schoolWeb_xls")
cwd = os.path.abspath('') 
files = os.listdir(cwd) 

f1 = pd.DataFrame()
for file in files:
    if file.endswith('.xls'):
        f1 = f1.append(pd.read_html(file, skiprows=5, header=0))
f1.to_csv("combined_xls.csv", index=False, encoding='utf-8-sig')

#read the csv file into a dataframe
df = pd.DataFrame()
df = df.append(pd.read_csv('combined_csv.csv'))

#merge the csv file with the xls file
mrg = pd.merge(df, f1, how='left', left_on='ID', right_on='NCES School ID')
mrg.to_csv( "all_NCES.csv", index=False, encoding='utf-8-sig')