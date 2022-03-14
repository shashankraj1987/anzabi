import os
import shutil 
import re 
import pandas as pd
import numpy as np
from datetime import datetime

#file_loc = "/home/shashankraj/Documents/DATA/"
file_loc = r'C:\Users\shash\OneDrive - Anza Services LLP\DATA_Dump'
unprocessed = file_loc+"\\Unprocessed"

skip_rows = {}
skip_rows['Client Billing Descending'] = 0
skip_rows["Fee Breakdown by Dept and Fee Earner"] = 3
skip_rows["Fee Summary by Dept and Fee Earner"] = 3
skip_rows["Fees Billed"] = 3
skip_rows["Matter Source of Business inc Matter Bills"] = 0
skip_rows["Matters Opened by FE"] = 3
skip_rows["Payment Received Analysis"] = 3
skip_rows["Total Hours by Fee Earner-With Billings"] = 0

def count_csv_files(fold_path):
    if os.path.exists(fold_path):
        count_csv = 0
        count_xlsx = 0
        for root,dirs,files in os.walk(fold_path):
            count_csv+= len([f for f in files if f.endswith('.csv')])
            count_xlsx+= len([f for f in files if f.endswith('.xlsx')])
        print("{} has {} CSV files ".format(fold_path,count_csv))
        print("{} has {} XLSX files ".format(fold_path,count_xlsx))

    for name in os.listdir(fold_path):
        path = os.path.join(fold_path,name)
        #print(path)
        if os.path.isdir(path):
            count_csv_files(path)

def categorize_files(loc):
    if os.name == 'posix':
        os.system('clear')
    else:
        os.system('cls')
    
    all_files = {}
    i=0
    j=0
    total = 0   
    if os.path.exists(unprocessed):
        print(f'\n[Unprocessed] already Exists in [{file_loc}]\n')
    else: 
        os.mkdir(unprocessed)
        print(f'\nCreating Folder {unprocessed} in {file_loc}\n')

    ## Seggregating the Files.
    all_files['all_pie'] = [files for files in os.listdir(loc) if len(re.compile(r'[\sa-zA-Z\s]+Pie \w+_\d+.csv').findall(files))]
    all_files['all_xlsx'] = [files for files in os.listdir(loc) if files.endswith(".xlsx")]    
    move_files = all_files['all_pie'],all_files['all_xlsx']

    # Move the above files to Unprocessed Folder before moving ahead
    for i in range(len(move_files)):
        for j in range(len(move_files[i])):
            shutil.move(file_loc+"\\"+move_files[i][j],unprocessed)
            print(f'Moving File -- {move_files[i][j]} to [{unprocessed}]')
            total+=1

    print(f'\nMoved total {total} files to Unprocessed Folder')

    all_files['all_client'] = [files for files in os.listdir(loc) if len(re.compile(r'([cC]lient[\sa-z-A-Z\s]*_\d+.csv)').findall(files))]
    all_files['all_fees'] = [files for files in os.listdir(loc) if len(re.compile(r'([fF]ee[\sa-z-A-Z\s]*_\d+.csv)').findall(files))]
    all_files['all_matter'] = [files for files in os.listdir(loc) if len(re.compile(r'([mM]atter[\sa-z-A-Z\s]*\(Bill Date\)_\d+.csv)').findall(files))]
    all_files['all_payments'] = [files for files in os.listdir(loc) if len(re.compile(r'([pP]ayment[\sa-z-A-Z\s]*_\d+.csv)').findall(files))]
    all_files['all_total'] = [files for files in os.listdir(loc) if len(re.compile(r'([tT]otal[\sa-z-A-Z\s]*_\d+.csv)').findall(files))]
     
    
    return all_files
    #:TODO: 
    # Add these files individually to a database.
    # Then concatenate these files and add them to staging database. 
    # No Need to move files in separate folders. Create a Dataframe in memory and perform operations. 

def remove_cols(df):
    """ 
    This will remove all the columns that contain the word Textbox in them. 
    This Function takes a DataFrame as in input and returns all the columns except TextBox. 

    """
    cols = df.columns
    new_cols = []

    for x in cols:
        txt_chk = re.compile(r'Textbox')
        if txt_chk.search(x)== None:
            new_cols.append(x)
        else: 
            continue
    
    return(new_cols)

def get_rows(dictnry, match):
    """
        Takes a Dictionary and a filename as inputs and Returns how many rows need to be skipped for a filename. 
        Returns the Number of rows to skip, while creating a DataFrame.
    """
    for val in dictnry.keys():
        if re.match(val,match):
            return (dictnry[val])

def get_date_from_Filename(fname):
    """
        Accepts a Filename that has fname_date.csv format. 
        It Extracts the From Date form the File and Returns the same. 
        These Are Datetime Objects.  

        If the Filename has only start date, it will just return the same date for Both Start and End Date. 

    """
    
    pattern = re.compile(r'_\d*')
    match = pattern.findall(fname)
    dt = match[0]
    dt = dt.split("_")[1]
    start_date = pd.to_datetime(dt,format='%d%m%Y')
    
    return start_date

def get_unique_file_names(files):
    """ 
        This Function will Check a Given Location for all files and find the Unique File Names. 
        It Splits on the "_" as that is the current Naming Convention.
        Also, it only find .csv files
        Returns the Unique File Names.
    """
    
    all_csvs = []
    all_file_names = []

    for file in files:
        if file.endswith('.csv'):
            all_csvs.append(file)

    for file in all_csvs:
        fname = file.split("_")[0]
        all_file_names.append(fname)

    all_file_names =  np.unique(all_file_names)
    return all_file_names

def concat_files(file_loc,dict_list):
    df_final = pd.DataFrame()
    # If a Filename is not in this Dictionary, then it will not be Considered. 
    date = (datetime.now()).strftime("%m-%d-%y")
    for file_cat in dict_list.keys():
        print(f'Processing File {dict_list[file_cat]}')

        for file in dict_list[file_cat]:
            #dfc_file = pd.read_csv((file_loc+file), skiprows=get_rows(skip_rows,file))
            print(file)
