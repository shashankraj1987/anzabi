from asyncio.log import logger
import os
import shutil 
import re 
import pandas as pd
import numpy as np
from datetime import datetime
import log_config as lc

pd.set_option('display.float_format', lambda x: '%.3f' % x)

skip_rows = {}
skip_rows['Client Billing Descending'] = 0
skip_rows["Fee Breakdown by Dept and Fee Earner"] = 3
skip_rows["Fee Summary by Dept and Fee Earner"] = 3
skip_rows["Fees Billed"] = 3
skip_rows["Matter Source of Business inc Matter Bills"] = 0
skip_rows["Matters Opened by FE"] = 3
skip_rows["Payment Received Analysis"] = 3
skip_rows["Total Hours by Fee Earner-With Billings"] = 0

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
    file_date = pd.to_datetime(dt,format='%d%m%Y')
    
    return file_date

def categorize_files(file_loc):
    cat_file_logger = lc.start_log(file_loc)
    cat_scr_logger = lc.start_log_to_stream()
    unprocessed = file_loc+"\\Unprocessed"
    
    if os.name == 'posix':
        os.system('clear')
    else:
        os.system('cls')
    
    process_files = {}
    discard_files = {}
    i=0
    j=0
    total = 0   
    file_list = os.listdir(file_loc)

    if os.path.exists(unprocessed):
        cat_file_logger.info(f'\n[Unprocessed] already Exists in [{file_loc}]\n')
        cat_scr_logger.debug(f'\n[Unprocessed] already Exists in [{file_loc}]\n')
    else: 
        os.mkdir(unprocessed)
        cat_file_logger.info(f'\nCreating Folder {unprocessed} in {file_loc}\n')
        cat_scr_logger.debug(f'\nCreating Folder {unprocessed} in {file_loc}\n')

    ## Seggregating the Files.
    discard_files['all_pie'] = [files for files in file_list if len(re.compile(r'[\sa-zA-Z\s]+Pie \w+_\d+.csv').findall(files))]
    discard_files['all_xlsx'] = [files for files in file_list if files.endswith(".xlsx")]    

    # Move the above files to Unprocessed Folder before moving ahead
    for i in discard_files.keys():
        for j in discard_files[i]:
            shutil.move(file_loc+"\\"+j,unprocessed)
            cat_file_logger.info(f'Moving File -- {j} to [{unprocessed}]')
            cat_scr_logger.debug(f'\nCreating Folder {unprocessed} in {file_loc}\n')
            total+=1

    cat_file_logger.info(f'\nMoved total {total} files to Unprocessed Folder')
    cat_scr_logger.info(f'\nMoved total {total} files to Unprocessed Folder')

    process_files['client_billing'] = [files for files in file_list if len(re.compile(r'Client [a-zA-Z\s]+_\d+.csv').findall(files))]
    process_files['fee_brkdn_dept_fe'] = [files for files in file_list if len(re.compile(r'Fee Breakdown [a-zA-Z\s]+_\d+.csv').findall(files))]
    process_files['fee_summ_dept_fe'] = [files for files in file_list if len(re.compile(r'Fee Summary [a-zA-Z\s]+_\d+.csv').findall(files))]
    process_files['fees_billed'] = [files for files in file_list if len(re.compile(r'Fees B[a-zA-Z\s]+_\d+.csv').findall(files))]
    process_files['matter_src'] = [files for files in file_list if len(re.compile(r'Matter Source [a-zA-Z\s()]+_\d+.csv').findall(files))]
    process_files['matter_opened'] = [files for files in file_list if len(re.compile(r'Matters Open[\sa-zA-Z\s()]+_\d+.csv').findall(files))]
    process_files['payment_rcv'] = [files for files in file_list if len(re.compile(r'Payment [\sa-zA-Z\s()]+_\d+.csv').findall(files))]
    process_files['tot_hrs_fe'] = [files for files in file_list if len(re.compile(r'([tT]otal[\sa-z-A-Z\s]*_\d+.csv)').findall(files))]
    
    return process_files, total
    #:TODO: 
    # Add these files individually to a database.
    # Then concatenate these files and add them to staging database. 
    # No Need to move files in separate folders. Create a Dataframe in memory and perform operations. 

def concat_files(dict_list, file_loc, logfile_loc):
    concat_logger= lc.start_log(logfile_loc)
    df_all_files = {}
    tot_processed = 0
    # If a Filename is not in this Dictionary, then it will not be Considered. 
    date = (datetime.now()).strftime("%m-%d-%y")
    for file_cat in dict_list.keys():
        df_final = pd.DataFrame()
        concat_logger.info('*'*50)
        concat_logger.info(f'Processing Category {file_cat}')
        for file in dict_list[file_cat]:
            dict_fname = file.split("_")[0]
            dfc_file = pd.read_csv((file_loc+"\\"+file), skiprows=get_rows(skip_rows,file))
            dfc_file = dfc_file[remove_cols(dfc_file)]
            processing_date = get_date_from_Filename(file)
            dfc_file["Date_Added"] = processing_date
            df_final = pd.concat([df_final,dfc_file],ignore_index=True)
            
            df_final.fillna(0)
            df_final = df_final.replace(re.compile(r'£'),"").replace(re.compile(r','),"").replace(re.compile(r'\('),"").replace(re.compile(r'\)'),"")

            for cols in df_final.columns:
                try:
                    df_final[cols].astype(float)
                except:
                    continue
                    #concat_logger.info(f'Skipping Column {cols}')
                else:
                    #concat_logger.info(f'Converting {cols} to float')
                    df_final[cols] = df_final[cols].astype(float)
        df_all_files[dict_fname] = df_final
    for f in df_all_files.keys():
        rows = df_all_files[f].shape[0]
        concat_logger.info(f'Will Add -> {rows} entries for [{f}] to the database')
        
    return df_all_files