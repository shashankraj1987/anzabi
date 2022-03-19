from ast import Raise
import os
import shutil
import re
import pandas as pd
from datetime import datetime
import log_config as lc
import numpy as np 

pd.set_option('display.float_format', lambda x: '%.3f' % x)

skip_rows = {'Client Billing Descending': 0, "Fee Breakdown by Dept and Fee Earner": 3,
             "Fee Summary by Dept and Fee Earner": 3, "Fees Billed": 3, "Matter Source of Business inc Matter Bills": 0,
             "Matters Opened by FE": 3, "Payment Received Analysis": 3, "Total Hours by Fee Earner-With Billings": 0}


def remove_cols(df):
    """ 
        This will remove all the columns that contain the word Textbox in them. 
        This Function takes a DataFrame as in input and returns all the columns except TextBox. 
    """

    cols = df.columns
    new_cols = []
    txt_chk = re.compile(r'Textbox')
    tot_hrs_col_name = ["RecordedHours2","NonChargeHours2","WOHours2","TotalHour2","bankRef"]
    new_cols = [col_name for col_name in cols if not(txt_chk.search(col_name)) and col_name not in tot_hrs_col_name]
    return new_cols

def get_rows(dct, match):
    """
        Takes a Dictionary and a filename as inputs and Returns how many rows need to be skipped for a filename. 
        Returns the Number of rows to skip, while creating a DataFrame.
    """
    for val in dct.keys():
        if re.match(val, match):
            return dct[val]


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
    file_date = pd.to_datetime(dt, format='%d%m%Y')
    return file_date

def categorize_files(file_loc):
    import sys
    
    if os.name == 'posix':
            os.system('clear')
    else:
        os.system('cls')

    log_loc = file_loc + "/" + "Logs"
    cat_file_logger = lc.start_log(log_loc)
    unprocessed = file_loc + "/Unprocessed"
    processed = file_loc + "/Processed"

    ess_fold_list = [log_loc,cat_file_logger,unprocessed,processed]

    for fold in ess_fold_list:
        if os.path.exists(fold):
            cat_file_logger.info(f'\n[{fold}] already Exists in [{file_loc}]\n')
        else:
            os.mkdir(file_loc+"/"+fold)
            cat_file_logger.info(f'\nCreating Folder {fold} in {file_loc}\n')

    total_csv = len([f for f in os.listdir(file_loc) if f.endswith('.csv')])
    if total_csv > 0:
        print(f"Found {total_csv} csv files")
    elif total_csv == 0:
        print("No CSV Files Found. Exiting.")
        sys.exit()

    process_files = {}
    discard_files = {}
    file_list = os.listdir(file_loc)

    cat_file_logger.info(f"Any xls file and files having Pie in the names will not be Processed")

    # Segregating the Files.
    discard_files['all_pie'] = [files for files in file_list if len(re.compile(r'[\sa-zA-Z\s]+Pie \w+_\d+.csv').findall(files))]
    discard_files['all_xlsx'] = [files for files in file_list if files.endswith(".xlsx")]

    # Move the above files to Unprocessed Folder before moving ahead
    for f in discard_files.keys():
                cat_file_logger.info(f'Moving out files of list [{f}] to folder [{unprocessed}]')
                # [shutil.move(file_loc + "/" + file, unprocessed)  for file in discard_files[f]]
                [cat_file_logger.info(f"Moving File {file} to  unprocessed")  for file in discard_files[f]]

    process_files['client_billing'] = [files for files in file_list if len(re.compile(r'Client [a-zA-Z\s]+_\d+.csv').findall(files))]
    cnt = len(process_files['client_billing'])
    cat_file_logger.info(f'Found [{cnt}] files of Client BIlling ')
    
    process_files['fee_brkdn_dept_fe'] = [files for files in file_list if len(re.compile(r'Fee Breakdown [a-zA-Z\s]+_\d+.csv').findall(files))]
    cnt = len(process_files['fee_brkdn_dept_fe'])
    cat_file_logger.info(f'Found [{cnt}] files of Fee Breakdown by Dept ')
    
    process_files['fee_summ_dept_fe'] = [files for files in file_list if len(re.compile(r'Fee Summary [a-zA-Z\s]+_\d+.csv').findall(files))]
    cnt = len(process_files['fee_summ_dept_fe'])
    cat_file_logger.info(f'Found [{cnt}] files of Fee Summary by Dept ')
        
    process_files['fees_billed'] = [files for files in file_list if len(re.compile(r'Fees B[a-zA-Z\s]+_\d+.csv').findall(files))]
    cnt = len(process_files['fees_billed'])
    cat_file_logger.info(f'Found [{cnt}] files of Fees BIlled ')
        
    process_files['matter_src'] = [files for files in file_list if len(re.compile(r'Matter Source [a-zA-Z\s()]+_\d+.csv').findall(files))]
    cnt = len(process_files['matter_src'])
    cat_file_logger.info(f'Found [{cnt}] files of Matter Source Reference ')
     
    process_files['matter_opened'] = [files for files in file_list if len(re.compile(r'Matters Open[\sa-zA-Z\s()]+_\d+.csv').findall(files))]
    cnt = len(process_files['matter_opened'])
    cat_file_logger.info(f'Found [{cnt}] files of Matter Opened by FE ')
    
    process_files['payment_rcv'] = [files for files in file_list if len(re.compile(r'Payment [\sa-zA-Z\s()]+_\d+.csv').findall(files))]
    cnt = len(process_files['payment_rcv'])
    cat_file_logger.info(f'Found [{cnt}] files of Payment Received ')
       
    process_files['tot_hrs_fe'] = [files for files in file_list if len(re.compile(r'([tT]otal[\sa-z-A-Z\s]*_\d+.csv)').findall(files))]
    cnt = len(process_files['tot_hrs_fe'])
    cat_file_logger.info(f'Found [{cnt}] files of Total Hours by Fee Earner ')

    for f in process_files.keys():
        cat_file_logger.info(f'Moving category [{f}]')
        try:
            [shutil.move(file_loc + "/" + file, processed)  for file in process_files[f]]
            [cat_file_logger.info(f' Moving files {file} to processed')  for file in process_files[f]]
        except:
            cat_file_logger.info("Error Moving File {}".format(f))
        else:
            #[cat_file_logger.info(f'Moving File --> {file}')  for file in process_files[f]]
            pass
        
    return process_files


def concat_files(dict_list, file_loc, logfile_loc):
    concat_logger = lc.start_log(logfile_loc)
    df_all_files = {}
    dict_fname= ""
     # If a Filename is not in this Dictionary, then it will not be Considered. 
    # date = (datetime.now()).strftime("%m-%d-%y")
    concat_logger.info(f'Following Keys will be processed - [{dict_list.keys()}]')
    for file_cat in dict_list.keys():
        df_final = pd.DataFrame()
        concat_logger.info('*' * 50)
        concat_logger.info(f'Processing Category {file_cat}')
        for file in dict_list[file_cat]:
            print("*"*50)
            print("Went Inside For Loop ")
            dict_fname = file.split("_")[0]
            print("Dict_fname assigned")
            dfc_file = pd.read_csv((file_loc + "/Processed/" + file), skiprows=get_rows(skip_rows, file))
            dfc_file = dfc_file[remove_cols(dfc_file)]
            processing_date = get_date_from_Filename(file)
            dfc_file["Date_Added"] = processing_date
            df_final = pd.concat([df_final, dfc_file], ignore_index=True)
            df_final.fillna(0)
            df_final = df_final.replace(re.compile(r'£'), "").replace(re.compile(r','), "").replace(re.compile(r'\('),"").replace(re.compile(r'\)'), "")

            for cols in df_final.columns:
                try:
                    df_final[cols].astype(float)
                except:
                    continue
                    # concat_logger.info(f'Skipping Column {cols}')
                else:
                    # concat_logger.info(f'Converting {cols} to float')
                    df_final[cols] = df_final[cols].astype(float)

        df_all_files[dict_fname] = df_final

    for f in df_all_files.keys():
        rows = df_all_files[f].shape[0]
        concat_logger.info(f'Will Add -> {rows} entries for [{f}] to the database')

    return df_all_files
