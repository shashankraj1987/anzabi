from datetime import datetime
import pandas as pd
import sys
import os
from sqlalchemy import create_engine
from datetime import datetime
import logging

from notebooks.check_server import chk_srvr
from log_config import start_log
import notebooks.organize_files as of

logging.basicConfig(level=logging.DEBUG)

SCHEMA_REF = {
    "Client Billing Descending":"client_billing","Fee Breakdown by Dept and Fee Earner":"fee_brkdn_dept_fe","Fee Summary by Dept and Fee Earner":"fee_smry_dept_fe",
    "Fees Billed":"fees_billed","Matter Source of Business inc Matter Bills (Bill Date)":"mttr_src_ref","Total Hours by Fee Earner-With Billings All":"tot_hrs_by_fe",
    "Matters Opened by FE":"mtrs_by_fe","Payment Received Analysis":"pmt_rcv_analysis"}

def main(info_file):
    if os.path.exists(info_file):
        logging.info(f"Found file at [{info_file}]")
        loc = info_file
        opened_file = open(loc, encoding='utf8')
        from csv import reader
        read_file = reader(opened_file)
        db_creds = list(read_file)
        opened_file.close()

        info_dict = {}

        for info in db_creds:
            info_dict[info[0]] = info[1]
        
        file_loc = info_dict["base_loc"]
        log_file = file_loc+"\\Logs"
        final_files = file_loc+"\\Final_Df"
        backup = file_loc+"\\Backup"
        trigger_file = file_loc+"\\file_trigger\\new_data_received.txt"
        init_logger = start_log(log_file)

        db = info_dict["db_name"]
        db_user = info_dict["db_user"]
        db_password = info_dict["db_password"]
        db_host = info_dict["db_host"]
        db_port = info_dict["db_port"]

    elif chk_srvr.chk_base_dirs() & chk_srvr.chk_creds():

        logging.info("Checking Environment Variables for information")
        srv_dirs = chk_srvr.chk_base_dirs()
        db_creds = chk_srvr.chk_creds()

        if srv_dirs:
            file_loc = srv_dirs[0]
            log_file = srv_dirs[1]
            final_files = srv_dirs[2]
            backup = srv_dirs[3]
            trigger_file = srv_dirs[4]
            init_logger = start_log(log_file)
            logging.info("Variables Set Successfully for File Locations")
        else:
            logging.error("Environment Variables for Files is not set")
            logging.info("Checking Environment Variables for DB Credentials ")

        if db_creds:
            db = db_creds[0]
            db_user = db_creds[1]
            db_password = db_creds[2]
            db_host = db_creds[3]
            db_port = db_creds[4]
            logging.info("Variables Set successfully for DB Creds ")
        else:
            logging.error("Environment Variables not set for DB Credentials.")

    else:
        logging.error("Important Variables not set.")
        logging.error("Exiting ...")
        sys.exit()
   
    try:
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db}',echo=True)
    except:
        init_logger.error("Unable to connect to the Database")
    else:
        init_logger.info("Database Connection Established")

    dict_list = of.categorize_files(file_loc)
    all_files = of.concat_files(dict_list, file_loc, log_file)

    for file in all_files.keys():
        init_logger.debug(f"Converting all Columns to Lowercase for [{file}]")
        all_files[file].columns = [cols.lower() for cols in all_files[file].columns]

        fname = final_files+"/"+file+".csv"
        if os.path.exists(fname):
            init_logger.info(f'{fname} already exists')
            all_files[file].to_csv(final_files+"/"+file+".csv",header=False,mode="a",index=False)
        else:
            init_logger.info(f'Creating DF {fname}')
            all_files[file].to_csv(final_files+"/"+file+".csv",mode="a",index=False)

        init_logger.debug(f"Appending all data in Postgresql Server for [{file}]")

        try:            
            all_files[file].to_sql(SCHEMA_REF[file],con=engine,if_exists='append',index=None)   # Send the data to database
        except:
            init_logger.error(f"***** Unable to write to Database for file [{file}]********* ")
        else:
            continue
        init_logger.info(f'Processed File [{file}]')
    
    for csv_file in os.listdir(final_files):
        init_logger.info(f'Processing {csv_file}')
        if csv_file.endswith('.csv'):
            tmp_df = pd.read_csv(final_files+"/"+csv_file)
            tmp_df['date_added'] = pd.to_datetime(tmp_df['date_added'])
            fname = backup+"/"+csv_file.split(".")[0]+".parquet.gzip"
            init_logger.info(f'Name is {fname}')
            if os.path.exists(fname):
                init_logger.info(f'File already Exists, Removing')
                os.remove(fname)
            tmp_df.to_parquet(fname,compression = 'gzip')
            init_logger.info("*"*50)

    # ## Remove the Trigger file
    # if os.path.exists(trigger_file):
    #      os.remove(trigger_file)
    #      print("Trigger File Removed")

    print(f'\nLog Files are located at {log_file}')

if __name__ == '__main__':
    main()
