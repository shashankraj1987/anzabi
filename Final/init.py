from log_config import start_log
from datetime import datetime

def pre_checks():
    # Importing system Libraries
    import sys

    # Import User Libraries 
    from init_checks import pre_main
    import check_server

    #
    srv_params = check_server.get_hostname()
    host_name = srv_params[0]
    log_time = srv_params[1]

    srv_details = check_server.chk_details(host_name, log_time)
    logfile = srv_details[2]
    srvr_type = srv_details[3]
    dataloc = srv_details[1]

    # 
    try:
        init_logger = start_log(logfile)
        init_check_status = pre_main()
    except FileNotFoundError:
        print(f' The Log Location could not be found.\n Is OneDrive Mounted?')
        print(f'The Current Server is [{srvr_type}]')
        sys.exit()
    else:
        init_logger.info("All OK")

    # 
    if init_check_status:
        init_logger.info(f'Found Data Location.')
        init_logger.info(f'On [{srvr_type}]')
        init_logger.info(f'[Log Location is]: {logfile}')
        init_logger.info(f'[Data Location is]: {dataloc} ')
        init_logger.info(f' Init Checks Done. Moving Forward')
    else:
        init_logger.critical(f' Init Check Failed. Exiting.')
        sys.exit()


def load_to_db():
    # TODO: Script to load the final files to the Database.
    print("Here the final file will be loaded to the Database. ")
    print("Probably by using linux shell script")


def main():
    import organize_files as of
    import pandas as pd
    import os
    from sqlalchemy import create_engine
    from datetime import datetime

    #file_loc = r"C:\Users\shash\Offline_Docs\Anza\DATA_Dump"
    file_loc = "/home/srv_admin/One_Drive/DATA_Dump"
    log_file = file_loc+"/"+"Logs"
    final_files = file_loc+"/"+"Final_Df"
    backup = file_loc+"/"+"Backup"
    today = datetime.today().date()
    trigger_file = file_loc+"/file_trigger/"+"new_data_received.txt"

    db = 'AnzaBI'
    db_user = 'db_admin'
    db_password = 'password'
    db_host = '219.91.145.98'
    db_port = '5432'
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db}',echo=True,future=True)

    schema_ref = {
    "Client Billing Descending":"client_billing","Fee Breakdown by Dept and Fee Earner":"fee_brkdn_dept_fe","Fee Summary by Dept and Fee Earner":"fee_smry_dept_fe",
    "Fees Billed":"fees_billed","Matter Source of Business inc Matter Bills (Bill Date)":"mttr_src_ref","Total Hours by Fee Earner-With Billings All":"tot_hrs_by_fe",
    "Matters Opened by FE":"mtrs_by_fe","Payment Received Analysis":"pmt_rcv_analysis"}

    dict_list = of.categorize_files(file_loc)
    all_files = of.concat_files(dict_list, file_loc, log_file)

    print(f'\nLog Files are located at {log_file}')

    for file in all_files.keys():
        all_files[file].columns = [cols.lower() for cols in all_files[file].columns]                                                            # Convert the Columns to lower case for easy updation in Database
        all_files[file].to_csv(final_files+"/"+file+"_"+today+".csv",index=False)                                                         # Create Final Dataframe in csv
        all_files[file].to_parquet(backup+"/"+file+"_"+today+".parquet.gzip",compression = 'gzip',index=False)   # Create a Backup Paraquet FIle as well. 
        all_files[file].to_sql(schema_ref[file],con=engine,if_exists='append',schema='Raw_Data',index=None)         # Send the data to database
        print(f'Processed File [{file}]')
        pd.DataFrame.to_parquet()

    ## Remove the Trigger file
    if os.path.exists(trigger_file):
         os.remove(trigger_file)
         print("Trigger File Removed")

if __name__ == '__main__':
    main()
