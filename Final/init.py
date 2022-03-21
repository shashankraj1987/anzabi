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
    import sys
    import os
    from sqlalchemy import create_engine
    from datetime import datetime

      
    if os.name == 'posix':
            file_loc = "/home/srv_admin/One_Drive/DATA_Dump"
    else:
        file_loc = r"D:\One Drive Anza\OneDrive - Anza Services LLP\DATA_Dump"
    
    log_file = file_loc+"/"+"Logs"
    final_files = file_loc+"/"+"Final_Df"
    backup = file_loc+"/"+"Backup"
    #today = str(datetime.today().date())
    trigger_file = file_loc+"/file_trigger/"+"new_data_received.txt"

    db = 'AnzaBI'
    db_user = 'db_Admin'
    db_password = 'password'
    db_host = '219.91.145.98'
    db_port = '5432'
    try:
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db}',echo=True)
    except:
        print("Unable to connect to the Database")
    else:
        print("Database Connection Established")

    schema_ref = {
    "Client Billing Descending":"client_billing","Fee Breakdown by Dept and Fee Earner":"fee_brkdn_dept_fe","Fee Summary by Dept and Fee Earner":"fee_smry_dept_fe",
    "Fees Billed":"fees_billed","Matter Source of Business inc Matter Bills (Bill Date)":"mttr_src_ref","Total Hours by Fee Earner-With Billings All":"tot_hrs_by_fe",
    "Matters Opened by FE":"mtrs_by_fe","Payment Received Analysis":"pmt_rcv_analysis"}

    dict_list = of.categorize_files(file_loc)
    all_files = of.concat_files(dict_list, file_loc, log_file)

    for file in all_files.keys():
        print(f"Converting all Columns to Lowercase for {file}")
        all_files[file].columns = [cols.lower() for cols in all_files[file].columns]
        print(f"Writing the Dataframe to CSV for  [{file}]")                                                                                         # Convert the Columns to lower case for easy updation in Database
        all_files[file].to_csv(final_files+"/"+file+".csv",index=False,mode="a")                                                         # Create Final Dataframe in csv
        print(f"Creating Backup in Paraquet Format for  [{file}]")
        all_files[file].to_parquet(backup+"/"+file+".parquet.gzip",compression = 'gzip')   # Create a Backup Paraquet FIle as well. 
        try: 
            print(f"Putting all data in Postgresql Server for  [{file}]")
            all_files[file].to_sql(schema_ref[file],con=engine,if_exists='append',schema='Bowling_Data',index=None)   # Send the data to database
        except:
            print(f"***** Unable to write to Database for file [{file}]********* ")
        else:
            continue
        print(f'Processed File [{file}]')

    ## Remove the Trigger file
    if os.path.exists(trigger_file):
         os.remove(trigger_file)
         print("Trigger File Removed")

    print(f'\nLog Files are located at {log_file}')

if __name__ == '__main__':
    main()
