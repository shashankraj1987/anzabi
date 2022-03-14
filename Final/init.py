from  log_config import start_log
from datetime import datetime

def pre_checks():

    # Importing sytem Libraries
    import sys

    # Import User Libraries 
    from init_checks import pre_main
    import check_server

    #
    srv_params = check_server.get_hostname()  
    host_name = srv_params[0]
    log_time = srv_params[1]

    srv_details = check_server.chk_details(host_name,log_time)
    logfile = srv_details[2]
    srvr_type = srv_details[3]
    dataloc = srv_details[1]
    
    # 
    try:
        init_logger = start_log(logfile)
        init_check_status = pre_main()
    except FileNotFoundError:
        print(f' The Log Location could not be found.\n Is OneDrive Mounted?')
        print(f'The Curerent Server is [{srvr_type}]')
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
    #TODO: Script to load the final files to the Database. 
    print("Here the final file will be loaded to the Database. ")
    print("Probably by using linux shell script")

def main():
    import organize_files as of

    file_loc = r"C:\\Users\\shash\\OneDrive - Anza Services LLP\\DATA_Dump"
    log_file = r"C:\\Users\\shash\\OneDrive - Anza Services LLP\\Logs"
    
    # now = datetime.now()
    # log_time = now.strftime("%m_%d_%y_%H_%M_%S")
    # log_file = file_loc+"/"+log_time+".log"

    # Step 1 
    #pre_checks()

    # Step 2
    dict_list, total_processed = of.categorize_files(file_loc)
    of.concat_files(dict_list,file_loc, log_file)

    print(f'\nLog Files are located at {log_file}')

if __name__ == '__main__':
    main()