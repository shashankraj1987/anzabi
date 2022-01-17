from tabnanny import check


def main():

    # Importing sytem Libraries
    import sys

    # Import User Libraries 
    from init_checks import pre_main
    from  log_config import start_log
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


if __name__ == '__main__':
    main()
        