from ast import Raise
from mimetypes import init
from os import uname
from platform import node
from tabnanny import check


def main():
    import os
    from datetime import datetime
    import logging
    import re 

    import log_config
    import check_server

    now = datetime.now()
    log_time = now.strftime("%m_%d_%y_%H_%M_%S")

    u_name = str(os.uname())
    h_name = re.compile(r'\'.*?\'')  ## This is Anything Between Parentheses
    matches = h_name.findall(u_name)
    host = matches[1].split("\'")[1]

    srv_info = check_server.chk_details(host,log_time)
    log_loc = srv_info[0]
    data_loc = srv_info[1]
    log_file = srv_info[2]
    srv_type = srv_info[3]

    init_logger = log_config.start_log(log_file)

    if os.path.exists(data_loc):
        init_logger.info(f'Found Data Location.')
        init_logger.info(f'On [{srv_type}]')
        init_logger.info(f'[Log Location is]: {log_loc}')
        init_logger.info(f'[Data Location is]: {data_loc} ')
    else: 
        init_logger.error(f'\n[OneDrive] is Not Mounted on {srv_type}.\n Exiting.')
        ## System Exit. 

    # Defining the Logging Parameters 

if __name__ == '__main__':
    main()
        