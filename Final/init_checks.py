def pre_main():
    
    # Import Python Libraries
    # **********************************************************
    import os
    from datetime import datetime

    # Import User Libraries
    # **********************************************************
    import check_server

    # Check whether this script is running from the Cloud Server or Local Machine
    # **********************************************************
    srv_info = check_server.chk_details(check_server.get_hostname()[0],check_server.get_hostname()[1])
    # srv_info = check_server.chk_details(host,log_time)
    data_loc = srv_info[1]
    
    # Check if the One Drive is mounted? 
    # **********************************************************
    if os.path.exists(data_loc):
        return 1
    else: 
        return 0