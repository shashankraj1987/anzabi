def chk_details(hostname,time):
        if hostname == "fedora.anzaservices.local":
            base_loc = "/home/srv_admin"
            log_loc = base_loc+"/python_logs"
            data_loc=base_loc+"/cloud_drive"
            log_file = log_loc+"/"+time+".log"
            srv_type = "Cloud"
        elif hostname == "fedora":
            base_loc = "/home/shashankraj/Documents/One_Drv_Anza"
            log_loc = base_loc+"/Logs"
            data_loc=base_loc+"/DB_Dump"
            log_file = log_loc+"/"+time+".log"
            srv_type = "Local"

        return log_loc,data_loc,log_file,srv_type

def get_hostname():
    
    from datetime import datetime
    import os
    import re

    now = datetime.now()
    log_time = now.strftime("%m_%d_%y_%H_%M_%S")

    u_name = str(os.uname())
    h_name = re.compile(r'\'.*?\'')  ## This is Anything Between Parentheses
    matches = h_name.findall(u_name)
    host = matches[1].split("\'")[1]

    return host, log_time