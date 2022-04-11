import os
import log_config

class chk_srvr:
    def __init__(self):
        self.base_loc = ""
        self.db_name = ""
        self.db_user = ""
        self.db_pwd = ""
        self.db_host = ""
        self.db_port = ""

    def chk_creds(self):
        try:
            self.db_name = os.environ['db']
            self.db_user = os.environ['db_user']
            self.db_pwd = os.environ['db_password']
            self.db_host = os.environ['db_host']
            self.db_port = os.environ['db_port']
        except:
            return -1
        else:
            return self.db_name,self.db_user,self.db_pwd,self.db_host,self.db_port
        
    def chk_base_dirs(self):
        try:
            self.base_loc = os.environ['cloud_base_loc']
        except:
            return -1
        else:
            if os.path.exists(os.environ['cloud_base_loc']):
                self.log_file = self.base_loc+"/"+"Logs"
                self.final_files = self.base_loc+"/"+"Final_Df"
                self.backup = self.base_loc+"/"+"Backup"
                self.trigger_file = self.base_loc+"/file_trigger/"+"new_data_received.txt"

                return self.base_loc,self.log_file, self.final_files,self.backup,self.trigger_file
            else:
                FileNotFoundError
                #print("Path Doesn't Exist")