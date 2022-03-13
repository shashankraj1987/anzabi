import os
import shutil 
import re 

file_loc = "/home/shashankraj/Documents/DATA/"
pattern = re.compile(r'.*?_\d*to\d*.csv')
base_location = "/home/shashankraj/Documents/DATA/"
src = base_location

def count_csv_files(fold_path):
    if os.path.exists(fold_path):
        count_csv = 0
        count_xlsx = 0
        for root,dirs,files in os.walk(fold_path):
            count_csv+= len([f for f in files if f.endswith('.csv')])
            count_xlsx+= len([f for f in files if f.endswith('.xlsx')])
        print("{} has {} CSV files ".format(fold_path,count_csv))
        print("{} has {} XLSX files ".format(fold_path,count_xlsx))

    for name in os.listdir(fold_path):
        path = os.path.join(fold_path,name)
        #print(path)
        if os.path.isdir(path):
            count_csv_files(path)

def move_files():
    os.system('clear')


    ## All folder list 
    all_fees = []
    all_matters = []
    all_payments = []
    all_client = []
    all_range = []

    ## All Folder Location 
    dest_fees = file_loc+"/fees"
    dest_matters = file_loc+"/matters"
    dest_payments = file_loc+"/payments"
    dest_clients = file_loc+"/clients"
    dest_fe = file_loc+"/fe"
    dest_excel = file_loc+"/excel_files"
    dest_range = file_loc+"/Range_Files"

    ## Seggregating the Files.
    #all_csvs = [files for files in os.listdir(base_location) if files.endswith(".csv")]
    range_files = [files for files in os.listdir(base_location) if len(re.compile(r'.*?_\d*to\d*.csv').findall(files))]
    all_client = [files for files in os.listdir(base_location) if len(re.compile(r'^Client').findall(files))]
    all_xlsx = [files for files in os.listdir(base_location) if files.endswith(".xlsx")] 
    all_client 
    for file in all_csvs:
        if len(re.compile(r'^Client').findall(file)) > 0:
            client.append(file)
        elif len(re.compile(r'^Fee').findall(file)) > 0:
            fees.append(file)
        elif len(re.compile(r'^Payment').findall(file)) > 0:
            payments.append(file)
        elif len(re.compile(r'^Matter').findall(file)) > 0:
            matters.append(file)
    
    ## Check if the Folders Exist otherwise Create it
    fold_list = []
    fold_list = [dest_clients,dest_fe,dest_fees,dest_matters,dest_payments,dest_excel]

    for fold in fold_list:
        if os.path.exists(fold):
            print("{} already Exists in {}".format(fold,file_loc))
            continue
        else: 
            os.mkdir(fold)
            print("Creating Folder {} in {}".format(fold,file_loc))
    
    files = os.listdir(file_loc)

    for file in all_csvs:
        src = file_loc+"/"+file
        if file in client:
            shutil.move(src,dest_clients)
            print("Moved {} to {} ".format(file,dest_clients))
        elif file in fees:
            shutil.move(src,dest_fees)
            print("Moved {} to {} ".format(file,dest_fees))
        elif file in payments:
            shutil.move(src,dest_payments)
            print("Moved {} to {} ".format(file,dest_payments))
        elif file in matters: 
            shutil.move(src,dest_matters)
            print("Moved {} to {} ".format(file,dest_matters))
        elif file in range_files:
            shutil.move(src,)

        else:
            shutil.move(src,dest_fe)
            print("Moved {} to {} ".format(file,dest_fe))
    
    for file in all_xlsx:
        src = file_loc+"/"+file
        shutil.move(src,dest_excel)
        print("Moved {} to {} ".format(src,dest_excel))
