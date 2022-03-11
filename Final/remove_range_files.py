import os
import shutil 
import re 

file_loc = "/home/shashankraj/Documents/DATA/"
pattern = re.compile(r'.*?_\d*to\d*.csv')
base_location = "/home/shashankraj/Documents/DATA/"

def collect_range_files():
    os.system('clear')

    csv_files = [files for files in os.listdir(base_location) if files.endswith(".csv")]
    monthly_files = [files for files in os.listdir(base_location) if len(pattern.findall(files))]