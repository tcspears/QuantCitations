# Script to update the flat cache directory structure to the nested structure.

import glob
import os

cache_location = "/Users/taylor/QuantCitesDataCache_real/"

# Fix repec files

files_repec = glob.glob(cache_location + "repec/*.html")

for file in files_repec:
    splitted = file.split("repec/")
    dir = splitted[1].split(":")[1]
    dir_path = cache_location + "repec/" + dir
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    new_file_path = dir_path + "/" + splitted[1]
    os.rename(file, new_file_path)

files_citec = glob.glob(cache_location + "citec/*.xml")

for file in files_citec:
    splitted = file.split("citec/")
    dir = splitted[1].split(":")[1]
    dir_path = cache_location + "citec/" + dir
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    new_file_path = dir_path + "/" + splitted[1]
    os.rename(file, new_file_path)

