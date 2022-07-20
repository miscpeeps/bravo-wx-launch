# This file reads all files in the raw-data directory and transforms them for use in our ML models

# import required packages
import os
import datetime
import csv
import logging

# start logging
timestamp = str(datetime.datetime.now())

# remove characters from timestamp
timestamp_char_remove = {":", "-", " ", "."}
for char in timestamp_char_remove:
    timestamp = timestamp.replace(char, "")

log_filename = "log-" + timestamp + ".log"

logging.basicConfig(filename=log_filename, encoding="utf-8", level=logging.DEBUG)

# name a timestamped directory to hold all data results
def make_results_directory(timestamp: str) -> str:
    """Makes a new timestamped directory.
    Returns string of new directory name."""

    new_dir = "data-transform-" + timestamp

    # make directory
    if not os.path.exists(new_dir):
        try:
            os.mkdir(new_dir)
            logging.debug("Created data results directory %s", new_dir)
        except:
            logging.error("Can't create destination directory %s", new_dir)
            raise OSError("Can't create destination directory (%s)!" % (new_dir)) 
    
    return new_dir

# create list of all folders in raw-data directory
def raw_data_folder_list(raw_data_directory: str) -> list:
    """Scans all subfolders in raw-data directory.
    Returns list of strings of subfolders in raw-data directory"""

    folders_list = [f.name for f in os.scandir(raw_data_directory) if f.is_dir()]
    logging.debug("Found the following raw-data directories:")
    logging.debug(folders_list)

    return folders_list

# create list of all files in each folder in raw-data
def 
    
# main program
results_directory = make_results_directory(timestamp)
raw_data_folders = raw_data_folder_list("./raw-data")


