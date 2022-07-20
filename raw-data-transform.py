# This file reads all files in the raw-data directory and transforms them for use in our ML models

# import required packages
import os
import datetime
import csv
import logging
import pandas as pd

# start logging
timestamp = str(datetime.datetime.now())

# remove characters from timestamp
timestamp_char_remove = {":", "-", " ", "."}
for char in timestamp_char_remove:
    timestamp = timestamp.replace(char, "")

log_filename = "data-transform-" + timestamp + ".log"

logging.basicConfig(filename=log_filename, encoding="utf-8", level=logging.DEBUG)

# name a timestamped directory to hold all data results
def make_results_directory(timestamp: str) -> str:
    """Makes a new timestamped directory.
    Returns string of new directory name."""

    new_dir = "./data-transform-" + timestamp +"/"

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
def raw_data_directory_list(raw_data_directory: str) -> list:
    """Scans all subdirectories in raw-data directory.
    Returns list of strings of subdirectories in raw-data directory"""

    dirs_list = [f.name for f in os.scandir(raw_data_directory) if f.is_dir()]
    logging.debug("Found the following raw-data directories:")
    logging.debug(dirs_list)

    return dirs_list

# create dictionary of all files in each folder in raw-data
def raw_data_files_dict(raw_data_folders: list) -> dict:
    """Scans each directory in raw-data and lists the files in each.
    Returns a dictionary object with:
    key = string -> directory name
    value = list -> files in directory"""
    
    raw_data_dict = {}
    for index, value in enumerate(raw_data_folders):
        # make path string for file scan location
        path = "./raw-data/" + raw_data_folders[index] + "/"
        logging.debug("Scanning folder %s", path)
        files_list = [f.name for f in os.scandir(path) if f.is_file()]
        if len(files_list) < 7:
            logging.warning("Found %s files:", str(len(files_list)))
        else:
            logging.debug("Found %s files:", str(len(files_list)))
        logging.debug(files_list)
        raw_data_dict[path] = files_list
    
    logging.debug("Returned raw data file dictionary")
    logging.debug(raw_data_dict)

    return raw_data_dict

def test_transform(raw_data_files: dict, results_directory: str) -> None:
    """Test transform that loads and works with csvs to generate a new csv"""

    # number of unique data points processed
    total_data_points = 0

    # dict to hold transformed dataframes
    df_dict = {}

    for key in raw_data_files:
        # determine if data is from launch or scrub
        if "launch" in key:
            data_type = "launch"
            logging.debug("Directory %s detected as launch data directory", key)
        elif "scrub" in key:
            data_type = "scrub"
            logging.debug("Directory %s detected as scrub data directory", key)
        else:
            logging.error("ERROR: Could not determine launch or scrub data type from folder name %s. Exiting.", key)
            raise Exception("ERROR: Could not determine launch or scrub data type from folder name. Exiting.")

        files = raw_data_files[key]
        for index, value in enumerate(files):
            file_name = key + files[index]
            logging.debug("Opening raw data file %s", file_name)
            ext = os.path.splitext(file_name)[-1].lower()
            if ext == ".csv":
                if "amps" in file_name:
                    logging.debug("Applying transform to amps-low file")
                    amps_df = pd.read_csv(file_name)
                    total_data_points += amps_df.shape[0] * amps_df.shape[1]
                    # call amps low transform
                elif "field-mill" in file_name:
                    logging.debug("Applying transform to field mill (lplws) file")
                    fm_df = pd.read_csv(file_name)
                    total_data_points += fm_df.shape[0] * fm_df.shape[1]
                    # call lplws field mill transform
                elif "merlin" in file_name:
                    logging.debug("Applying transform to merlin c-g file")
                    merlin_df = pd.read_csv(file_name)
                    total_data_points += merlin_df.shape[0] * merlin_df.shape[1]
                    # call merlin c-g transform
                elif "rainfall" in file_name:
                    logging.debug("Applying transform to rainfall file")
                    rain_df = pd.read_csv(file_name)
                    total_data_points += rain_df.shape[0] * rain_df.shape[1]
                    # call rainfall transform
                elif "tower" in file_name:
                    logging.debug("Applying transform to weather tower file")
                    wt_df = pd.read_csv(file_name)
                    total_data_points += wt_df.shape[0] * wt_df.shape[1]
                    # call weather tower transform
                elif "50mhz" in file_name:
                    logging.debug("Applying transform to 50MHz wind file")
                    mhz50_df = pd.read_csv(file_name)
                    total_data_points += mhz50_df.shape[0] * mhz50_df.shape[1]
                    # call 50Mhz wind transform
                elif "915mhz" in file_name:
                    logging.debug("Applying transform to 915MHz wind file")
                    mhz915_df = pd.read_csv(file_name)
                    total_data_points += mhz915_df.shape[0] * mhz915_df.shape[1]
                    # call 915Mhz wind transform
                else:
                    logging.warning("%s is not a valid csv file. Ignoring", file_name)
            else:
                logging.warning("%s is not a valid data file. Ignoring", file_name)
    logging.debug("Loaded %s total data points", "{:,}".format(total_data_points))

 
# main program
results_directory = make_results_directory(timestamp)

raw_data_folders = raw_data_directory_list("./raw-data")

raw_data_files = raw_data_files_dict(raw_data_folders)

test_transform(raw_data_files, results_directory)