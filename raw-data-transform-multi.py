# This file reads all files in the raw-data directory and transforms them for use in our ML models
# Uses multiprocessing to speed data transformations

# import required packages
import os
import datetime
import time
import logging
from numpy import size
import pandas as pd
from tqdm import tqdm
from itertools import islice
from multiprocessing import Process, Pool

# import other python files for data transform
import merlin_transform
import amps_low_transform
import field_mill_transform
import raingauge_transform
import weather_tower_transform
import wind_profiler_50_transform
import wind_profiler_915_transform

# supress pandas warnings
import warnings
warnings.filterwarnings("ignore")

# start logging
timestamp = str(datetime.datetime.now())

# make logs directory
if not os.path.exists("./logs/"):
    try:
        os.makedirs("./logs/")
        logging.debug("Created logging directory")
    except:
        logging.error("Can't create logging directory")
        raise OSError("Can't create logging directory") 
    else:
        logging.debug("Logging directory already exists")       

# remove characters from timestamp
timestamp_char_remove = {":", "-", " ", "."}
for char in timestamp_char_remove:
    timestamp = timestamp.replace(char, "")

log_filename = "./logs/run-" + timestamp + ".log"

logging.basicConfig(filename=log_filename, encoding="utf-8", level=logging.DEBUG)

# name a timestamped directory to hold all data results
def make_results_directory(timestamp: str) -> str:
    """Makes a new timestamped directory.
    Returns string of new directory name."""

    new_dir = "./transformed-data/run-" + timestamp +"/"

    print("Attempting to create results directory:" + new_dir)

    # make directory
    if not os.path.exists(new_dir):
        try:
            os.makedirs(new_dir)
            logging.debug("Created data results directory %s", new_dir)
        except:
            logging.error("Can't create destination directory %s", new_dir)
            raise OSError("Can't create destination directory (%s)!" % (new_dir)) 
    
    print("Successfully created results directory")

    return new_dir

# create list of all folders in raw-data directory
def raw_data_directory_list(raw_data_directory: str) -> list:
    """Scans all subdirectories in raw-data directory.
    Returns list of strings of subdirectories in raw-data directory"""

    print("Attempting to scan raw-data directory")

    dirs_list = [f.name for f in os.scandir(raw_data_directory) if f.is_dir()]
    logging.debug("Found %s raw-data directories:", str(len(dirs_list)))
    logging.debug(dirs_list)

    print("Successfully scanned raw-data directory, found " + str(len(dirs_list)) + " directories")

    return dirs_list

# create dictionary of all files in each folder in raw-data
def raw_data_files_dict(raw_data_folders: list, data_directory: str) -> dict:
    """Scans each directory in raw-data and lists the files in each.
    Returns a dictionary object with:
    key = string -> directory name
    value = list -> files in directory"""
    
    # metrics for logs
    number_files_scanned = 0
    
    print("Attempting to scan for files in raw-data directory")

    raw_data_dict = {}
    for index, value in enumerate(raw_data_folders):
        # make path string for file scan location
        path = data_directory + raw_data_folders[index] + "/"
        logging.debug("Scanning folder %s", path)
        files_list = [f.name for f in os.scandir(path) if f.is_file()]
        number_files_scanned += len(files_list)
        if len(files_list) < 7:
            logging.warning("Found %s files (7 files expected):", str(len(files_list)))
        else:
            logging.debug("Found %s files:", str(len(files_list)))
        logging.debug(files_list)
        raw_data_dict[path] = files_list
    
    logging.debug("Returned raw data file dictionary, found %s files", str(number_files_scanned))
    logging.debug(raw_data_dict)

    print("Successfully scanned raw-data files, found " + str(number_files_scanned) + " files")

    return raw_data_dict, number_files_scanned

def make_events_dict(launch_list_file_path: str, scrub_list_file_path: str) -> dict:
    """Extracts event (launches and scrubs) times and dates from a
    given input file. Returns a dictionary with a key of dates and
    value pair of datetime objects corresponding to event times"""

    print("Attempting to create list of launch/scrub dates and times")

    events_list = {}

    # load and transform launch events
    try:
        df = pd.read_csv(launch_list_file_path)
        logging.debug("Loaded launch list file %s", launch_list_file_path)
    except:
        logging.error("Can't find launch file to load %s", launch_list_file_path)
        raise OSError("Can't find launch file to load (%s)!" % (launch_list_file_path))
    logging.debug("Launch events table:")
    logging.debug(df)
    
    # isolate launch data
    df.drop(df.columns.difference(["time (z)","launch date"]), axis=1, inplace=True)
    logging.debug("Edited launch table:")
    logging.debug(df)

    # translate launch dataframe info to dictionary key (date):value (time)
    for index, row in df.iterrows():
        events_list[row["launch date"]] = row["time (z)"]

    # load and transform scrub events
    try:
        df = pd.read_csv(scrub_list_file_path)
        logging.debug("Loaded scrub list file %s", scrub_list_file_path)
    except:
        logging.error("Can't find scrub file to load %s", scrub_list_file_path)
        raise OSError("Can't find scrub file to load (%s)!" % (scrub_list_file_path))
    logging.debug("Scrub events table:")
    logging.debug(df)
    
    # isolate scrub data
    df.drop(df.columns.difference(["Time of Scrub (Z)","Date of Scrub"]), axis=1, inplace=True)
    logging.debug("Edited scrub table:")
    logging.debug(df)

    # translate launch dataframe info to dictionary key (date):value (time)
    for index, row in df.iterrows():
        events_list[row["Date of Scrub"]] = row["Time of Scrub (Z)"]

    logging.debug("%s event times by date string:", str(len(events_list)))
    logging.debug(events_list)    

    # transform times into associated datetime objects
    for key in events_list:
        # split launch csv MM/DD/YYYY date strings into parts
        date_parts = key.split("/")
        time_parts = events_list[key].split(":")
        # replace value with datetime object
        events_list[key] = datetime.datetime(int(date_parts[2]), int(date_parts[0]), int(date_parts[1]),
                                             int(time_parts[0]), int(time_parts[1]))
    
    logging.debug("%s event datetime objects by date string:", str(len(events_list)))
    logging.debug(events_list)    

    print("Successfully created launch/scrub list for " + str(len(events_list)) + " events")
    
    return events_list

def transform_data(raw_data_files: dict, results_directory: str, 
                   event_times: dict, number_raw_data_files: int,
                   worker_number: int) -> None:
    """Transforms raw data to format suitable for ML modeling. Writes consolidated
    data files to disk as new csv files
    
    Now running in parallel"""

    # gather neat info
    total_data_points = 0
    number_csvs_written = 0
    number_merge_errors = 0
    number_raw_data_files = 0
    for key in raw_data_files:
        number_raw_data_files += len(raw_data_files[key])
    worker_file_count = 0

    print("Worker " + str(worker_number) + " beginning data transforms on " + str(number_raw_data_files) + " files in " + str(len(raw_data_files)) + " directories")
    # uncomment below and in imports for neat status bar

    transform_start_time = time.time()
    logging.debug("Started data transforms at %s", str(transform_start_time))
    
    # uncomment below for single directory tests
    """
    raw_data_files = {}
    raw_data_files["./Scraped_Files/20220309-launch/"] = ['AmpsLowResolution.csv', 'FieldMill.csv', 'MerlinCloudToGround.csv', 
                                                          'Rainfall.csv', 'WeatherTower.csv', 'WindProfiler50.csv', 'WindProfiler915.csv']
    """

    for key in raw_data_files:
        
        # initialize dataframe joiner for each new directory
        df_dict = {}
        
        # determine if data is from launch or scrub for later tagging
        if "launch" in key:
            data_type = "launch"
            logging.debug("Directory %s detected as launch data directory", key)
        elif "scrub" in key:
            data_type = "scrub"
            logging.debug("Directory %s detected as scrub data directory", key)
        else:
            logging.error("ERROR: Could not determine launch or scrub data type from folder name %s. Exiting.", key)
            raise Exception("ERROR: Could not determine launch or scrub data type from folder name. Exiting.")

        # list all files in selected directory
        files = raw_data_files[key]
        
        # construct date_key for event_times datetime objects
        date = key.split("/")
        date = date[-2].split("-")
        isodate = date[0]
        date = date[0]
        # build MM/DD/YYYY string
        date = date[4:6] + "/" + date[6:] + "/" + date[:4]
        logging.debug("Initial date string: %s", date)
        if "0" in date[3:4]:
            # fix leading 0 in day
            date = date[0:3] + date[4:]
            logging.debug("Fixed day: %s", date)
        if "0" in date[0:1]:
            # fix leading 0 in month
            date = date[1:]
            logging.debug("Fixed month: %s", date)
        date_key = date
        logging.debug("Parsed date key as: %s", date_key)

        # perform data transforms on files in directory
        for index, value in enumerate(files):
            file_name = key + files[index]
            logging.debug("Opening raw data file %s", file_name)
            # Get file extension for checking and path for passing correct datetime object to transformers
            path, ext = os.path.splitext(file_name)
            # switch case based on what kind of data file
            if ext == ".csv":
                if "amps" in file_name.lower():
                    logging.debug("Applying transform to amps-low file")
                    df_count = pd.read_csv(file_name)
                    total_data_points += df_count.shape[0] * df_count.shape[1]
                    # call amps low transform
                    df_dict["amps_df"] = amps_low_transform.lowamps(file_name, event_times[date_key])
                elif "field" in file_name.lower():
                    logging.debug("Applying transform to field mill (lplws) file")
                    df_count = pd.read_csv(file_name)
                    total_data_points += df_count.shape[0] * df_count.shape[1]
                    # call lplws field mill transform
                    df_dict["fm_df"] = field_mill_transform.field_mill(file_name, event_times[date_key])
                elif "merlin" in file_name.lower():
                    logging.debug("Applying transform to merlin c-g file")
                    df_count = pd.read_csv(file_name)
                    total_data_points += df_count.shape[0] * df_count.shape[1]
                    # call merlin c-g transform
                    df_dict["mcg_df"] = merlin_transform.cg(file_name, event_times[date_key])
                elif "rain" in file_name.lower():
                    logging.debug("Applying transform to rainfall file")
                    df_count = pd.read_csv(file_name)
                    total_data_points += df_count.shape[0] * df_count.shape[1]
                    # call rainfall transform
                    df_dict["rain_df"] = raingauge_transform.rainfall(file_name, event_times[date_key])
                elif "tower" in file_name.lower():
                    logging.debug("Applying transform to weather tower file")
                    df_count = pd.read_csv(file_name)
                    total_data_points += df_count.shape[0] * df_count.shape[1]
                    # call weather tower transform
                    df_dict["wt_df"] = weather_tower_transform.weather_towers(file_name, event_times[date_key])
                elif "er50" in file_name.lower():
                    logging.debug("Applying transform to 50MHz wind file")
                    df_count = pd.read_csv(file_name)
                    total_data_points += df_count.shape[0] * df_count.shape[1]
                    # call 50Mhz wind transform
                    df_dict["50_df"] = wind_profiler_50_transform.wind_profiler_50(file_name, event_times[date_key])
                elif "er915" in file_name.lower():
                    logging.debug("Applying transform to 915MHz wind file")
                    df_count = pd.read_csv(file_name)
                    total_data_points += df_count.shape[0] * df_count.shape[1]
                    # call 915Mhz wind transform
                    df_dict["915_df"] = wind_profiler_915_transform.wind_profiler_915(file_name, event_times[date_key])
                else:
                    logging.warning("%s is not a valid csv file. Ignoring", file_name)
            else:
                logging.warning("%s is not a valid data file. Ignoring", file_name)
            worker_file_count += 1
        print("Worker " + str(worker_number) + " processed " + str(worker_file_count) + " of " + str(number_raw_data_files) + " files")

        # Check if dataframe joiner has all 7 expected dataframes and merge
        if len(df_dict) == 7:
            logging.debug("Have the expected 7 dataframes. Beginning merge for date %s", isodate)
            logging.debug("Info on amps low:")
            logging.debug(df_dict["amps_df"])
            logging.debug("Info on field mill:")
            logging.debug(df_dict["fm_df"])     
            logging.debug("Info on merlin c-g:")
            logging.debug(df_dict["mcg_df"])
            logging.debug("Info on rainfall:")
            logging.debug(df_dict["rain_df"])
            logging.debug("Info on weather tower:")
            logging.debug(df_dict["wt_df"])
            logging.debug("Info on 50 MHz:")
            logging.debug(df_dict["50_df"])
            logging.debug("Info on 915 MHz:")
            logging.debug(df_dict["915_df"])

            # make an ordered list of dataframes to always join in the same sequence
            dataframes = [df_dict["amps_df"], df_dict["fm_df"], df_dict["mcg_df"], df_dict["rain_df"],
                          df_dict["wt_df"], df_dict["50_df"], df_dict["915_df"]]
            merged_data = dataframes[0].join(dataframes[1:])
            logging.debug("Successfully merged dataframe %s", isodate)

            # add launch or scrub identifier column (1 = scrub, 0 = launch)
            if data_type == "scrub":
                id_col = [1] * 49
            else:
                id_col = [0] * 49
            logging.debug("Successfully merged scrub_id for %s", data_type)
            logging.debug(merged_data)

            # merge scrub identifier column to dataframe
            merged_data["scrub_id"] = id_col            

            # write to new csv in results folder
            merged_filename = results_directory + isodate + "-" + data_type + ".csv"
            merged_data.to_csv(merged_filename, na_rep="NaN")
            number_csvs_written += 1
            logging.debug("Wrote merged data file to %s", merged_filename)
        else:
            number_merge_errors += 1
            logging.warning("Insufficient dataframes for merge for date %s. %s merge errors so far this run",
                            isodate, str(number_merge_errors))
    
    transform_stop_time = time.time()
    logging.debug("Completed data transforms at %s", str(transform_start_time))

    # print and log metrics
    transform_seconds = transform_stop_time - transform_start_time
    transform_time = time.gmtime(transform_seconds)
    transform_time_string = time.strftime("%H:%M:%S",transform_time)
    logging.debug("Data transforms took %s", transform_time_string)
    print("Data transforms completed in " + transform_time_string)
    logging.debug("Loaded %s total data points", "{:,}".format(total_data_points))
    print("Successfully loaded " + "{:,}".format(total_data_points) + " total data points")
    logging.debug("Wrote %s transformed data files, expected %s", "{:,}".format(number_csvs_written), "{:,}".format(len(raw_data_files)))
    print("Successfully transformed " + "{:,}".format(number_csvs_written) + " files, expected " + "{:,}".format(len(raw_data_files)))
    logging.debug("Had %s dataframe merge errors", "{:,}".format(number_merge_errors))
    print("Had " + "{:,}".format(number_merge_errors) + " dataframe merge errors")
    return total_data_points, worker_number, transform_seconds, worker_file_count, len(raw_data_files), number_csvs_written, number_merge_errors

def split_raw_data_dict(raw_data_files: dict) -> list:
    split = []
    s1 = dict(list(raw_data_files.items())[len(raw_data_files)//2:])
    s2 = dict(list(raw_data_files.items())[:len(raw_data_files)//2])
    s1_1 = dict(list(s1.items())[len(s1)//2:])
    s1_2 = dict(list(s1.items())[:len(s1)//2])
    s2_1 = dict(list(s2.items())[len(s2)//2:])
    s2_2 = dict(list(s2.items())[:len(s2)//2])
    split.append(dict(list(s1_1.items())[len(s1_1)//2:]))
    split.append(dict(list(s1_1.items())[:len(s1_1)//2]))
    split.append(dict(list(s1_2.items())[len(s1_2)//2:]))
    split.append(dict(list(s1_2.items())[:len(s1_2)//2]))
    split.append(dict(list(s2_1.items())[len(s2_1)//2:]))
    split.append(dict(list(s2_1.items())[:len(s2_1)//2]))
    split.append(dict(list(s2_2.items())[len(s2_2)//2:]))
    split.append(dict(list(s2_2.items())[:len(s2_2)//2]))
    return split

def metrics(total_data_points: list) -> None:
    """Performs metrics across multiple data transform process workers.

    total_data_points data structure is a list of tuples in the following order (per worker):
    total_data_points, 
    worker_number, 
    transform_seconds, 
    worker_file_count, 
    number_csvs_expected, 
    number_csvs_written, 
    number_merge_errors
    """

    # sums
    sum_total_data_points = 0
    sum_transform_seconds = 0
    sum_worker_file_count = 0
    sum_number_csvs_expected = 0
    sum_number_csvs_written = 0
    sum_number_merge_errors = 0

    for index, value in enumerate(total_data_points):
        sum_total_data_points += total_data_points[index][0]
        sum_transform_seconds += total_data_points[index][2]
        sum_worker_file_count += total_data_points[index][3]
        sum_number_csvs_expected += total_data_points[index][4]
        sum_number_csvs_written += total_data_points[index][5]
        sum_number_merge_errors += total_data_points[index][6]
        print(f"Worker {total_data_points[index][1]} processed {total_data_points[index][3]} files and "
              f"{total_data_points[index][0]} data points into {total_data_points[index][5]} csv files "
              f"(expected {total_data_points[index][4]} csv files) with {total_data_points[index][6]} "
              f"merge processing errors in {total_data_points[index][2]} seconds")
    
    average_transform_seconds = sum_transform_seconds / len(total_data_points)
    average_transform_time = time.gmtime(average_transform_seconds)
    transform_time_string = time.strftime("%H:%M:%S",average_transform_time)

    sum_worker_file_count = "{:,}".format(sum_worker_file_count)
    sum_total_data_points = "{:,}".format(sum_total_data_points)
    sum_number_csvs_written = "{:,}".format(sum_number_csvs_written)
    sum_number_csvs_expected = "{:,}".format(sum_number_csvs_expected)
    sum_number_merge_errors = "{:,}".format(sum_number_merge_errors)
    
    print(f"8 workers processed {sum_worker_file_count} files and {sum_total_data_points} data points "
          f"into {sum_number_csvs_written} csv files (expected {sum_number_csvs_expected} csv files) "
          f"with {sum_number_merge_errors} merge processing errors in an average of {transform_time_string}")
        
# main program
if __name__ == '__main__':
    # directory for raw data files
    data_directory = "./Scraped_Files/"
    # directory for transformed data
    results_directory = make_results_directory(timestamp)
    # all folders with raw data
    raw_data_folders = raw_data_directory_list(data_directory)
    # all raw data files and the number of raw data files
    raw_data_files, number_raw_data_files = raw_data_files_dict(raw_data_folders, data_directory)
    # all launches and scrubs from given csv
    event_times = make_events_dict(launch_list_file_path="launches.csv", scrub_list_file_path="scrubs.csv")
    # split raw_data_files into multiple dictionaries for multiprocessing
    split_raw_data = split_raw_data_dict(raw_data_files)
    
    """ debug only
    print("split data:")
    for index, item in enumerate(split_raw_data):
        print(index+1)
        print(split_raw_data[index])
        print("/n")
    """

    with Pool(processes=8) as pool:
        total_data_points = pool.starmap(transform_data,
                                        [(split_raw_data[0], results_directory, event_times, number_raw_data_files, 1),
                                         (split_raw_data[1], results_directory, event_times, number_raw_data_files, 2),
                                         (split_raw_data[2], results_directory, event_times, number_raw_data_files, 3),
                                         (split_raw_data[3], results_directory, event_times, number_raw_data_files, 4),
                                         (split_raw_data[4], results_directory, event_times, number_raw_data_files, 5),
                                         (split_raw_data[5], results_directory, event_times, number_raw_data_files, 6),
                                         (split_raw_data[6], results_directory, event_times, number_raw_data_files, 7),
                                         (split_raw_data[7], results_directory, event_times, number_raw_data_files, 8)])
    metrics(total_data_points)

