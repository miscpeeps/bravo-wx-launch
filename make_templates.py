import os, shutil

def raw_data_files_dict(raw_data_folders: list, data_directory: str) -> dict:
    """Scans each directory in raw-data and lists the files in each.
    Returns a dictionary object with:
    key = string -> directory name
    value = list -> files in directory"""

    filenames=['AmpsLowResolution.csv',
               'FieldMill.csv',
               'MerlinCloudToGround.csv',
               'Rainfall.csv',
               'WeatherTower.csv',
               'WindProfiler50.csv',
               'WindProfiler915.csv']


    # metrics for logs
    number_files_scanned = 0

    print("Attempting to scan for files in raw-data directory")

    raw_data_dict = {}
    for index, value in enumerate(raw_data_folders):
        # make path string for file scan location
#        files_list=[]

        path = data_directory + raw_data_folders[index] + "/"
        files_list = [f.name for f in os.scandir(path) if f.is_file()]
        number_files_scanned += len(files_list)
        for f in files_list:
            if "Amps" in f:
                f=filenames[0]
            elif "Field" in f:
                f = filenames[1]
            elif "Merlin" in f:
                f = filenames[2]
            elif "Rainfall" in f:
                f = filenames[3]
            elif "Tower" in f:
                f = filenames[4]
            elif "Profiler50" in f:
                f = filenames[5]
            elif "Profiler915" in f:
                f = filenames[0]
        if len(files_list) < 7:
            print(f'Only {len(files_list)} Found in {path}')
            missing_files = list(set(filenames) - set(files_list))
            print(missing_files)
            copy_files(path,missing_files)
        raw_data_dict[path] = files_list

    print("Successfully scanned raw-data files, found " + str(number_files_scanned) + " files")

    return raw_data_dict, number_files_scanned

def raw_data_directory_list(raw_data_directory: str) -> list:
    """Scans all subdirectories in raw-data directory.
    Returns list of strings of subdirectories in raw-data directory"""

    print("Attempting to scan raw-data directory")

    dirs_list = [f.name for f in os.scandir(raw_data_directory) if f.is_dir()]
    print("Successfully scanned raw-data directory, found " + str(len(dirs_list)) + " directories")

    return dirs_list


def copy_files(path,missing_files):
    template_path='./Data_Templates'
    for f in missing_files:
        orig_path=template_path+'/'+f
        print(orig_path)
        shutil.copy(orig_path, path)

def add_templates(data_directory):
    raw_data_folders = raw_data_directory_list(data_directory)
    raw_data_files_dict(raw_data_folders, data_directory)
#    dirs_list = [f.name for f in os.scandir(raw_data_directory) if f.is_dir()]

add_templates("./Scraped_Files/")