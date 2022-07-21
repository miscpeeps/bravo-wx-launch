import mechanize
import pandas as pd
import datetime
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import os
from time import sleep
import shutil
from glob import glob


def get_data(start_date, start_time, end_date, end_time, site, download_dir):
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    prefs = {"profile.default_content_settings.popups": 0,
                 "download.default_directory": download_dir + r"/", # IMPORTANT - ENDING SLASH V IMPORTANT
                 "directory_upgrade": True}
    options.add_experimental_option('prefs', prefs)
    br = webdriver.Chrome(options=options)

    br = webdriver.Chrome()
    br.get("https://kscweather.ksc.nasa.gov/wxarchive/" + site)

    br.find_element(by="id", value="startDate").clear()
    br.find_element(by="id", value="startDate").send_keys(start_date)
    br.find_element(by="id", value="startTime").clear()
    br.find_element(by="id", value="startTime").send_keys(start_time)
    br.find_element(by="id", value="endDate").clear()
    br.find_element(by="id", value="endDate").send_keys(end_date)
    br.find_element(by="id", value="endTime").clear()
    br.find_element(by="id", value="endTime").send_keys(end_time)

    button = br.find_element(by="xpath", value="//*[@id='btnSearch']")
    br.execute_script("arguments[0].click();", button)

    sleep(30)

    url_split = br.current_url.split("/")
    print(url_split)
    dl_url = url_split[0] + "//" + url_split[2] + "/" + url_split[3] + "/" + url_split[4] + "/" + "Export" + "/" + url_split[6]

    br.get(dl_url)
    sleep(30)
    br.close()

'''
    br_search_request = br.click(id="btnSearch")
    print(br_search_request)
    br.open(br_search_request)
    url_split = (br.geturl().split("/"))
    print(url_split)
    dl_url = url_split[0] + "//" + url_split[2] + "/" + url_split[3] + "/" + url_split[4] + "/" + "Export" + "/" + \
             url_split[6]
    print(dl_url)
    br.retrieve(dl_url, "download.csv")
'''

def get_datetime(date, time):
    date = date.split("/")
    date = datetime.date(int(date[2]), int(date[0]), int(date[1]))
    time = time.split(":")
    time = datetime.time(int(time[0]), int(time[1]))

    launch_time = datetime.datetime.combine(date, time)

    POI_start = launch_time - datetime.timedelta(hours=4)

    launch_date = str(launch_time.date()).split("-")
    launch_date = launch_date[1] + "/" + launch_date[2] + "/" + launch_date[0]
    launch_time = str(launch_time.time())

    poi_date = str(POI_start.date()).split("-")
    start_date = poi_date[1] + "/" + poi_date[2] + "/" + poi_date[0]
    start_time = str(POI_start.time())

    return start_date, start_time, launch_date, launch_time


base_dir = 'D:/Weather/'
launches = pd.read_csv("launches.csv")
site_list = ["WeatherTower"]
for index, row in launches.iterrows():
    start_date, start_time, launch_date, launch_time = get_datetime(row["launch date"], row["time (z)"])
    print("running...")
    dir_name = launch_date.split("/")
    dir_name = dir_name[2] + dir_name[0] + dir_name[1] + "-launch"
    path = os.path.join(base_dir, dir_name)
    try:
        os.mkdir(path)
    except:
        print("Directory exists.")

    for site in site_list:
        get_data(start_date, start_time, launch_date, launch_time, site, path)
        sleep(5)
        for file in glob(r"C:\Users\Brad\Downloads\weather-tower*"):
            shutil.move(file, path + "/" + site + ".csv")
