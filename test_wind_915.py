import pandas as pd

from wind_profiler_915_transform import time_date,wind_profiler_915

paths=[
'https://raw.githubusercontent.com/miscpeeps/bravo-wx-launch/main/raw-data/20220401-launch/wind-profiler-915mhz-export-20223919083944.csv',
'https://raw.githubusercontent.com/miscpeeps/bravo-wx-launch/main/raw-data/20220421-launch/wind-profiler-915mhz-export-20220119080152.csv',
'https://raw.githubusercontent.com/miscpeeps/bravo-wx-launch/main/raw-data/20220429-launch/wind-profiler-915mhz-export-20224419074438.csv',
'https://raw.githubusercontent.com/miscpeeps/bravo-wx-launch/main/raw-data/20220519-launch/wind-profiler-915mhz-export-20222919072959.csv',
'https://raw.githubusercontent.com/miscpeeps/bravo-wx-launch/main/raw-data/20220525-launch/wind-profiler-915mhz-export-20223419043427.csv',
'https://raw.githubusercontent.com/miscpeeps/bravo-wx-launch/main/raw-data/20220608-launch/wind-profiler-915mhz-export-20222319042358.csv',
'https://raw.githubusercontent.com/miscpeeps/bravo-wx-launch/main/raw-data/20220612-launch/wind-profiler-915mhz-export-20221519041522.csv',
'https://raw.githubusercontent.com/miscpeeps/bravo-wx-launch/main/raw-data/20220619-launch/wind-profiler-915mhz-export-20220619040611.csv',
'https://raw.githubusercontent.com/miscpeeps/bravo-wx-launch/main/raw-data/20220629-launch/wind-profiler-915mhz-export-20225619035615.csv',
'https://raw.githubusercontent.com/miscpeeps/bravo-wx-launch/main/raw-data/20220701-launch/wind-profiler-915mhz-export-20223719033733.csv',
'https://raw.githubusercontent.com/miscpeeps/bravo-wx-launch/main/raw-data/20220707-launch/wind-profiler-915mhz-export-20222319032318.csv',
'https://raw.githubusercontent.com/miscpeeps/bravo-wx-launch/main/raw-data/20220717-launch/wind-profiler-915mhz-export-20220519030505.csv'
]

for path in paths:
    get_date=pd.read_csv(path)
    get_date=get_date.apply(time_date, axis=1)
    launchtime=get_date.iloc[0,-1]
    test_df=wind_profiler_915(path,launchtime)
    print(path)
    print(test_df.isnull().sum().sum())
    print('Success')