import datetime
import pandas as pd
import wind_profiler_915_transform
import wind_profiler_50_transform

path915='Scraped_Files/20220309-launch/WindProfiler915.csv'
launchtime915=datetime.datetime(2022,3,9,13,45,0)

path50='Scraped_Files/20220309-launch/WindProfiler50.csv'
launchtime50=datetime.datetime(2022,3,9,13,45,0)

df_915=wind_profiler_915_transform.wind_profiler_915(path915,launchtime915)
#df_50=wind_profiler_50_transform.wind_profiler_50(path50,launchtime50)
#df_2=pd.read_csv('test-runs/test-run-20220721-1132 (no tower or rain)/20220309-launch.csv')
#print(df.head())
#print(df_2.head())
#print(df_50.join(df_915))
print(df_915.columns)
