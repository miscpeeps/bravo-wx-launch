import datetime
from datetime import datetime as dt
import numpy as np
import pandas as pd

def time_date(row):
    row['datetime'] = f"{row['Event Date']} {row['Event Time']}"
    row['datetime'] = pd.to_datetime(row['datetime'])
    return row

def weather_towers(path, launchtime):
    try:
        # load
        df = pd.read_csv(path)
        # merge date and time
        df = df.apply(time_date, axis=1)
        
        
#         # remove excess columns
        df.drop(columns=['Event Date', 'Event Time','Time Interval', 'Avg Wind Direction',
                         'Peak Wind Direction','Peak Wind Direction 10 Min','Peak Wind Speed 10 Min',
                         'Dew Point', 'Relative Humidity'], inplace=True)
        filled = df.groupby(by=['datetime','Tower Measurement Location']).fillna(method='ffill').fillna(method='bfill')
        df['Avg Wind Speed'] = filled['Avg Wind Speed']
        df['Peak Wind Speed'] = filled['Peak Wind Speed']
        df['Deviation'] = filled['Deviation']
        df['Temp'] = filled['Temp']
        df['Temperature Difference'] = filled['Temperature Difference'] 
        df['Barometric Pressure'] = filled['Barometric Pressure'] 
        
        groupby = df.groupby(by=['datetime', 'Tower Measurement Location']).mean()
        groupby.drop(columns='Height', inplace=True)
        pivoted = groupby.reset_index(level=1).pivot(columns='Tower Measurement Location')
        pivoted.columns = [' '.join(col).strip() for col in pivoted.columns.values]
        groupby = pivoted

#           # create empty dataframe in 5 minute time increments in the time zero
        l = (pd.DataFrame(columns=['NULL'],index=pd.date_range(launchtime - datetime.timedelta(hours=4), launchtime,freq='5T')))

#           # merge_asof groups nearby indices with a tolerance of 5 minutes
        groupby = pd.merge_asof(l, groupby, left_index=True, right_index=True, tolerance=pd.Timedelta("5m"))

# #          # created NULL column
        groupby.drop(columns='NULL', inplace=True)
        #display(groupby)
        groupby.interpolate(inplace=True)
        groupby.bfill(inplace=True)
        
    except:
        print(f"error populating winds towers for {launchtime}")
        groupby=pd.DataFrame(columns=['Avg Wind Speed 0002 NW  SE', 'Avg Wind Speed 0002 SE  SE',
       'Avg Wind Speed 0006 NW  SE', 'Avg Wind Speed 0006 SE  SE',
       'Avg Wind Speed 0110 NW  SE', 'Avg Wind Speed 0110 SE  SE',
       'Avg Wind Speed 0313 NE  SW', 'Avg Wind Speed 0313 SW  SW',
       'Avg Wind Speed SLC 40', 'Avg Wind Speed SLC 41',
       'Avg Wind Speed VAB 01', 'Peak Wind Speed 0002 NW  SE',
       'Peak Wind Speed 0002 SE  SE', 'Peak Wind Speed 0006 NW  SE',
       'Peak Wind Speed 0006 SE  SE', 'Peak Wind Speed 0110 NW  SE',
       'Peak Wind Speed 0110 SE  SE', 'Peak Wind Speed 0313 NE  SW',
       'Peak Wind Speed 0313 SW  SW', 'Peak Wind Speed SLC 40',
       'Peak Wind Speed SLC 41', 'Peak Wind Speed VAB 01',
       'Deviation 0002 NW  SE', 'Deviation 0002 SE  SE',
       'Deviation 0006 NW  SE', 'Deviation 0006 SE  SE',
       'Deviation 0110 NW  SE', 'Deviation 0110 SE  SE',
       'Deviation 0313 NE  SW', 'Deviation 0313 SW  SW', 'Deviation SLC 40',
       'Deviation SLC 41', 'Deviation VAB 01', 'Temp 0002 NW  SE',
       'Temp 0002 SE  SE', 'Temp 0006 NW  SE', 'Temp 0006 SE  SE',
       'Temp 0110 NW  SE', 'Temp 0110 SE  SE', 'Temp 0313 NE  SW',
       'Temp 0313 SW  SW', 'Temp SLC 40', 'Temp SLC 41', 'Temp VAB 01',
       'Temperature Difference 0002 NW  SE',
       'Temperature Difference 0002 SE  SE',
       'Temperature Difference 0006 NW  SE',
       'Temperature Difference 0006 SE  SE',
       'Temperature Difference 0110 NW  SE',
       'Temperature Difference 0110 SE  SE',
       'Temperature Difference 0313 NE  SW',
       'Temperature Difference 0313 SW  SW', 'Temperature Difference SLC 40',
       'Temperature Difference SLC 41', 'Temperature Difference VAB 01',
       'Barometric Pressure 0002 NW  SE', 'Barometric Pressure 0002 SE  SE',
       'Barometric Pressure 0006 NW  SE', 'Barometric Pressure 0006 SE  SE',
       'Barometric Pressure 0110 NW  SE', 'Barometric Pressure 0110 SE  SE',
       'Barometric Pressure 0313 NE  SW', 'Barometric Pressure 0313 SW  SW',
       'Barometric Pressure SLC 40', 'Barometric Pressure SLC 41',
       'Barometric Pressure VAB 01'], index=pd.date_range(launchtime - datetime.timedelta(hours=4), launchtime,freq='5T'))
    
    return groupby
    