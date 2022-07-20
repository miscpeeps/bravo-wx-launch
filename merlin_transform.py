import datetime
from datetime import datetime as dt
import numpy as np
import pandas as pd

def time_date(row):
    row['datetime'] = f"{row['Event Date']} {row['Event Time']}"
    row['datetime'] = pd.to_datetime(row['datetime'])
    row['datetime'] = dt(row['datetime'].year,row['datetime'].month,row['datetime'].day,row['datetime'].hour,row['datetime'].minute)
    return row

def cg(path, launchtime):
    try:
        # load
        df = pd.read_csv(path)


        # merge date and time
        df = df.apply(time_date, axis=1)    
        # remove excess columns
        df.drop(columns=['Event Date', 'Event Time', 'Latitude', 'Longitude','Event', 'SemiMajor Axis 50% CI', 'SemiMinor Axis 50% CI',
           'Ellipse Angle', 'Sensors',], inplace=True)

        df['Signal Strength'] = np.abs(df['Signal Strength'])

        # taking max
        groupby = df.groupby(by='datetime').sum()
        groupby_2 = df.groupby(by='datetime').count()


        groupby = groupby.merge(groupby_2, left_index=True,right_index=True)
        groupby.rename(columns={'Signal Strength_x':'Sum of Lightning Strike Signals', 'Signal Strength_y':'Count of Lightning Strikes'}, inplace=True)

        # create empty dataframe in 5 minute time increments in the time zero
        l = (pd.DataFrame(columns=['NULL'],index=pd.date_range(launchtime - datetime.timedelta(hours=4), launchtime,freq='5T')))

        # merge_asof groups nearby indices with a tolerance of 5 minutes
        groupby = pd.merge_asof(l, groupby, left_index=True, right_index=True, tolerance=pd.Timedelta("5m"))

        # created NULL column
        groupby.drop(columns='NULL', inplace=True)
        
    except:
        groupby=pd.DataFrame(columns=['Sum of Lightning Strike Signals','Count of Lightning Strikes'], index=pd.date_range(launchtime - datetime.timedelta(hours=4), launchtime,freq='5T'))

    # fill NAs with ground value
    groupby = groupby.fillna(0)
    
    return groupby
    
    