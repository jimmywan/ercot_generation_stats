from datetime import datetime
import pandas as pd
from fuel_mappings import *

def get_ercot_data():
    all_dfs = []
    for year in (2020, 2021, 2022):
        for month in list(range(1, 13)):
            newdf = _get_df(year, month)
            all_dfs.append(newdf)
            if year == 2022 and month == 5:
                break
    newdf = pd.concat(all_dfs)
    # Ignore incomplete weeks.
    newdf = newdf.query('Date < "2022-05-29"').copy()
    # Standardize on GWh
    newdf.loc[:, 'Total'] = newdf['Total'].divide(1000).round()
    newdf.loc[:, 'pct'] = newdf['pct'].round(3)
    return newdf

def _get_df(year: int, month: int):
    dateparse = lambda x: datetime.strptime(x, '%m/%d/%Y')
    decimalparse = lambda x: float(x.replace(',',''))

    df = pd.read_table(
        f'ERCOT/IntGenByFuel{year}{month:02d}.tsv',
        usecols=['Date','Fuel','Total', 'Settlement Type'],
        converters={'Date':dateparse,'Total':decimalparse},
    )
    
        # Find daily total
    alltotal=df.groupby('Date').sum()
    alltotal['Fuel']='ALL SOURCES'
    alltotal['Settlement Type']='Combined'
    alltotal = alltotal.reset_index(drop=False)
    newdf = pd.concat([df, alltotal])

    maxdf = newdf.groupby('Date').max()['Total'].reset_index(drop=False)
    
    newdf = newdf.merge(maxdf, left_on='Date', right_on='Date', suffixes=['', '_y']).rename({'Total_y': 'DailyMax'}, axis=1)

    newdf = newdf.assign(pct=lambda x: x['Total'] / x['DailyMax'])
    
    # Generate some type mappings
    newdf['FuelType'] = newdf['Fuel'].apply(get_fuel_type)
    newdf['LoadType'] = newdf['Fuel'].apply(get_load_type)
    newdf['SourceType'] = newdf['LoadType'] + ' ' + newdf['FuelType']
    newdf.loc[newdf['SourceType'] == 'Other Other', 'SourceType'] = 'Other'

    return newdf

