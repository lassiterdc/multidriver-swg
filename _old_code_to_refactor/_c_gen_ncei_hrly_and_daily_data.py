#%%
from _inputs import def_inputs_for_c
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

f_daily_summaries, f_hourlyprecip, fld_out_c_plts, fld_out_c_processed_data, min_record_length = def_inputs_for_c()

#%% load data
df_daily = pd.read_csv(f_daily_summaries, parse_dates=['DATE'], low_memory=False)
df_hrlyprcp = pd.read_csv(f_hourlyprecip, parse_dates=['DATE'], low_memory=False)

#%% define functions
def find_stations_with_min_record_length(df, sta_colname, date_colname, cutoff):
    df = df[[sta_colname, date_colname]]
    df_starts = df.groupby(by=sta_colname).min()[date_colname]
    df_ends = df.groupby(by=sta_colname).max()[date_colname]
    df_dur = df_ends.dt.year - df_starts.dt.year
    df_stations_meeting_critiera = df_dur[df_dur > min_record_length]
    print("{} stations had a record length of at least {} years. {} stations were dropped.".format(len(df_stations_meeting_critiera), cutoff, len(df_ends)-len(df_stations_meeting_critiera)))
    return list(df_stations_meeting_critiera.index.values)

def filter_stations_with_min_record(df, sta_colname = "STATION", date_colname = "DATE", cutoff=min_record_length):
    sta_ids = find_stations_with_min_record_length(df, sta_colname, date_colname, cutoff)
    df_subset = df[df[sta_colname].isin(sta_ids)]
    return df_subset

def gen_qaqc_plots(df, resolution, cutoff_date=None, colname_date = 'DATE', colname_station = 'STATION', colname_precip = 'PRCP'):
    # df = df_daily.copy()
    """
    df: data frame of NCEI precipitation data
    resolution: perported time resolution of the data readable by the pandas df.resmple() method (e.g., '1h', '1d')
    cutoff_date: date prior to which is sliced off, readable by a pandas conditional statement (e.g. '12/31/1953')
    colname_date: column name with pandas datetime
    colname_station: column name with unique NCEI station name
    colname_precip: column name with precipitation data in inches
    """
    lst_res_test = ['1h', '1d']
    if resolution not in lst_res_test:
        print("Resolution needs to be in this list: {}".format(lst_res_test))
        return
    # pull unique station IDs for faceting the plots
    sta_ids = df[colname_station].unique()
    # df = df_daily.copy()
    # formatting
    df = df[df.PRCP.notna()] # remove na precip values
    df = df[[colname_date, colname_station, colname_precip]]
    df = df.sort_values([colname_station, colname_date])
    df = df.set_index(colname_date, drop=True)
    df = df.groupby(colname_station, as_index=False).resample(resolution).asfreq(fill_value=0).reset_index()
    tstep_hrs = int(df.DATE.diff().mode().astype('timedelta64[h]')[0]) # ensure even time step; fill with zeros
    df = df[df.PRCP>0] # remove observations with no rainfall
    df = df[df.PRCP!=999.99] # remove observations with the 999.99 flag
    # df = df.reset_index()
    df['tdiff_hr'] = df.groupby(colname_station).diff().DATE.astype('timedelta64[h]')-tstep_hrs # compute the hourly time difference between non-zero observations
    df = df.set_index("DATE", drop=False) # add date index back to dataset
    if cutoff_date is not None:
        df = df[df.DATE>cutoff_date]

    for id in sta_ids:
        fig, ax = plt.subplots(dpi=300)
        df_plt = df[df.STATION==id]
        s = df_plt['tdiff_hr']
        upperlim = s.quantile(q=0.99)
        s = s[s<upperlim]
        s.resample('1y').mean().plot(ax =ax)
        ax.set_ylabel("Annual Average Time Difference Between \n Non-Zero Rainfall Observations (hours)")
        ax.set_xlabel("Year")
        ax.set_title("Station: {} ({}hr timestep) \n Time Differences Between Non-Zero Observations \n (0.99 quantile and up excluded)".format(id, tstep_hrs))

        fig, ax = plt.subplots(dpi=300)
        df_plt['PRCP'].resample('1y').sum().plot(ax =ax)
        ax.set_ylabel("Total Annual Rainfall (in)")
        ax.set_xlabel("Year")
        ax.set_title("Station: {} ({}hr timestep) \n Annual Precipitation Totals".format(id, tstep_hrs))
    return

#%% remove stations with less than minimum duration
df_daily = df_daily[['STATION', 'NAME', 'LATITUDE', 'LONGITUDE', 'DATE', 'PRCP']]
df_daily = filter_stations_with_min_record(df_daily)

df_hrlyprcp = df_hrlyprcp[['STATION', 'STATION_NAME', 'LATITUDE', 'LONGITUDE','DATE', 'HPCP']]
df_hrlyprcp = df_hrlyprcp.rename(columns={'HPCP':'PRCP', 'STATION_NAME':'NAME'})
df_hrlyprcp = filter_stations_with_min_record(df_hrlyprcp)