"""
This script downloads tide gage data and tide prediction data. It also
computes storm surge (the difference
between the two) and saves the data to a .csv. This script also 
downloads the station metadata and saves it to a JSON and it creates a shapefile
at the location of the gage.

Last significant edits: 12/16/22
"""
#%% import libraries and define parameters
from _inputs import *
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
import json
import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gp
import noaa_coops as nc
import numpy as np
from datetime import date
from tqdm import tqdm

# begin_year, f_out_a_meta, f_water_level_storm_surge, f_out_a_shp, f_out_swmm_waterlevel= def_inputs_for_a()

# parameters for downloading data
b_md = "0101" # start date of each year downloaded
e_md = "1231" # end date of each year downloaded
record_start = "19270701" # beginning of record for station
begin_year = int(record_start[0:4])
sta_id = "8638610" # sewells point gage id



# set up to download tide gage and tide prediction data
tday = date.today()
end_year = tday.year
end_md = "{}{}".format(str(tday.month).zfill(2), str(tday.day).zfill(2))

years = np.arange(begin_year, end_year+1, 1) 

prods = ["water_level", 'predictions', "hourly_height", "high_low", "wind", "air_pressure", "one_minute_water_level", "datums"]
datum = "NAVD"
units = "english"
time_zone = "lst" # local standard time
sta = nc.Station(sta_id)

lat, lon = sta.lat_lon

# for downloading the day prior to the first desired day in case timezone conversion cuts time off
dwnlded_time_cushion = False

#%% define functions
def dwnld_data(sta, b_date, e_date):
    data_wl = data_tide_pred = data_hourly_height = data_high_low = None
    lst_errors = []
    try:
        data_wl = sta.get_data(begin_date=b_date,
                            end_date=e_date,
                            product=prods[0],
                            datum=datum,
                            units=units,
                            time_zone=time_zone
            )
    except Exception as e:
        lst_errors.append("water_level failed to download. Error: {}".format(e))
        pass
    try:
        data_tide_pred = sta.get_data(begin_date=b_date,
                            end_date=e_date,
                            product=prods[1],
                            datum=datum,
                            units=units,
                            time_zone=time_zone)
    except Exception as e:
        lst_errors.append("predictions failed to download. Error: {}".format(e))
        pass
    try:
        data_hourly_height = sta.get_data(begin_date=b_date,
                            end_date=e_date,
                            product=prods[2],
                            datum=datum,
                            units=units,
                            time_zone=time_zone)
    except Exception as e:
        lst_errors.append("hourly_height failed to download. Error: {}".format(e))
        pass
    try:
        data_high_low = sta.get_data(begin_date=b_date,
                            end_date=e_date,
                            product=prods[3],
                            datum=datum,
                            units=units,
                            time_zone=time_zone)
    except Exception as e:
        lst_errors.append("high_low failed to download. Error: {}".format(e))
        pass
    
    return data_wl, data_tide_pred, data_hourly_height, data_high_low, lst_errors

#%% execute loop to download data
lst_problems = []
lst_dfs_wl = []
lst_dfs_tide_pred = []
lst_dfs_hourly_height = []
lst_dfs_high_low = []
count = -1
for y in years:
    # if y == 2020:
    #     break
    # else:
    #     continue
    count += 1
    # print("downloading data for year {}".format(y))
    problem = "none"
    yr = str(y)
    # e_yr = str(y)

    if y == 1927:
        b_date = record_start
    else:
        b_date = yr + b_md

    if y == end_year:
        e_date = yr + end_md
    else:
        e_date = yr + e_md
    
    try:
        # download day prior to first day desired (in case time zone conversion causes truncation)
        if dwnlded_time_cushion == False:
            b_date_cushion = str(y-1) + e_md
            e_date_cushion = yr + b_md

            data_wl, data_tide_pred, data_hourly_height, data_high_low, lst_errors = dwnld_data(sta, b_date_cushion, e_date_cushion)
            # data_wl, data_tide_pred = dwnld_data(sta, b_date_cushion, e_date_cushion)
            if data_wl is not None:
                lst_dfs_wl.append(data_wl)
            if data_tide_pred is not None:
                lst_dfs_tide_pred.append(data_tide_pred)
            if data_hourly_height is not None:
                lst_dfs_hourly_height.append(data_hourly_height)
            if data_high_low is not None:
                lst_dfs_high_low.append(data_high_low)
            dwnlded_time_cushion = True
        
        err = True
        download_attempt = 0
        print("Begin: {}, End: {}".format(b_date, e_date))
        while err == True:
            download_attempt += 1
            # print('download attempt {}...'.format(download_attempt))
            try:
                data_wl, data_tide_pred, data_hourly_height, data_high_low, lst_errors = dwnld_data(sta, b_date, e_date)
                # data_wl, data_tide_pred = dwnld_data(sta, b_date, e_date)
                err = False
                # print("####################################")
            except:
                pass
        if data_wl is not None:
            lst_dfs_wl.append(data_wl)
        if data_tide_pred is not None:
            lst_dfs_tide_pred.append(data_tide_pred)
        if data_hourly_height is not None:
            lst_dfs_hourly_height.append(data_hourly_height)
        if data_high_low is not None:
            lst_dfs_high_low.append(data_high_low)        
        # dfs_wl.append(data_wl)
        # dfs_tide_pred.append(data_tide_pred)
    except Exception as e:
        problem = e
        print("Failed to download data for year {}. Problem: {}".format(yr, problem))
        print(e)
    if (len(lst_errors) == 3) and (data_tide_pred is not None):
        print("no data was downloaded (other than tidal predictions)")
    else:
        for item in lst_errors:
            print(item)
    lst_problems.append(problem)
    # dcl work
    # if count == 5:
    #     break
    # end dcl work

#%% create dataset
metadata = sta.metadata

df_wl = pd.concat(lst_dfs_wl)
df_wl = df_wl[~df_wl.index.duplicated(keep='first')]
s_wl = df_wl.v.dropna()
s_wl.name = "waterlevel_ft"

df_tide_pred = pd.concat(lst_dfs_tide_pred)
df_tide_pred = df_tide_pred[~df_tide_pred.index.duplicated(keep='first')]
s_tide_pred = df_tide_pred.v.dropna()
s_tide_pred.name = "tide_ft"

df_comb = pd.concat([s_wl, s_tide_pred], axis = 1).dropna()
# df_comb = s_wl.join(s_tide_pred, how='left')
df_comb['surge_ft']=df_comb.waterlevel_ft - df_comb.tide_ft
df_comb.index.name = "date_time"

df_hourly_height = pd.concat(lst_dfs_hourly_height)
df_hourly_height = df_hourly_height[~df_hourly_height.index.duplicated(keep='first')]
s_hourly_height = df_hourly_height.v.dropna()
s_hourly_height.name = "waterlevel_ft_hrly"
s_hourly_height.index.name = "date_time"

df_comb_hrly = pd.concat([s_hourly_height, s_tide_pred], axis = 1).dropna()
# df_comb = s_wl.join(s_tide_pred, how='left')
df_comb_hrly['surge_ft_hrly']=df_comb_hrly.waterlevel_ft_hrly - df_comb_hrly.tide_ft
df_comb_hrly.index.name = "date_time"

# convert from eastern standard time to UTC
new_idx = df_comb.index.tz_localize('EST').tz_convert('UTC')
new_idx = new_idx.tz_localize(None)
df_comb.index = new_idx
new_idx = df_comb_hrly.index.tz_localize('EST').tz_convert('UTC')
new_idx = new_idx.tz_localize(None)
df_comb_hrly.index = new_idx


# saving tide data and metadata
with open(f_out_a_meta, 'w', encoding='utf-8') as outfile:
    json.dump(metadata,outfile,ensure_ascii=False, indent=4)

df_comb.to_csv(f_water_level_storm_surge)
df_comb_hrly.to_csv(f_water_level_storm_surge_hrly)

keys=['name', 'lat', 'lng']
geo_data={key:metadata[key] for key in keys}

df = pd.DataFrame(geo_data, index=[0])

gdf = gp.GeoDataFrame(df, geometry=gp.points_from_xy(df.lat, df.lng))

gdf.to_file(f_out_a_shp)

# exporting to SWMM .dat file
df_wlevel = pd.read_csv(f_water_level_storm_surge, parse_dates=["date_time"])
df_wlevel = df_wlevel.loc[:, ['date_time', 'waterlevel_ft']]

df_wlevel['date'] = df_wlevel.date_time.dt.strftime('%m/%d/%Y')
df_wlevel['time'] = df_wlevel.date_time.dt.time

with open(f_out_swmm_waterlevel, "w+") as file:
    file.write(";;Sewells Point Water Level Data\n")
    file.write(";;Water Level (ft)\n")
df_wlevel = df_wlevel[['date', 'time', 'waterlevel_ft']]
df_wlevel.to_csv(f_out_swmm_waterlevel, sep = '\t', index = False, header = False, mode="a")

# s_hourly_heights are instantaneous measurements
def sum_square_diffs(s1, s2):
    df_combined =  pd.concat([s1, s2], axis = 1).dropna()
    s_diffs = df_combined.diff(axis = 1).iloc[:,1]
    sse = (s_diffs**2).sum()
    frac_zero = s_diffs.value_counts().loc[0] / len(s_diffs)
    print("{:.4f}% of the diffs are zero".format(frac_zero*100))
    return frac_zero, sse, s_diffs, df_combined

# sse_vs_mean = sum_square_diffs(s_wl.resample("1H").mean(), s_hourly_height)
# sse_vs_min = sum_square_diffs(s_wl.resample("1H").min(), s_hourly_height)
# sse_vs_max = sum_square_diffs(s_wl.resample("1H").max(), s_hourly_height)
frac_zero_exact, sse_exact, s_diffs_exact, df_combined_exact = sum_square_diffs(s_wl, s_hourly_height)
# df_wlevel_comparison = pd.concat([s_wl, s_hourly_height], axis = 1).dropna()

# are the high_low datasets instananeous? hard to say
df_high_low = pd.concat(lst_dfs_high_low)
df_high_low = df_high_low[~df_high_low.index.duplicated(keep='first')]
s_high_low = df_high_low.v.dropna()
s_high_low.name = "waterlevel_ft_high_low"
s_high_low.index.name = "date_time"
# they don't seem to be instantaneous readings
frac_zero_exact, sse_exact, s_diffs_exact, df_combined_exact = sum_square_diffs(s_wl, s_high_low)

# they are not the same as the tide readings
frac_zero_exact, sse_exact, s_diffs_exact, df_combined_exact = sum_square_diffs(s_tide_pred, s_high_low)

# compare daily highs and lows to see if the readings are some kind of aggregated statistics, but nope
frac_zero_max, sse_max, s_diffs_max, df_combined_max = sum_square_diffs(s_wl.resample("1D").max(), s_high_low.resample("1D").max())
frac_zero_min, sse_min, s_diffs_min, df_combined_min = sum_square_diffs(s_wl.resample("1D").min(), s_high_low.resample("1D").min())

#%% comparing statistics of hourly vs. 6-minute data
import matplotlib.pyplot as plt
from scipy import stats
def perform_statistical_test(test, x, y, alpha):
    res = test(x, y)
    pval = res.pvalue
    if res.pvalue < alpha:
        result = 'reject'
    else:
        result = 'fail_to_reject'
    return pval, result


def compare_stats(s1, s2, txt_about_data, plot = True):
    pval, cmv_result = perform_statistical_test(stats.cramervonmises_2samp, s1, s2, 0.05)
    txt_cvm_results = "CVM: {} that data come from same dist. P-val = {:.3f}".format(cmv_result, pval)

    # rank sum test
    pval, ranksum_result = perform_statistical_test(stats.ranksums, s1, s2, 0.05)
    txt_ranksum_results = "Ranksum: {} that data come from same dist. P-val = {:.3f}".format(ranksum_result, pval)

    if plot:
        s1_mean = s1.mean()
        s2_mean = s2.mean()
        txt_means = "{} mean, {} mean = {:.1f}, {:.1f} (perc dif = {:.0f}%)".format(s2.name, s1.name, s2_mean, s1_mean, (s2_mean-s1_mean)/s1_mean*100)

        s1_med = s1.median()
        s2_med = s2.median()
        txt_meds = "{} median, {} median = {:.1f}, {:.1f} (perc dif = {:.0f}%)".format(s2.name, s1.name, s2_med, s1_med, (s2_med-s1_med)/s1_med*100)

        df_combined = pd.concat([s1, s2])
        hist, bin_edges = np.histogram(df_combined, density=True, bins = 10)
        fig, ax = plt.subplots(1, 2, figsize = (7,4), sharex=True, sharey=True)
        s1.hist(ax = ax[1], density = True, bins = bin_edges)
        ax[1].set_title(s1.name)
        s2.hist(ax = ax[0], density = True, bins = bin_edges)
        ax[0].set_title(s2.name)
        txt_fig_cap_txt = "{}\n{}\n{}\n{}\n{}".format(txt_about_data, txt_means, txt_meds, txt_cvm_results, txt_ranksum_results)
        fig.text(.5, -0.18, txt_fig_cap_txt, ha='center')
        fig.tight_layout()
    return cmv_result, ranksum_result

def succeeded_stat_test(result):
    if result == "reject":
        return False
    if result == "fail_to_reject":
        return True

# compare_stats(df_comb.surge_ft, df_comb_hrly.surge_ft_hrly, txt_about_data = "Comparing 6-minute surge data to hourly surge data")
# compare_stats(df_comb.surge_ft.resample("1D").max().dropna(), df_comb_hrly.surge_ft_hrly.resample("1D").max().dropna(), txt_about_data = "Comparing daily maxes of the 6-minute surge data to hourly surge data")

#%% moving through months of hourly data until we get evidence that the hourly and daily data come from same distribution
for start_date in tqdm(pd.Series(df_comb_hrly.index.date).unique()):
    if (start_date.day == 1) and (start_date.month == 1): # move forward a year at a time
        pass
    else:
        continue
    # print('investigating {} onward...'.format(start_date))
    df_comb_hrly_subset = df_comb_hrly.loc[start_date:]
    cmv_result_daily_max, ranksum_result_daily_max = compare_stats(df_comb.surge_ft.resample("1D").max().dropna(),
                                                                    df_comb_hrly_subset.surge_ft_hrly.resample("1D").max().dropna(),
                                                                    txt_about_data = "Comparing daily maxes of the 6-minute surge data to hourly surge data",
                                                                    plot = False)
    cmv_result_all, ranksum_result_all = compare_stats(df_comb.surge_ft,
                                                        df_comb_hrly_subset.surge_ft_hrly,
                                                        txt_about_data = "Comparing 6-minute surge data to hourly surge data",
                                                        plot = False)
    
    if (succeeded_stat_test(cmv_result_all) + succeeded_stat_test(ranksum_result_all)) > 0:
        print("succeeded a statistical test for hourly dates {} and onward".format(start_date))
        compare_stats(df_comb.surge_ft, df_comb_hrly_subset.surge_ft_hrly,
                       txt_about_data = "Comparing 6-minute surge data to hourly surge data")
        break

    if (succeeded_stat_test(cmv_result_daily_max) + succeeded_stat_test(ranksum_result_daily_max)) > 0:
        print("succeeded a statistical test for hourly dates {} and onward".format(start_date))
        compare_stats(df_comb.surge_ft.resample("1D").max().dropna(),
                      df_comb_hrly_subset.surge_ft_hrly.resample("1D").max().dropna(),
                      txt_about_data = "Comparing daily maxes of the 6-minute surge data to hourly surge data")
        break

#%% transferred to script b2
event_duration = 72 # hours
wlevel_threshold = 4.5 # feet
s_wlevel = df_comb.surge_ft

def surge_event_selection(s_wlevel, wlevel_threshold, event_duration):
    possible_event_peaks = pd.Series((s_wlevel[s_wlevel >= wlevel_threshold]).index)

    lst_event_start = []
    lst_event_end = []
    lst_event_peak = []
    lst_event_peak_time = []

    df_wlevel = pd.DataFrame()

    event_id = -1
    for possible_peaktime in possible_event_peaks:
        # break
        # ensure events do not overlap in time
        if possible_peaktime in df_wlevel.index:
            continue
        event_id += 1
        # assume event is centered around the peak time
        event_start = possible_peaktime - pd.Timedelta(event_duration/2, "hour")
        event_end = possible_peaktime + pd.Timedelta(event_duration/2, "hour")
        event_date_range_is_stable = False
        prev_start = event_start
        # print("Previous event start time: {}".format(event_start))
        attempts = 0
        while event_date_range_is_stable is False:
            # print("updating event start time....")
            # subset water level based on date range
            s_wlevel_subset = s_wlevel.loc[event_start:event_end]
            # find time of peak
            time_of_peak_wlevel = s_wlevel_subset.idxmax()
            # update event date range so it is 72 hours, centered on the peak
            event_start = time_of_peak_wlevel - pd.Timedelta(event_duration/2, "hour")
            event_end = time_of_peak_wlevel + pd.Timedelta(event_duration/2, "hour")
            # print("New event start time: {}".format(event_start))
            if prev_start == event_start:
                event_date_range_is_stable = True
                break
            prev_start = event_start
            attempts += 1
            if attempts == 15:
                print("There is some problem causing the while-loop to hang up.")
                break
        if time_of_peak_wlevel in df_wlevel.index:
            continue
        lst_event_start.append(event_start)
        lst_event_end.append(event_end)
        lst_event_peak.append(s_wlevel_subset.max())
        lst_event_peak_time.append(time_of_peak_wlevel)

        df_wlevel_subset = pd.DataFrame(s_wlevel_subset)
        df_wlevel_subset["event_id"] = event_id
        df_wlevel = pd.concat([df_wlevel, df_wlevel_subset])

    df_wlevel_events = pd.DataFrame(dict(
        event_start = lst_event_start,
        event_end = lst_event_end,
        peak_surge = lst_event_peak,
        peak_time = lst_event_peak_time,
    ))

# df_wlevel_events

    return df_wlevel_events, df_wlevel

#%% trying to figure out optimal threshold
event_duration = 72 # hours
wlevel_threshold = 4 # feet
s_wlevel = df_comb.surge_ft

target_n_events_per_year = 5
total_n_events_to_target = len(pd.Series(df_comb.index.year).unique()) * target_n_events_per_year

for wlevel_threshold in tqdm(np.linspace(4,0,100)):
    df_wlevel_events, df_wlevel = wlevel_event_selection(s_wlevel, wlevel_threshold, event_duration)
    if len(df_wlevel_events) >= total_n_events_to_target:
        print("With a threshold of {}, there are {} {}-hr events per year".format(wlevel_threshold, len(df_wlevel_events),
                                                                                  event_duration))
        wlevel_threshold_selection = wlevel_threshold
        break

#%% comparing distributions of the event peaks from the hourly vs the 6-minute time series
s_wlevel_hrly = df_comb_hrly.surge_ft_hrly

df_wlevel_events, df_wlevel = wlevel_event_selection(s_wlevel, wlevel_threshold_selection, event_duration)
df_wlevel_events_hrly, df_wlevel_hrly = wlevel_event_selection(s_wlevel_hrly, wlevel_threshold_selection, event_duration)
df_wlevel_events_hrly = df_wlevel_events_hrly.rename(columns = dict(peak_surge = "peak_surge_hourly"))
txt_about = "\nComparing 72-hour event peaks selected using a surge threshold of {:.2f} feet\n".format(wlevel_threshold_selection)
txt_n = "N-events hourly, n-events 6-minute: {}, {}".format(len(df_wlevel_events_hrly), len(df_wlevel_events))
compare_stats(df_wlevel_events.peak_surge, df_wlevel_events_hrly.peak_surge_hourly,
                txt_about_data = txt_about+txt_n)

df_events_tseries = df_wlevel.join(df_comb.drop("surge_ft", axis = 1), how = "left").reset_index().set_index(["event_id", "date_time"])
df_events_tseries_hourly = df_wlevel_hrly.join(df_comb_hrly.drop("surge_ft_hrly", axis = 1), how = "left").reset_index().set_index(["event_id", "date_time"])

df_wlevel_events.to_csv(f_water_level_storm_surge_event_summaries, index = False)
df_events_tseries.to_csv(f_water_level_storm_surge_event_tseries)
df_wlevel_events_hrly.to_csv(f_water_level_hourly_storm_surge_event_summaries, index = False)
df_events_tseries_hourly.to_csv(f_water_level_hourly_storm_surge_event_tseries)
#%% confirm that all the hourly events are present in the 6-minute events
# as long as none of the print statements are triggered, then we're good!
df_wlevel_events_hrly_subset = df_wlevel_events_hrly[df_wlevel_events_hrly.peak_time > min(df_events_tseries.reset_index().date_time)]
df_wlevel_events_hrly_subset_missing = df_wlevel_events_hrly_subset[~df_wlevel_events_hrly_subset.peak_time.isin(df_events_tseries.reset_index().date_time)]
if len(df_wlevel_events_hrly_subset_missing) > 0:
    print("For some reason, there events in the hourly dataset that are not present from event selection using the 6-minute data.")

# full resolution event peak times rounded to closest hour
s_peaktimes_fullres_rnded_to_hr = df_wlevel_events.peak_time.dt.round("H")

# these are full resolution events that are NOT in the hourly time series for some reason
fullres_events_not_in_hrly_events = df_wlevel_events[~s_peaktimes_fullres_rnded_to_hr.isin(df_events_tseries_hourly.reset_index().date_time)]
lst_hourly_max_vals = []
for idx, row in fullres_events_not_in_hrly_events.iterrows():
    s_wlevel_hrly_subset = s_wlevel_hrly[row.event_start:row.event_end]
    max_wlevel_in_hrly_data = s_wlevel_hrly_subset.max()
    lst_hourly_max_vals.append(max_wlevel_in_hrly_data)
    if wlevel_threshold_selection <= max_wlevel_in_hrly_data:
        print("WARNING: This event SHOULD show up in the events selected with the hourly dataset.")
        print(row)

