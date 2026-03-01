#%% Import libraries and load directories
import pandas as pd
import sys
import numpy as np
from _inputs import *
import xarray as xr
from scipy import stats
from matplotlib import pyplot as plt
from datetime import datetime
import shutil
from _utils import *
from tqdm import tqdm
fldr_plt_selected_events = dir_event_selection + "plots_rain_and_surge_event_selection/selected_events/"
alpha = 0.05
n_bootstrap = 1000
event_date_column = "rain_event_start"
t_window_d = 365
nbins = 30
# make sure output folders exist
Path(dir_surge_events).mkdir(parents=True, exist_ok=True)
Path(dir_compound_events_plots).mkdir(parents=True, exist_ok=True)
Path(dir_mrms_events).mkdir(parents=True, exist_ok=True)
Path(dir_ncei_events).mkdir(parents=True, exist_ok=True)
Path(fldr_plt_selected_events).mkdir(parents=True, exist_ok=True)
#%% load data
# ncei data from the airport
# df_rainfall_ncei_daily_in = pd.read_csv(f_daily_summaries_airport, parse_dates=["DATE"])
df_rainfall_ncei_hourly_in = pd.read_csv(f_hourlyprecip_airport, parse_dates=["DATE"])

# drop duplicates and return just the precipitation series
# s_rainfall_ncei_daily_in = df_rainfall_ncei_daily_in.loc[:, ["DATE", "PRCP"]].drop_duplicates().set_index("DATE").PRCP
s_rainfall_ncei_hourly_in = df_rainfall_ncei_hourly_in.loc[:, ["DATE", "HPCP"]].drop_duplicates().set_index("DATE").HPCP
# replace missing values with 0 for event selection
s_rainfall_ncei_hourly_in[s_rainfall_ncei_hourly_in == 999.99] = 0


# s_rainfall_ncei_mrms_res_mm = s_rainfall_ncei_mrms_res_mm_per_hr * (5/60) # mm/hour * 5 min/tstep * 1/60 hr/min = mm per tstep
# mrms data
df_mrms_rainfall = pd.read_csv(f_mrms_rainfall, parse_dates=True, index_col = "time")
df_mrms_rainfall.index.name = "date_time"
s_mrms_bias_corrected = df_mrms_rainfall.bias_corrected
s_mrms_rainfall_mm_per_hr = df_mrms_rainfall.rain_mean
s_mrms_rainfall_mm_per_hr.name = "mm_per_hr"
tstep_mrms = pd.Series(s_mrms_rainfall_mm_per_hr.index.diff()).mode().iloc[0]
tstep_mrms_min = tstep_mrms / np.timedelta64(1, "h")*60
s_mrms_rainfall_mm = s_mrms_rainfall_mm_per_hr * (tstep_mrms_min/60)
s_mrms_rainfall_mm.name = "mm"
# water level data
df_6min_water_levels = pd.read_csv(f_water_level_storm_surge, parse_dates=True, index_col="date_time")
df_hrly_water_levels = pd.read_csv(f_water_level_storm_surge_hrly, parse_dates=True, index_col="date_time")
df_hrly_water_levels.rename(columns = dict(waterlevel_ft_hrly = "waterlevel_ft", surge_ft_hrly = "surge_ft"), inplace = True)
# preprocessing water level data
## resample water level time series to same timestep as rainfall
df_hrly_water_levels_1min = df_hrly_water_levels.loc[:, ["waterlevel_ft", "tide_ft", "surge_ft"]].resample('1min').mean().ffill()
df_hrly_water_levels_mrms_res = df_hrly_water_levels_1min.loc[:, ["waterlevel_ft", "tide_ft", "surge_ft"]].resample(f'{tstep_mrms_min:.0f}min').mean()
df_6min_water_levels_1min = df_6min_water_levels.loc[:, ["waterlevel_ft", "tide_ft", "surge_ft"]].resample('1min').mean().ffill()
df_6min_water_levels_mrms_res = df_6min_water_levels_1min.loc[:, ["waterlevel_ft", "tide_ft", "surge_ft"]].resample(f'{tstep_mrms_min:.0f}min').mean()

# df_6min_water_levels_mrms_res.to_csv(f_6min_wlevel_resampled_to_5_min)
df_6min_water_levels_mrms_res.to_xarray().to_netcdf(f_6min_wlevel_resampled_to_mrms_res_nc)


# load rainyda results
# ds_rainyday = xr.open_dataset(f_rain_realizations)
# # extract rainyday rainfall
# da_rainyday_depth = (ds_rainyday.rain * ds_rainyday.timestep_min / 60) # mm/hr * 5 min/testp * 1/60 hr/min = mm/tstep
# da_rainyday_mean_depth_per_event = da_rainyday_depth.sum(["time"]).mean(["latitude", "longitude"]) # sum over time then take spatial mean
# df_rainyday_mean_depth_per_event = da_rainyday_mean_depth_per_event.to_dataframe()

s_rainfall_ncei_hrly_mrms_res_mm_per_hr, s_rainfall_ncei_hrly_mrms_res_mm = process_ncei_data(s_rainfall_ncei_hourly_in, tstep_mrms_min)


# aorc data
df_aorc_rainfall = pd.read_csv(f_aorc_rainfall, parse_dates=True, index_col = "time")
df_aorc_rainfall.index.name = "date_time"
# change to mrms resolution
og_tstep_aorc = pd.Series(df_aorc_rainfall.index.diff()).mode().iloc[0]
df_aorc_rainfall = df_aorc_rainfall.resample(f'{tstep_mrms_min:.0f}min').asfreq()
df_aorc_rainfall = df_aorc_rainfall.groupby(pd.Grouper(level='date_time', freq=og_tstep_aorc)).ffill() # grouping by date makes sure i don't accidentally fill long swaths of missing data

s_aorc_rainfall_mm_per_hr = df_aorc_rainfall.rain_mean
s_aorc_rainfall_mm_per_hr.name = "mm_per_hr"
tstep_aorc = pd.Series(s_aorc_rainfall_mm_per_hr.index.diff()).mode().iloc[0]
tstep_aorc_min = tstep_aorc / np.timedelta64(1, "h")*60
s_aorc_rainfall_mm = s_aorc_rainfall_mm_per_hr * (tstep_aorc_min/60)
s_aorc_rainfall_mm.name = "mm"

#%% functions
def find_dates_with_data(s_full_dataset):
  s_valid_dates = pd.Series(pd.Series(s_full_dataset.index.date).unique())
  df_dates_with_data = pd.DataFrame(dict(valid = True), index = s_valid_dates)
  s_all_dates = pd.date_range(start = s_valid_dates.min(), end = s_valid_dates.max())
  df_valid_dates = df_dates_with_data.reindex(s_all_dates, fill_value=False)
  return df_valid_dates

def generate_annual_event_occurance_df(df_valid_dates, df_event_summary, event_date_column):
  event_idx_start_date = df_event_summary[event_date_column]#.dt.date
  df_events = pd.DataFrame(dict(event_occurred = True), index=event_idx_start_date)
  df_events["event_date"] = pd.to_datetime(df_events.index.date)
  df_events_per_date = df_events.groupby("event_date").sum()
  idx_dates = pd.date_range(df_valid_dates.index.min(), df_valid_dates.index.max())
  df_valid_and_occurance = pd.DataFrame(index = idx_dates)
  df_valid_and_occurance = df_valid_and_occurance.join(df_events_per_date, how = "outer").fillna(0)
  df_valid_and_occurance = df_valid_and_occurance.join(df_valid_dates, how = "outer")
  df_valid_and_occurance[f'n_events_prev{t_window_d}d'] = df_valid_and_occurance['event_occurred'].rolling(window=f'{t_window_d}D').sum()
  df_valid_and_occurance[f'n_valid_prev{t_window_d}d'] = df_valid_and_occurance['valid'].rolling(window=f'{t_window_d}D').sum()
  df_valid_and_occurance = df_valid_and_occurance[df_valid_and_occurance["n_valid_prev365d"] == t_window_d]
  return df_valid_and_occurance
#%% investigating rain event selection using a range of thresholds
manual_threshold_mm = 40
print(f"Running event selection on a range of thresholds....")
columns = ["thresholds_mm", "mrms_event_return_rate_n_per_year", "ncei_hrly_event_return_rate_n_per_year", "cvm_pval", "cvm_result", "ranksum_pval", "ranksum_result"]
window_extend = 0.5
ar_thresholds = np.linspace(manual_threshold_mm, manual_threshold_mm*(1+window_extend), 5)
idx = np.arange(0, len(ar_thresholds))

dic_mrms_event_results = {}
dic_aorc_event_results = {}
dic_ncei_hrly_event_results = {}

for i, threshold in enumerate(ar_thresholds):
  #
  str_data_source = "ncei_hrly"
  print(f"performing event selection on {str_data_source} data....")
  df_ncei_hrly_rainevent_summary, df_ncei_hrly_rainevent_tseries, ncei_hrly_threshold_mm, ncei_hrly_event_return_rate_n_per_year = event_selection_threshold_or_nstorms(s_rainfall_ncei_hrly_mrms_res_mm_per_hr,
                                                                                                  min_interevent_time,
                                                                                                  max_strm_len, str_data_source,
                                                                                                  threshold = threshold)
  dic_ncei_hrly_event_results[f"threshold_{threshold:.1f}mm_event_summary"] = df_ncei_hrly_rainevent_summary
  dic_ncei_hrly_event_results[f"threshold_{threshold:.1f}mm_event_tseries"] = df_ncei_hrly_rainevent_tseries
  dic_ncei_hrly_event_results[f"threshold_{threshold:.1f}mm"] = ncei_hrly_threshold_mm
  dic_ncei_hrly_event_results[f"threshold_{threshold:.1f}mm_event_return_rate"] = ncei_hrly_event_return_rate_n_per_year
  #
  str_data_source = "mrms"
  print(f"performing event selection on {str_data_source} data....")
  df_mrms_rainevent_summary, df_mrms_rainevent_tseries, mrms_threshold_mm, mrms_event_return_rate_n_per_year = event_selection_threshold_or_nstorms(s_mrms_rainfall_mm_per_hr,
                                                                              min_interevent_time, max_strm_len, str_data_source,
                                                                              threshold = threshold)
  dic_mrms_event_results[f"threshold_{threshold:.1f}mm_event_summary"] = df_mrms_rainevent_summary
  dic_mrms_event_results[f"threshold_{threshold:.1f}mm_event_tseries"] = df_mrms_rainevent_tseries
  dic_mrms_event_results[f"threshold_{threshold:.1f}mm"] = mrms_threshold_mm
  dic_mrms_event_results[f"threshold_{threshold:.1f}mm_event_return_rate"] = mrms_event_return_rate_n_per_year
  #
  str_data_source = "aorc"
  print(f"performing event selection on {str_data_source} data....")
  df_aorc_rainevent_summary, df_aorc_rainevent_tseries, aorc_threshold_mm, aorc_event_return_rate_n_per_year = event_selection_threshold_or_nstorms(s_aorc_rainfall_mm_per_hr,
                                                                              min_interevent_time, max_strm_len, str_data_source,
                                                                              threshold = threshold)
  dic_aorc_event_results[f"threshold_{threshold:.1f}mm_event_summary"] = df_aorc_rainevent_summary
  dic_aorc_event_results[f"threshold_{threshold:.1f}mm_event_tseries"] = df_aorc_rainevent_tseries
  dic_aorc_event_results[f"threshold_{threshold:.1f}mm"] = aorc_threshold_mm
  dic_aorc_event_results[f"threshold_{threshold:.1f}mm_event_return_rate"] = aorc_event_return_rate_n_per_year

#%% analyze results of varying precip thresholds
fldr_plots_threshold_selection = dir_event_selection + "plots_rain_and_surge_event_selection/threshold_selection_rain/"
try:
    shutil.rmtree(fldr_plots_threshold_selection)
except Exception as e:
  if os.path.exists(fldr_plots_threshold_selection):
      print(f"Problem removing directory {fldr_plots_threshold_selection}: {e}")
Path(fldr_plots_threshold_selection).mkdir(parents=True, exist_ok=True)

n_years_of_mrms = len(s_mrms_rainfall_mm_per_hr.index.year.unique())
n_years_of_aorc = len(s_aorc_rainfall_mm_per_hr.index.year.unique())

data_columns = ["depth_mm", "max_1hr_mean_mm_per_hour"]

df_threhsold_experiment_results = pd.DataFrame(index = idx, columns=columns)
for i, threshold in enumerate(ar_thresholds):
  df_mrms_rainevent_summary = dic_mrms_event_results[f"threshold_{threshold:.1f}mm_event_summary"]
  df_mrms_rainevent_tseries = dic_mrms_event_results[f"threshold_{threshold:.1f}mm_event_tseries"]
  
  mrms_threshold_mm = dic_mrms_event_results[f"threshold_{threshold:.1f}mm"]
  mrms_event_return_rate_n_per_year = dic_mrms_event_results[f"threshold_{threshold:.1f}mm_event_return_rate"]

  df_aorc_rainevent_summary = dic_aorc_event_results[f"threshold_{threshold:.1f}mm_event_summary"]
  df_aorc_rainevent_tseries = dic_aorc_event_results[f"threshold_{threshold:.1f}mm_event_tseries"]
  aorc_threshold_mm = dic_aorc_event_results[f"threshold_{threshold:.1f}mm"]
  aorc_event_return_rate_n_per_year = dic_aorc_event_results[f"threshold_{threshold:.1f}mm_event_return_rate"]

  df_ncei_hrly_rainevent_summary = dic_ncei_hrly_event_results[f"threshold_{threshold:.1f}mm_event_summary"].copy()
  df_ncei_hrly_rainevent_tseries = dic_ncei_hrly_event_results[f"threshold_{threshold:.1f}mm_event_tseries"].copy()
  ncei_hrly_threshold_mm = dic_ncei_hrly_event_results[f"threshold_{threshold:.1f}mm"]
  ncei_hrly_event_return_rate_n_per_year = dic_ncei_hrly_event_results[f"threshold_{threshold:.1f}mm_event_return_rate"]

  # compute rolling 1 hour max mean rainfall intensity
  df_rain_stats_ncei_hrly = compute_event_timeseries_statistics(df_ncei_hrly_rainevent_tseries["mm_per_hour"], lst_time_durations_h_to_analyze=lst_time_durations_h_to_analyze, varname="mm_per_hour",idx_name_time = "date_time",
                                            agg_stat = "mean")
  df_rain_stats_aorc = compute_event_timeseries_statistics(df_aorc_rainevent_tseries["mm_per_hour"], lst_time_durations_h_to_analyze=lst_time_durations_h_to_analyze, varname="mm_per_hour", idx_name_time = "date_time",
                                            agg_stat = "mean")
  df_rain_stats_mrms = compute_event_timeseries_statistics(df_mrms_rainevent_tseries["mm_per_hour"], lst_time_durations_h_to_analyze=lst_time_durations_h_to_analyze, varname="mm_per_hour", idx_name_time = "date_time",
                                            agg_stat = "mean")
  
  # new_data_columns = list(df_rain_stats_ncei_hrly.columns[df_rain_stats_ncei_hrly.columns.str.contains(r'^(?!.*tstep).*$')])
  df_ncei_hrly_rainevent_summary = df_ncei_hrly_rainevent_summary.join(df_rain_stats_ncei_hrly, rsuffix = "_right")
  df_aorc_rainevent_summary = df_aorc_rainevent_summary.join(df_rain_stats_aorc, rsuffix = "_right")
  df_mrms_rainevent_summary = df_mrms_rainevent_summary.join(df_rain_stats_mrms, rsuffix = "_right")

  # build dataset of n events in every valid continuous 365-day moving window
  df_valid_dates_aorc = find_dates_with_data(s_aorc_rainfall_mm_per_hr)
  df_valid_dates_mrms = find_dates_with_data(s_mrms_rainfall_mm_per_hr)
  df_valid_dates_ncei_hrly = find_dates_with_data(s_rainfall_ncei_hrly_mrms_res_mm_per_hr)

  df_valid_and_occurance_aorc = generate_annual_event_occurance_df(df_valid_dates_aorc, df_aorc_rainevent_summary, event_date_column)
  df_valid_and_occurance_mrms = generate_annual_event_occurance_df(df_valid_dates_mrms, df_mrms_rainevent_summary, event_date_column)
  df_valid_and_occurance_ncei = generate_annual_event_occurance_df(df_valid_dates_ncei_hrly, df_ncei_hrly_rainevent_summary, event_date_column)

  # perform statistical test on the resulting number of storms per year
  n_per_year_aorc = df_valid_and_occurance_aorc[f'n_events_prev{t_window_d}d']
  n_per_year_mrms = df_valid_and_occurance_mrms[f'n_events_prev{t_window_d}d']
  n_per_year_ncei_hrly = df_valid_and_occurance_ncei[f'n_events_prev{t_window_d}d']

  # analyze n-per-year
  dataset = "n_per_year"
  n_per_sample = n_years_of_mrms
  s_dataset1 = n_per_year_mrms
  s_dataset1.name = "mrms"
  n_per_sample = n_years_of_aorc
  s_dataset2 = n_per_year_aorc
  s_dataset2.name = "aorc"
  # s_dataset2 = n_per_year_ncei_hrly
  # s_dataset2.name = "ncei_hourly"
  figtitle_supplementary_text = f"(threshold = {threshold:.1f}mm)"
  dir_savefig = fldr_plots_threshold_selection
  fig_fname_prefix = f"threshold_{threshold:.1f}mm"
  bins = np.arange(0, max(s_dataset1.max(), s_dataset2.max()) + 1, 1)
  compare_2_distributions(s_dataset1, s_dataset2, dataset, n_bootstrap, n_per_sample, alpha, bins = bins, include_kde = False,
                                dir_savefig = dir_savefig, figtitle_supplementary_text = figtitle_supplementary_text,
                                  fig_fname_prefix = fig_fname_prefix)
  
  s_dataset2 = n_per_year_ncei_hrly
  s_dataset2.name = "ncei_hourly"
  figtitle_supplementary_text = f"(threshold = {threshold:.1f}mm)"
  dir_savefig = fldr_plots_threshold_selection
  fig_fname_prefix = f"threshold_{threshold:.1f}mm"
  bins = np.arange(0, max(s_dataset1.max(), s_dataset2.max()) + 1, 1)
  compare_2_distributions(s_dataset1, s_dataset2, dataset, n_bootstrap, n_per_sample, alpha, bins = bins, include_kde = False,
                                dir_savefig = dir_savefig, figtitle_supplementary_text = figtitle_supplementary_text,
                                  fig_fname_prefix = fig_fname_prefix)


  #% end work
  # analyze rain event statistics
  cols_to_compare = data_columns
  # event_threshold_mm = threshold
  # dir_saveplots = fldr_plots_threshold_selection
  # compare_mrms_and_ncei_event_statistics(cols_to_compare, df_mrms_rainevent_summary, alpha,
  #                                             df_ncei_hrly_rainevent_summary, event_threshold_mm, 
  #                                             n_bootstrap, dir_saveplots)
  
  for dataset in cols_to_compare:
    s_dataset1 = df_mrms_rainevent_summary[dataset]
    s_dataset1.name = "mrms"
    s_dataset2 = df_aorc_rainevent_summary[dataset]
    s_dataset2.name = "aorc"
    # s_dataset2 = df_ncei_hrly_rainevent_summary[dataset]
    n_per_sample = len(df_mrms_rainevent_summary)
    compare_2_distributions(s_dataset1, s_dataset2, dataset, n_bootstrap, n_per_sample, alpha, bins = 30, include_kde = True,
                                dir_savefig = dir_savefig, figtitle_supplementary_text = figtitle_supplementary_text,
                                  fig_fname_prefix = fig_fname_prefix)
    
    s_dataset2 = df_ncei_hrly_rainevent_summary[dataset]
    s_dataset2.name = "ncei_hrly"
    n_per_sample = len(df_mrms_rainevent_summary)
    compare_2_distributions(s_dataset1, s_dataset2, dataset, n_bootstrap, n_per_sample, alpha, bins = 30, include_kde = True,
                                dir_savefig = dir_savefig, figtitle_supplementary_text = figtitle_supplementary_text,
                                  fig_fname_prefix = fig_fname_prefix)
  
#%% compare 
  # df_threhsold_experiment_results.loc[i, "thresholds_mm"] = threshold
  # df_threhsold_experiment_results.loc[i, "mrms_event_return_rate_n_per_year"] = mrms_event_return_rate_n_per_year
  # df_threhsold_experiment_results.loc[i, "ncei_hrly_event_return_rate_n_per_year"] = ncei_hrly_event_return_rate_n_per_year
#%% after inspecting plots, define threshold and perform final event selection
rain_event_threshold_mm_selected = threshold = 55
assign_0_to_missing = True
target_events_per_year = None
# final run of event selection using selected threshold
str_data_source = "mrms"
print(f"performing event selection on {str_data_source} data....")
df_mrms_rainevent_summary, df_mrms_rainevent_tseries, mrms_threshold_mm, mrms_event_return_rate_n_per_year = event_selection_threshold_or_nstorms(s_mrms_rainfall_mm_per_hr,
                                                                            min_interevent_time, max_strm_len, str_data_source,
                                                                            threshold = threshold, assign_0_to_missing = assign_0_to_missing)
#
str_data_source = "aorc"
print(f"performing event selection on {str_data_source} data....")
df_aorc_rainevent_summary, df_aorc_rainevent_tseries, aorc_threshold_mm, aorc_event_return_rate_n_per_year = event_selection_threshold_or_nstorms(s_aorc_rainfall_mm_per_hr,
                                                                            min_interevent_time, max_strm_len, str_data_source,
                                                                            threshold = threshold)
#
str_data_source = "ncei_hrly"
print(f"performing event selection on {str_data_source} data....")
df_ncei_hrly_rainevent_summary, df_ncei_hrly_rainevent_tseries, ncei_hrly_threshold_mm, ncei_hourly_event_return_rate_n_per_year = event_selection_threshold_or_nstorms(s_rainfall_ncei_hrly_mrms_res_mm_per_hr,
                                                                                                min_interevent_time,
                                                                                                max_strm_len, str_data_source,
                                                                                                threshold = threshold, assign_0_to_missing = assign_0_to_missing)
# exporting
df_rain_stats_ncei_hrly = compute_event_timeseries_statistics(df_ncei_hrly_rainevent_tseries["mm_per_hour"], lst_time_durations_h_to_analyze=lst_time_durations_h_to_analyze, varname="mm_per_hour", idx_name_time = "date_time",
                                          agg_stat = "mean")
df_rain_stats_aorc = compute_event_timeseries_statistics(df_aorc_rainevent_tseries["mm_per_hour"], lst_time_durations_h_to_analyze=lst_time_durations_h_to_analyze, varname="mm_per_hour", idx_name_time = "date_time",
                                          agg_stat = "mean")
df_rain_stats_mrms = compute_event_timeseries_statistics(df_mrms_rainevent_tseries["mm_per_hour"], lst_time_durations_h_to_analyze=lst_time_durations_h_to_analyze, varname="mm_per_hour", idx_name_time = "date_time",
                                          agg_stat = "mean")

new_data_columns = list(df_rain_stats_ncei_hrly.columns[df_rain_stats_ncei_hrly.columns.str.contains(r'^(?!.*tstep).*$')])
df_ncei_hrly_rainevent_summary = df_ncei_hrly_rainevent_summary.join(df_rain_stats_ncei_hrly, rsuffix = "_right")
df_mrms_rainevent_summary = df_mrms_rainevent_summary.join(df_rain_stats_mrms, rsuffix = "_right")
df_aorc_rainevent_summary = df_aorc_rainevent_summary.join(df_rain_stats_aorc, rsuffix = "_right")

# save outputs to csv files
## mrms
Path(f_mrms_event_summaries).parent.mkdir(parents=True, exist_ok=True)
df_mrms_rainevent_summary.to_csv(f_mrms_event_summaries)
df_mrms_rainevent_tseries.to_csv(f_mrms_event_timeseries)
## aorc
Path(f_aorc_event_summaries).parent.mkdir(parents=True, exist_ok=True)
df_aorc_rainevent_summary.to_csv(f_aorc_event_summaries)
df_aorc_rainevent_tseries.to_csv(f_aorc_event_timeseries)
## ncei
Path(f_ncei_hrly_event_summaries).parent.mkdir(parents=True, exist_ok=True)
df_ncei_hrly_rainevent_summary.to_csv(f_ncei_hrly_event_summaries)
df_ncei_hrly_rainevent_tseries.to_csv(f_ncei_hrly_event_timeseries)
# df_ncei_daily_rainevent_summary.to_csv(f_ncei_daily_event_summaries)
# df_ncei_daily_rainevent_tseries.to_csv(f_ncei_daily_event_timeseries)

#%% compare mrms to aorc where they overlap
df_mrms_rainevent_tseries.reset_index().date_time.values

s_mrms_event_id = df_mrms_rainevent_tseries.reset_index().set_index("date_time")["event_id"]
s_mrms_mm_per_hr = df_mrms_rainevent_tseries.reset_index().set_index("date_time")["mm_per_hour"]
s_mrms_mm_per_hr.name = "mm_per_hr_mrms"
s_aorc_mm_per_hr = df_aorc_rainevent_tseries.reset_index().set_index("date_time")["mm_per_hour"]
s_aorc_mm_per_hr.name = "mm_per_hr_aorc"

# df_mrms_aorc_rainrate_comarison = pd.concat([s_mrms_mm_per_hr, s_aorc_mm_per_hr, s_mrms_event_id, s_mrms_bias_corrected], axis = 1).dropna()
df_mrms_aorc_rainrate_comarison = pd.concat([s_mrms_mm_per_hr, s_aorc_mm_per_hr, s_mrms_event_id], axis = 1).dropna().reset_index().set_index(["event_id", "date_time"])

df_mrms_aorc_rainrate_comarison_hourly = (
    df_mrms_aorc_rainrate_comarison.groupby(level='event_id')  # Group by 'event_id'
      .resample('1h', level='date_time')  # Resample 'date_time' index to 1 hour
      .mean()  # Take the mean
)

df_mrms_aorc_rainrate_comarison_hourly = pd.concat([df_mrms_aorc_rainrate_comarison_hourly.reset_index().set_index("date_time"), s_mrms_bias_corrected], axis = 1).dropna()

df_mrms_aorc_rainrate_comarison_hourly.plot.scatter(x = "mm_per_hr_mrms", y = "mm_per_hr_aorc")

df_mrms_aorc_rainrate_comarison_hourly.groupby("event_id").sum().plot.scatter(x = "mm_per_hr_mrms", y = "mm_per_hr_aorc")
#%% plotting
fig_title = "MRMS Events"
fname_savefig = f"{fldr_plt_selected_events}rain_event_nstorms_per_year_mrms.png"
plot_annual_rainfall_totals(df_mrms_rainevent_summary, s_mrms_rainfall_mm_per_hr,
                             rain_event_threshold_mm_selected, mrms_event_return_rate_n_per_year, fig_title, fname_savefig)

fig_title = "AORC Events"
fname_savefig = f"{fldr_plt_selected_events}rain_event_nstorms_per_year_aorc.png"
plot_annual_rainfall_totals(df_aorc_rainevent_summary, s_aorc_rainfall_mm_per_hr,
                             rain_event_threshold_mm_selected, aorc_event_return_rate_n_per_year, fig_title, fname_savefig)

fig_title = "NCEI Hourly Events"
fname_savefig = f"{fldr_plt_selected_events}rain_event_nstorms_per_year_ncei_hrly.png"
plot_annual_rainfall_totals(df_ncei_hrly_rainevent_summary, s_rainfall_ncei_hrly_mrms_res_mm_per_hr,
                             rain_event_threshold_mm_selected, ncei_hourly_event_return_rate_n_per_year, fig_title, fname_savefig)

n_bootstrap = 1000
dir_savefig = fldr_plt_selected_events
figtitle_supplementary_text = f"(threshold = {rain_event_threshold_mm_selected}mm)"
fig_fname_prefix = f"threshold_{rain_event_threshold_mm_selected}mm"
cols_to_compare = new_data_columns
cols_to_compare.append("depth_mm")
for dataset in cols_to_compare:
  s_dataset1 = df_mrms_rainevent_summary[dataset]
  s_dataset1.name = "mrms"
  s_dataset2 = df_ncei_hrly_rainevent_summary[dataset]
  s_dataset2.name = "ncei_hrly"
  n_per_sample = len(df_mrms_rainevent_summary)
  compare_2_distributions(s_dataset1, s_dataset2, dataset, n_bootstrap, n_per_sample, alpha, bins = 30, include_kde = True,
                              dir_savefig = dir_savefig, figtitle_supplementary_text = figtitle_supplementary_text,
                                fig_fname_prefix = fig_fname_prefix)
  s_dataset2 = df_aorc_rainevent_summary[dataset]
  s_dataset2.name = "aorc"
  n_per_sample = len(df_mrms_rainevent_summary)
  compare_2_distributions(s_dataset1, s_dataset2, dataset, n_bootstrap, n_per_sample, alpha, bins = 30, include_kde = True,
                              dir_savefig = dir_savefig, figtitle_supplementary_text = figtitle_supplementary_text,
                                fig_fname_prefix = fig_fname_prefix)
  
#%% compare n events per year for overlapping years
mrms_annual_counts = df_mrms_rainevent_summary.rain_event_start.dt.year.value_counts().sort_index()
mrms_annual_counts.name = "mrms_counts"
aorc_annual_counts = df_aorc_rainevent_summary.rain_event_start.dt.year.value_counts().sort_index()
aorc_annual_counts.name = "aorc_counts"
ncei_annual_counts = df_ncei_hrly_rainevent_summary.rain_event_start.dt.year.value_counts().sort_index()
ncei_annual_counts.name = "ncei_counts"


def test_whether_mean_is_zero(count_diffs, alpha = 0.05):
  from scipy.stats import ttest_1samp
  t_stat, p_value = ttest_1samp(count_diffs, 0)
  # alpha = 0.05
  if p_value < alpha:
      print(f"The mean is significantly different from zero (reject H0). p-value = {p_value}")
      result = "reject"
  else:
      print(f"The mean is not significantly different from zero (fail to reject H0). p-value = {p_value}")
      result = "fail_to_reject"
  return result

count_diffs_mrms_vs_aorc = pd.concat([mrms_annual_counts, aorc_annual_counts], axis = 1).dropna().diff(axis = 1).iloc[:, 1].values
test_whether_mean_is_zero(count_diffs_mrms_vs_aorc, alpha = 0.05)

count_diffs_mrms_vs_ncei = pd.concat([mrms_annual_counts, ncei_annual_counts], axis = 1).dropna().diff(axis = 1).iloc[:, 1].values
test_whether_mean_is_zero(count_diffs_mrms_vs_ncei, alpha = 0.05)
#%% comparing hourly to 6 minute surge events using a range of thresholds
# columns = ["thresholds_ft", "surge_6min_event_n_per_year", "surge_hrly_event_n_per_year", "cvm_pval", "cvm_result", "ranksum_pval", "ranksum_result"]
ar_thresholds = np.linspace(3.5, 2.2, 5)
idx = np.arange(0, len(ar_thresholds))
df_threhsold_experiment_results = pd.DataFrame(index = idx, columns=columns)
# s_hrly_surge = df_hrly_water_levels_mrms_res.surge_ft
# s_6min_surge = df_6min_water_levels_mrms_res.surge_ft

print('6-minute water level data has been resampled to hourly for comparing event statistics between hourly and 6-minute datasets')
s_6min_surge = df_6min_water_levels_mrms_res["surge_ft"].resample('h').nearest()
s_hrly_surge = df_hrly_water_levels["surge_ft"]

print(f"Running water level event selection on a range of thresholds....")
dic_hrly_wlevel_results = {}

dic_6min_wlevelt_results = {}

for i, threshold in tqdm(enumerate(ar_thresholds)):
  str_data_source = "hourly_surge_data"
  n_events_per_year = 4
  df_hrly_wlevel_event_summaries, df_hrly_wlevel_event_tseries, hrly_surge_event_threshold, surge_hrly_event_n_per_year = surge_event_selection(s_hrly_surge,
                                                                                                    surge_threshold_to_determine_event_limits, max_event_duration_h,
                                                                                                    min_interevent_time, str_data_source, threshold = threshold)
  dic_hrly_wlevel_results[f"wlevel_threshold{threshold:.1f}ft_event_summaries"] = df_hrly_wlevel_event_summaries
  dic_hrly_wlevel_results[f"wlevel_threshold{threshold:.1f}ft_event_tseries"] = df_hrly_wlevel_event_tseries
  dic_hrly_wlevel_results[f"wlevel_threshold{threshold:.1f}ft_event_threshold"] = hrly_surge_event_threshold
  dic_hrly_wlevel_results[f"wlevel_threshold{threshold:.1f}ft_event_n_per_year"] = surge_hrly_event_n_per_year
  
  str_data_source = "6min_surge_data"
  df_6min_wlevel_event_summaries, df_6min_wlevel_event_tseries, _6min_surge_event_threshold, surge_6min_event_n_per_year = surge_event_selection(s_6min_surge,
                                                                                                    surge_threshold_to_determine_event_limits, max_event_duration_h,
                                                                                                    min_interevent_time, str_data_source, threshold = threshold)

  dic_6min_wlevelt_results[f"wlevel_threshold{threshold:.1f}ft_event_summaries"] = df_6min_wlevel_event_summaries
  dic_6min_wlevelt_results[f"wlevel_threshold{threshold:.1f}ft_event_tseries"] = df_6min_wlevel_event_tseries
  dic_6min_wlevelt_results[f"wlevel_threshold{threshold:.1f}ft_event_threshold"] = _6min_surge_event_threshold
  dic_6min_wlevelt_results[f"wlevel_threshold{threshold:.1f}ft_event_n_per_year"] = surge_6min_event_n_per_year

  # perform statistical test on the resulting number of storms per year
  counts_surge_hrly_event_n_per_year = df_hrly_wlevel_event_summaries.surge_event_start.dt.year.value_counts()
  counts_surge_6min_event_n_per_year = df_6min_wlevel_event_summaries.surge_event_start.dt.year.value_counts()

  # cmv_pval, cmv_result = perform_statistical_test(stats.cramervonmises_2samp, counts_surge_hrly_event_n_per_year, counts_surge_6min_event_n_per_year, 0.05)
  # txt_cvm_results = "CVM: {} that data come from same dist. P-val = {:.3f}".format(cmv_result, cmv_pval)
  # ranksum_pval, ranksum_result = perform_statistical_test(stats.ranksums, counts_surge_hrly_event_n_per_year, counts_surge_6min_event_n_per_year, 0.05)
  # txt_ranksum_results = "Ranksum: {} that data come from same dist. P-val = {:.3f}".format(ranksum_result, ranksum_pval)
  # df_threhsold_experiment_results.loc[i, "cvm_pval"] = cmv_pval
  # df_threhsold_experiment_results.loc[i, "cvm_result"] = cmv_result
  # df_threhsold_experiment_results.loc[i, "ranksum_pval"] = ranksum_pval
  # df_threhsold_experiment_results.loc[i, "ranksum_result"] = ranksum_result
  # print(txt_cvm_results)
  # print(txt_ranksum_results)
  # df_threhsold_experiment_results.loc[i, "thresholds_ft"] = threshold
  # df_threhsold_experiment_results.loc[i, "surge_hrly_event_n_per_year"] = surge_hrly_event_n_per_year
  # df_threhsold_experiment_results.loc[i, "surge_6min_event_n_per_year"] = surge_6min_event_n_per_year
  # print(df_threhsold_experiment_results)

# create water level plots to see about using the hourly and 6 minute datasets
n_yrs_wlevel_6min = int(np.ceil(len(pd.Series(s_6min_surge.index.date).unique())/365))

n_bootstrap = 50

fldr_plots_threshold_selection = dir_event_selection + "plots_rain_and_surge_event_selection/threshold_selection_surge/"
try:
    shutil.rmtree(fldr_plots_threshold_selection)
except:
    pass
Path(fldr_plots_threshold_selection).mkdir(parents=True, exist_ok=True)

# df_threhsold_experiment_results = pd.DataFrame(index = idx, columns=columns)
for i, threshold in tqdm(enumerate(ar_thresholds)):
  # load data from dictionaries
  df_hrly_wlevel_event_summaries = dic_hrly_wlevel_results[f"wlevel_threshold{threshold:.1f}ft_event_summaries"]
  df_hrly_wlevel_event_tseries = dic_hrly_wlevel_results[f"wlevel_threshold{threshold:.1f}ft_event_tseries"]
  # hrly_surge_event_threshold = dic_hrly_wlevel_results[f"wlevel_threshold{threshold:.1f}ft_event_threshold"]
  surge_hrly_event_n_per_year = dic_hrly_wlevel_results[f"wlevel_threshold{threshold:.1f}ft_event_n_per_year"]

  df_6min_wlevel_event_summaries = dic_6min_wlevelt_results[f"wlevel_threshold{threshold:.1f}ft_event_summaries"]
  df_6min_wlevel_event_tseries = dic_6min_wlevelt_results[f"wlevel_threshold{threshold:.1f}ft_event_tseries"]
  # _6min_surge_event_threshold = dic_6min_wlevelt_results[f"wlevel_threshold{threshold:.1f}ft_event_threshold"]

  surge_6min_event_n_per_year = len(df_6min_wlevel_event_summaries) / (len(pd.Series(s_6min_surge.index.date).unique())/365)

  # compute rolling max mean intensities
  df_surge_stats_6min = compute_event_timeseries_statistics(df_6min_wlevel_event_tseries["surge_ft"], lst_time_durations_h_to_analyze=lst_time_durations_h_to_analyze, varname="surge_ft",
                                            agg_stat = "mean")
  df_surge_stats_hrly = compute_event_timeseries_statistics(df_hrly_wlevel_event_tseries["surge_ft"], lst_time_durations_h_to_analyze=lst_time_durations_h_to_analyze, varname="surge_ft",
                                            agg_stat = "mean")

  data_columns = list(df_surge_stats_hrly.columns[df_surge_stats_hrly.columns.str.contains(r'^(?!.*tstep).*$')])
  # data_columns.append("max_surge_ft")
  df_hrly_wlevel_event_summaries = df_hrly_wlevel_event_summaries.join(df_surge_stats_hrly, rsuffix = "_right")
  df_6min_wlevel_event_summaries = df_6min_wlevel_event_summaries.join(df_surge_stats_6min, rsuffix = "_right")

  # build dataset of n events in every valid continuous 365-day moving window
  df_valid_dates_6min = find_dates_with_data(s_6min_surge)
  df_valid_dates_hrly = find_dates_with_data(s_hrly_surge)

  df_valid_and_occurance_6min = generate_annual_event_occurance_df(df_valid_dates_6min, df_6min_wlevel_event_summaries, event_date_column="surge_event_start")
  df_valid_and_occurance_hrly = generate_annual_event_occurance_df(df_valid_dates_hrly, df_hrly_wlevel_event_summaries, event_date_column="surge_event_start")

  # perform statistical test on the resulting number of storms per year
  n_per_year_6min = df_valid_and_occurance_6min[f'n_events_prev{t_window_d}d']
  n_per_year_6min.name = "wlevel_6min_resampled_to_hourly"
  n_per_year_hrly = df_valid_and_occurance_hrly[f'n_events_prev{t_window_d}d']
  n_per_year_hrly.name = "wlevel_hrly"
  
  # figuring out how far back i can go with hourly data
  increment_yr = 10
  for start_year in np.arange(n_per_year_6min.index.min().year, n_per_year_hrly.index.min().year-increment_yr, -increment_yr):
    dataset = "n_per_year"
    n_per_sample = n_yrs_wlevel_6min
    s_dataset1 = n_per_year_6min.resample('YE').nearest()
    s_dataset2 = n_per_year_hrly[n_per_year_hrly.index.year >= start_year].resample('YE').nearest()
    figtitle_supplementary_text = f"\n(threshold = {threshold:.1f}ft, hrly data from {start_year} to {s_dataset2.index.max().year}, 6min data from {s_dataset1.index.min().year} to {s_dataset1.index.max().year})"
    dir_savefig = fldr_plots_threshold_selection
    fig_fname_prefix = f"threshold_{threshold:.1f}ft_hrly_data_startyear_{start_year}"
    bins = np.arange(0, max(s_dataset1.max(), s_dataset2.max()) + 1, 1)
    compare_2_distributions(s_dataset1, s_dataset2, dataset, n_bootstrap, n_per_sample, alpha, bins = bins, include_kde = False,
                                  dir_savefig = dir_savefig, figtitle_supplementary_text = figtitle_supplementary_text,
                                    fig_fname_prefix = fig_fname_prefix, do_bootstrapping = False, plot_hist = True)
    for dataset in data_columns:
      s_dataset1 = df_6min_wlevel_event_summaries[dataset]
      filter_daterange = df_hrly_wlevel_event_summaries["surge_event_start"].dt.year >= start_year
      s_dataset2 = df_hrly_wlevel_event_summaries[filter_daterange][dataset]
      s_dataset1.name = "wlevel_6min_resampled_to_hourly"
      s_dataset2.name = "wlevel_hrly"

      # calculate n per year for each one
      filter_s_hrly_data = s_hrly_surge.index.year >= start_year
      surge_hourly_event_n_per_year = len(s_dataset2) / (len(pd.Series(s_hrly_surge[filter_s_hrly_data].index.date).unique())/365)
      surge_6min_event_n_per_year = len(s_dataset1) / (len(pd.Series(s_6min_surge.index.date).unique())/365)

      figtitle_supplementary_text = f"\nthreshold = {threshold:.1f}ft\nhrly data from {start_year} to {s_hrly_surge.index.max().year}, 6min data from {s_6min_surge.index.min().year} to {s_6min_surge.index.max().year})"
      figtitle_supplementary_text += f"\nn and n-per-year for 6min ({len(s_dataset1):.0f}, {surge_6min_event_n_per_year:.2f}) and hourly ({len(s_dataset2):.0f}, {surge_hourly_event_n_per_year:.2f}) datasets"
      n_per_sample = len(s_dataset1)
      compare_2_distributions(s_dataset1, s_dataset2, dataset, n_bootstrap, n_per_sample, alpha, bins = 30, include_kde = True,
                                  dir_savefig = dir_savefig, figtitle_supplementary_text = figtitle_supplementary_text,
                                    fig_fname_prefix = fig_fname_prefix, do_bootstrapping = False, plot_hist = True)


#%% final water level event selection, aiming for similar return period as rainfall (2-2.5 events per year)
surge_event_threshold_ft_selected = 2.5
s_6min_surge = df_6min_water_levels_mrms_res["surge_ft"]

str_data_source = "6min_surge_data"
df_6min_wlevel_event_summaries, df_6min_wlevel_event_tseries, _6min_surge_event_threshold, surge_6min_event_n_per_year = surge_event_selection(s_6min_surge,
                                                                                                  surge_threshold_to_determine_event_limits, max_event_duration_h,
                                                                                                  min_interevent_time, str_data_source, threshold = surge_event_threshold_ft_selected)

nperyear = len(df_6min_wlevel_event_summaries) / (len(pd.Series(s_6min_surge.index.date).unique())/365)

df_surge_stats_6min = compute_event_timeseries_statistics(df_6min_wlevel_event_tseries["surge_ft"], lst_time_durations_h_to_analyze=lst_time_durations_h_to_analyze,
                                                           varname="surge_ft", idx_name_time = "date_time",agg_stat = "mean", idx_event_id = "surge_event_id")

df_6min_wlevel_event_summaries = df_6min_wlevel_event_summaries.join(df_surge_stats_6min)

#%% plot and save
fname_save = f"{fldr_plt_selected_events}max_surge_hist.png"
plot_wlevel_histogram(df_6min_wlevel_event_summaries, df_6min_wlevel_event_tseries, surge_event_threshold_ft_selected,
               s_6min_surge, surge_6min_event_n_per_year,
               fname_save = fname_save)

tstep_wlevel = pd.Series(s_6min_surge.index.diff()).mode().iloc[0]

wlevel_record_length = len(s_6min_surge.dropna().index) * tstep_wlevel

wlevel_return_rate_n_per_year = 365/((wlevel_record_length / len(df_6min_wlevel_event_summaries)) / np.timedelta64(1, "D"))

fig_title = "Surge Events"
fname_savefig = f"{fldr_plt_selected_events}surge_event_nstorms_per_year.png"
plot_annual_rainfall_totals(df_6min_wlevel_event_summaries, s_6min_surge, surge_event_threshold_ft_selected,
                             wlevel_return_rate_n_per_year, fig_title = fig_title, fname_savefig = fname_savefig, threshold_units = "feet")

df_6min_wlevel_event_summaries.to_csv(f_6min_surge_event_summaries)
df_6min_wlevel_event_tseries.to_csv(f_6min_surge_event_timeseries)
#%% compound event analysis
# any event that meets the surge characteristic that also experiences the rain event threshold within a 12-hour
# # moving window of the peak surge i am considering a compound event

# df_wlevel_event_tseries, df_wlevel_event_summaries, df_rainevent_tseries, df_rainevent_summary, df_water_levels_mrms_res, s_rainfall_mm_per_hr, s_rainfall_mm = df_6min_wlevel_event_tseries, df_6min_wlevel_event_summaries, df_mrms_rainevent_tseries,df_mrms_rainevent_summary,df_6min_water_levels_mrms_res,s_mrms_rainfall_mm_per_hr, s_mrms_rainfall_mm


df_6min_wlevel_mrms_event_summary_stats, df_6min_wlevel_mrms_event_tseries = determine_combined_event_statistics(df_6min_wlevel_event_tseries, df_6min_wlevel_event_summaries, df_mrms_rainevent_tseries,
                                                                                                                  df_mrms_rainevent_summary,df_6min_water_levels_mrms_res,
                                                                                                                    s_mrms_rainfall_mm_per_hr, s_mrms_rainfall_mm)

df_6min_wlevel_aorc_event_summary_stats, df_6min_wlevel_aorc_event_tseries = determine_combined_event_statistics(df_6min_wlevel_event_tseries, df_6min_wlevel_event_summaries, df_aorc_rainevent_tseries,
                                                                                                                  df_aorc_rainevent_summary,df_6min_water_levels_mrms_res,
                                                                                                                    s_aorc_rainfall_mm_per_hr, s_aorc_rainfall_mm)

# check to make sure that the total rain depth in each event summary matches the manual sum from the time series

df_6min_wlevel_hrly_ncei_event_summary_stats, df_6min_wlevel_hrly_ncei_event_tseries = determine_combined_event_statistics(df_6min_wlevel_event_tseries, df_6min_wlevel_event_summaries, df_ncei_hrly_rainevent_tseries,
                                                                                                                  df_ncei_hrly_rainevent_summary,df_6min_water_levels_mrms_res,
                                                                                                                    s_rainfall_ncei_hrly_mrms_res_mm_per_hr, s_rainfall_ncei_hrly_mrms_res_mm)


df_6min_wlevel_mrms_event_summary_stats.to_csv(f_6m_wlevel_mrms_event_summaries)
df_6min_wlevel_mrms_event_tseries.to_csv(f_6m_wlevel_mrms_event_timeseries)

df_6min_wlevel_aorc_event_summary_stats.to_csv(f_6m_wlevel_aorc_event_summaries)
df_6min_wlevel_aorc_event_tseries.to_csv(f_6m_wlevel_aorc_event_timeseries)

df_6min_wlevel_hrly_ncei_event_summary_stats.to_csv(f_6min_wlevel_hrly_ncei_event_summaries)
df_6min_wlevel_hrly_ncei_event_tseries.to_csv(f_6min_wlevel_hrly_ncei_event_timeseries)