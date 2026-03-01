#%% import libraries and load directories
import pandas as pd
import sys
import numpy as np
from _inputs import *
import xarray as xr
from scipy import stats
from matplotlib import pyplot as plt
from datetime import datetime
from _utils import *

# make sure output folders exist

Path(dir_compound_events_stats_plots).mkdir(parents=True, exist_ok=True)
Path(dir_event_slxn_validing_distributions).mkdir(parents=True, exist_ok=True)

#%% load hydro observations
df_rainfall = pd.read_csv(f_mrms_rainfall, parse_dates=True, index_col = "time")
df_rainfall.index.name = "date_time"
s_mrms_rainfall_mm_per_hr = df_rainfall.rain_mean

df_6min_water_levels = pd.read_csv(f_water_level_storm_surge, parse_dates=True, index_col="date_time")
# df_hrly_water_levels = pd.read_csv(f_water_level_storm_surge_hrly, parse_dates=True, index_col="date_time")
#%% load compound event data
# 6 min water level and mrms
df_6min_wlevel_mrms_event_summary_stats = pd.read_csv(f_6m_wlevel_mrms_event_summaries, parse_dates = ["event_start", "event_end"])
df_6min_wlevel_mrms_event_tseries = pd.read_csv(f_6m_wlevel_mrms_event_timeseries, parse_dates = ["date_time"])
if "_mm" in df_6min_wlevel_mrms_event_tseries.columns:
    df_6min_wlevel_mrms_event_tseries = df_6min_wlevel_mrms_event_tseries.rename(columns={"_mm":"mm"})

df_6min_wlevel_aorc_event_summary_stats = pd.read_csv(f_6m_wlevel_aorc_event_summaries, parse_dates = ["event_start", "event_end"])
df_6min_wlevel_aorc_event_tseries = pd.read_csv(f_6m_wlevel_aorc_event_timeseries, parse_dates = ["date_time"])
if "_mm" in df_6min_wlevel_aorc_event_tseries.columns:
    df_6min_wlevel_aorc_event_tseries = df_6min_wlevel_aorc_event_tseries.rename(columns={"_mm":"mm"})

df_6min_wlevel_hrly_ncei_event_summary_stats = pd.read_csv(f_6min_wlevel_hrly_ncei_event_summaries, parse_dates = ["event_start", "event_end"])
df_6min_wlevel_hrly_ncei_event_tseries = pd.read_csv(f_6min_wlevel_hrly_ncei_event_timeseries, parse_dates = ["date_time"])

df_6min_wlevel_hrly_ncei_event_summary_stats = classify_events(df_6min_wlevel_hrly_ncei_event_summary_stats)
df_6min_wlevel_mrms_event_summary_stats = classify_events(df_6min_wlevel_mrms_event_summary_stats)
df_6min_wlevel_aorc_event_summary_stats = classify_events(df_6min_wlevel_aorc_event_summary_stats)
#%% full time series vs. event time series
df_mrms_rainfall = pd.read_csv(f_mrms_rainfall, parse_dates=True, index_col = "time")
df_mrms_rainfall.index.name = "date_time"
s_mrms_bias_corrected = df_mrms_rainfall.bias_corrected
s_mrms_rainfall_mm_per_hr = df_mrms_rainfall.rain_mean
s_mrms_rainfall_mm_per_hr.name = "mm_per_hr"
tstep_mrms = pd.Series(s_mrms_rainfall_mm_per_hr.index.diff()).mode().iloc[0]
tstep_mrms_min = tstep_mrms / np.timedelta64(1, "h")*60
s_mrms_rainfall_mm = s_mrms_rainfall_mm_per_hr * (tstep_mrms_min/60)
s_mrms_rainfall_mm.name = "mm"
#%%

fig, ax = plt.subplots(dpi = 300, figsize = (6, 4))
# s_mrms_rainfall_mm.plot(ax=ax)
s_mrms_rainfall_mm.loc["2015":].plot(ax=ax)
ax.set_xticklabels("")
ax.set_yticklabels("")
ax.set_xlabel("date time")
ax.set_ylabel("precipitation intensity")
# plt.clf()
#%%


# df_6min_wlevel_mrms_event_summary_stats.set_index("event_start").reset_index

# merged = pd.merge_asof(s_mrms_rainfall_mm.reset_index(),
#                        df_6min_wlevel_mrms_event_summary_stats,
#                        left_on='date_time',
#                        right_on='event_start',
#                        direction='nearest')  # options: 'backward', 'forward', 'nearest'


# merged.set_index("event_start").loc["2015":]\
#     .reset_index().plot("event_start", "precip_depth_mm", ax=ax)
# ax.set_xticklabels("")
# ax.set_yticklabels("")
# ax.set_xlabel("date time")
# ax.set_ylabel("precipitation intensity")



# Plot using plt.bar with datetime x-values
# plt.figure(figsize=(10, 4))
df_6min_wlevel_mrms_event_summary_stats_no_na = df_6min_wlevel_mrms_event_summary_stats.set_index("event_start").loc["2015":]
df_6min_wlevel_mrms_event_summary_stats_no_na = df_6min_wlevel_mrms_event_summary_stats_no_na.loc[df_6min_wlevel_mrms_event_summary_stats_no_na["precip_depth_mm"].dropna().index]

fig, ax = plt.subplots(dpi = 300, figsize = (6, 4))
ax.bar(df_6min_wlevel_mrms_event_summary_stats_no_na.index, df_6min_wlevel_mrms_event_summary_stats_no_na["precip_depth_mm"]
        , width=pd.Timedelta(days=5))  # width controls bar thickness
ax.set_xticklabels("")
ax.set_yticklabels("")
ax.set_xlabel("date time")
ax.set_ylabel("precipitation intensity")
fig.tight_layout()


#%% pie charts
fname_savefig = f"{dir_compound_events_stats_plots}pie_frac_surge_rain_and_combo_6min_wlevel_and_ncei.png"
n_valid_events = len(df_6min_wlevel_hrly_ncei_event_summary_stats.dropna())
n_compound = (df_6min_wlevel_hrly_ncei_event_summary_stats.dropna()["compound"]==True).sum()
frac_overlapping = n_compound / n_valid_events
fig, ax = plt.subplots()
df_6min_wlevel_hrly_ncei_event_summary_stats["data_source"] = df_6min_wlevel_hrly_ncei_event_summary_stats["rain_data_source"] + "_and_" + df_6min_wlevel_hrly_ncei_event_summary_stats["wlevel_data_source"]
df_6min_wlevel_hrly_ncei_event_summary_stats.dropna()["event_type"].value_counts().plot.pie(ax=ax)
ax.set_ylabel('')
txt = f"{n_compound}/{n_valid_events} ({frac_overlapping * 100:.1f}%) of events exceed both the rainfall and surge thresholds"
fig.text(0, 0, txt)
plt.savefig(fname_savefig, bbox_inches='tight')
plt.clf()

fname_savefig = f"{dir_compound_events_stats_plots}pie_frac_surge_rain_and_combo_6min_wlevel_and_mrms.png"
n_valid_events = len(df_6min_wlevel_mrms_event_summary_stats.dropna())
n_compound = (df_6min_wlevel_mrms_event_summary_stats.dropna()["compound"]==True).sum()
frac_overlapping = n_compound / n_valid_events
fig, ax = plt.subplots()
df_6min_wlevel_mrms_event_summary_stats.dropna()["event_type"].value_counts().plot.pie(ax=ax)
ax.set_ylabel('')
txt = f"{n_compound}/{n_valid_events} ({frac_overlapping * 100:.1f}%) of events exceed both the rainfall and surge thresholds"
fig.text(0, 0, txt)
plt.savefig(fname_savefig, bbox_inches='tight')
plt.clf()

fname_savefig = f"{dir_compound_events_stats_plots}pie_frac_surge_rain_and_combo_6min_wlevel_and_aorc.png"
n_valid_events = len(df_6min_wlevel_aorc_event_summary_stats.dropna())
n_compound = (df_6min_wlevel_aorc_event_summary_stats.dropna()["compound"]==True).sum()
frac_overlapping = n_compound / n_valid_events
fig, ax = plt.subplots()
df_6min_wlevel_aorc_event_summary_stats.dropna()["event_type"].value_counts().plot.pie(ax=ax)
ax.set_ylabel('')
txt = f"{n_compound}/{n_valid_events} ({frac_overlapping * 100:.1f}%) of events exceed both the rainfall and surge thresholds"
fig.text(0, 0, txt)
plt.savefig(fname_savefig, bbox_inches='tight')
plt.clf()
#%%  mrms data when available (only using 6-min water level currently)
# mrms and ncei
# lst_df_tseries_in_order_of_preferred_data_source = [df_6min_wlevel_mrms_event_tseries, df_6min_wlevel_hrly_ncei_event_tseries]
# lst_df_summaries_in_order_of_preferred_data_source = [df_6min_wlevel_mrms_event_summary_stats, df_6min_wlevel_hrly_ncei_event_summary_stats]


# mrms and aorc
lst_df_tseries_in_order_of_preferred_data_source = [df_6min_wlevel_mrms_event_tseries, df_6min_wlevel_aorc_event_tseries]
lst_df_summaries_in_order_of_preferred_data_source = [df_6min_wlevel_mrms_event_summary_stats, df_6min_wlevel_aorc_event_summary_stats]


# mrms, aorc, and ncei
# lst_df_tseries_in_order_of_preferred_data_source = [df_6min_wlevel_mrms_event_tseries, df_6min_wlevel_aorc_event_tseries, df_6min_wlevel_hrly_ncei_event_tseries]
# lst_df_summaries_in_order_of_preferred_data_source = [df_6min_wlevel_mrms_event_summary_stats, df_6min_wlevel_aorc_event_summary_stats, df_6min_wlevel_hrly_ncei_event_summary_stats]


# lst_corresponding_target_data_sources = [df_6min_wlevel_mrms_event_summary_stats.dropna()["rain_data_source"].iloc[0],
#                                        df_6min_wlevel_hrly_ncei_event_summary_stats.dropna()["rain_data_source"].iloc[0]]
# lst_corresponding_nontarget_data_sources = [df_6min_wlevel_mrms_event_summary_stats.dropna()["wlevel_data_source"].iloc[0],
#                                        df_6min_wlevel_hrly_ncei_event_summary_stats.dropna()["wlevel_data_source"].iloc[0]]


col_target_data_source = "rain_data_source"
# col_nontarget_data_source = "wlevel_data_source"
col_data = "mm_per_hr"

df_combined_tseries, df_combined_summaries = combine_dataset_according_to_priority(col_target_data_source, # col_nontarget_data_source,
                                                                                    lst_df_tseries_in_order_of_preferred_data_source,
                                                                                    col_data, lst_df_summaries_in_order_of_preferred_data_source)

# # years with mrms data available
# years_with_mrms = pd.Series(s_mrms_rainfall_mm_per_hr.index.year).unique()
# df_mrms_years = pd.DataFrame(dict(years_with_mrms = years_with_mrms),
#                              index = years_with_mrms)

# # years with 6-min water level data available
# years_with_6min_wlevel = pd.Series(df_6min_water_levels.index.year).unique()
# df_6min_wlevel_years = pd.DataFrame(dict(years_with_6min_wlevel = years_with_6min_wlevel),
#                                     index = years_with_6min_wlevel)
# df_6min_wlevel_mrms_overlap = pd.concat([df_mrms_years, df_6min_wlevel_years], axis = 1)
# years_with_6min_wlevel_and_mrms = df_6min_wlevel_mrms_overlap.dropna().index.values
# years_with_6min_wlevel_and_NO_mrms = df_6min_wlevel_mrms_overlap[df_6min_wlevel_mrms_overlap.years_with_mrms.isna()].index.values
# years_with_NO_6min_wlevel_and_mrms = df_6min_wlevel_mrms_overlap[df_6min_wlevel_mrms_overlap.years_with_6min_wlevel.isna()].index.values
# if len(years_with_NO_6min_wlevel_and_mrms) > 0:
#     print("WARNING: I had not coded for this situation and would need to do so if this print statement is triggered.")

# # where 6-min water level is available and MRMS data is NOT available, replace those rows with NCEI hourly data and 6min water level data
# idx_to_keep = df_6min_wlevel_hrly_ncei_event_summary_stats[~df_6min_wlevel_hrly_ncei_event_summary_stats.event_start.dt.year.isin(years_with_6min_wlevel_and_mrms)].index
# df_to_append1 = df_6min_wlevel_hrly_ncei_event_summary_stats.loc[idx_to_keep,:].dropna()
# df_tseries_to_append1 = df_6min_wlevel_hrly_ncei_event_tseries.set_index(["event_id", "date_time"]).loc[df_to_append1.event_id]

# ## retrieve rows to add
# idx_to_keep = df_6min_wlevel_mrms_event_summary_stats[df_6min_wlevel_mrms_event_summary_stats.event_start.dt.year.isin(years_with_6min_wlevel_and_mrms)].index
# df_to_append2 = df_6min_wlevel_mrms_event_summary_stats.loc[idx_to_keep, :]
# df_tseries_to_append2 = df_6min_wlevel_mrms_event_tseries.set_index(["event_id", "date_time"]).loc[df_to_append2.event_id]

# ## combine all non-overlapping hydro data into a single df
# df_combined_summaries = pd.concat([df_to_append1, df_to_append2]).sort_values("event_start").reset_index(drop = True)
# df_wlevel_rain_tseries = pd.concat([df_tseries_to_append1, df_tseries_to_append2]).reset_index().sort_values("date_time")

# # combine time series
# # index by event_start
# df_tseries = df_wlevel_rain_tseries
# df_event_summary = df_combined_summaries
# def re_index_tseries_by_event_start(df_tseries, datetime_col = "date_time"):
#     s_event_starts = df_tseries.groupby("event_id").min()[datetime_col]
#     s_event_starts.name = "event_start"
#     # df_tseries.set_index("event_id").join(s_event_starts, how = "left")
#     df_tseries_reindexed = df_tseries.set_index("event_id").join(s_event_starts, how = "left").reset_index(drop=True).set_index(["event_start", "date_time"])
#     # ds_tseries = df_tseries_reindexed.to_xarray()
#     return df_tseries_reindexed.sort_index()

# df_combined_summaries = df_combined_summaries.drop(columns = "event_id").set_index("event_start")
# df_combined_summaries = classify_event(df_combined_summaries)

# df_wlevel_rain_tseries_reindexed = re_index_tseries_by_event_start(df_wlevel_rain_tseries, datetime_col = "date_time")
# df_wlevel_rain_tseries_reindexed = df_wlevel_rain_tseries_reindexed.join(df_combined_summaries.event_selection_category, how = "left")
# df_wlevel_rain_tseries_reindexed = df_wlevel_rain_tseries.reset_index().set_index(["date_time"])

# df_combined_summaries = df_combined_summaries.reset_index().set_index(["event_selection_category", "event_start"])
# ds_wlevel_rain_summary_stats = df_combined_summaries.to_xarray()
ds_combined_tseries = df_combined_tseries.set_index(["date_time"]).to_xarray()
# ds_wlevel_rain_events = xr.merge([df_combined_summaries, ds_wlevel_rain_summary_stats])



#%% write to file
df_combined_summaries.to_csv(f_combined_event_summaries, index=False)
df_combined_tseries.to_csv(f_combined_event_tseries_csv, index = False)
comp = dict(zlib=True, complevel=to_netcdf_compression_level)
encoding = {var: comp for var in ds_combined_tseries.data_vars}
ds_combined_tseries.to_netcdf(f_combined_event_tseries_netcdf, encoding=encoding, engine = "h5netcdf")

#%% compute event stats
df_mrms_rainevent_summary = pd.read_csv(f_mrms_event_summaries, index_col = 0)
df_mrms_rainevent_tseries = pd.read_csv(f_mrms_event_timeseries, index_col = [0,1], parse_dates=["date_time"])
# aorc
df_aorc_rainevent_summary = pd.read_csv(f_aorc_event_summaries, index_col = 0)
df_aorc_rainevent_tseries = pd.read_csv(f_aorc_event_timeseries, index_col = [0,1], parse_dates=["date_time"])
## ncei
df_ncei_hrly_rainevent_summary = pd.read_csv(f_ncei_hrly_event_summaries, index_col = 0)
df_ncei_hrly_rainevent_tseries = pd.read_csv(f_ncei_hrly_event_timeseries, index_col = [0,1], parse_dates=["date_time"])

lst_time_durations_h_to_analyze = [1, 2, 4, 8, 16, 24]

df_stats_mrms = compute_event_timeseries_statistics(df_mrms_rainevent_tseries.dropna().drop(columns = ["rain_data_source"]), lst_time_durations_h_to_analyze,
                                                    "mm_per_hr", idx_name_time = "date_time", agg_stat = "mean")

df_stats_aorc = compute_event_timeseries_statistics(df_aorc_rainevent_tseries.dropna().drop(columns = ["rain_data_source"]), lst_time_durations_h_to_analyze,
                                                    "mm_per_hr", idx_name_time = "date_time", agg_stat = "mean")

df_stats_ncei_hrly = compute_event_timeseries_statistics(df_ncei_hrly_rainevent_tseries.dropna().drop(columns = ["rain_data_source"]), lst_time_durations_h_to_analyze,
                                                    "mm_per_hr", idx_name_time = "date_time", agg_stat = "mean")

stat = "depth_mm"
fig_title = "Comparison of event totals"
s1 = df_mrms_rainevent_summary[stat]
s1.name = "mrms_{}".format(stat)
s2 = df_ncei_hrly_rainevent_summary[stat]
s2.name = "ncei_hrly_{}".format(stat)
fname_savefig = f"{dir_event_slxn_validing_distributions}mrms_vs_ncei_hrly_{stat}.png"
compare_stats(s1, s2, fig_title, plot = True, fname_savefig=fname_savefig)

s2 = df_aorc_rainevent_summary[stat]
s2.name = "aorc_{}".format(stat)
fname_savefig = f"{dir_event_slxn_validing_distributions}mrms_vs_aorc_{stat}.png"
compare_stats(s1, s2, fig_title, plot = True, fname_savefig=fname_savefig)

# comparing peak 24 hour mean intensity per event
def define_bins(lst_dfs, stat, nbins):
    vals = []
    for df in lst_dfs:
        vals = vals + list(df[stat])
    hist, bin_edges = np.histogram(vals, nbins)
    return bin_edges

# comparing different statistical aggregations
for dur in lst_time_durations_h_to_analyze:
    if dur < 1:
        continue # the min time duration is 1 hour
    stat = "max_{}hr_mean_mm_per_hr".format(dur)
    fig_title = "Comparison of {}".format(stat)
    s1 = df_stats_mrms[stat].dropna()
    s1.name = "mrms_{}".format(stat)
    s2 = df_stats_ncei_hrly[stat].dropna()
    s2.name = "ncei_hrly_{}".format(stat)
    cmv_result, ranksum_result = compare_stats(s1, s2, fig_title, plot = False)
    if (cmv_result == ranksum_result) and (cmv_result == "reject"):
        print(f"According to both the cmv and ranksum tests, {stat} in NCEI and MRMS do not come from the same distribution")
        # continue
    fname_savefig = f"{dir_event_slxn_validing_distributions}mrms_vs_ncei_hrly_{stat}.png"
    compare_stats(s1, s2, fig_title, plot = True, fname_savefig = fname_savefig)

    s2 = df_stats_aorc[stat].dropna()
    s2.name = "aorc_{}".format(stat)
    cmv_result, ranksum_result = compare_stats(s1, s2, fig_title, plot = False)
    if (cmv_result == ranksum_result) and (cmv_result == "reject"):
        print(f"According to both the cmv and ranksum tests, {stat} in AORC and MRMS do not come from the same distribution")
        # continue
    fname_savefig = f"{dir_event_slxn_validing_distributions}mrms_vs_aorc_{stat}.png"
    compare_stats(s1, s2, fig_title, plot = True, fname_savefig = fname_savefig)

#%% investigate quantiles and quantile correlations for each event category
# surge
df_combined_summaries = pd.read_csv(f_combined_event_summaries)
cols_to_drop_for_analysis = ["compound", "rain_event", "surge_event", "rain_data_source", "wlevel_data_source", "event_type", "wlevel_data_source"]

# df_combined_summaries = df_combined_summaries.reset_index()
df_surge_events = df_combined_summaries[df_combined_summaries["event_type"] == "surge"]
df_surge_events = df_surge_events.drop(columns=cols_to_drop_for_analysis)
df_combined_summaries_corrs_of_interest_surge = calculate_quantile_correlations(df_surge_events)

# rain
df_rain_events = df_combined_summaries[df_combined_summaries["event_type"] == "rain"]
df_rain_events = df_rain_events.drop(columns=cols_to_drop_for_analysis)
df_combined_summaries_corrs_of_interest_rain = calculate_quantile_correlations(df_rain_events)

# all
# df_combined_summaries_corrs_of_interest_all = calculate_quantile_correlations(df_combined_summaries)


# just combo
df_combo_events = df_combined_summaries[df_combined_summaries["event_type"] == "compound"]
df_combo_events = df_combo_events.drop(columns=cols_to_drop_for_analysis)
df_combined_summaries_corrs_of_interest_combo = calculate_quantile_correlations(df_combo_events)


############## MANUAL SELECTION OF IMPORTANT RELATIONSHIPS ##############
"""
Process:
- Find the strongest relationship between 1 hour max rainfall intensity and a surge statistic
- Find strongest relationship between 1 hour max rainfall intensity and and time delay statistic
- Find the strongest relationship between total event depth and a surge statistic
- Find the strongest relationship between total event depth and a time delay statistic
- Find strongest relationship between surge peak and a rain statistic
"""
target_rainfall_intensity_variable = "max_4hr_mean_mm_per_hr" # ncei, mrms, and aorc come from same distribution for this variable
#%%******* SURGE EVENTS *******
# figure out key variables for surge events
event_subset = "Threshold-based surge events"
lst_s_selections = []
df_corrs_of_interest = df_combined_summaries_corrs_of_interest_surge

# selecting surge statistics related to max_1hr_mean_mm_per_hr
var_of_interest = target_rainfall_intensity_variable
corrs_of_interest = "surge"
vars_to_filter_out = "after"
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest, vars_to_filter_out)
# print(df_correlations_to_analyze.head())
var1_manal_selection = "surge_peak_ft"
var2_manual_selection = target_rainfall_intensity_variable
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# looking at all time delay statistics related to max 1 hour rainfall intensity
corrs_of_interest = "after"
vars_to_filter_out = ""
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest)
# print(df_correlations_to_analyze.head())
var1_manal_selection = "max_mean_1hrsurge_peak_after_4hrrain_peak_h"
var2_manual_selection = target_rainfall_intensity_variable
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# looking at surge statistics related to precip_depth_mm
var_of_interest = "precip_depth_mm"
corrs_of_interest = "surge"
vars_to_filter_out = "after"
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest, vars_to_filter_out)
# print(df_correlations_to_analyze.head())
var1_manal_selection = "surge_peak_ft"
var2_manual_selection = "precip_depth_mm"
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# looking at time delay statistics related to precip_depth_mm
corrs_of_interest = "after"
vars_to_filter_out = "precip_depth_mm"
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest)
# print(df_correlations_to_analyze.head())
var1_manal_selection = "max_mean_1hrsurge_peak_after_4hrrain_peak_h"
var2_manual_selection = "precip_depth_mm"
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# looking at all rainfall statistics related to surge_peak_ft
var_of_interest =  "surge_peak_ft"
corrs_of_interest = "mm"
vars_to_filter_out = ""
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest)
# print(df_correlations_to_analyze.head())
var1_manal_selection = "surge_peak_ft"
var2_manual_selection = "precip_depth_mm"
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

df_variables_selected = pd.concat(lst_s_selections,axis = 1).T
df_variables_selected.index.names = ["var1", "var2"]
df_variables_selected.sort_values("abs_corr", ascending=False, inplace = True)

print("Variables with most promising correlations for {}".format(event_subset))
df_variables_selected_surge = df_variables_selected
print(df_variables_selected_surge)


#%%******* RAIN EVENTS *******
event_subset = "Threshold-based rain events"
lst_s_selections = []
df_corrs_of_interest = df_combined_summaries_corrs_of_interest_rain

# selecting surge statistics related to max_1hr_mean_mm_per_hr
var_of_interest = target_rainfall_intensity_variable
corrs_of_interest = "surge"
vars_to_filter_out = "after"
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest, vars_to_filter_out)
# print(df_correlations_to_analyze.head())
var1_manal_selection = "max_16hr_mean_surge"
var2_manual_selection = target_rainfall_intensity_variable
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# looking at time delay statistics related to max_1hr_mean_mm_per_hr
corrs_of_interest = "after"
vars_to_filter_out = ""
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest)
# print(df_correlations_to_analyze.head())
# var1_manal_selection = "max_mean_24hrsurge_peak_after_1hrrain_peak_h"
# var2_manual_selection = "max_4hr_mean_mm_per_hr"
# lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)
# variable 2
var1_manal_selection = "max_mean_1hrsurge_peak_after_16hrrain_peak_h"
var2_manual_selection = target_rainfall_intensity_variable
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# looking at surge statistics related to precip_depth_mm
var_of_interest = "precip_depth_mm"
corrs_of_interest = "surge"
vars_to_filter_out = "after"
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest, vars_to_filter_out)
# print(df_correlations_to_analyze.head())
# var1_manal_selection = "max_1hr_mean_surge"
# var2_manual_selection = "precip_depth_mm"
# lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)
# selecting a second variable
var1_manal_selection = "surge_peak_ft"
var2_manual_selection = "precip_depth_mm"
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# looking at time delay statistics related to precip_depth_mm
corrs_of_interest = "after"
vars_to_filter_out = "precip_depth_mm"
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest)
# print(df_correlations_to_analyze.head())
var1_manal_selection = "max_mean_1hrsurge_peak_after_16hrrain_peak_h"
var2_manual_selection = "precip_depth_mm"
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)
# var1_manal_selection = "max_mean_24hrsurge_peak_after_8hrrain_peak_h"
# var2_manual_selection = "precip_depth_mm"
# lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# looking at all statistics related to surge_peak_ft
var_of_interest =  "surge_peak_ft"
corrs_of_interest = "mm"
vars_to_filter_out = ""
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest)
# print(df_correlations_to_analyze.head())
var1_manal_selection = "surge_peak_ft"
var2_manual_selection = "precip_depth_mm"
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)
# second variable
# var1_manal_selection = "surge_peak_ft"
# var2_manual_selection = "max_24hr_mean_mm_per_hr"
# lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)
# third variable
# var1_manal_selection = "surge_peak_ft"
# var2_manual_selection = "precip_depth_mm"
# lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

df_variables_selected = pd.concat(lst_s_selections,axis = 1).T
df_variables_selected.index.names = ["var1", "var2"]
df_variables_selected.sort_values("abs_corr", ascending=False, inplace = True)

print("Variables with most promising correlations for {}".format(event_subset))
df_variables_selected_rain = df_variables_selected
print(df_variables_selected_rain)
#%%******* RAIN AND SURGE EVENTS *******
event_subset = "threshold-based surge AND rain events"
lst_s_selections = []
df_corrs_of_interest = df_combined_summaries_corrs_of_interest_combo

# selecting surge statistics related to max_1hr_mean_mm_per_hr
var_of_interest = target_rainfall_intensity_variable
corrs_of_interest = "surge"
vars_to_filter_out = "after"
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest, vars_to_filter_out)
# print(df_correlations_to_analyze.head())
var1_manal_selection = "surge_peak_ft"
var2_manual_selection = target_rainfall_intensity_variable
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)


# looking at time delay statistics related to max_1hr_mean_mm_per_hr
corrs_of_interest = "after"
vars_to_filter_out = ""
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest)
# print(df_correlations_to_analyze.head())
var1_manal_selection = "max_mean_16hrsurge_peak_after_24hrrain_peak_h"
var2_manual_selection = target_rainfall_intensity_variable
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)
# var1_manal_selection = "max_mean_24hrsurge_peak_after_16hrrain_peak_h"
# var2_manual_selection = target_rainfall_intensity_variable
# lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# looking at surge statistics related to precip_depth_mm
var_of_interest = "precip_depth_mm"
corrs_of_interest = "surge"
vars_to_filter_out = "after"
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest, vars_to_filter_out)
# print(df_correlations_to_analyze.head())
var1_manal_selection = "surge_peak_ft"
var2_manual_selection = "precip_depth_mm"
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# looking at time delay statistics related to precip_depth_mm
corrs_of_interest = "after"
vars_to_filter_out = "precip_depth_mm"
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest)
# print(df_correlations_to_analyze.head())
var1_manal_selection = "max_mean_16hrsurge_peak_after_24hrrain_peak_h"
var2_manual_selection = "precip_depth_mm"
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# looking at all statistics related to surge_peak_ft
var_of_interest =  "surge_peak_ft"
corrs_of_interest = "mm"
vars_to_filter_out = ""
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest)
# print(df_correlations_to_analyze.head())
var1_manal_selection = "surge_peak_ft"
var2_manual_selection = "precip_depth_mm"
lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# looking at all statistics related to surge_peak_ft
corrs_of_interest = "after"
vars_to_filter_out = "surge_peak_ft"
df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest)

df_variables_selected = pd.concat(lst_s_selections,axis = 1).T
df_variables_selected.index.names = ["var1", "var2"]
df_variables_selected.sort_values("abs_corr", ascending=False, inplace = True)

print("Variables with most promising correlations for {}".format(event_subset))
df_variables_selected_surge_and_rain = df_variables_selected
print(df_variables_selected_surge_and_rain)

#%%******* all events ******* (not doing)
# event_subset = "all events"
# lst_s_selections = []
# df_corrs_of_interest = df_combined_summaries_corrs_of_interest_all

# # selecting surge statistics related to max_*_mean_mm_per_hr
# var_of_interest = target_rainfall_intensity_variable
# corrs_of_interest = "surge"
# vars_to_filter_out = "after"
# df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest, vars_to_filter_out)
# # print(df_correlations_to_analyze.head())
# var1_manal_selection = "max_24hr_mean_surge"
# var2_manual_selection = target_rainfall_intensity_variable
# lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# # var1_manal_selection = "max_24hr_mean_surge"
# # var2_manual_selection = target_rainfall_intensity_variable
# # lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# # selecting rain statistics related to max_.*hr_mean_surge
# # var_of_interest = "max_.*hr_mean_surge"
# # corrs_of_interest = "mm_per_hr"
# # vars_to_filter_out = "after"
# # df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest, vars_to_filter_out)
# # # print(df_correlations_to_analyze.head())
# # var1_manal_selection = "max_1hr_mean_surge"
# # var2_manual_selection = target_rainfall_intensity_variable
# # lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# # looking at time delay statistics related to max_1hr_mean_mm_per_hr
# corrs_of_interest = "after"
# vars_to_filter_out = ""
# df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest)
# # print(df_correlations_to_analyze.head())
# var1_manal_selection = "surge_peak_after_rain_peak_h"
# var2_manual_selection = target_rainfall_intensity_variable
# lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)
# # var1_manal_selection = "max_mean_24hrsurge_peak_after_16hrrain_peak_h"
# # var2_manual_selection = target_rainfall_intensity_variable
# # lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# # looking at surge statistics related to precip_depth_mm
# var_of_interest = "precip_depth_mm"
# corrs_of_interest = "surge"
# vars_to_filter_out = "after"
# df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest, vars_to_filter_out)
# # print(df_correlations_to_analyze.head())
# var1_manal_selection = "max_24hr_mean_surge"
# var2_manual_selection = "precip_depth_mm"
# lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# # looking at time delay statistics related to precip_depth_mm (THERE ARE NO RELATIONSHIPS HERE)
# # corrs_of_interest = "after"
# # vars_to_filter_out = "precip_depth_mm"
# # df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest)
# # print(df_correlations_to_analyze.head())
# # # var1_manal_selection = "max_mean_24hrsurge_peak_after_1hrrain_peak_h"
# # # var2_manual_selection = "precip_depth_mm"
# # # lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections)

# # looking at all statistics related to surge_peak_ft
# var_of_interest =  "surge_peak_ft"
# corrs_of_interest = "mm"
# vars_to_filter_out = ""
# df_correlations_to_analyze = analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest)
# # print(df_correlations_to_analyze.head())
# var1_manal_selection = "surge_peak_ft"
# var2_manual_selection = target_rainfall_intensity_variable
# lst_s_selections = append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze)

# df_variables_selected = pd.concat(lst_s_selections,axis = 1).T
# df_variables_selected.index.names = ["var1", "var2"]
# df_variables_selected.sort_values("abs_corr", ascending=False, inplace = True)

# print("Variables with most promising correlations for {}".format(event_subset))
# df_variables_selected_all = df_variables_selected
# print(df_variables_selected_all)

#%% plotting key relationships
## SURGE ONLY
fldr_plots = dir_selecting_maringals+"plots/"
try:
    shutil.rmtree(fldr_plots)
except:
    pass
Path(fldr_plots).mkdir(parents=True, exist_ok=True)
abs_cors = df_variables_selected_surge.abs_corr
var1s = df_variables_selected_surge.reset_index().var1
var2s = df_variables_selected_surge.reset_index().var2
s_savefig_fnames = fldr_plots + "surge_abs_corr_" + abs_cors.round(3).astype(str) + "_" + var1s.values + "." + var2s.values+  ".png"
plot_event_statistic_relationships(df_variables_selected_surge,
                                       df_surge_events, fig_title = "surge_events", s_savefig_fnames=s_savefig_fnames)
df_variables_selected_surge.drop_duplicates().to_csv(f"{dir_selecting_maringals}surge_event_marginals.csv")

## RAIN ONLY
abs_cors = df_variables_selected_rain.abs_corr
var1s = df_variables_selected_rain.reset_index().var1
var2s = df_variables_selected_rain.reset_index().var2
s_savefig_fnames = fldr_plots + "rain_abs_corr_" + abs_cors.round(3).astype(str) + "_" + var1s.values + "." + var2s.values+  ".png"

plot_event_statistic_relationships(df_variables_selected_rain,
                                       df_rain_events, fig_title = "rain_events",s_savefig_fnames=s_savefig_fnames)
df_variables_selected_rain.drop_duplicates().to_csv(f"{dir_selecting_maringals}rain_event_marginals.csv")

# COMPOUND
abs_cors = df_variables_selected_surge_and_rain.abs_corr
var1s = df_variables_selected_surge_and_rain.reset_index().var1
var2s = df_variables_selected_surge_and_rain.reset_index().var2
s_savefig_fnames = fldr_plots + "compound_abs_corr_" + abs_cors.round(3).astype(str) + "_" + var1s.values + "." + var2s.values+  ".png"

plot_event_statistic_relationships(df_variables_selected_surge_and_rain,
                                       df_combo_events, fig_title = "compound_events", s_savefig_fnames=s_savefig_fnames)
df_variables_selected_surge_and_rain.to_csv(f"{dir_selecting_maringals}compound_event_marginals.csv")

#%%  ALL
# abs_cors = df_variables_selected_all.abs_corr
# var1s = df_variables_selected_all.reset_index().var1
# var2s = df_variables_selected_all.reset_index().var2
# s_savefig_fnames = fldr_plots + "all_event_abs_corr_" + abs_cors.round(3).astype(str) + "_" + var1s.values + "." + var2s.values+  ".png"

# plot_event_statistic_relationships(df_variables_selected_all,
#                                        df_combined_summaries, fig_title = "all_events", s_savefig_fnames=s_savefig_fnames)
# df_variables_selected_all.to_csv(f"{dir_selecting_maringals}all_event_marginals.csv")

#%% export dataframe with the selected statistics
