#%% load libraries and directories
import pandas as pd
import sys
import numpy as np
from copulas.multivariate import GaussianMultivariate
from copulas.visualization import compare_3d
import itertools
import matplotlib.pyplot as plt 
import xarray as xr
import geopandas as gpd
from pathlib import Path
from _inputs import def_inputs_for_d
import warnings

yr = 94

f_mrms_event_summaries, f_mrms_event_timeseries, f_water_level_storm_surge, f_realizations, f_key_subnames_gridind, nrealizations, sst_tstep_min, start_date, time_buffer, dir_time_series, wlevel_threshold = def_inputs_for_d()

#%% load data
ds_rlztns = xr.open_dataset(f_realizations)
df_key = pd.read_csv(f_key_subnames_gridind)

df_mrms_event_summaries = pd.read_csv(f_mrms_event_summaries, parse_dates=["start", "end", "max_intensity_tstep"])
df_mrms_event_summaries["duration"] = pd.to_timedelta(df_mrms_event_summaries["duration"])
df_mrms_event_tseries = pd.read_csv(f_mrms_event_timeseries, parse_dates=True, index_col="date_time")
df_water_levels = pd.read_csv(f_water_level_storm_surge, parse_dates=True, index_col="date_time")

#%% extracting sst realization data
if nrealizations < len(ds_rlztns.realization_id.values):
    realization_ids = np.arange(1, nrealizations+1)
    print("Using just {} out of {} available realizations based on user inputs in __utils.py.".format(nrealizations, len(ds_rlztns.realization_id.values)))
else:
    realization_ids = ds_rlztns.realization_id.values

x,y = np.meshgrid(ds_rlztns.longitude.values, ds_rlztns.latitude.values, indexing="ij")
grid_length = x.shape[0] * x.shape[1]
x = x.reshape(grid_length)
y = y.reshape(grid_length)

df_coords = pd.DataFrame({"x_lon":x, "y_lat":y})

# extract the grid IDs that overlap subcatchments
grid_ids = list(df_key.grid_index.unique())
grid_ids.sort()

df_coords = df_coords.loc[grid_ids, :]

rz = 1
strm = 1

lst_storm_mean_tseries = []
keys = []
for rz in realization_ids:
    for strm in ds_rlztns.storm_id:
        idx = dict(realization_id = rz, year = yr, storm_id = strm)
        ds_subset = ds_rlztns.sel(idx)
        strm_tseries = dict()
        for row in df_coords.iterrows():
            mrms_index, coords = row
            loc_idx = dict(latitude = coords.y_lat, longitude = coords.x_lon)
            ds_subset_1loc = ds_subset.sel(loc_idx)
            strm_tseries["grid_ind{}".format(mrms_index)] = ds_subset_1loc.rainrate.values
        df_strm_tseries = pd.DataFrame(strm_tseries)
        # convert negative rainrates to 0
        df_strm_tseries[df_strm_tseries<0] = 0
        s_strm_avg = df_strm_tseries.mean(axis=1)
        s_strm_avg.name = "precip_mm_per_hour"
        lst_storm_mean_tseries.append(s_strm_avg)
        keys.append("{}_{}_{}".format(rz, yr, strm))

df_sst_storms = pd.concat(lst_storm_mean_tseries, keys = keys, names = ["rz_yr_strm", "tstep_ind"])
df_sst_storms = df_sst_storms.reset_index()
df_sst_storms.rz_yr_strm.str.split("_")
df_idx = df_sst_storms.rz_yr_strm.str.split("_", expand=True)
df_idx.columns = ["realization_id", "year", "storm_id"]
# df_sst_storms = df_sst_storms.drop(columns=["rz_yr_strm"])
df_sst_storms = pd.concat([df_idx, df_sst_storms], axis = 1)


#%% trying alternative configuration to speed thigns up
# for row in df_coords.iterrows():
#     mrms_index, coords = row
#     loc_idx = dict(latitude = coords.y_lat, longitude = coords.x_lon)
#     ds_subset_1loc = ds_rlztns.sel(loc_idx)

#%% extract storm surge time series at each event and compute features (e.g., peak time, lag time)
# convert rain time series to same timestep as water levels
## compute timestep of water level data
# s_wl_dates = pd.Series(df_water_levels.index)
# tdif = s_wl_dates.diff().dropna().unique()
# tdif_min = tdif[0].astype('timedelta64[m]').astype(int)
# tstep = "{}min".format(tdif_min)

# if len(tdif) > 1:
#     sys.exit("Script failed. Water level does not have consistent timestep.")

# convert first to 1 min then to the desired timestep
# s_rainfall = df_mrms_event_tseries.mrms_mm_per_hour
# s_rainfall_1min = s_rainfall.resample('1min').mean().fillna(method = 'ffill')
# s_rainfall_trgt_tstep = s_rainfall_1min.resample(tstep).mean()
#%% join water level and time series data
df_water_rain_tseries = df_water_levels.join(df_mrms_event_tseries, how="inner")


#%% compute summary statistics by event for the water levels
s_surge_event_group = df_water_rain_tseries.groupby("event_id").surge_ft
s_peak_surge_time = s_surge_event_group.idxmax().rename("max_surge_tstep")
s_peak_surge = s_surge_event_group.max().rename("max_surge_ft")

df_compound_summary = df_mrms_event_summaries.join(s_peak_surge, on = "event_id")
df_compound_summary = df_compound_summary.join(s_peak_surge_time, on = "event_id")

s_lag_min = (df_compound_summary.max_intensity_tstep - df_compound_summary.max_surge_tstep) / pd.Timedelta(minutes=1)
df_compound_summary["surge_peak_after_rain_peak_min"] = s_lag_min

df_compound_summary["duration_hr"] = df_compound_summary['duration'] / pd.Timedelta("1H")
#%% fit statistical models
# predictors: [duration_hr, depth_mm, mean_mm_per_hour, max_mm_per_hour]
# response: [max_surge_ft, surge_peak_after_rain_peak_min]
# https://scikit-learn.org/stable/modules/preprocessing.html#scaling-features-to-a-range
# vars_all = ["duration_hr", "depth_mm", "mean_mm_per_hr", "max_mm_per_hour", "max_surge_ft", "surge_peak_after_rain_peak_min"]
# vars_cond = ["duration_hr", "depth_mm", "mean_mm_per_hr", "max_mm_per_hour"]
vars_all = ["depth_mm", "mean_mm_per_hr", "max_mm_per_hour", "max_surge_ft", "surge_peak_after_rain_peak_min"]
vars_cond = ["depth_mm", "mean_mm_per_hr", "max_mm_per_hour"]
vars_sim = ["max_surge_ft", "surge_peak_after_rain_peak_min"]

df_vars_all = df_compound_summary.loc[:, vars_all]

def fxn():
    warnings.warn("RuntimeWarning", RuntimeWarning)

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    cop_hydro = GaussianMultivariate()
    cop_hydro.fit(df_vars_all)

#%% compare correlation structure
obs_corr = df_vars_all.corr()
cop_corr = pd.DataFrame(cop_hydro.to_dict()["covariance"], columns = df_vars_all.corr().columns, index = df_vars_all.corr().index)

dfs = [obs_corr, cop_corr]



#%% using heatmapz library
from heatmap import heatmap, corrplot

corrplot(obs_corr)
corrplot(cop_corr)

corrplot(cop_corr-obs_corr)


#%% functions from https://towardsdatascience.com/better-heatmaps-and-correlation-matrix-plots-in-python-41445d0f2bec
# import seaborn as sns
# n_colors = 256 # Use 256 colors for the diverging color palette
# palette = sns.diverging_palette(20, 220, n=n_colors) # Create the palette
# color_min, color_max = [-1, 1] # Range of values that will be mapped to the palette, i.e. min and max possible correlation

# def value_to_color(val):
#     val_position = float((val - color_min)) / (color_max - color_min) # position of value in the input range, relative to the length of the input range
#     ind = int(val_position * (n_colors - 1)) # target index in the color palette
#     return palette[ind]

# def heatmap(x, y, size):
#     fig, ax = plt.subplots()
    
#     # Mapping from column names to integer coordinates
#     x_labels = [v for v in sorted(x.unique())]
#     y_labels = [v for v in sorted(y.unique())]
#     x_to_num = {p[1]:p[0] for p in enumerate(x_labels)} 
#     y_to_num = {p[1]:p[0] for p in enumerate(y_labels)} 
    
#     size_scale = 500
#     # ax.scatter(
#     #     x=x.map(x_to_num), # Use mapping for x
#     #     y=y.map(y_to_num), # Use mapping for y
#     #     s=size * size_scale, # Vector of square sizes, proportional to size parameter
#     #     marker='s' # Use square as scatterplot marker
#     # )
#     ax.scatter(
#         x=x.map(x_to_num),
#         y=y.map(y_to_num),
#         s=size * size_scale,
#         c=color.apply(value_to_color), # Vector of square color values, mapped to color palette
#         marker='s'
#     )
    
#     # Show column labels on the axes
#     ax.set_xticks([x_to_num[v] for v in x_labels])
#     ax.set_xticklabels(x_labels, rotation=45, horizontalalignment='right')
#     ax.set_yticks([y_to_num[v] for v in y_labels])
#     ax.set_yticklabels(y_labels)

#     ax.grid(False, 'major')
#     ax.grid(True, 'minor')
#     ax.set_xticks([t + 0.5 for t in ax.get_xticks()], minor=True)
#     ax.set_yticks([t + 0.5 for t in ax.get_yticks()], minor=True)

#     ax.set_xlim([-0.5, max([v for v in x_to_num.values()]) + 0.5]) 
#     ax.set_ylim([-0.5, max([v for v in y_to_num.values()]) + 0.5])





# #%% creating plots
# corr = pd.melt(obs_corr.reset_index(), id_vars='index') # Unpivot the dataframe, so we can get pair of arrays for x and y
# corr.columns = ['x', 'y', 'value']
# heatmap(
#     x=corr['x'],
#     y=corr['y'],
#     size=corr['value'].abs()
# )


#%% define functions
def scatter_3d(data, fig=None, title=None, position=None):
    """Plot 3 dimensional data in a scatter plot."""
    fig = fig or plt.figure()
    # BEGIN WORK
    # fig = plt.figure(figsize=(10,4))    
    # END WORK
    position = position or 111

    # ax = plt.subplot(position, projection='3d', kwargs = dict(figure = fig, xlabel = data.columns[0], ylabel = data.columns[1]))
    ax = fig.add_subplot(position, projection='3d')
    ax.scatter(*(
        data[column]
        for column in data.columns
    ))
    # ax.set_xlabel = data.columns[0]
    # ax.set_ylabel = data.columns[1]
    # ax.set_zlabel = data.columns[2]
    ax.set_xlabel(data.columns[0])
    ax.set_ylabel(data.columns[1])
    ax.set_zlabel(data.columns[2])
    if title:
        ax.set_title(title)
        ax.title.set_position([.5, 1.05])
    return ax

def plot_snth_vs_real(df_real, df_synth, col_names_for3d):
    for cols in col_names_for3d:
        columns = list(cols)
        fig = plt.figure(figsize=(10,4))

        ax = scatter_3d(df_real[columns], fig=fig, title='Real Data', position=121)
        ax = scatter_3d(df_synth[columns], fig=fig, title='Synthetic Data', position=122)

        plt.tight_layout()
    # compare_3d(df_vars_all, df_synth_hydro, columns = list(cols))

def gen_conditioned_samples(cop, df_cond, n_samples):
    cond_vars = list(df_cond.columns)
    d_cond = dict()

    lst_df_samps = []
    for i in np.arange(n_samples):
        d_cond = dict(df_cond.loc[i, :])
        synth_cond = cop.sample(num_rows = 1, conditions = d_cond)
        lst_df_samps.append(synth_cond)
        
    df_synth_cond = pd.concat(lst_df_samps, ignore_index=True)
    return df_synth_cond

def plot_hist_of_each_var(df_real, df_synth):
    fig = plt.figure()
    position = 111
    # for col in df_real.columns:
    return

def comp_hists(var, df_obs, df_synth):
    fig, axes = plt.subplots(1, 2, figsize = (10,4))
    (n, bins, patches) = axes[0].hist(df_obs.loc[:, var], bins = 20, label="Observed")
    axes[0].set_title("Observed")
    axes[1].hist(df_synth.loc[:, var], bins = bins, label = "Synthetic")
    axes[1].set_title("Synthetic")

    max_y = -9999
    for ax in axes:
        ylim = ax.get_ylim()
        max_y = max([max_y, max(ylim)])

    for ax in axes:
        # ax.set_ylim(0,max_y)
        ax.set_xlabel(var)
        ax.set_ylabel('count')
        ax.set_title(ax.get_label(), fontfamily='serif', loc='left', fontsize='medium')

    plt.tight_layout()
    return
#%% defining df_cond using the sst data
vars_cond
df_sst_storms
# df_sst_storms_grp = df_sst_storms.groupby("rz_yr_strm")

durations = []
depths = []
mean_int = []
max_int = []
lst_tstep_max = []
lst_tstep_last = []
lst_no_rain = []

event_id = -1
for rz_yr_strm in df_sst_storms.rz_yr_strm.unique():
    event_id += 1
    rain = True
    df_subset = df_sst_storms[df_sst_storms.rz_yr_strm == rz_yr_strm]
    # check if there is rain
    if df_subset.precip_mm_per_hour.sum() == 0:
        rain = False
        durations.append(np.nan)
        depths.append(np.nan)
        mean_int.append(np.nan)
        max_int.append(np.nan)
        lst_tstep_max.append(np.nan)
        lst_tstep_last.append(np.nan)
        lst_no_rain.append(rain)
        continue
        # sys.exit()
    lst_no_rain.append(rain)
    # compute duration in hours
    non_zero_indices = df_subset[df_subset.precip_mm_per_hour != 0].index
    first_tstep_with_rain = non_zero_indices[0]
    last_tstep_with_rain = non_zero_indices[-1]
    df_subset = df_subset.loc[first_tstep_with_rain:last_tstep_with_rain, :]
    df_subset.reset_index(drop=True, inplace = True)
    durations.append(len(df_subset))
    # compute total depth
    depths.append(df_subset.precip_mm_per_hour.sum())
    # compute mean intensity
    mean_int.append(df_subset.precip_mm_per_hour.mean())
    # compute peak intensity
    max_int.append(df_subset.precip_mm_per_hour.max())
    # compute timing of peak
    tstep_max_ind = df_subset.precip_mm_per_hour.idxmax()
    tstep_max = pd.to_datetime(start_date) + pd.Timedelta(tstep_max_ind*sst_tstep_min, "minutes")
    lst_tstep_max.append(tstep_max)
    # compute last tstep
    lst_tstep_last.append(pd.to_datetime(start_date) + pd.Timedelta(len(df_subset)*sst_tstep_min, "minutes"))

df_sst_storm_summaries = pd.DataFrame(dict(rz_yr_strm = df_sst_storms.rz_yr_strm.unique(), 
                              duration_hr = durations, depth_mm = depths, 
                              mean_mm_per_hr = mean_int, max_mm_per_hour = max_int, rain_in_sst_tseries = lst_no_rain,
                              tstep_of_max_intensity = lst_tstep_max, last_timestep_w_rainfall = lst_tstep_last))

# df_sst_storm_summaries[df_sst_storm_summaries.max_mm_per_hour < 0] = np.nan

#%% generating synthetic data with conditions
df_cond = df_sst_storm_summaries.loc[:, vars_cond].dropna()
n_samples = len(df_cond)
df_synth_hydro_cond = gen_conditioned_samples(cop_hydro, df_cond, n_samples)

# if the lag is excessively high or low, shift it to be within the accepted range
idx_of_excessive_high_lag = df_synth_hydro_cond.surge_peak_after_rain_peak_min > 24 * 60
idx_of_excessive_low_lag = df_synth_hydro_cond.surge_peak_after_rain_peak_min < 24 * 60 * -1

for ind in np.arange(len(df_synth_hydro_cond)):
    if idx_of_excessive_high_lag[ind] or idx_of_excessive_low_lag[ind]:
        df_synth_hydro_cond.loc[ind, "surge_peak_after_rain_peak_min"] = np.random.uniform(0,24*60)

#%% plot synthetically generated data
# define columns names
col_names_for3d = []
for cols in list(itertools.combinations(vars_all,3)):
    sim_var_in_combo = False
    for var in vars_sim:
        if var in cols:
            sim_var_in_combo = True
    if sim_var_in_combo:
        col_names_for3d.append(cols)

plot_snth_vs_real(df_vars_all, df_synth_hydro_cond, col_names_for3d)
#%%
plot_snth_vs_real(df_vars_all, cop_hydro.sample(len(df_vars_all)), col_names_for3d)
#%% plot histograms
for var in vars_sim:
    comp_hists(var, df_vars_all, df_synth_hydro_cond)

#%%
for var in vars_sim:
    comp_hists(var, df_vars_all, cop_hydro.sample(len(df_vars_all)))

#%% generate water level time series from synthetic values
df_water_rain_tseries
df_vars_all
df_cond

df_sims = df_synth_hydro_cond.loc[:, vars_sim]

#%% using K means to identify time series to use
# from sklearn.cluster import KMeans
# kmeans = KMeans(n_clusters=8, random_state=0, n_init="auto")

#%% determine number of clusters
# https://www.w3schools.com/python/python_ml_k-means.asp
from sklearn.cluster import KMeans
from sklearn import preprocessing
vars_k = ["depth_mm", "max_mm_per_hour", "max_surge_ft"]
df_vars_stormclass = df_compound_summary.loc[:, vars_k]

df_vars_stormclass_scaler = preprocessing.StandardScaler().fit(df_vars_stormclass)
df_vars_stormclass_scaled = df_vars_stormclass_scaler.transform(df_vars_stormclass)

inertias = []

ks_to_try = 20

for i in range(1,ks_to_try):
    kmeans = KMeans(n_clusters=i)
    kmeans.fit(df_vars_stormclass_scaled)
    inertias.append(kmeans.inertia_)

plt.plot(range(1,ks_to_try), inertias, marker='o')
plt.title('Elbow method')
plt.xlabel('Number of clusters')
plt.ylabel('Inertia')
plt.show()

n_clusters = 5

kmeans = KMeans(n_clusters=n_clusters)
kmeans.fit(df_vars_stormclass_scaled)

#%%
# plt.scatter(x = df_vars_all.max_surge_ft, y= df_vars_all.duration_hr, c=kmeans.labels_)
# plt.show()

for v_sim in vars_sim:
    fig, axes = plt.subplots(2, 2, figsize = (10,8))
    row = 0
    col = 0
    for v_cond in vars_cond:
        axes[row, col].scatter(df_vars_all[v_sim], df_vars_all[v_cond], c=kmeans.labels_)
        axes[row, col].set_xlabel(v_sim)
        axes[row, col].set_ylabel(v_cond)
        col += 1
        if col > 1:
            row += 1
            col = 0

#%% predict k label of synthetic data
df_synth_hydro_cond_scaled = df_vars_stormclass_scaler.transform(df_synth_hydro_cond.loc[:, vars_k])

pred_ks = kmeans.predict(df_synth_hydro_cond_scaled)
obs_ks = kmeans.labels_
np.unique(obs_ks, return_counts = True)

# randomly select another event with the same category
def get_storm_to_rescale(pred_k):
    ind_obs_same_class = np.where(obs_ks==pred_k)[0]
    obs_event_index = np.random.choice(ind_obs_same_class)
    obs_event_id = df_compound_summary.event_id[obs_event_index] # necessary because the event IDs are 1-indexed
    return obs_event_id

#%% create synthetic time series
# df_water_levels
# prelim calcs
wlevel_tdiff = (pd.Series(df_water_rain_tseries.index).diff().dropna().mode())[0]

wlevel_freq = pd.tseries.frequencies.to_offset(wlevel_tdiff).freqstr

# run loop
min_obs_wlevel = df_water_levels.water_level.min()
max_obs_wlevel = df_water_levels.water_level.max()

i = -1
lst_s_wlevel_tseries = []
lst_keys = []
source_event_id = []
min_sim_wlevels = []
max_sim_wlevels = []
lst_event_starts = []
lst_event_ends = []
lst_event_durations = []
lst_peak_surge_tsteps = []


        

for ind, s_sim_event_summary in df_synth_hydro_cond.iterrows():
    absurd_simulation = True
    i += 1
    while absurd_simulation == True:
        # compute predicted
        s_sim_event_summary_scaled = df_vars_stormclass_scaler.transform(pd.DataFrame(s_sim_event_summary.loc[vars_k]).T)
        pred_k = kmeans.predict(s_sim_event_summary_scaled)
        obs_event_id = get_storm_to_rescale(pred_k)
        source_event_id.append(obs_event_id)
        df_obs_event_tseries = df_water_rain_tseries[df_water_rain_tseries.event_id == obs_event_id]
        df_obs_event_summary = df_compound_summary.loc[df_compound_summary.event_id == obs_event_id, vars_all]
        # df_sst_strm_summary = df_sst_storm_summaries.loc[i, :]

        # sim_idx_max = df_sim_tseries.idxmax()
        # print("length: {}, idx_max: {}".format(len(df_sim_tseries), sim_idx_max))

        # compute timestep of peak storm surge
        sim_tstep_max_int = df_sst_storm_summaries.tstep_of_max_intensity[i]
        sim_tstep_max_surge = sim_tstep_max_int + pd.Timedelta(s_sim_event_summary["surge_peak_after_rain_peak_min"], "minutes")
        # round to the closest timestep
        sim_tstep_max_surge = sim_tstep_max_surge.round(wlevel_freq)
        # calculate the start of the event
        event_starttime = min(pd.to_datetime(start_date), sim_tstep_max_surge-pd.Timedelta(time_buffer, "hours"))
        # event_endtime =max(pd.to_datetime(start_date) + pd.Timedelta(s_sim_event_summary["duration_hr"], "hr"), sim_tstep_max_surge+pd.Timedelta(time_buffer, "hours"))
        # end time is either the time of the last tstep or the time of peak surge
        sim_tstep_lastrain = df_sst_storm_summaries.last_timestep_w_rainfall[i]
        event_endtime = max(sim_tstep_lastrain, sim_tstep_max_surge+pd.Timedelta(time_buffer, "hours"))

        # duration is start minus end
        duration = event_endtime - event_starttime

        
        sim_wlevel_times = pd.date_range(event_starttime, event_endtime, freq=wlevel_freq)

        time_to_peak_surge = sim_tstep_max_surge - min(sim_wlevel_times)
        tstep_peak = event_starttime + time_to_peak_surge

        # extract observed surge data
        obs_tstep_max_surge = df_obs_event_tseries.surge_ft.idxmax()
        obs_start_time = obs_tstep_max_surge - time_to_peak_surge
        obs_end_time = obs_start_time + duration
        obs_surge_times = pd.date_range(obs_start_time, obs_end_time, freq=wlevel_freq)
        obs_surges = df_water_levels.surge_ft.loc[obs_surge_times]
        obs_peak_tstep = obs_surges.index[0]+time_to_peak_surge
        obs_peak = obs_surges[obs_peak_tstep]


        # add predicted water level with a random shift of plus or minus 12 hours
        tide_shift = pd.Timedelta(np.random.choice(np.arange(-12, 12+1)), "hr")
        s_tides_times = pd.date_range(obs_start_time+tide_shift, obs_end_time+tide_shift, freq=wlevel_freq)
        s_tides = df_water_levels.predicted_wl.loc[s_tides_times]
        s_tides.index = sim_wlevel_times

        # rescaling
        ## compute multiplier
        obs_frac_of_max_tseries = obs_surges / obs_peak # unit surge as fraction of the maximum
        # rescaling
        s_sim_surge_tseries = (s_sim_event_summary["max_surge_ft"] * obs_frac_of_max_tseries).reset_index(drop=True)
        s_sim_surge_tseries.index = sim_wlevel_times
        
        # adding tide
        s_sim_wlevel = s_sim_surge_tseries + s_tides
        s_sim_wlevel.name = "water_level_ft"

        min_sim_wlevel = s_sim_wlevel.min()
        max_sim_wlevel = s_sim_wlevel.max()

        if (max_sim_wlevel < (1+wlevel_threshold)*max_obs_wlevel) and (min_sim_wlevel > (1+wlevel_threshold)*min_obs_wlevel):
            absurd_simulation = False
        else:
            print("Absurd simulation encountered. Max simulated water level = {}; Min simulated water level = {}. Resampling from observed events...".format(max_sim_wlevel, min_sim_wlevel))

    min_sim_wlevels.append(min_sim_wlevel)
    max_sim_wlevels.append(max_sim_wlevel)
    lst_event_starts.append(event_starttime)
    lst_event_ends.append(event_endtime)
    lst_event_durations.append(duration)
    lst_peak_surge_tsteps.append(sim_tstep_max_surge)

    # writing to a file
    ## id the realization, and storm
    rz, yr, strm = df_sst_storm_summaries.rz_yr_strm.loc[i].split("_")

    f_out = dir_time_series + "weather_realization{}/year{}/_waterlevel_rz{}_yr{}_strm{}.dat".format(rz, yr, rz, yr, strm)

    # create data frame with proper formatting to be read in SWMM
    df = pd.DataFrame(dict(date = s_sim_wlevel.index.strftime('%m/%d/%Y'),
                 time = s_sim_wlevel.index.time,
                 water_level = s_sim_wlevel.values))

    Path(f_out).parent.mkdir(parents=True, exist_ok=True)

    with open(f_out, "w+") as file:
        file.write(";;synthetic water level\n")
        file.write(";;Water Level (ft)\n")
    df.to_csv(f_out, sep = '\t', index = False, header = False, mode="a")


#%% export event summaries

df_idx = df_sst_storm_summaries.rz_yr_strm.str.split("_", expand=True)
df_idx.columns = ["realization_id", "year", "storm_id"]

df_simulated_event_summaries = pd.DataFrame(dict(min_sim_wlevel = min_sim_wlevels,max_sim_wlevel = max_sim_wlevels,
                                                 event_start = lst_event_starts, event_end = lst_event_ends,
                                                 event_duration_hr = lst_event_durations, tstep_peak_surge = lst_peak_surge_tsteps))

df_sim_summary = df_idx.join(df_simulated_event_summaries, how="outer")
df_sim_summary = df_sim_summary.join(df_sst_storm_summaries, how="outer")

df_sim_summary = df_sim_summary.join(df_synth_hydro_cond.surge_peak_after_rain_peak_min, how = "outer")

df_sim_summary = df_sim_summary.rename(columns=dict(tstep_of_max_intensity = "tstep_max_rain_intensity",
                                                    duration_hr = "rainfall_duration_hr"))

df_sim_summary.drop(columns=["rz_yr_strm"], inplace=True)

f_summary = dir_time_series + "_event_summary_year{}.csv".format(yr)
df_sim_summary.to_csv(f_summary, index=False)
