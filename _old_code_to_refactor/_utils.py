#%%
import pandas as pd
import sys
import numpy as np
from _inputs import *
import xarray as xr
from scipy import stats
from matplotlib import pyplot as plt
from datetime import datetime
import matplotlib.patches as mpatches
import seaborn as sns
from __ref_ams_functions import *
import textwrap
from matplotlib.lines import Line2D
from scipy.stats import gaussian_kde, kendalltau
import shutil
##% storm rescaling functions

def define_zarr_compression(ds, clevel=5):
    import zarr
    encoding = {}
    for da_name in ds.data_vars:
        encoding[da_name] = {"compressor": zarr.Blosc(cname="zstd", clevel=clevel, shuffle=zarr.Blosc.SHUFFLE)}
    return encoding

def diff_btwn_sim_tseries_depth_and_sim_smry_depth(sim_time_series, target_sim_depth, tstep_hr):
    sim_precip_depth_mm = (sim_time_series["mm_per_hr"] * tstep_hr).sum()
    return abs(target_sim_depth - sim_precip_depth_mm)

# def calculate_optimal_tstep(sim_time_series, target_sim_depth):
#     # Compute the sum of the time series
#     sim_sum = sim_time_series.sum() # sum of all intensities (mm per hour per 5 minute timestep)
#     # Calculate the optimal tstep_hr
#     tstep_hr = target_sim_depth / sim_sum
#     return tstep_hr

def rescale_rain_tseries_to_match_target_depth(sim_time_series, target_sim_depth, target_tstep_tdelta):
    og_index_name = sim_time_series.index.name
    # new_tstep_hr = calculate_optimal_tstep(sim_time_series["mm_per_hr"], target_sim_depth)
    # current_sim_depth = sim_time_series["mm_per_hr"].mean() * n_current_hours
    # how would we have to modify n_current_hours such that the result matches the target sim depth?
    # n_hours = target_sim_depth / sim_time_series["mm_per_hr"].mean()

    new_tstep_hr = target_sim_depth / sim_time_series["mm_per_hr"].sum()
    sim_precip_depth_mm = (sim_time_series["mm_per_hr"] * new_tstep_hr).sum()
    if not np.isclose(sim_precip_depth_mm, target_sim_depth):
        sys.exit(F"ERROR: TOTAL PRECIP DEPTH FROM SIMULATED EVENT TIME SERIES DOES NOT EQUAL THE SIMULATED EVENT STATISTIC")
    # reindex the time series with the new timestep
    new_idx = pd.timedelta_range(start = "0 hours", periods = len(sim_time_series),
                                freq=pd.Timedelta(hours=new_tstep_hr))
    sim_time_series.index = new_idx
    # now re-index at the original time delta using a time-weighted average intensity
    idx_back_to_typical = pd.timedelta_range(start='0 hours', end=new_idx[-1], freq=target_tstep_tdelta)
    idx_back_to_typical.name = "start_time"
    df_idx_back_to_typical = idx_back_to_typical.to_frame().reset_index(drop = True)
    df_idx_back_to_typical["end_time"] = df_idx_back_to_typical["start_time"] + target_tstep_tdelta

    # initialize list for final calculated precip values
    lst_precip_values = []
    # place the indices into a dataframe representing each tsteps coverage for easy indexing
    new_idx.name = "start_time"
    df_new_idx = new_idx.to_frame().reset_index(drop = True)
    df_new_idx["end_time"] = df_new_idx["start_time"] + pd.Timedelta(hours=new_tstep_hr)
    for idx_idx, idx_tdelta in enumerate(idx_back_to_typical):
        row_target_idx = df_idx_back_to_typical.iloc[idx_idx]
        # find overlapping indices from the re-indexed dataset that overlap with the target index
        fltr_ovrlp_start = (row_target_idx["start_time"] <= df_new_idx["end_time"])
        fltr_ovrlp_end = (row_target_idx["end_time"] >= df_new_idx["start_time"])
        df_overlapping_irreg_indices = df_new_idx[fltr_ovrlp_start & fltr_ovrlp_end]
        if len(df_overlapping_irreg_indices) == 1:
            weighted_val = sim_time_series.loc[df_overlapping_irreg_indices.iloc[0]["start_time"], :]
            weighted_val.name = idx_tdelta
        elif len(df_overlapping_irreg_indices) > 1:    
            # define end of time window for calculating time weighted value
            time_window_end = row_target_idx["end_time"]
            # initialize time weighted average rainfall intensity
            weighted_val = pd.Series(data=0, index = sim_time_series.columns).astype(float)
            # compute weighted value based on each overlapping timestep
            for irreg_idx in df_overlapping_irreg_indices["start_time"][::-1]:
                # find duration of overlap with the target regular tstep
                dur_overlap = (time_window_end - max(irreg_idx, row_target_idx["start_time"])) 
                # compute the weighting as frac of overlap over the target tstep
                weight = dur_overlap / target_tstep_tdelta
                # compute the weighted value and sum it with previous results
                weighted_val += weight * sim_time_series.loc[irreg_idx, :]
                # print(f"dur_overlap: {dur_overlap}| weight: {weight:.2f} | weighted_val: {weighted_val:.2f} | time_window_end: {time_window_end}")
                time_window_end -= dur_overlap # this is just relevant for the print statement
            weighted_val.name = idx_tdelta
        lst_precip_values.append(weighted_val)
    # update precip depth time series
    # updated_idx = pd.timedelta_range(start='0 hours', periods=len(lst_precip_values), freq=target_tstep_tdelta)
    # sim_time_series = pd.DataFrame({obs_tseries_mm_p_h.columns[0]:lst_precip_values}, index = updated_idx)
    sim_time_series = pd.concat(lst_precip_values, axis = 1).T
    sim_time_series.index.name = og_index_name
    return sim_time_series

def compute_sim_max_mean_rain_intensity(sim_time_series, rain_intensity_stat):
    dur = int(rain_intensity_stat.split("_")[1].split("hr")[0])
    df_rain_summaries = compute_event_timeseries_statistics(sim_time_series, lst_time_durations_h_to_analyze=[dur],
                                                            varname=sim_time_series.columns[0], agg_stat = "mean")
    # first rescale to match targeted intensity
    sim_max_intensity = df_rain_summaries.loc[sim_time_series.columns[0], rain_intensity_stat]
    return sim_max_intensity

def rescale_rainfall_timeseries(row_sim_event, obs_tseries_mm_p_h, max_obs_strm_duration, n_cutoff, rtol, atol, rain_intensity_stat, verbose = False):
    # row_sim_event, obs_tseries_mm_p_h, max_obs_strm_duration, n_cutoff, rtol, atol, verbose = False
    if verbose:
        print(f"Rescaling observed rainfall time series match targeted event stats with a relative tolerance of {rtol} and an absolute tolerance of {atol}")
    target_tstep_tdelta = tstep_tdelta = pd.Series(obs_tseries_mm_p_h.index.diff()).mode().iloc[0]
    target_sim_intensity = row_sim_event[rain_intensity_stat]
    target_sim_depth = row_sim_event["precip_depth_mm"]
    tstep_hr = tstep_tdelta / np.timedelta64(1, "h")
    sim_time_series = obs_tseries_mm_p_h.copy()
    threshold_unmet = True
    n_attempts = 0
    txt_problems = ""
    cols_gridcell = [col for col in sim_time_series.columns if 'mm_per_hr' not in col]
    while threshold_unmet:
        n_attempts += 1
        # first compute the max intensity statistic (rain_intensity_stat)
        sim_max_intensity = compute_sim_max_mean_rain_intensity(sim_time_series["mm_per_hr"].to_frame(), rain_intensity_stat)
        mltplier = target_sim_intensity / sim_max_intensity
        sim_time_series = sim_time_series * mltplier
        # recalc watershed wide mean intensity
        sim_time_series["mm_per_hr"]  = sim_time_series.loc[:, cols_gridcell].mean(axis = 1)
        # recompute max intensity to perform check
        sim_max_intensity = compute_sim_max_mean_rain_intensity(sim_time_series["mm_per_hr"].to_frame(), rain_intensity_stat)
        # check to make sure target intensity is met
        if not np.isclose(sim_max_intensity, target_sim_intensity):
            continue
            sys.exit(F"ERROR: {rain_intensity_stat} FROM SIMULATED EVENT TIME SERIES DOES NOT EQUAL THE SIMULATED EVENT STATISTIC")
        # sys.exit("work")
        # modify timesteps to achieve desired precip depth
        sim_time_series = rescale_rain_tseries_to_match_target_depth(sim_time_series, target_sim_depth, target_tstep_tdelta)
        if (sim_time_series.index.diff().dropna() != tstep_tdelta).sum() > 0:
            sys.exit("irregular timestep encountered")
        # recalculate watershed wide mean intensity
        sim_time_series["mm_per_hr"]  = sim_time_series.loc[:, cols_gridcell].mean(axis = 1)
        # re-verify precip statistics
        sim_precip_depth_mm = (sim_time_series["mm_per_hr"] * tstep_hr).sum()
        # recompute max intensity after scaling
        sim_max_intensity = compute_sim_max_mean_rain_intensity(sim_time_series["mm_per_hr"].to_frame(), rain_intensity_stat)
        if n_attempts == n_cutoff:
            threshold_unmet = False
            txt_problems = f"Cutting off for loop after {n_cutoff} attempts while there is still a difference between simulated max intensity and target max intensity (sim. - target: {sim_max_intensity:.4f} - {target_sim_intensity:.4f} = {sim_max_intensity-target_sim_intensity:.4f})"
            if verbose:
                print(txt_problems)
            break
        elif sim_time_series.index.max() >= max_obs_strm_duration*2: # cutoff if the duration is too long
            txt_problems = f"|WARNING: breaking rescaling loop because simulated rainfall tseries already double the max observed storm length ({sim_time_series.index.max()} vs. {max_obs_strm_duration})"
            if verbose:
                print(txt_problems)
            break
        elif not np.isclose(sim_precip_depth_mm, target_sim_depth, rtol = rtol, atol = atol): 
            # sys.exit(F"ERROR: {rain_stat} FROM SIMULATED EVENT TIME SERIES DOES NOT EQUAL THE SIMULATED EVENT STATISTIC")
            if verbose:
                print(f"The precip depth statistic is not close enough (sim. - target: {sim_precip_depth_mm:.4f} - {target_sim_depth:.4f} = {(sim_precip_depth_mm-target_sim_depth):.4f})")
            continue
        # sim_time_series = obs_tseries_surge * mltplier
        elif not np.isclose(sim_max_intensity, target_sim_intensity, rtol = rtol, atol = atol):
            # sys.exit(F"ERROR: {rain_stat} FROM SIMULATED EVENT TIME SERIES DOES NOT EQUAL THE SIMULATED EVENT STATISTIC")
            if verbose:
                print(f"The max intensity statistic is not close enough (sim. - target: {sim_max_intensity:.4f} - {target_sim_intensity:.4f} = {(sim_max_intensity-target_sim_intensity):.4f})")
            continue
        else:
            # if n_attempts > 1:
            if verbose:
                print(f"Observed event successfully rescaled to match target event statistics after {n_attempts} iterations")
            threshold_unmet = False
            break
    return sim_time_series, txt_problems

def return_df_of_neighbors_w_rank_weighted_slxn_prob(df_obs_target_source, df_sim_smry, data_cols, n_neighbors):
    from sklearn.neighbors import NearestNeighbors
    from sklearn.preprocessing import StandardScaler
    og_idx_df_obs_target_source = df_obs_target_source.index
    df_obs_target_source.reset_index(drop = True, inplace = True)
    s_obs_event_index_lookup = pd.Series(data = og_idx_df_obs_target_source, index = df_obs_target_source.index)
    s_obs_event_index_lookup.name = 'obs_storm_id'
    # subset the relevant statistics
    df_obs_smry_1hydro = df_obs_target_source[data_cols]
    df_sim_smry_1hydro = df_sim_smry[data_cols]
    scaler = StandardScaler()
    scaler.fit(df_obs_smry_1hydro)
    # scale the data
    df_obs_smry_1hydro_scaled = scaler.transform(df_obs_smry_1hydro)
    df_sim_smry_1hydro_scaled = scaler.transform(df_sim_smry_1hydro)
    # Initialize NearestNeighbors model
    nbrs = NearestNeighbors(n_neighbors=n_neighbors, algorithm='auto')
    # Fit the model using df_obs_smry_1hydro_scaled
    nbrs.fit(df_obs_smry_1hydro_scaled)
    # Find the nearest neighbors for each row in df_sim_smry_1hydro_scaled
    distances, indices = nbrs.kneighbors(df_sim_smry_1hydro_scaled)
    # define rank based weighting for sampling from nearest neighbors
    s_weighting_numerator = pd.Series(1, index = np.arange(1, n_neighbors+1)) # index is the rank
    s_weighting_numerator = s_weighting_numerator / s_weighting_numerator.index
    s_weighting = s_weighting_numerator / s_weighting_numerator.sum()

    lst_s_neighbors = []
    for col_name, col_data in pd.DataFrame(indices).items():
        # break
        new_col_name = f"nbr_{col_name}"
        s_nearest_obs_event = pd.merge(col_data.to_frame(), s_obs_event_index_lookup,
                                                left_on=col_name, right_index=True)["obs_storm_id"]
        s_nearest_obs_event.name = new_col_name
        lst_s_neighbors.append(s_nearest_obs_event)
    df_neighbors = pd.concat(lst_s_neighbors, axis = 1)

    # re-order the observed event ids in each row using weighted rank-based sampling
    df_neighbors = df_neighbors.apply(lambda row: pd.Series(row.sample(n=n_neighbors, replace = False, weights = s_weighting.values).values, index=df_neighbors.columns), axis=1)
    df_neighbors.index = df_sim_smry.index
    return df_neighbors

def retrieve_obs_and_sim_info_for_rescaling(data_cols, df_neighbors, sim_event_id, n_rescaling_attempts, df_all_events_summary, df_sim_smry):
    # define obs event id for trial
    row_obs_neighbors = df_neighbors.loc[sim_event_id,:]
    obs_event_id = row_obs_neighbors.iloc[n_rescaling_attempts]
    
    # extract event stats
    event_start = df_all_events_summary.loc[obs_event_id,'event_start']
    event_end = df_all_events_summary.loc[obs_event_id,'event_end']
    og_event_category = df_all_events_summary.loc[obs_event_id,'event_type']
    row_obs_event = df_all_events_summary.loc[obs_event_id, ]

    nbr_id = row_obs_neighbors[row_obs_neighbors == obs_event_id].index[0]
    row_sim_event = df_sim_smry.loc[sim_event_id, data_cols]
    return nbr_id, event_start, event_end, obs_event_id, row_obs_event, row_sim_event

def lookup_obs_surge_event_start(row_surge_event, arbirary_start_date, s_tseries_sim_surge):
    # if the event was not shifted or was shifted backward, obs start is unchanged (since we are only filling in trailing timesteps)
    if s_tseries_sim_surge.dropna().index.min() <= arbirary_start_date:
        obs_start = row_surge_event["obs_event_start_surge"]
    else: # if the event was shifted FORWARD, need to retrieve previous timesteps;
        obs_start = row_surge_event["obs_event_start_surge"] - (s_tseries_sim_surge.dropna().index.min() - arbirary_start_date)
    return obs_start

def generate_randomized_tide_component(s_tseries_sim_surge, ds_6min_water_levels_mrms_res, t_window_h_to_shift_tidal_tseries, obs_start, obs_end):
    years_to_choose_from = ds_6min_water_levels_mrms_res.date_time.to_series().dt.year.unique()[-(n_years_to_look_back_for_tidal_tseries+1):-1]
    if len(s_tseries_sim_surge.index.names) > 1:
        s_tseries_sim_surge = s_tseries_sim_surge.reset_index().set_index('timestep')
    tstep = pd.Series(s_tseries_sim_surge.index.diff()).mode().iloc[0]
    # randomly shift the tidal time series
    t_shift_hrs = np.random.uniform(low=-t_window_h_to_shift_tidal_tseries/2, high=t_window_h_to_shift_tidal_tseries/2, size=None)
    tdelt_shift_tide = pd.Timedelta(t_shift_hrs, "h").round(tstep) 
    valid_time_series_created = False
    while valid_time_series_created == False:
        year = np.random.choice(years_to_choose_from)
        dur = obs_end - obs_start
        obs_start_selected = pd.to_datetime(f"{year}-{obs_start.month}-{obs_start.day} {str(obs_start.time())}")
        obs_end_selected = obs_start_selected + dur
        s_obs_tseries_tide_shifted = ds_6min_water_levels_mrms_res.sel(date_time = slice((obs_start_selected+tdelt_shift_tide),(obs_end_selected+tdelt_shift_tide)))["tide_ft"].to_dataframe()["tide_ft"]
        rescaled_dur = s_obs_tseries_tide_shifted.index.max() - s_obs_tseries_tide_shifted.index.min()
        if (rescaled_dur == dur) and (s_obs_tseries_tide_shifted.isna().sum() == 0):
            valid_time_series_created = True
        else: # resample a different year
            next_years_to_choose_from = []
            for next_year in years_to_choose_from:
                if next_year == year:
                    continue
                next_years_to_choose_from.append(int(next_year))
            years_to_choose_from = next_years_to_choose_from
            valid_time_series_created = False
            if len(years_to_choose_from) == 0:
                sys.exit("there were no valid tidal time series in the years selected for identifying tidal time series")
    tdelt_shift_tide = s_obs_tseries_tide_shifted.index.min() - obs_start
    s_obs_tseries_tide_shifted.index = s_tseries_sim_surge.index
    s_tseries_sim_wlevel = s_tseries_sim_surge + s_obs_tseries_tide_shifted
    s_tseries_sim_wlevel.index = s_tseries_sim_surge.index
    s_tseries_sim_wlevel.name = "waterlevel_ft"
    return s_obs_tseries_tide_shifted, s_tseries_sim_wlevel, tdelt_shift_tide

def fill_rain_and_extend_surge_series(df_tseries_sim_rain, s_tseries_sim_surge, ds_6min_water_levels_mrms_res,
                                       prev_obs_start = None, row_surge_event = None, arbirary_start_date = None, obs_start_hardcode = None):
    # WORK
    # df_tseries_sim_rain, s_tseries_sim_surge, ds_6min_water_levels_mrms_res = df_tseries_combined[col_rain], df_tseries_combined["surge_ft"], ds_6min_water_levels_mrms_res
    # prev_obs_start = obs_start
    # arbirary_start_date = arbirary_start_date
    # END WORK
    # fill missing rain data with 0, fill missing surge data by retrieving more data from the observed event
    # time series and shifting it using the same adjustment as the previous rescaling
    df_tseries_sim_rain_filled = df_tseries_sim_rain.fillna(0)
    s_tseries_sim_surge_filled = s_tseries_sim_surge.copy()
    if prev_obs_start is not None:
        first_idx_with_surge = s_tseries_sim_surge.dropna().index.min()
        first_idx = s_tseries_sim_surge.index.min()
        # if the first timestep with surge has values, there is no need to backfill
        if first_idx_with_surge <= first_idx:
             obs_start = prev_obs_start
        else: # otherwise, compute how far back we must look in the record to complete the dataset
            time_needed = first_idx_with_surge - first_idx
            obs_start = prev_obs_start - time_needed
    # elif row_surge_event is not None: # index based on index of max surge
    #     obs_start = lookup_obs_surge_event_start(row_surge_event, arbirary_start_date, s_tseries_sim_surge)
    elif obs_start_hardcode is not None:
        obs_start = obs_start_hardcode
        pass
    # define event end to ensure the number of necessary tsteps are gathered by adding the entire duration to the obs_start
    sim_duration = (s_tseries_sim_surge.index.max() - s_tseries_sim_surge.index.min())
    obs_end = obs_start + sim_duration
    if (obs_end - obs_start) != sim_duration:
        sys.exit("ERROR: The observed event duration does not align with the simulated event duration")
    if s_tseries_sim_surge.isna().any():
        s_obs_tseries_surge = ds_6min_water_levels_mrms_res.sel(date_time = slice(obs_start,obs_end))["surge_ft"].to_dataframe()["surge_ft"]
        if row_surge_event is not None:
            surge_adjustement = row_surge_event["surge_peak_ft"] - row_surge_event["obs_surge_peak_ft"]
            if not np.isclose((row_surge_event["obs_surge_peak_ft"] + surge_adjustement), row_surge_event["surge_peak_ft"]):
                sys.exit("error: i messed up in my calculation of the observed event surge scaling")
            if (s_obs_tseries_surge.index.max() - s_obs_tseries_surge.index.min()) != sim_duration:
                sys.exit("ERROR: Duration of simulated and observed time series does not line up")
            if len(s_tseries_sim_surge) != len(s_obs_tseries_surge):
                sys.exit("ERROR: The number of timesteps in the simulated and observed time series does not line up")
            s_obs_surge_shifted_tomatch_sim = s_obs_tseries_surge + surge_adjustement
        else: # no change
            s_obs_surge_shifted_tomatch_sim = s_obs_tseries_surge
        # make sure the time series line up
        # reindex the simulated surge tseries
        s_tseries_sim_surge_reindexed = s_tseries_sim_surge.copy()
        s_tseries_sim_surge_reindexed.index = s_tseries_sim_surge_reindexed.index - s_tseries_sim_surge_reindexed.index.min()
        # define the locs to compare witht the extended series
        locs_to_compare = s_tseries_sim_surge_reindexed.dropna().index
        # reindex the observed time series
        s_obs_surge_shifted_tomatch_sim_reindexed = s_obs_surge_shifted_tomatch_sim.copy()
        s_obs_surge_shifted_tomatch_sim_reindexed.index = s_obs_surge_shifted_tomatch_sim_reindexed.index - s_obs_surge_shifted_tomatch_sim_reindexed.index.min()
        abs_diffs = abs(s_obs_surge_shifted_tomatch_sim_reindexed.loc[locs_to_compare] - s_tseries_sim_surge_reindexed.loc[locs_to_compare]).sum()
        if not np.isclose(abs_diffs.sum(), 0):
            s_obs_surge_shifted_tomatch_sim_reindexed.name = "obs"
            s_tseries_sim_surge_reindexed.name = "sim"
            sim_surge_peak_lead = (s_tseries_sim_surge_reindexed.idxmax() - s_obs_surge_shifted_tomatch_sim_reindexed.idxmax()) / np.timedelta64(1, "h") * 60
            print(f"Peaks are off by {sim_surge_peak_lead} minutes")
            pd.concat([s_obs_surge_shifted_tomatch_sim_reindexed, s_tseries_sim_surge_reindexed], axis = 1).plot()
            sys.exit("ERROR: The matching indices of the rescaled surge event does not align with the surge dataset used for filling missing values")
        s_tseries_sim_surge_filled = pd.Series(s_obs_surge_shifted_tomatch_sim.values, index = s_tseries_sim_surge.index)
        s_tseries_sim_surge_filled.name = s_tseries_sim_surge.name
    return s_tseries_sim_surge_filled, df_tseries_sim_rain_filled, obs_start, obs_end

def reindex_to_buffer_significant_rain_and_surge(df_tseries, arbirary_start_date = None, wlevel_varname = "surge_ft"):
    idx_name = df_tseries.index.name
    # reindex to honor time buffers if adjustments are needed
    new_tstep_index = df_tseries.index.copy()
    tstep = pd.Series(df_tseries.index.diff()).mode().iloc[0]
    if not df_tseries[wlevel_varname].isna().all():
        ## reindex to buffer around peak surge
        idx_peak_surge = df_tseries[wlevel_varname].dropna().idxmax()
        dur_after_peak_surge_h = (new_tstep_index.max() - idx_peak_surge) / np.timedelta64(1, "h")
        dur_before_peak_surge_h = (idx_peak_surge - new_tstep_index.min()) / np.timedelta64(1, "h")
        ### add buffer after peak surge
        if dur_after_peak_surge_h < timeseries_buffer_around_peaks_h:
            # if verbose:
            #     print(f"Extending time series so it ends {timeseries_buffer_around_peaks_h} hours after the peak surge index")
            new_end = idx_peak_surge + pd.Timedelta(timeseries_buffer_around_peaks_h, "h")
            new_tstep_index = pd.date_range(start = new_tstep_index.min(), end = new_end, freq = tstep)
        ### add buffer before peak surge
        if dur_before_peak_surge_h < timeseries_buffer_around_peaks_h:
            # if verbose:
            #     print(f"Extending time series so it starts {timeseries_buffer_around_peaks_h} hours before the peak surge index")
            new_start = idx_peak_surge - pd.Timedelta(timeseries_buffer_around_peaks_h, "h")
            new_tstep_index = pd.date_range(start = new_start, end = new_tstep_index.max(), freq = tstep)
    if not df_tseries["mm_per_hr"].isna().all():
        ## reindex based on rainfall
        ### make sure there is a warm up period before the first 
        s_rain_depth = df_tseries["mm_per_hr"].dropna() * (tstep / np.timedelta64(1, "h"))
        idx_nonzero = s_rain_depth[s_rain_depth>0].index
        s_rain_depth = s_rain_depth.loc[idx_nonzero.min():idx_nonzero.max()]
        idx_first_tstep_with_rain = idx_nonzero.min()
        dur_before_first_rain = (idx_first_tstep_with_rain - new_tstep_index.min()) / np.timedelta64(1, "h")
        ### make sure that there is a buffer after the last plug of significant rainfall
        tot_depth = s_rain_depth.sum()
        s_rain_depth_preceding_sums = s_rain_depth.resample(rule = pd.Timedelta(timeseries_buffer_around_peaks_h, "h"), origin = "end", closed = "left").sum()
        s_rain_depth_preceding_raindepth_fractions = s_rain_depth_preceding_sums / tot_depth
        s_rain_depth_preceding_significant_raindepth_fractions =s_rain_depth_preceding_raindepth_fractions[s_rain_depth_preceding_raindepth_fractions >= buffer_window_raindepth_fraction_to_trigger_tserties_extension]
        idx_last_significant_plug_of_rainfall = s_rain_depth_preceding_significant_raindepth_fractions.index.max()
        dur_after_last_significant_rain = (new_tstep_index.max() - idx_last_significant_plug_of_rainfall) / np.timedelta64(1, "h")
        ### add buffer after last significant rain plug
        if dur_after_last_significant_rain < timeseries_buffer_around_peaks_h:
            # if verbose:
            #     print(f"Extending time series so it ends {timeseries_buffer_around_peaks_h} hours after the last significant rainfall plug ({timeseries_buffer_around_peaks_h}hr rain depth that equals or exceeds {buffer_window_raindepth_fraction_to_trigger_tserties_extension*100}% of the total rain depth)")
            new_end = idx_last_significant_plug_of_rainfall + pd.Timedelta(timeseries_buffer_around_peaks_h, "h")
            new_tstep_index = pd.date_range(start = new_tstep_index.min(), end = new_end, freq = tstep)
        ### add buffer before first rain fall
        if dur_before_first_rain < timeseries_buffer_before_first_rain_h:
            # if verbose:
            #     print(f"Extending time series so it starts {timeseries_buffer_before_first_rain_h} hours before first rainfall")
            # sys.exit("WORK - insepcting to make sure reindexing is working like it's supposed to")
            new_start = idx_first_tstep_with_rain - pd.Timedelta(timeseries_buffer_before_first_rain_h, "h")
            new_tstep_index = pd.date_range(start = new_start, end = new_tstep_index.max(), freq = tstep)
    # if the index is unchanged, do nothing; otherwise, reindex the dataset
    reindex = True
    if len(new_tstep_index) == len(df_tseries.index): # if their lengths are the same and
        if (new_tstep_index != df_tseries.index).sum() == 0: # every index value is identical
            reindex = False # do not reindex
    if reindex: # otherwise, reindex
        df_tseries = df_tseries.reindex(new_tstep_index)
        # if new min index is before arbitrary start date, shift it
        if arbirary_start_date is not None:
            if df_tseries.index.min() != arbirary_start_date:
                df_tseries.index = df_tseries.index - df_tseries.index.min() + arbirary_start_date
        df_tseries.index.name = idx_name
    return df_tseries


def create_folder(dir, clear_folder=False):
    if clear_folder:
        try:
            shutil.rmtree(dir)
            print(f"Deleteing and recreating folder {dir}")
        except:
            pass
    Path(dir).mkdir(parents=True, exist_ok=True)

#%% Simulate data from the fitted vine copula
def format_fitted_cdf_values_for_fitting_copula(event_type_key, verbose = True):
    df_fitted_vs_empirical = pd.read_csv(f"{dir_fitting_maringals}{event_type_key}_empirical_vs_fitted.csv")
    df_data = df_fitted_vs_empirical[["observation_id", "data", "cdf_fit"]]
    df_obs = df_data.pivot(index='observation_id', columns='data', values='cdf_fit')
    n_rows_with_atleast1_missing_val = df_obs.isnull().any(axis = 1).sum()
    if n_rows_with_atleast1_missing_val > 0:
        df_obs = df_obs.dropna() # only include those where all values are valid
        if verbose:
            print(f"Dropping {n_rows_with_atleast1_missing_val} obs. with missing rain or surge data from dataset leaving {len(df_obs)} observations for fitting")
    return df_obs

def simulate_from_copula(df_obs, n, cop_fitted, seeds = None):
    # df_obs, n = df_obs_subset, len(df_obs_subset)
    if seeds is not None:
        try:
            seeds = list(seeds)
        except:
            seeds = [seeds]
        simulated_data = cop_fitted.simulate(n=n, seeds = seeds)
    else:
        simulated_data = cop_fitted.simulate(n=n)
    df_simulated  = pd.DataFrame(simulated_data)
    df_simulated.columns = df_obs.columns
    return df_simulated

def get_quantile(value, series, empirical_quantiles = None, plot = False, alpha = None, two_sided = True):
    sorted_series = series.sort_values().reset_index(drop = True)
    if empirical_quantiles is None:
        empirical_quantiles = stats.mstats.plotting_positions(sorted_series, alpha=0.4, beta=0.4) # cunnane plotting position
    quant = np.interp(value, sorted_series, empirical_quantiles)
    if plot:
        # Define thresholds for shading and vertical line

        vertical_line_position = value  # Position of the vertical line
        fig, ax = plt.subplots(dpi=300)
        # quantiles = np.linspace(0, 1, 41)
        # bins = series.quantile(quantiles)
        n, bins, patches = ax.hist(sorted_series, bins=40, edgecolor='black')
        if alpha is not None:
            left_threshold = sorted_series.quantile(alpha/2)
            right_threshold = sorted_series.quantile(1 - alpha/2)
            for patch in patches:
                if (patch.get_x() < left_threshold or patch.get_x() + patch.get_width() > right_threshold) and two_sided:
                    patch.set_facecolor('red')  # Shade red outside the thresholds
                elif (patch.get_x() < left_threshold) and not two_sided:
                    patch.set_facecolor('red')  # Shade red outside the thresholds
                else:
                    patch.set_facecolor('green')  # Shade green inside the thresholds
        ax.axvline(vertical_line_position, color='blue', linestyle='--', linewidth=2)
        txt_line = f"quantile: {quant:.2f}"
        ax.text(vertical_line_position, max(n)*0.9, txt_line, color='blue', fontsize=12, ha='center',
        bbox=dict(facecolor='white', alpha=0.9))
        return quant, fig, ax 
    else:
        return quant, None, None

#%% create a histogram of the num events being used
def compare_sim_vs_obs_event_occurances(df_occurences_sim, df_occurences_obs, txt_fig_caption = None, savefig_filename=None, plot_n_count = False):
    df_occurences_obs = df_occurences_obs.astype(int)
    txt_x, txt_y = 0.97, 0.97
    txt_xlab = "Annual event occurances"

    s_sim, s_obs = df_occurences_sim["any"], df_occurences_obs["any"].dropna()
    bin_edges = np.arange(min(s_sim.min(), s_obs.min()), max(s_sim.max(), s_obs.max()) + 2) - 0.5

    # rwidth_value = 0.8
    # align = "left"
    # Create the figure and define a 2x2 GridSpec layout with custom spans
    fig = plt.figure(figsize=(12, 8), dpi = 250)
    gs = fig.add_gridspec(2, 3, height_ratios=[2, 1])

    # Add the top histogram, spanning all columns in the first row
    ax_top = fig.add_subplot(gs[0, :])
    ax_top.hist([s_sim, s_obs], bins=bin_edges, density=True, label=['Simulated', 'Observed'], color=['blue', 'green'], rwidth=0.85)
    ax_top.legend()
    # ax_top.hist(df_occurences["any"].dropna(), bins=bins, color='blue', align=align, rwidth=rwidth_value)
    ax_top.set_title("any")
    ax_top.set_ylabel("density")
    if plot_n_count:
        # x_lim = ax_top.get_xlim()[1]*.98
        # y_lim = ax_top.get_ylim()[1]*.98
        ax_top.text(txt_x, txt_y, f'n 365-day\nobservations = {len(s_obs)}\nn obs. events = {s_obs.sum()}', 
            horizontalalignment='right',
            verticalalignment='top',
            fontsize=10,
            transform=plt.gca().transAxes)
    # # Get the range of ticks you want to label (adjust based on your data range)
    # x_ticks = range(int(ax_top.get_xlim()[0]), int(ax_top.get_xlim()[1]) + 1)

    # # Set the x-axis ticks and labels for the top plot to label every tick (0, 1, 2, etc.)
    # ax_top.set_xticks(x_ticks)

    # Add the three histograms below, one in each column
    s_sim, s_obs = df_occurences_sim["rain"], df_occurences_obs["rain"].dropna()
    bin_edges = np.arange(min(s_sim.min(), s_obs.min()), max(s_sim.max(), s_obs.max()) + 2) - 0.5

    txt_shift = .8
    ax_bottom1 = fig.add_subplot(gs[1, 0])
    ax_bottom1.hist([s_sim, s_obs], bins=bin_edges, density=True, label=['Simulated', 'Observed'], color=['blue', 'green'], rwidth=0.85)
    ax_bottom1.set_title("rain")
    ax_bottom1.set_ylabel("count")
    if plot_n_count:
        # x_lim = ax_bottom1.get_xlim()[1]*txt_shift
        # y_lim = ax_bottom1.get_ylim()[1]*.98
        ax_bottom1.text(txt_x, txt_y, f'n 365-day\nobservations = {len(s_obs)}\nn obs. events = {s_obs.sum()}', 
            horizontalalignment='right',
            verticalalignment='top',
            fontsize=9,
            transform=plt.gca().transAxes)

    s_sim, s_obs = df_occurences_sim["surge"], df_occurences_obs["surge"].dropna()
    bin_edges = np.arange(min(s_sim.min(), s_obs.min()), max(s_sim.max(), s_obs.max()) + 2) - 0.5
    ax_bottom2 = fig.add_subplot(gs[1, 1])
    ax_bottom2.hist([s_sim, s_obs], bins=bin_edges, density=True, label=['Simulated', 'Observed'], color=['blue', 'green'], rwidth=0.85)
    ax_bottom2.set_title("surge")
    ax_bottom2.set_xlabel(txt_xlab)
    if plot_n_count:
        # x_lim = ax_bottom2.get_xlim()[1]*txt_shift
        # y_lim = ax_bottom2.get_ylim()[1]*.98
        ax_bottom2.text(txt_x, txt_y, f'n 365-day\nobservations = {len(s_obs)}\nn obs. events = {s_obs.sum()}', 
            horizontalalignment='right',
            verticalalignment='top',
            fontsize=9,
            transform=plt.gca().transAxes)

    s_sim, s_obs = df_occurences_sim["compound"], df_occurences_obs["compound"].dropna()
    bin_edges = np.arange(min(s_sim.min(), s_obs.min()), max(s_sim.max(), s_obs.max()) + 2) - 0.5
    ax_bottom3 = fig.add_subplot(gs[1, 2])
    ax_bottom3.hist([s_sim, s_obs], bins=bin_edges, density=True, label=['Simulated', 'Observed'], color=['blue', 'green'], rwidth=0.85)
    ax_bottom3.set_title("compound")
    if plot_n_count:
        # x_lim = ax_bottom3.get_xlim()[1]*txt_shift
        # y_lim = ax_bottom3.get_ylim()[1]*.98
        ax_bottom3.text(txt_x, txt_y, f'n 365-day\nobservations = {len(s_obs)}\nn obs. events = {s_obs.sum()}', 
            horizontalalignment='right',
            verticalalignment='top',
            fontsize=9,
            transform=plt.gca().transAxes)


    # update xlims so they are all the same
    x_min = -0.5

    x_max = max(df_occurences_sim["rain"].max(),
                df_occurences_sim["surge"].max(),
                df_occurences_sim["compound"].max()) + .5

    ax_bottom1.set_xlim(x_min, x_max)
    ax_bottom2.set_xlim(x_min, x_max)
    ax_bottom3.set_xlim(x_min, x_max)

    if txt_fig_caption is not None:
        import textwrap
        wrapped_caption = "\n".join(textwrap.wrap(txt_fig_caption, width=120))
        fig.text(0.05, -.02*len(wrapped_caption.split("\n")), wrapped_caption, ha='left', fontsize=12)
    # Adjust layout to prevent overlap
    fig.tight_layout()

    if savefig_filename is not None:
        plt.savefig(savefig_filename, bbox_inches='tight')
        plt.clf()
    else:
        plt.show()


# build reference distribution of probability plot correlations
def ppcf_randint(s_obs, mc_samps_for_dist, low, high, plot = False, alpha = 0.05):
    from scipy.stats import randint
    s_obs = s_obs.sort_values().reset_index(drop = True)
    s_obs_cdf_emp = stats.mstats.plotting_positions(s_obs)
    s_obs_cdf_fitted = randint.cdf(s_obs, low = low, high = high)

    obs_stat = stats.pearsonr(s_obs_cdf_emp, s_obs_cdf_fitted).statistic
    from scipy.stats import randint
    s_samp_stats = pd.Series(data = np.nan, index = np.arange(mc_samps_for_dist)).astype(float)
    for samp_id in s_samp_stats.index:
        s_samp = pd.Series(randint.rvs(low, high, size = len(s_obs))).sort_values().reset_index(drop = True)
        s_samp_cdf_emp = stats.mstats.plotting_positions(s_samp)
        s_samp_cdf_fitted = randint.cdf(s_samp, low = low, high = high)
        samp_stat = stats.pearsonr(s_samp_cdf_emp, s_samp_cdf_fitted).statistic
        s_samp_stats.loc[samp_id] = samp_stat
    quant, fig, ax = get_quantile(obs_stat, s_samp_stats, plot = plot, alpha = alpha, two_sided = False)
    if plot:
        ax.set_ylabel(f"Count (based on {mc_samps_for_dist} Monte-Carlo n={len(s_obs)} samples)")
        ax.set_xlabel(f"Pearson R between Empirical and Fitted CDF Values")
    return quant, fig, ax

def get_quantile_from_value(series, value):
    # Calculate the rank of each value in the series as a percentage (quantile)
    ranks = series.rank(pct=True)
    # Find the closest value in the series
    closest_value = series.iloc[(series - value).abs().argsort()[:1]].values[0]
    # Return the quantile of the closest value
    return ranks[series == closest_value].iloc[0]

def compute_common_percentiles(df_obs, df_simulated, num_levels, percentile):
    from scipy.stats import gaussian_kde
    all_z_obs = []
    all_z_sim = []
    all_diffs = []
    n_vars = df_obs.shape[1]
    # Loop through all pairs of variables
    for i in range(n_vars):
        for j in range(i + 1, n_vars):
            # Get observed and simulated data for the current pair
            df_obs_subset = df_obs.iloc[:, [i, j]].dropna()
            obs_x, obs_y = df_obs_subset.iloc[:, 0], df_obs_subset.iloc[:, 1]
            # obs_x, obs_y = df_obs.iloc[:, i], df_obs.iloc[:, j]
            sim_x, sim_y = df_simulated.iloc[:, i], df_simulated.iloc[:, j]

            # Create grid for KDE evaluation
            x_min = min(obs_x.min(), sim_x.min())
            x_max = max(obs_x.max(), sim_x.max())
            y_min = min(obs_y.min(), sim_y.min())
            y_max = max(obs_y.max(), sim_y.max())

            x_grid, y_grid = np.meshgrid(np.linspace(x_min, x_max, 100), np.linspace(y_min, y_max, 100))
            grid_coords = np.vstack([x_grid.ravel(), y_grid.ravel()])

            # KDE for observed and simulated data
            kde_obs = gaussian_kde(np.vstack([obs_x, obs_y]))
            kde_sim = gaussian_kde(np.vstack([sim_x, sim_y]))

            z_obs = kde_obs(grid_coords).reshape(x_grid.shape)
            z_sim = kde_sim(grid_coords).reshape(x_grid.shape)

            # Collect all values of z_obs and z_sim to compute percentiles later
            all_z_obs.extend(z_obs.ravel())
            all_z_sim.extend(z_sim.ravel())

            # Calculate the difference and store it for the percentile calculation
            diff = z_obs - z_sim
            all_diffs.extend(diff.ravel())
    
    # Convert lists to numpy arrays for percentile calculation
    all_z_obs = np.array(all_z_obs)
    all_z_sim = np.array(all_z_sim)
    all_diffs = np.array(all_diffs)

    # Calculate the specified percentile for z_obs, z_sim, and diff
    z_max = np.percentile(np.concatenate([all_z_obs, all_z_sim]), percentile)
    diff_min = np.percentile(all_diffs, 100 - percentile)
    diff_max = np.percentile(all_diffs, percentile)

    # Return the common z_max, and the difference percentiles
    return {
        'z_max': z_max,
        'diff_min': diff_min,
        'diff_max': diff_max
    }


def plot_obs_vs_montecarlo_simulated_statstic(varname_test, s_mc_simulated_stat, val_obs_stat, pvalue, string_two_sided_test_result, alpha,
                                                n_obs, n_mc_sims, fig_maintitle = "", fig = None, ax = None, clearfigs = True, f_savefig = None,
                                                sim_type = "monte_carlo"):
    if sim_type not in ["monte_carlo", "bootstrap"]:
        sys.exit("sim_type not recognized")
    if (ax is None) and (fig is None):
        fig, ax = plt.subplots(figsize=(8,6), dpi = 300)
    sns.histplot(s_mc_simulated_stat, bins=30, kde=False, ax=ax)
    ax.set_xlabel(f"{varname_test}")
    lower_boundary = s_mc_simulated_stat.quantile(alpha/2)
    upper_boundary = s_mc_simulated_stat.quantile(1-alpha/2)
    # Shade reject regions red
    ax.axvspan(-9999, lower_boundary, color='red', alpha=alpha)
    ax.axvspan(upper_boundary, 9999, color='red', alpha=alpha)
    ax.axvspan(lower_boundary, upper_boundary, color='green', alpha=alpha)
    # define x limits
    ax.set_xlim((s_mc_simulated_stat.min(), s_mc_simulated_stat.max()))
    # Add the blue dashed vertical line for the observed cvm stat
    ax.axvline(x=val_obs_stat, color='blue', linestyle='--')
    # Add text saying the percentile of the observed cvm stat in the distribution
    fig_maintitle = textwrap.fill(fig_maintitle, width = 80)
    if sim_type == "monte_carlo":
        txt_ax_title = f"{fig_maintitle}\n{varname_test} Distribution from\n{n_mc_sims} {n_obs}-sample Monte-Carlo Simulations\n{string_two_sided_test_result} at significance = {alpha}"
    elif sim_type == "bootstrap":
        txt_ax_title = f"{fig_maintitle}\n{varname_test} Distribution from\n{n_mc_sims} n={n_obs} bootstrapped samples\n{string_two_sided_test_result} at significance = {alpha}"

    ax_title = txt_ax_title
    ax.set_title(ax_title, fontsize=12)
    percentile_rounded = round(pvalue*100, 0)
    remainder = percentile_rounded % 10
    if remainder == 1:
        sfx = "st"
    elif remainder == 2:
        sfx = "nd"
    elif percentile_rounded == 12:
        sfx = "th"
    elif remainder == 3:
        sfx = "rd"
    else:
        sfx = "th"
    line_text = f"({(pvalue*100):.0f}{sfx}\npercentile)"
    ax.text(val_obs_stat + 0.01, ax.get_ylim()[1]*0.88, line_text, color='blue')
    # Create a custom legend
    red_patch = mpatches.Patch(color='red', alpha=alpha, label='Reject')
    green_patch = mpatches.Patch(color='green', alpha=alpha, label='Fail to\nReject')
    blue_line = Line2D([0], [0], color='blue', linestyle='--', label=f'Observed\n{varname_test}')
    # Add the legend
    ax.legend(handles=[red_patch, green_patch, blue_line], loc='best')
    if ax is None:
        fig.tight_layout()
    if f_savefig is not None:
        plt.savefig(f_savefig, bbox_inches='tight')
    if clearfigs:
        plt.clf()
    return fig, ax


def monte_carlo_tau_test(df_obs, col1_iloc, col2_iloc, df_simulated = None, cop_fitted = None, n_samples=1000, alpha=0.05):
    df_obs_subset = df_obs.iloc[:, [col1_iloc, col2_iloc]].dropna()
    # obs_x, obs_y = df_obs_subset.iloc[:, 0], df_obs_subset.iloc[:, 1]
    tau_obs, _ = kendalltau(df_obs_subset.iloc[:, 0], df_obs_subset.iloc[:, 1])

    simulated_taus = []

    # Simulate n_simulations samples from the vine copula
    for _ in range(n_samples):
        # Simulate a sample from the fitted vine copula
        if cop_fitted is not None:
            df_sampled_for_tau = simulate_from_copula(df_obs, len(df_obs_subset), cop_fitted=cop_fitted)
        else:
            df_sampled_for_tau = df_simulated.sample(n = len(df_obs_subset), replace = True)
        tau_simulated, _ = kendalltau(df_sampled_for_tau.iloc[:, col1_iloc], df_sampled_for_tau.iloc[:, col2_iloc])
        simulated_taus.append(tau_simulated)

    simulated_taus = pd.Series(simulated_taus)

    pvalue = get_quantile_from_value(simulated_taus, tau_obs)
    if pvalue < alpha/2:
        mc_result = "reject"
    elif pvalue > (1-(alpha/2)):
        mc_result = "reject"
    else:
        mc_result = "fail to reject"
    return mc_result, pvalue, simulated_taus, tau_obs


def evaluate_copula_fit_based_on_comparing_kendalltaus_for_each_variable_pair(df_obs, alpha,n_for_montecarlo_tau_test, cop_fitted=None,n_cop_sim_for_dnsty_plts = 10000,
                                                                                df_simulated=None, plot = True, fldr_plots = None,
                                                                                fig_main_title=""):
    # work
    # cop_fitted = vinecop
    # plot = False
    # n_cop_sim_for_dnsty_plts = 10000
    # end work

    if cop_fitted is not None:
        df_simulated = simulate_from_copula(df_obs, n_cop_sim_for_dnsty_plts, cop_fitted=cop_fitted)
    n_sim = len(df_simulated)
    n_vars = len(df_obs.columns)
    df_mc_results_combined = pd.DataFrame(columns = ["var1", "var2", "obs_kendalltau", "mc_result", "mc_pvalue", "lowerbound_sim_tau", "upperbound_sim_tau"])
    idx_row = -1
    for i in range(n_vars):
        for j in range(i + 1, n_vars):  # Loop through each pair (i, j) where i != j
            idx_row+=1
            df_mc_results_combined.loc[idx_row,"var1"] = df_obs.columns[i]
            df_mc_results_combined.loc[idx_row,"var2"] = df_obs.columns[j]
            df_obs_subset = df_obs.iloc[:, [i, j]].dropna()
            obs_x, obs_y = df_obs_subset.iloc[:, 0], df_obs_subset.iloc[:, 1]
            # perform mc testing
            if cop_fitted is not None:
                sim_type = "monte_carlo"
                n_samples=n_for_montecarlo_tau_test
                col2_iloc=j
                col1_iloc=i
                mc_result, pvalue, simulated_taus, tau_obs = monte_carlo_tau_test(df_obs = df_obs, col1_iloc=i, col2_iloc=j, cop_fitted=cop_fitted,  n_samples=n_for_montecarlo_tau_test, alpha=alpha)
            else:
                # technically this is bootstrapping when a simulation dataframe is applied
                sim_type = "bootstrap"
                mc_result, pvalue, simulated_taus, tau_obs = monte_carlo_tau_test(df_obs = df_obs, col1_iloc=i, col2_iloc=j, df_simulated=df_simulated, n_samples=n_for_montecarlo_tau_test, alpha=alpha)

            df_mc_results_combined.loc[idx_row,"obs_kendalltau"] = tau_obs
            df_mc_results_combined.loc[idx_row,"mc_pvalue"] = pvalue
            df_mc_results_combined.loc[idx_row,"mc_result"] = mc_result
            df_mc_results_combined.loc[idx_row,"lowerbound_sim_tau"] = simulated_taus.quantile(alpha/2)
            df_mc_results_combined.loc[idx_row,"upperbound_sim_tau"] = simulated_taus.quantile(1-alpha/2)
            if plot:
                sim_x, sim_y = df_simulated.iloc[:, i], df_simulated.iloc[:, j]
                tau_sim, _ = kendalltau(sim_x, sim_y)

                n_cols = 3

                # Calculate the common x and y limits for all three plots
                x_min = min(obs_x.min(), sim_x.min())
                x_max = max(obs_x.max(), sim_x.max())
                y_min = min(obs_y.min(), sim_y.min())
                y_max = max(obs_y.max(), sim_y.max())

                # Set the grid limits (slightly expand for better view)
                x_limits = (min(x_min, 0),max(x_max, 1))
                y_limits = (min(y_min, 0),max(y_max, 1))

                # Define a grid over which to calculate densities
                x_grid, y_grid = np.meshgrid(np.linspace(x_min, x_max, 100),
                                            np.linspace(y_min, y_max, 100))

                # Flatten the grid for input into the KDEs
                grid_coords = np.vstack([x_grid.ravel(), y_grid.ravel()])

                # Evaluate KDEs for observed and simulated data
                kde_obs = gaussian_kde(np.vstack([obs_x, obs_y]))
                kde_sim = gaussian_kde(np.vstack([sim_x, sim_y]))
                z_obs = kde_obs(grid_coords).reshape(x_grid.shape)
                z_sim = kde_sim(grid_coords).reshape(x_grid.shape)

                # Determine the color limits for the density plots
                dic_plot_params = compute_common_percentiles(df_obs, df_simulated, num_levels=20, percentile=98)
                z_min = 0
                z_max = round(dic_plot_params["z_max"], 2)
                levels = np.linspace(z_min, z_max, 20)

                # 1. Contour plot for observed data with a colorbar
                ## also include scatter plot
                fig, axes = plt.subplots(1, n_cols, figsize=(6*n_cols, 6))
                fig.suptitle(f'{fig_main_title}\n[{df_obs.columns[i]}] vs. [{df_obs.columns[j]}]', fontsize=16)
                contour_obs = axes[0].contourf(x_grid, y_grid, z_obs, cmap="Blues", levels=levels,
                                                vmin=z_min, vmax=z_max, extend='both')
                fig.colorbar(contour_obs, ax=axes[0])
                axes[0].scatter(obs_x, obs_y, facecolors='none', edgecolors='black')
                axes[0].set_xlim(x_limits)
                axes[0].set_ylim(y_limits)
                axes[0].set_title(f"Observed Data (n={len(df_obs_subset)})\nKendall's Tau: {tau_obs:.2f}")

                # 2. Contour plot for simulated data with the same color scale and colorbar
                contour_sim = axes[1].contourf(x_grid, y_grid, z_sim, cmap="Blues", levels=levels,
                                                vmin=z_min, vmax=z_max, extend='both')
                fig.colorbar(contour_sim, ax=axes[1])
                axes[1].set_xlim(x_limits)
                axes[1].set_ylim(y_limits)
                axes[1].set_title(f"Simulated Data (n={n_sim})\nKendall's Tau: {tau_sim:.2f}")
                # perform monte carlo test to see if simulated alpha dataset is plausible compared to observed tau
                varname_test="Kendall Tau"
                fig, new_axis = plot_obs_vs_montecarlo_simulated_statstic(varname_test=varname_test, s_mc_simulated_stat = simulated_taus,
                                                        val_obs_stat = tau_obs, pvalue = pvalue, string_two_sided_test_result = mc_result, alpha=alpha,
                                                                    n_obs = len(df_obs_subset), n_mc_sims=n_for_montecarlo_tau_test,
                                                                    fig_maintitle = fig_main_title, ax = axes[2], fig = fig, clearfigs = False, f_savefig = None,
                                                                    sim_type = sim_type)
                fig.tight_layout()
                # plt.show()
                if fldr_plots is not None:
                    f_savefig = f'{fldr_plots}contours_{df_obs.columns[i]}_vs_{df_obs.columns[j]}.png'
                    plt.savefig(f_savefig, bbox_inches='tight')
                    plt.clf()
    #
    return df_mc_results_combined

def perform_two_sample_tests(sample1, sample2, alpha=0.05):
    # sample1, sample2 = s_dataset1, s_dataset2
    sample1 = np.asarray(sample1, dtype=float)
    sample2 = np.asarray(sample2, dtype=float)

    results = {}

    # 0. Cramer-von Mises (CVM test)
    cvm_result = stats.cramervonmises_2samp(sample1, sample2)
    cvm_pvalue = cvm_result.pvalue
    results['Cramer-von Mises'] = (cvm_pvalue, 
                                     "reject" 
                                     if cvm_pvalue < alpha else 
                                     "fail_to_reject")

    # 1. Kolmogorov-Smirnov test (KS test)
    ks_stat, ks_pvalue = stats.ks_2samp(sample1, sample2)
    results['Kolmogorov-Smirnov'] = (ks_pvalue, 
                                     "reject" 
                                     if ks_pvalue < alpha else 
                                     "fail_to_reject")

    # 2. Mann-Whitney U test (non-parametric)
    mannwhitney_stat, mannwhitney_pvalue = stats.mannwhitneyu(sample1, sample2, alternative='two-sided')
    results['Mann-Whitney U'] = (mannwhitney_pvalue, 
                                 "reject" 
                                 if mannwhitney_pvalue < alpha else 
                                 "fail_to_reject")

    # 3. Wilcoxon rank-sum test (non-parametric, same as Mann-Whitney U but from another perspective)
    wilcoxon_stat, wilcoxon_pvalue = stats.ranksums(sample1, sample2)
    results['Wilcoxon rank-sum'] = (wilcoxon_pvalue, 
                                    "reject" 
                                    if wilcoxon_pvalue < alpha else 
                                    "fail_to_reject")

    # 4. Student’s t-test (parametric, assumes equal variances)
    ttest_stat, ttest_pvalue = stats.ttest_ind(sample1, sample2, equal_var=True)
    results['Student’s t-test (equal variances)'] = (ttest_pvalue, 
                                                     "reject" 
                                                     if ttest_pvalue < alpha else 
                                                     "fail_to_reject")

    # 5. Welch’s t-test (parametric, does not assume equal variances)
    welch_stat, welch_pvalue = stats.ttest_ind(sample1, sample2, equal_var=False)
    results['Welch’s t-test (unequal variances)'] = (welch_pvalue, 
                                                     "reject" 
                                                     if welch_pvalue < alpha else 
                                                     "fail_to_reject")

    # 6. Anderson-Darling test
    # This test returns a statistic and critical values, but to compare p-values we'll compare it to ks test
    anderson_stat = stats.anderson_ksamp([sample1, sample2])
    anderson_pvalue = anderson_stat.pvalue
    results['Anderson-Darling'] = (anderson_pvalue, 
                                   "reject" 
                                   if anderson_pvalue < alpha else 
                                   "fail_to_reject")
    df_results = pd.DataFrame({
    'Test': list(results.keys()),
    'p-value': [result[0] for result in results.values()],
    'Conclusion': [result[1] for result in results.values()]
        })
    return df_results

def plot_pvalue_distributions(df_mc_results, alpha,
            fname_savefig = None, fig_title = None):
    # Get the unique tests
    unique_tests = df_mc_results['Test'].unique()
    
    # Set up the subplots (adjust layout)
    n_tests = len(unique_tests)
    rows = 2
    cols = (n_tests + 1) // 2
    fig, axes = plt.subplots(rows, cols, figsize=(15, 8))  # Wider figure

    # Flatten the axes array for easier indexing
    axes = axes.flatten()

    # Define bin edges (identical for all plots)
    bin_edges =np.linspace(0,1,21)

    # Loop through each test and create a histogram
    for test_idx, test_name in enumerate(unique_tests):
        ax = axes[test_idx]
        
        # Filter the results for the current test
        test_data = df_mc_results[df_mc_results['Test'] == test_name]

        # Calculate the percentage of rejections
        n_rejected = len(test_data[test_data['p-value'] < alpha])
        rejection_rate = n_rejected / len(test_data) * 100
        
        # Plot the histogram of p-values using identical bin edges
        sns.histplot(test_data['p-value'], bins=bin_edges, kde=False, ax=ax)
        
        # Shade the area left of alpha (reject) red
        ax.axvspan(0, alpha, color='red', alpha=0.2)
        # Shade the area right of alpha (fail to reject) green
        ax.axvspan(alpha, 1, color='green', alpha=0.2)
        
        # Set the title and labels, adding the percentage of rejections
        ax.set_title(f'{test_name}\n{rejection_rate:.2f}% rejected', fontsize=12, pad=20)
        ax.set_xlabel('p-value')
        ax.set_ylabel('Frequency')

        # Set x-limits
        ax.set_xlim(0, 1)

        # If it's the Anderson-Darling test, change the upper x-limit to 0.25
        if test_name == 'Anderson-Darling':
            ax.set_xlim(0, 0.25)

    # Remove any unused subplots
    for unused_idx in range(test_idx + 1, len(axes)):
        fig.delaxes(axes[unused_idx])

    # Create a custom legend (only show it once)
    red_patch = mpatches.Patch(color='red', alpha=0.2, label='Reject')
    green_patch = mpatches.Patch(color='green', alpha=0.2, label='Fail to Reject')
    
    # Add the legend to the last subplot
    axes[0].legend(handles=[red_patch, green_patch], loc='upper left')

    if fig_title is not None:
        fig.suptitle(fig_title, fontsize=16)
    # Adjust layout to prevent overlap
    plt.tight_layout()
    if fname_savefig is not None:
        plt.savefig(fname_savefig)
        plt.clf()
    else:
        plt.show()


def compare_2_distributions(s_dataset1, s_dataset2, dataset_name, n_bootstrap, n_per_sample, alpha, bins = 30, include_kde = True,
                              dir_savefig = None, figtitle_supplementary_text = None, fig_fname_prefix = None, do_bootstrapping = True, plot_hist = True):
  
  if (s_dataset1.name is not None) and (s_dataset2.name is not None):
      txt_data_desc = f"{s_dataset1.name} vs. {s_dataset2.name}"
  else:
      txt_data_desc = ""
  fig_title = f"{dataset_name}\n2-sample test results {txt_data_desc}\nn-per-sample: {n_per_sample} | n samples: {n_bootstrap}"
  if figtitle_supplementary_text is not None:
    fig_title += f"\n{figtitle_supplementary_text}"
  if fig_fname_prefix is not None:
      fig_fname_prefix = f"{fig_fname_prefix}"
  else:
      fig_fname_prefix = ""
  if dir_savefig is not None:
      Path(dir_savefig).mkdir(parents=True, exist_ok=True)
      fname_savefig = f"{dir_savefig}{txt_data_desc}_{dataset_name}_{fig_fname_prefix}.png"
  else:
      fname_savefig = None
  if do_bootstrapping:
    lst_results = []
    for samp_id in np.arange(n_bootstrap):
        df_results = perform_two_sample_tests(s_dataset1.sample(n = n_per_sample, replace = True, random_state=None),
                                          s_dataset2.sample(n = n_per_sample, replace = True, random_state=None), alpha=alpha)
        df_results["samp_id"] = samp_id
        lst_results.append(df_results)
    df_mc_results = pd.concat(lst_results).reset_index(drop = True)
    plot_pvalue_distributions(df_mc_results, alpha=alpha,
                fig_title = fig_title,
                fname_savefig = fname_savefig)
  if plot_hist:
    fig, ax = plt.subplots(dpi = 250, figsize = (7,7))
    rwidth_value = .8
    align = "left"
    if type(bins) == int:
      min_val = min(s_dataset1.min(), s_dataset2.min())
      max_val = max(s_dataset1.max(), s_dataset2.max())
      bin_edges = np.linspace(min_val, max_val, bins + 1)
    else:
      bin_edges = bins
    ax.hist(s_dataset1, bins=bin_edges, color='blue', alpha=0.3, label=s_dataset1.name, density=True, align=align, rwidth=rwidth_value)
    ax.hist(s_dataset2, bins=bin_edges, color='green', alpha=0.3, label=s_dataset2.name, density=True, align=align, rwidth=rwidth_value)
    if include_kde:
      sns.kdeplot(s_dataset1, color='blue', label=s_dataset1.name, fill=False, ax=ax)
      sns.kdeplot(s_dataset2, color='green', label=s_dataset2.name, fill=False, ax=ax)
    df_results = perform_two_sample_tests(s_dataset1, s_dataset2, alpha=alpha)
    txt = ''
    for result_idx, result_row in df_results.iterrows():
        txt += f'{result_row["Test"]}: {result_row["p-value"]:.2f} ({result_row["Conclusion"]})\n'
    ax.text(0.2, -0.3, txt, ha='left', va='center', transform=ax.transAxes, fontsize=8)
    ax.set_title(f'{dataset_name} {figtitle_supplementary_text}')
    ax.set_xlabel(dataset_name, fontsize=12)
    ax.set_ylabel('Density', fontsize=12)
    ax.legend()
    fig.suptitle(f"{txt_data_desc}", fontsize=16)
    fig.tight_layout()
    if dir_savefig is not None:
        fname_savefig = f"{dir_savefig}{dataset_name}_{fig_fname_prefix}{txt_data_desc}_histogram.png"
        plt.savefig(f"{fname_savefig}")
        plt.clf()
    else:
        fname_savefig = None
        plt.show()

# fitting marginal distributions
def return_best_n_rows(df_perf, perf_colname, best_val, n_best):
    if best_val == "lowest":
        ascending = True
    elif best_val == "highest":
        ascending = False
    df_perf_subset = df_perf.sort_values(perf_colname, ascending = ascending).head(n_best)
    return df_perf_subset

def plot_subset_of_fits(dic_event_summaries, dic_event_stats, dir_plots_all_fitted_marginals, cvm_cutoff, ks_cutoff,
                         mode = "all", which_event_key = None,
                           fname_of_interest = None, subdir_name = None, return_fit = False):
    # work
    # cvm_cutoff = ks_cutoff = 0.2
    # mode = "all"
    # subdir_name = None
    # return_fit = True
    # key = "surge_events"
    # df_event_summary = dic_event_summaries[key]
    # vars_all = dic_event_stats[key]
    # f_pdf_performance = f"{dir_plots_all_fitted_marginals}{key}.csv"
    # df_perf = pd.read_csv(f_pdf_performance)
    # fldr_plt = f"{dir_fitting_maringals}plots_{key}/"
    # v = "precip_depth_mm"
    # df_perf_singlevar = df_perf[df_perf["data"] == v]
    # df_best = df_perf_singlevar[(df_perf_singlevar.cvm_pval > cvm_cutoff) & (df_perf_singlevar.ks_pval > ks_cutoff)]
    # end work
    import shutil
    if mode not in ["all", "from_fname"]:
        sys.exit('mode not recognized')
    if mode == "from_fname":
        plotted_succesfully = False
        if which_event_key == None:
            sys.exit(f"If mode == {mode}, which_event_key argument must be specified.")
        if fname_of_interest == None:
            sys.exit(f"If mode == {mode}, fname_of_interest argument must be specified.")
        if subdir_name == None:
            sys.exit(f"If mode == {mode}, subdir_name argument must be specified.")
    for key in dic_event_summaries:
        if mode == "from_fname":
            if key != which_event_key:
                continue
            else:
                # print(f"inspecting plots for {key}")
                pass
        # print(f"Processing {which_event_key}.....")
        df_event_summary = dic_event_summaries[key]
        vars_all = dic_event_stats[key]
        f_pdf_performance = f"{dir_plots_all_fitted_marginals}{key}.csv"
        df_perf = pd.read_csv(f_pdf_performance)
        fldr_plt = f"{dir_fitting_maringals}plots_{key}/"
        if mode == "all":
            try:
                shutil.rmtree(fldr_plt)
            except:
                pass
            Path(fldr_plt).mkdir(parents=True, exist_ok=True)
        if mode == "from_fname":
            f_plt_subdir = f"{fldr_plt}{subdir_name}/"
            Path(f_plt_subdir).mkdir(parents=True, exist_ok=True)
        for v in vars_all:
            df_perf_singlevar = df_perf[df_perf["data"] == v]
            # return the top n best rows among transformed and untransformed datasets based on each performance metric
            # lst_transformed = [True, False]
            # lst_dfs_best = []
            # for perf_colname in dic_perf_metrics:
            #     for transformed in lst_transformed:
            #         filter_any_transformation = (~np.isnan(df_perf_singlevar["upper_bound"])) | (df_perf_singlevar["scalar_shift"]!=0) | df_perf_singlevar["normalize"] | df_perf_singlevar["boxcox"]
            #         # return all rows with a cvm of greater than 0.2
            #         if transformed == True:
                        
            #             df_perf_singlevar_subset = return_best_n_rows(df_perf_singlevar[filter_any_transformation], perf_colname=perf_colname, best_val=dic_perf_metrics[perf_colname], n_best=n_best)
            #         else:
            #             df_perf_singlevar_subset = return_best_n_rows(df_perf_singlevar[~filter_any_transformation], perf_colname=perf_colname, best_val=dic_perf_metrics[perf_colname], n_best=n_best)
            #         lst_dfs_best.append(df_perf_singlevar_subset)
            # df_best = pd.concat(lst_dfs_best).drop_duplicates()
            # also remove any that are rejected by the ks test or cvm test at a prespecified tolerance
            df_best = df_perf_singlevar[(df_perf_singlevar.cvm_pval > cvm_cutoff) & (df_perf_singlevar.ks_pval > ks_cutoff)]
            if len(df_best) == 0:
                print(f"WARNING: NO GOOD FITS WERE FOUND FOR VARIABLE {v}")
                break
            # plotting all the possible fits to support manual selection of PDFs
            for index, row in df_best.iterrows():
                # total_count += 1
                # print(total_count)
                any_transformation = (not np.isnan(row["upper_bound"])) or (row["scalar_shift"]!=0) or row["normalize"] or row["boxcox"]
                # if "log" in row.loc["distribution"]:
                #     any_transformation = True
                fx = getattr(stats, row.loc["distribution"])
                v = row.data
                s_data = df_event_summary[v].dropna()
                fix_loc_to_min = row["fix_loc_to_min"]
                n_params = row["n_params"]
                upper_bound = row["upper_bound"]
                scalar_shift = row["scalar_shift"]
                normalize = row["normalize"]
                boxcox = row["boxcox"]
                plot = True
                # define figname
                if row.aic < 0:
                    aic_desc = "n" + str(round(row.aic, 1)*-1)
                else:
                    aic_desc = str(round(row.aic, 1))
                if any_transformation:
                    count_transformations = 0
                    fname_prefix = "trns_{}"
                    if not np.isnan(row["upper_bound"]):
                        fname_prefix += "_ub"
                        count_transformations+=1
                    if row["scalar_shift"]!=0:
                        fname_prefix += "_shift"
                        count_transformations+=1
                    if row["normalize"]:
                        fname_prefix += "_nrmlz"
                        count_transformations+=1
                    if row["boxcox"]:
                        fname_prefix += "_bxcx"
                        count_transformations+=1
                    fname_prefix = fname_prefix.format(count_transformations)
                else:
                    fname_prefix = "notrns"
                fname = f"{v}_cvm-{round(row.cvm_pval, 2)}_{fname_prefix}_dist-{row.distribution}"
                if fix_loc_to_min:
                    fname += "_fixed_loc"
                fname_savefig = fldr_plt + fname + ".png"
                if mode == "from_fname":
                    if fname != fname_of_interest:
                        continue
                    else:
                        fname_savefig = f_plt_subdir + fname + ".png"
                        # print(f"Plotting and saving figure {fname_savefig}")
                        pass
                s_fit, df_emp_vs_fitted = fit_dist(s_data, fx, n_params=n_params, fix_loc_to_min=fix_loc_to_min, upper_bound=upper_bound, scalar_shift=scalar_shift, normalize = normalize, boxcox = boxcox, plot = plot, fname_savefig = fname_savefig)
                plt.clf()
                plotted_succesfully = True
    if mode == "from_fname":
        if not plotted_succesfully:
            print("WARNING: FOR SOME REASONG THE FOLLOWING PLOT NOT CREATED:")
            print(f"{fname_of_interest}")
        if return_fit == True:
            return s_fit, df_emp_vs_fitted

def final_selection_plot_and_write_csv(lst_fname_of_interest, which_event_key, dic_event_stats, dic_event_summaries,
                                        dir_plots_all_fitted_marginals, cvm_cutoff, ks_cutoff):
    lst_fits = []            
    lst_emp_vs_fitted = []            
    lst_marginals_accounted_for = []
    for fname_of_interest in lst_fname_of_interest:
        s_fit, df_emp_vs_fitted = plot_subset_of_fits(dic_event_summaries, dic_event_stats, dir_plots_all_fitted_marginals, cvm_cutoff, ks_cutoff, mode = "from_fname", which_event_key = which_event_key,
                                                       fname_of_interest = fname_of_interest, subdir_name = "slxn_final", return_fit = True)
        df_cols = df_emp_vs_fitted.columns.tolist()
        s_fit = pd.concat([pd.Series([which_event_key], index = ["event_type"]), s_fit])
        lst_fits.append(s_fit)
        df_emp_vs_fitted["data"] = s_fit["data"]
        df_emp_vs_fitted["distribution"] = s_fit["distribution"]
        df_emp_vs_fitted["event_type"] = s_fit["event_type"]
        df_emp_vs_fitted["any_transformation"] = s_fit["any_transformation"]
        if not s_fit["any_transformation"]:
            df_emp_vs_fitted["observations_transformed"] = np.nan
            df_emp_vs_fitted["x_fit_at_empirical_cdf_val_transformed"] = np.nan
        df_emp_vs_fitted["data_source"] = f_combined_event_summaries
        # re-order columns
        df_emp_vs_fitted = df_emp_vs_fitted.reset_index().rename(columns={'index': 'observation_id'})
        column_order = ['event_type', 'observation_id', 'data', 'data_source', 'any_transformation', 'distribution'] + df_cols
        df_emp_vs_fitted = df_emp_vs_fitted[column_order]
        df_emp_vs_fitted = df_emp_vs_fitted.set_index(["event_type", "observation_id", "data"])
        lst_emp_vs_fitted.append(df_emp_vs_fitted)
        for identified_marginal in dic_event_stats[which_event_key]:
            if identified_marginal in fname_of_interest:
                lst_marginals_accounted_for.append(identified_marginal)
    s_marginals_accounted_for = pd.Series(lst_marginals_accounted_for).sort_values().reset_index(drop=True)
    s_all_marginals_identified_when_analyzing_event_statistic_correlations = pd.Series(dic_event_stats[which_event_key]).sort_values().reset_index(drop=True)
    lst_vars_not_accounted_for = []
    for var in s_all_marginals_identified_when_analyzing_event_statistic_correlations:
        if var not in list(s_marginals_accounted_for):
            lst_vars_not_accounted_for.append(var)
    if len(lst_vars_not_accounted_for) > 0:
        print(f"Warning: not all marginals identified during event statistic correlation analysis have been modeled.\nNon-modeled variables: {lst_vars_not_accounted_for}\nModeled variables: {list(s_marginals_accounted_for.values)}")
    df_fits = pd.concat(lst_fits, axis = 1).T.reset_index(drop = True)
    df_fitted_vs_empirical = pd.concat(lst_emp_vs_fitted)
    df_fits.to_csv(f"{dir_fitting_maringals}{which_event_key}_distributions.csv", index = False)
    df_fitted_vs_empirical.to_csv(f"{dir_fitting_maringals}{which_event_key}_empirical_vs_fitted.csv", index = True)

def reproduce_select_plots_and_verify_all_vars_accounted_for(dic_event_stats, which_event_key, lst_fname_of_interest, dic_event_summaries,
                                                             dir_plots_all_fitted_marginals, cvm_cutoff, ks_cutoff):
    vars_all = dic_event_stats[which_event_key]
    s_vars_accounted_for = pd.Series(data=False, index = vars_all).astype(bool)
    for fname_of_interest in lst_fname_of_interest:
        mode = "from_fname"
        subdir_name = "slxn_coarse"
        plot_subset_of_fits(dic_event_summaries, dic_event_stats, dir_plots_all_fitted_marginals, cvm_cutoff, ks_cutoff,
                             mode = mode, which_event_key = which_event_key, fname_of_interest = fname_of_interest, subdir_name = subdir_name)
        for var in vars_all:
            if var in fname_of_interest:
                s_vars_accounted_for.loc[var] = True
    if (~s_vars_accounted_for).sum() != 0:
        print("WARNING! NOT ALL EVENT STATISTICS ACCOUNTED FOR!")
    return s_vars_accounted_for


#%% event selection stuff
# def compute_possible_event_endtimes(s_rainfall_mm,min_event_threshold_mm, rolling_sum_period_h):
#     s_rain_rollingsum_mm = s_rainfall_mm.rolling('{}h'.format(rolling_sum_period_h), min_periods = 1).sum()
#     possible_ends = s_rain_rollingsum_mm[s_rain_rollingsum_mm > min_event_threshold_mm]
#     return possible_ends

def return_bm_string(previous_benchmark, activity_desc, timedelta_units = "s", round_figs = 2, verbose = False):
    tdiff = round((pd.Timestamp(datetime.now()) - pd.Timestamp(previous_benchmark)) / np.timedelta64(1, timedelta_units),round_figs)
    str_bm = "time to {} ({}): {}".format(activity_desc, timedelta_units, str(tdiff))
    if verbose:
        print(str_bm)
    return tdiff
    

def event_selection_threshold_or_nstorms(s_rainfall_mm_per_hr, min_interevent_time, max_strm_len,
                                           str_data_source, target_events_per_year = None,
                                           threshold = None, threshold_duration = None, assign_0_to_missing = True):
    sys.exit("need to update the code to include as a parameter threshold_duration; currently, the min_interevent_time is being treated as the threshold duration")
    # s_rainfall_mm_per_hr, min_interevent_time, max_strm_len, str_data_source = s_rainfall_ncei_hrly_mrms_res_mm_per_hr,min_interevent_time,max_strm_len, str_data_source
    if (target_events_per_year is not None) and (threshold is not None):
        sys.exit("both target_events_per_year and threshold arguments passed, so event selection methodology is ambiguous. Remove one of these arguments.")
    if threshold is not None:
        print(f"Performing threshold-based event selection using a threshold of {threshold} mm...")
    if target_events_per_year is not None:
        print(f"Performing event selection targeting the largest {target_events_per_year} events per year...")
        
    bm_function = datetime.now()
    # process rainfall data
    # assign consistent index names
    s_rainfall_mm_per_hr.index.name = "date_time"
    # create a timeseries with depths in mm (converting from mm/hr to mm)
    rain_tstep = pd.Series(s_rainfall_mm_per_hr.index.diff()).mode().iloc[0]
    tstep_min = rain_tstep / np.timedelta64(1, "h")*60
    s_rainfall_mm = s_rainfall_mm_per_hr * (tstep_min/60)
    # perform event selection
    # define lists
    lst_rain_event_starts = []
    lst_rain_event_ends = []
    event_duration = []
    event_ids = []
    mean_intensities = []
    max_intensities = []
    max_intensity_tsteps = []
    total_precip = []
    event_id = 1
    lst_s_intensities = []

    event_selection_ongoing = True

    

    new_idx = pd.date_range(s_rainfall_mm.index.min(), s_rainfall_mm.index.max(), freq = rain_tstep) 
    s_rainfall_mm = s_rainfall_mm.reindex(new_idx)
    if s_rainfall_mm.isna().sum() > 0:
        print("Warning: there are missing values in the rainfall dataset")
        if assign_0_to_missing:
            n_missing = s_rainfall_mm.isna().sum()
            min_missing = tstep_min * n_missing
            min_total = len(new_idx)*tstep_min
            frac_missing = min_missing / min_total
            print(f"Filling {n_missing} missing values with 0 for event selection representing {frac_missing*100:.1f}% of time from the first timestep to the last ({s_rainfall_mm.index.min()} to {s_rainfall_mm.index.max()})")
            s_rainfall_mm = s_rainfall_mm.fillna(0)

    # perform event selection finding n largest events per year
    s_rainfall_mm_events_removed = s_rainfall_mm.copy()

    while event_selection_ongoing:
        # bm_whole_loop = datetime.now()
        # dcl work
        # print("extracting event {} out of {}....".format(event_id, round(tot_events)))
        # end dcl work

        # insert zeros for rainfall that has been accounted for
        # bm_time_to_recompute_rolling_sum = datetime.now()
        # compute 12-hour rolling sum to find possible event end times
        rolling_sum_period_h = int(min_interevent_time / np.timedelta64(1, "h"))
        min_periods = int(rolling_sum_period_h / (rain_tstep/np.timedelta64(1, "h")))
        s_rain_rollingsum_mm = s_rainfall_mm_events_removed.rolling('{}h'.format(rolling_sum_period_h), min_periods = min_periods, closed = "left").sum().dropna()
        # return_bm_string(bm_time_to_recompute_rolling_sum, "compute rolling sum after removing previous events")
        # s_possible_storms = s_rain_rollingsum_mm[s_rain_rollingsum_mm > rainfall_threshold_mm]
        # print(len(s_possible_storms))
        # if len(s_possible_storms) == 0:
        #     print("Found all storms in this time series")
        #     all_events_found = True
        #     break
        # bm_pick_event_and_adjust_time_period = datetime.now()
        s_event_gaps = s_rain_rollingsum_mm[s_rain_rollingsum_mm==0]

        # pick largest event
        threshold_mm = s_rain_rollingsum_mm.max()
        end_most_intense_period_end = s_rain_rollingsum_mm.idxmax()
        # find the nearest 12-hour period before and after this intense period with zero rainfall
        rain_event_start = s_event_gaps[(s_event_gaps.index <= end_most_intense_period_end)].index.max() # - min_interevent_time
        rain_event_end = s_event_gaps[(s_event_gaps.index >= end_most_intense_period_end)].index.min() - min_interevent_time
        s_rainfall_mm_subset = s_rainfall_mm.loc[rain_event_start-rain_tstep:rain_event_end+rain_tstep]
        # remove preceding and trailing zeros
        idx_w_rain = s_rainfall_mm_subset[s_rainfall_mm_subset>0].index
        rain_event_start = idx_w_rain.min()
        rain_event_end = idx_w_rain.max()
        s_rainfall_mm_subset = s_rainfall_mm_subset.loc[rain_event_start:rain_event_end]
        if (rain_event_end - rain_event_start) > max_strm_len:
            # choose the max storm duration with the largest rainfall
            rolling_sum_period_h = int(max_strm_len / np.timedelta64(1, "h"))
            min_periods = int(rolling_sum_period_h / (rain_tstep/np.timedelta64(1, "h")))
            s_rain_rollingsum_mm = s_rainfall_mm_subset.rolling('{}h'.format(rolling_sum_period_h), min_periods = min_periods, closed = "left").sum().dropna()
            rain_event_end = s_rain_rollingsum_mm[s_rain_rollingsum_mm == s_rain_rollingsum_mm.max()].index.max()
            rain_event_start = rain_event_end - max_strm_len
            s_rainfall_mm_subset = s_rainfall_mm.loc[rain_event_start:rain_event_end]
            # remove preceding and trailing zeros
            idx_w_rain = s_rainfall_mm_subset[s_rainfall_mm_subset>0].index
            rain_event_start = idx_w_rain.min()
            rain_event_end = idx_w_rain.max()
            s_rainfall_mm_subset = s_rainfall_mm_subset.loc[rain_event_start:rain_event_end]
        # return_bm_string(bm_pick_event_and_adjust_time_period, "select the largest event and update its time period if necessary")
        # append the lists with relevant data
        intensities = s_rainfall_mm_per_hr[rain_event_start:rain_event_end]
        lst_rain_event_starts.append(rain_event_start)
        lst_rain_event_ends.append(rain_event_end)
        event_duration.append(rain_event_end - rain_event_start)
        mean_intensities.append(intensities.mean())
        max_intensities.append(intensities.max())
        max_intensity_tsteps.append(intensities.idxmax())
        total_precip.append(s_rainfall_mm_subset.sum())
        event_ids.append(event_id)
        event_id += 1
        intensities.name = "mm_per_hour" # name becomes columnname of the concatenated dataframe
        lst_s_intensities.append(intensities)
        # return_bm_string(bm_whole_loop, "select single event")
        # remove event from time series
        event_index = ((s_rainfall_mm_events_removed.index >= rain_event_start) & (s_rainfall_mm_events_removed.index <= rain_event_end))
        s_rainfall_mm_events_removed[event_index] = 0
        # end loop if conditions are met based on threshold or target n storms
        if threshold is not None:
            if s_rain_rollingsum_mm.max() < threshold:
                event_selection_ongoing = False
                break
        if target_events_per_year is not None:
            tot_events = (len(pd.Series(s_rainfall_mm_per_hr.index.date).unique())/365.25) * target_events_per_year
            if event_id < round(tot_events)+1:
                event_selection_ongoing = True
            else:
                break

    # combine results into dataframes
    df_rainevent_summary = pd.DataFrame(dict(event_id = event_ids, rain_event_start = lst_rain_event_starts, rain_event_end = lst_rain_event_ends, 
                                duration = event_duration, depth_mm = total_precip, 
                                mean_mm_per_hr = mean_intensities, max_mm_per_hour = max_intensities,
                                max_intensity_tstep = max_intensity_tsteps))
    df_rainevent_summary.set_index("event_id", inplace=True)

    # process time series data
    df_rainevent_tseries = pd.concat(lst_s_intensities, keys=event_ids).reset_index()
    df_rainevent_tseries = df_rainevent_tseries.rename(columns = {"level_0":"event_id"})
    df_rainevent_tseries = df_rainevent_tseries.set_index(["event_id", s_rainfall_mm_per_hr.index.name])
    
    return_bm_string(bm_function, "complete event selection", verbose = True)

    df_rainevent_summary["rain_data_source"] = str_data_source
    df_rainevent_tseries["rain_data_source"] = str_data_source
    n_events = len(df_rainevent_summary)
    n_years = (len(pd.Series(s_rainfall_mm_per_hr.index.date).unique())/365.25)
    event_return_rate_n_per_year = n_events/n_years
    if threshold is not None:
        threshold_mm = threshold

    return df_rainevent_summary, df_rainevent_tseries, threshold_mm, event_return_rate_n_per_year # the threshold is the minimum 12-hour rainfall to trigger an event

def surge_event_selection(s_surge, surge_threshold_to_determine_event_limits,
                           max_event_duration_h, min_interevent_time, str_data_source,
                           target_events_per_year = None, threshold = None):

    tstep = pd.Series(s_surge.index.diff()).mode().iloc[0]

    lst_surge_event_start = []
    lst_surge_event_end = []
    lst_event_peak = []
    lst_event_max_surge_tstep = []
    lst_event_ids = []
    lst_event_duration = []

    df_wlevel_event_tseries = pd.DataFrame()

    s_surge_events_removed = s_surge.copy()
    if target_events_per_year is not None:
        tot_events = (len(pd.Series(s_surge.index.date).unique())/365.25) * target_events_per_year # (total years) * (events per year)
    else:
        tot_events = np.nan
    possible_event_boundaries = s_surge_events_removed[s_surge_events_removed<=surge_threshold_to_determine_event_limits]
    event_id = 1
    event_selection_ongoing = True
    while event_selection_ongoing:
        max_surge_ft = s_surge_events_removed.max()
        if threshold is not None:
            if max_surge_ft < threshold:
                event_selection_ongoing = False
                break
        tsep_max_surge_ft = s_surge_events_removed.idxmax()
        # find next timestep below threshold
        surge_event_start = possible_event_boundaries[:tsep_max_surge_ft].index[-1]
        # find prior timestep below threshold
        surge_event_end = possible_event_boundaries[tsep_max_surge_ft:].index[0]
        # if the duration is less than the minimum interevent time, adjust start and end times to achieve that
        if (surge_event_end - surge_event_start)< min_interevent_time:
            buffer = (min_interevent_time - (surge_event_end - surge_event_start))/2
            buffer = buffer.ceil(freq = tstep)
            # buffer event start earlier
            surge_event_start -= buffer
            # buffere event end later
            surge_event_end += buffer
        # subset the surge time series
        s_surge_event_tseries = s_surge[surge_event_start:surge_event_end]
        # if duration is greater than max duration, subset portion with largest rolling mean
        duration_h = (surge_event_end-surge_event_start)/np.timedelta64(1, "h")
        if duration_h > max_event_duration_h:
            duration_h = 0
            preceding_hr = tsep_max_surge_ft - pd.Timedelta(1,"hour")
            proceding_hr = tsep_max_surge_ft + pd.Timedelta(1,"hour")
            surge_event_start = surge_event_end = tsep_max_surge_ft
            while duration_h < max_event_duration_h:
                # go hour by hour to build a max_event_duration_h event working out from the peak
                preceding_hr_mean = s_surge[preceding_hr:preceding_hr+pd.Timedelta(1,"hour")].mean()
                proceding_hr_mean = s_surge[proceding_hr-pd.Timedelta(1,"hour"):proceding_hr].mean()
                use_proceding = True
                if preceding_hr_mean > proceding_hr_mean:
                    use_proceding = False
                if use_proceding:
                    surge_event_end = proceding_hr
                    proceding_hr = proceding_hr + pd.Timedelta(1,"hour")
                else:
                    surge_event_start = preceding_hr
                    preceding_hr = preceding_hr - pd.Timedelta(1,"hour")
                s_surge_event_tseries = s_surge[surge_event_start:surge_event_end]
                duration_h = (surge_event_end-surge_event_start)/np.timedelta64(1, "h")
                # if the first and last timesteps are below the event limit threshold, break the loop
                if (s_surge_event_tseries.iloc[0] <= surge_threshold_to_determine_event_limits) and (s_surge_event_tseries.iloc[-1] <= surge_threshold_to_determine_event_limits):
                    break
                if duration_h >= max_event_duration_h:
                    break
        # remove selected event from the time series
        s_tsteps_to_remove = s_surge[(surge_event_start-min_interevent_time):(surge_event_end+min_interevent_time)]
        s_surge_events_removed = s_surge_events_removed[~s_surge_events_removed.index.isin(s_tsteps_to_remove.index)]

        # append lists
        lst_surge_event_start.append(surge_event_start)
        lst_surge_event_end.append(surge_event_end)
        lst_event_peak.append(max_surge_ft)
        lst_event_max_surge_tstep.append(tsep_max_surge_ft)
        lst_event_ids.append(event_id)
        lst_event_duration.append(surge_event_end - surge_event_start)
        df_wlevel_event_tseries_subset = pd.DataFrame(s_surge_event_tseries)
        df_wlevel_event_tseries_subset["surge_event_id"] = event_id
        df_wlevel_event_tseries = pd.concat([df_wlevel_event_tseries, df_wlevel_event_tseries_subset])
        # s_surge_event_tseries.plot()
        event_id += 1
        # break
        # continue
        if target_events_per_year is not None:
            if event_id < round(tot_events)+1:
                event_selection_ongoing = True
            else:
                break
    df_wlevel_event_summaries = pd.DataFrame(dict(
        surge_event_id = lst_event_ids,
        surge_event_start = lst_surge_event_start,
        surge_event_end = lst_surge_event_end,
        duration = lst_event_duration,
        max_surge_ft = lst_event_peak,
        max_surge_tstep = lst_event_max_surge_tstep,
    ))

    df_wlevel_event_summaries.set_index('surge_event_id', inplace=True)
    df_wlevel_event_tseries = df_wlevel_event_tseries.reset_index().set_index(["surge_event_id", "date_time"])
    # compute derived surge event threshold
    surge_event_threshold = df_wlevel_event_summaries.max_surge_ft.min()

    df_wlevel_event_summaries["wlevel_data_source"] = str_data_source
    df_wlevel_event_tseries["wlevel_data_source"] = str_data_source

    n_events = len(df_wlevel_event_summaries)
    n_years = (len(pd.Series(s_surge.index.date).unique())/365.25)
    event_return_rate_n_per_year = n_events/n_years

    return df_wlevel_event_summaries, df_wlevel_event_tseries, surge_event_threshold, event_return_rate_n_per_year

def compute_event_timeseries_statistics(df_tseries_og, lst_time_durations_h_to_analyze, varname, idx_name_time = "timestep",
                                         agg_stat = "mean", idx_event_id = "event_id"):
    df_tseries = df_tseries_og.copy()
    if len(df_tseries.index.names) > 1:
        # idx_name_time = "timestep"
        idxs_to_drop = []
        for other_idxs in df_tseries.index.names:
            if other_idxs not in [idx_event_id, idx_name_time]:
                idxs_to_drop.append(other_idxs)
        df_tseries = df_tseries.droplevel(idxs_to_drop)
    else:
        idx_event_id = None
        idx_name_time = df_tseries.index.names[0]
    # calculating additional rainfall statistics
    # add columns to event summary table with max rainfall intensities over different periods of time
    # convert to minutes
    lst_time_durations_min_to_analyze = (np.asarray(lst_time_durations_h_to_analyze) * 60).astype(int)
    lst_s_statistics = []
    lst_s_statistic_index = []
    for i, dur_min in enumerate(lst_time_durations_min_to_analyze):
        dur_h = lst_time_durations_h_to_analyze[i]
        # np.timedelta64(dur_h, "h")
        sys.exit("i need to update the statistic timestep to fall in the middle of the rolling window")
        if idx_event_id is not None:
            s_statistic = df_tseries.reset_index().set_index(idx_name_time).groupby(idx_event_id).rolling("{}min".format(dur_min)).agg(agg_stat).groupby(level=idx_event_id).max().iloc[:,0]
            s_statistic_tstep = df_tseries.reset_index().set_index(idx_name_time).groupby(idx_event_id).rolling("{}min".format(dur_min)).agg(agg_stat).reset_index().set_index(idx_name_time).dropna().groupby(idx_event_id).idxmax().iloc[:,0]
        else: # assume a single event time series was provided
            s_statistic = df_tseries.rolling("{}min".format(dur_min)).agg(agg_stat).max()
            s_statistic_tstep = df_tseries.rolling("{}min".format(dur_min)).agg(agg_stat).dropna().idxmax()
        if dur_h % 1 == 0:
            dur_h = int(dur_h)
        s_statistic.name = "max_{}hr_{}_{}".format(dur_h, agg_stat, varname)
        s_statistic_tstep.name = "max_{}hr_{}_{}_tstep".format(dur_h, agg_stat, varname)
        lst_s_statistics.append(s_statistic)
        lst_s_statistic_index.append(s_statistic_tstep)
    df_stats = pd.concat(lst_s_statistics+lst_s_statistic_index, axis = 1)
    return df_stats

def plot_annual_rainfall_totals(df_rainevent_summary, s_rainfall_mm_per_hr, threshold_mm, return_rate_n_per_year, fig_title = None, fname_savefig = None, threshold_units = "mm"):
    fig, ax = plt.subplots(figsize = (8,4), dpi = 200)
    event_start_column = [col for col in df_rainevent_summary.columns if "event_start" in col][0]
    df_rainevent_summary[event_start_column].dt.year.value_counts().sort_index().plot.bar(ax=ax)
    ax.set_ylabel("N events")
    fig_text1 = "The {} largest events were selected from {} years of data".format(len(df_rainevent_summary), len(s_rainfall_mm_per_hr.index.year.unique()))
    fig_text2 = "corresponding to an arrival rate of {:.2f} storms per year.".format(return_rate_n_per_year)
    fig_text3 = "The threshold corresponding to this"
    fig_text4 = "is {} {}".format(round(threshold_mm,2), threshold_units)
    txt_fig_cap_txt = "{}\n{}\n{}\n{}".format(fig_text1, fig_text2, fig_text3, fig_text4)
    fig.text(.5, -0.12, txt_fig_cap_txt, ha='center')
    if fig_title is not None:
        fig.suptitle(fig_title)
    fig.tight_layout()
    if fname_savefig is not None:
        plt.savefig(fname_savefig, bbox_inches='tight')
        plt.clf()

# plot and save to csv
def plot_wlevel_histogram(df_wlevel_event_summaries, df_wlevel_event_tseries, surge_event_threshold,
                   s_surge, n_events_per_year,
                   fname_save = None):
    fig, ax1 = plt.subplots(dpi = 200)
    df_wlevel_event_summaries.max_surge_ft.hist(ax = ax1)
    ax1.set_xlabel("max_surge_ft")
    fig_txt = f"Surge threshold for event selection: {surge_event_threshold:.2f} ft\n"
    fig_txt += f"n_events = {len(df_wlevel_event_summaries)} | n_years = {len(pd.Series(s_surge.index.date).unique())/365.25:.2f} | Events per year = {n_events_per_year:.2f}"
    fig.text(.5, -0.08, fig_txt, ha='center')
    fig.tight_layout()
    if fname_save is not None:
        plt.savefig(fname_save, bbox_inches='tight')
        plt.clf()
    else:
        plt.show()

# combine mrms and surge events during the overlapping period of record
def determine_compound_event_start_and_end_times(df_wlevel_event_tseries, df_wlevel_event_summaries, df_rainevent_tseries, df_rainevent_summary):
    """
    For each rain event, find the time series indices that overlap with a surge event. If the peak surge of that surge event occurs 
    during the rain event, I am considering them a compound event.
    """
    # define lists of the surge event IDs and rain event IDs that are being combined
    lst_surge_events_being_combined = [] 
    lst_rain_events_being_combined = []
    surge_event_datetimes = df_wlevel_event_tseries.reset_index().date_time
    rain_event_datetimes = df_rainevent_tseries.reset_index().date_time
    lst_compound_event_starts = []
    lst_compound_event_ends = []

    # for each rain event, determine the fraction of it that is contained within a surge event
    for rain_event_id, rain_event in df_rainevent_summary.iterrows():
        # does this overlap at all with a surge event?
        rain_event_duration = rain_event.rain_event_end - rain_event.rain_event_start
        idx_overlap = (surge_event_datetimes >= rain_event.rain_event_start) & (surge_event_datetimes <= rain_event.rain_event_end)
        surge_event_datetimes_overlap = surge_event_datetimes[idx_overlap]
        if len(surge_event_datetimes_overlap) == 0:
            continue
        overlap_duration = surge_event_datetimes_overlap.iloc[-1] - surge_event_datetimes_overlap.iloc[0]
        frac_overlap = overlap_duration / rain_event_duration
        # if overlap is non-zero, combine them into a single event
        if frac_overlap > 0:
            # initialize event starts and ends
            compound_event_start = rain_event.rain_event_start
            compound_event_end = rain_event.rain_event_end
            # loop through each overlapping surge event and update event start and end times
            df_wlevel_event_tseries_subset = df_wlevel_event_tseries.loc[(slice(None), surge_event_datetimes_overlap), :]
            # identify unique event IDs
            surge_event_ids_to_combine = df_wlevel_event_tseries_subset.reset_index().surge_event_id.unique()
            # see if there are any other rain events that these events span
            for surge_event_id in surge_event_ids_to_combine:
                surge_event = df_wlevel_event_summaries.loc[surge_event_id, :]
                # determine whether the peak surge event occurs during the rain event
                peak_surge_occurs_during_rain_event = (surge_event["max_surge_tstep"] >= rain_event.rain_event_start) and (surge_event["max_surge_tstep"] <= rain_event.rain_event_end)
                # if the peak surge does NOT occur during the rain event, do not combine them
                if not peak_surge_occurs_during_rain_event:
                    continue
                lst_surge_events_being_combined.append(surge_event_id)
                idx_overlap = (rain_event_datetimes >= surge_event.surge_event_start) & (rain_event_datetimes <= surge_event.surge_event_end)
                rain_event_datetimes_overlap = rain_event_datetimes[idx_overlap]
                df_rainevent_tseries_subset = df_rainevent_tseries.loc[(slice(None), rain_event_datetimes_overlap), :]
                # identify unique event IDs
                rain_event_ids_with_overlap = df_rainevent_tseries_subset.reset_index().event_id.unique()
                for overlapping_rain_event_id in rain_event_ids_with_overlap:
                    overlapping_rain_event = df_rainevent_summary.loc[overlapping_rain_event_id]
                    lst_rain_events_being_combined.append(overlapping_rain_event_id)
                    compound_event_start = min([compound_event_start, surge_event.surge_event_start, overlapping_rain_event.rain_event_start])
                    compound_event_end = max([compound_event_end, surge_event.surge_event_end, overlapping_rain_event.rain_event_end])
        lst_compound_event_starts.append(compound_event_start)
        lst_compound_event_ends.append(compound_event_end)
        # if len(rain_event_ids_with_overlap) > 1:
        #     print("Encountered situation with multiple rain events being combined. I want to inspect the loop to make sure code is doing what it's supposed to.")
        #     # break
        # if len(surge_event_ids_to_combine) > 1:
        #     print("Encountered situation with multiple surge events being combined. I want to inspect the loop to make sure code is doing what it's supposed to.")
        #     # break

    df_combined_events = pd.DataFrame(dict(
        combined_event_start = lst_compound_event_starts,
        combined_event_end = lst_compound_event_ends,
        surge_event = True,
        rain_event = True
    ))


    df_combined_events["compound"] = True
    df_combined_events["compound_event_duration"] = df_combined_events.combined_event_end - df_combined_events.combined_event_start
    # df_combined_events["wlevel_data_source"] = wlevel_source
    # df_combined_events["rain_data_source"] = rain_source
    # df_combined_events["data_source"] = "{}_and_{}".format(rain_source, wlevel_source)

    # combine all event start and end times
    df_rainevent_summary_combined_events_removed = df_rainevent_summary[~df_rainevent_summary.index.isin(lst_rain_events_being_combined)].copy()
    df_wlevel_event_summaries_combined_events_removed = df_wlevel_event_summaries[~df_wlevel_event_summaries.index.isin(lst_surge_events_being_combined)].copy()

    df_rainevent_summary_combined_events_removed["compound"] = False
    df_rainevent_summary_combined_events_removed["rain_event"] = True
    df_rainevent_summary_combined_events_removed["surge_event"] = False

    df_wlevel_event_summaries_combined_events_removed["compound"] = False
    df_wlevel_event_summaries_combined_events_removed["rain_event"] = False
    df_wlevel_event_summaries_combined_events_removed["surge_event"] = True

    s_starts = pd.concat([df_rainevent_summary_combined_events_removed.rain_event_start,
                        df_wlevel_event_summaries_combined_events_removed.surge_event_start,
                        df_combined_events.combined_event_start]).reset_index(drop=True)
    s_starts.name = "event_start"

    s_ends = pd.concat([df_rainevent_summary_combined_events_removed.rain_event_end,
                        df_wlevel_event_summaries_combined_events_removed.surge_event_end,
                        df_combined_events.combined_event_end]).reset_index(drop=True)
    s_ends.name = "event_end"

    s_compound = pd.concat([df_rainevent_summary_combined_events_removed["compound"],
                        df_wlevel_event_summaries_combined_events_removed["compound"],
                        df_combined_events["compound"]]).reset_index(drop=True)
    s_compound.name = "compound"

    s_rain_event = pd.concat([df_rainevent_summary_combined_events_removed["rain_event"],
                        df_wlevel_event_summaries_combined_events_removed["rain_event"],
                        df_combined_events["rain_event"]]).reset_index(drop=True)
    s_rain_event.name = "rain_event"

    s_surge_event = pd.concat([df_rainevent_summary_combined_events_removed["surge_event"],
                        df_wlevel_event_summaries_combined_events_removed["surge_event"],
                        df_combined_events["surge_event"]]).reset_index(drop=True)
    s_surge_event.name = "surge_event"

    # s_rain_source = pd.concat([df_rainevent_summary_combined_events_removed.rain_data_source,
    #                     df_wlevel_event_summaries_combined_events_removed.rain_data_source,
    #                     df_combined_events.rain_data_source]).reset_index(drop=True)
    # s_rain_source.name = "rain_data_source"

    # s_surge_source = pd.concat([df_rainevent_summary_combined_events_removed.wlevel_data_source,
    #                     df_wlevel_event_summaries_combined_events_removed.wlevel_data_source,
    #                     df_combined_events.wlevel_data_source]).reset_index(drop=True)
    # s_surge_source.name = "wlevel_data_source"
    df_compound_event_summaries = pd.concat([s_starts, s_ends, s_compound, s_rain_event, s_surge_event], axis = 1)
    # define data sources from input datasets:
    rain_source = df_rainevent_summary["rain_data_source"].values[0]
    wlevel_source = df_wlevel_event_summaries["wlevel_data_source"].values[0]
    df_compound_event_summaries["rain_data_source"] = rain_source
    df_compound_event_summaries["wlevel_data_source"] = wlevel_source
    df_compound_event_summaries.reset_index(drop = True, inplace = True)
    df_compound_event_summaries.index.name = "event_id"

    overlapping_events_present = True
    while overlapping_events_present:
        count_overlapping = 0
        lst_s_new_events = []
        lst_s_events_to_keep = []
        # lst_idx_events_combined = []
        for e_id, row in df_compound_event_summaries.iterrows():
            strt = row.event_start
            end = row.event_end
            # events overlap if the end comes before the end and after the start of another event
            # OR if the start comes after the start and before the end of another event
            filter_end = (end <= df_compound_event_summaries["event_end"]) & (end >= df_compound_event_summaries["event_start"])
            filter_start = (strt <= df_compound_event_summaries["event_end"]) & (strt >= df_compound_event_summaries["event_start"])
            if (filter_end | filter_start).sum() > 1:
                s_new_event = pd.Series(index = df_compound_event_summaries.columns).astype(object)
                # break
                count_overlapping += 1
                idx_overlapping = list(df_compound_event_summaries[filter_end | filter_start].index)
                
                df_overlapping = df_compound_event_summaries.loc[idx_overlapping,:]
                s_new_event["event_start"] = df_overlapping["event_start"].min()
                s_new_event["event_end"] = df_overlapping["event_end"].min()
                new_event_classification = df_overlapping.loc[:, ["rain_event", "surge_event"]].any()
                s_new_event["rain_event"] = new_event_classification["rain_event"]
                s_new_event["surge_event"] = new_event_classification["surge_event"]
                s_new_event["compound"] = new_event_classification.all()
                s_new_event["rain_data_source"] = rain_source
                s_new_event["wlevel_data_source"] = wlevel_source
                lst_s_new_events.append(s_new_event)
                # lst_idx_events_combined.append(idx_overlapping)
                # drop the event ids that were combined
                # df_compound_event_summaries = df_compound_event_summaries.drop(idx_overlapping)
            else:
                lst_s_events_to_keep.append(row)
        if len(lst_s_new_events) == 0:
            break
        # create dataframe of unique combined events
        df_newly_combined_events = pd.concat(lst_s_new_events, axis = 1).T.drop_duplicates().reset_index(drop = True)
        df_non_overlapping_events = pd.concat(lst_s_events_to_keep, axis = 1).T.reset_index(drop = True)
        df_compound_event_summaries = pd.concat([df_newly_combined_events, df_non_overlapping_events], ignore_index=True)
        df_compound_event_summaries = df_compound_event_summaries.sort_values("event_start").reset_index(drop = True)
    return df_compound_event_summaries

# df_compound_event_summaries = determine_compound_event_start_and_end_times(df_6min_wlevel_event_tseries, df_6min_wlevel_event_summaries, df_mrms_rainevent_tseries, df_mrms_rainevent_summary)
# extract the event statistics
def determine_combined_event_statistics(df_wlevel_event_tseries, df_wlevel_event_summaries, df_rainevent_tseries, df_rainevent_summary,
                                        df_water_levels_trgt_res, s_rainfall_mm_per_hr, s_rainfall_mm):
    # work
    # MRMS
    # df_wlevel_event_tseries, df_wlevel_event_summaries, df_rainevent_tseries, df_rainevent_summary, df_water_levels_trgt_res, s_rainfall_mm_per_hr, s_rainfall_mm = df_6min_wlevel_event_tseries, df_6min_wlevel_event_summaries, df_mrms_rainevent_tseries,df_mrms_rainevent_summary,df_6min_water_levels_5min,s_mrms_rainfall_mm_per_hr, s_mrms_rainfall_mm
    # NCEI
    # df_wlevel_event_tseries, df_wlevel_event_summaries, df_rainevent_tseries, df_rainevent_summary, df_water_levels_trgt_res, s_rainfall_mm_per_hr, s_rainfall_mm = df_6min_wlevel_event_tseries, df_6min_wlevel_event_summaries, df_ncei_hrly_rainevent_tseries,df_ncei_hrly_rainevent_summary,df_6min_water_levels_5min,s_rainfall_ncei_hrly_5min_mm_per_hr, s_rainfall_ncei_hrly_5min_mm
    # end work
    df_compound_event_summaries = determine_compound_event_start_and_end_times(df_wlevel_event_tseries, df_wlevel_event_summaries,
                                                                                df_rainevent_tseries, df_rainevent_summary)

    # do any events overlap?
    count_overlapping = 0
    lst_idx_overlapping = []
    for e_id, row in df_compound_event_summaries.iterrows():
        strt = row.event_start
        end = row.event_end
        # events overlap if the end comes before the end and after the start of another event
        # OR if the start comes after the start and before the end of another event
        filter_end = (end <= df_compound_event_summaries["event_end"]) & (end >= df_compound_event_summaries["event_start"])
        filter_start = (strt <= df_compound_event_summaries["event_end"]) & (strt >= df_compound_event_summaries["event_start"])
        if (filter_end | filter_start).sum() > 1:
            count_overlapping += 1
            lst_idx_overlapping += list(df_compound_event_summaries[filter_end | filter_start].index)
    if count_overlapping>0:
        print(f"found {count_overlapping} overlapping events")
        df_ovelap = df_compound_event_summaries.loc[pd.Series(lst_idx_overlapping).unique(), ["event_start", "event_end"]]
        print(df_ovelap)


    lst_dfs_compound_tseries = []
    lst_total_precip = []
    lst_event_peak = []
    lst_max_intensities = []
    lst_max_precip_intensity_tsteps = []
    lst_event_max_surge_tstep = []
    lst_surge_peak_after_rain_peak = []
    # lst_rainfall_data_source = []
    # lst_surge_data_source = []
    wlevel_data_source = np.nan
    rain_data_source = np.nan
    # lst_surge_event = []
    # lst_rain_event = []

    for event_id, event in df_compound_event_summaries.iterrows():
        # if event.event_start == pd.Timestamp('2015-10-03 11:06:00'):
        #     break
        # if event.event_end.year == 2020:
        #     break
        s_surge_subset = df_water_levels_trgt_res.surge_ft.loc[event.event_start:event.event_end]
        s_wlevel_subset = df_water_levels_trgt_res.waterlevel_ft.loc[event.event_start:event.event_end]
        s_rainfall_mm_per_hr_subset = s_rainfall_mm_per_hr.loc[event.event_start:event.event_end]
        s_rainfall_mm_subset = s_rainfall_mm.loc[event.event_start:event.event_end]
        
        # buffer surge and rainfall time series

        df_tseries = pd.concat([s_surge_subset,s_wlevel_subset,s_rainfall_mm_per_hr_subset, s_rainfall_mm_subset], axis = 1)
        og_dur = df_tseries.index.max() - df_tseries.index.min()
        df_tseries = reindex_to_buffer_significant_rain_and_surge(df_tseries, arbirary_start_date = None)#, idx_name = "date_time")
        if og_dur != (df_tseries.index.max() - df_tseries.index.min()):
            # update the time series and the event summary information
            s_surge_subset = df_water_levels_trgt_res.surge_ft.loc[df_tseries.index.min():df_tseries.index.max()]
            s_wlevel_subset = df_water_levels_trgt_res.waterlevel_ft.loc[df_tseries.index.min():df_tseries.index.max()]
            s_rainfall_mm_per_hr_subset = s_rainfall_mm_per_hr.loc[df_tseries.index.min():df_tseries.index.max()]
            s_rainfall_mm_subset = s_rainfall_mm.loc[df_tseries.index.min():df_tseries.index.max()]
            df_tseries = pd.concat([s_surge_subset,s_wlevel_subset,s_rainfall_mm_per_hr_subset, s_rainfall_mm_subset], axis = 1)

        df_tseries["event_id"] = event_id
        df_tseries = df_tseries.reset_index().set_index(["event_id", "date_time"])
        lst_dfs_compound_tseries.append(df_tseries)

        # compute summary statistics 
        ## rainfall
        tot_precip = max_intensity = tstep_max_intensity = np.nan
        if len(s_rainfall_mm_per_hr_subset) > 0:
            tstep = pd.Series(s_rainfall_mm_per_hr_subset.index.diff()).mode().iloc[0]
            tstep_min = tstep / np.timedelta64(1, "h")*60
            s_rainfall_mm_subset = s_rainfall_mm_per_hr_subset * (tstep_min/60)
            tot_precip = s_rainfall_mm_subset.sum()
            max_intensity = s_rainfall_mm_per_hr_subset.max()
            if s_rainfall_mm_per_hr_subset.isna().all():
                tstep_max_intensity = np.nan
            else:
                tstep_max_intensity = s_rainfall_mm_per_hr_subset.idxmax()
            rain_data_source = df_compound_event_summaries["rain_data_source"].iloc[0]
        # lst_rainfall_data_source.append(rain_data_source)
        lst_total_precip.append(tot_precip)
        lst_max_intensities.append(max_intensity)
        lst_max_precip_intensity_tsteps.append(tstep_max_intensity)
        # surge
        peak_surge = tstep_peak_surge = np.nan
        if len(s_surge_subset) > 0:
            peak_surge = s_surge_subset.max()
            tstep_peak_surge = s_surge_subset.idxmax()
            wlevel_data_source = df_compound_event_summaries["wlevel_data_source"].iloc[0]
        # lst_surge_data_source.append(wlevel_data_source)
        lst_event_peak.append(peak_surge)
        lst_event_max_surge_tstep.append(tstep_peak_surge)

        surge_peak_after_rain_peak = np.nan
        if not pd.Series([tstep_max_intensity,tstep_peak_surge]).isna().any():
            surge_peak_after_rain_peak = (tstep_max_intensity-tstep_peak_surge)/np.timedelta64(1, "h")
        lst_surge_peak_after_rain_peak.append(surge_peak_after_rain_peak)


    df_compound_event_tseries = pd.concat(lst_dfs_compound_tseries)

    df_rain_summaries = compute_event_timeseries_statistics(df_compound_event_tseries[s_rainfall_mm_per_hr_subset.name], lst_time_durations_h_to_analyze, varname=s_rainfall_mm_per_hr_subset.name,
                                                            idx_name_time = "date_time", agg_stat = "mean")

    df_stats_surge = compute_event_timeseries_statistics(df_compound_event_tseries.surge_ft, lst_time_durations_h_to_analyze, varname="surge",
                                                            idx_name_time = "date_time", agg_stat = "mean")
    # create event start and end datetimes (since these were derived from each datasets original timestep but are no consolidated to the same timestep)
    lst_event_starts = []
    lst_event_ends = []
    prev_e_id = -1
    for e_id, df_event_tseries in df_compound_event_tseries.groupby("event_id"):
        lst_event_starts.append(df_event_tseries.reset_index()["date_time"].min())
        lst_event_ends.append(df_event_tseries.reset_index()["date_time"].max())
        if e_id - prev_e_id != 1:
            sys.exit("event ids are out of order")
        prev_e_id = e_id

    df_compound_event_summary_stats = pd.DataFrame(dict(
        event_id = df_compound_event_summaries.index,
        event_start = lst_event_starts,
        event_end = lst_event_ends,
        compound = df_compound_event_summaries.compound,
        rain_event = df_compound_event_summaries.rain_event,
        surge_event = df_compound_event_summaries.surge_event,
        rain_data_source = rain_data_source,
        wlevel_data_source = wlevel_data_source,
        surge_peak_ft = lst_event_peak,
        precip_depth_mm = lst_total_precip,
        precip_max_intensity = lst_max_intensities,
        precip_max_intensity_tstep = lst_max_precip_intensity_tsteps,
        surge_peak_tstep = lst_event_max_surge_tstep,
        surge_peak_after_rain_peak_h = lst_surge_peak_after_rain_peak
    ))

    df_compound_event_summary_stats.set_index(["event_id"], inplace = True)

    df_compound_event_summary_stats = df_compound_event_summary_stats.join(df_rain_summaries).join(df_stats_surge)

    # compute time differences between surge and rainfall aggregated statistics
    lst_stats_tdiffs = []
    for agg_stat_duration in lst_time_durations_h_to_analyze:
        # return the surge and rainfall column names associated with this stat
        duration_substring_surge = "{}hr".format(agg_stat_duration)
        colnames = df_compound_event_summary_stats.columns[df_compound_event_summary_stats.columns.str.contains(duration_substring_surge)]
        colnames = colnames[colnames.str.contains("tstep")]
        surge_colname = colnames[colnames.str.contains("surge")][0]
        for agg_stat_duration in lst_time_durations_h_to_analyze:
            duration_substring_rain = "{}hr".format(agg_stat_duration)
            colnames = df_compound_event_summary_stats.columns[df_compound_event_summary_stats.columns.str.contains(duration_substring_rain)]
            colnames = colnames[colnames.str.contains("tstep")]
            rain_colname = colnames[colnames.str.contains("mm_per_hr")][0]
            surge_peak_after_rain_peak = df_compound_event_summary_stats[rain_colname]-df_compound_event_summary_stats[surge_colname]
            surge_peak_after_rain_peak = surge_peak_after_rain_peak / np.timedelta64(1, "h")
            surge_peak_after_rain_peak.name = "max_mean_{}surge_peak_after_{}rain_peak_h".format(duration_substring_surge, duration_substring_rain)
            lst_stats_tdiffs.append(surge_peak_after_rain_peak)

    df_stat_tdiffs = pd.concat(lst_stats_tdiffs, axis = 1)
    df_compound_event_summary_stats = df_compound_event_summary_stats.join(df_stat_tdiffs)


    # properly classify compound events
    ## surge events that are compound
    filter_surge_evnts = ((df_compound_event_summary_stats["surge_event"] == True) & (df_compound_event_summary_stats["compound"] == False))
    filter_precip_depth_meets_threshold = (df_compound_event_summary_stats["precip_depth_mm"] >= df_rainevent_summary["depth_mm"].min())
    idx_surge_events_that_are_actually_compound = df_compound_event_summary_stats[filter_surge_evnts & filter_precip_depth_meets_threshold].index
    ## rain events that are compound
    filter_rain_evnts = ((df_compound_event_summary_stats["rain_event"] == True) & (df_compound_event_summary_stats["compound"] == False))
    filter_surge_meets_threshold = (df_compound_event_summary_stats["surge_peak_ft"] >= df_wlevel_event_summaries["max_surge_ft"].min())
    idx_rain_events_that_are_actually_compound = df_compound_event_summary_stats[filter_rain_evnts & filter_surge_meets_threshold].index

    if len(idx_surge_events_that_are_actually_compound) > 0:
        print(f"{len(idx_surge_events_that_are_actually_compound)} surge events have been reclassified as a compound event because the rain threshold was exceeded during the event.")
    if len(idx_rain_events_that_are_actually_compound) > 0:
        print(f"{len(idx_rain_events_that_are_actually_compound)} rain events have been reclassified as a compound event because the surge threshold was exceeded during the event.")
    df_compound_event_summary_stats.loc[idx_surge_events_that_are_actually_compound, "compound"] = True
    df_compound_event_summary_stats.loc[idx_surge_events_that_are_actually_compound, "rain_event"] = True

    df_compound_event_summary_stats.loc[idx_rain_events_that_are_actually_compound, "compound"] = True
    df_compound_event_summary_stats.loc[idx_rain_events_that_are_actually_compound, "surge_event"] = True

    df_wlevel_rain_summary = df_compound_event_summary_stats.dropna()
    df_wlevel_rain_tseries = df_compound_event_tseries.dropna()
    for e_id, row in df_wlevel_rain_summary.iterrows():
        event_start = row["event_start"]
        event_end = row["event_end"]
        row["precip_depth_mm"]
        rainsum_tseries = df_wlevel_rain_tseries.loc[pd.IndexSlice[:, event_start:event_end], :]["mm"].sum()
        if not np.isclose(rainsum_tseries, row["precip_depth_mm"]):
            print("warning: discrepancy between rain series sum and rain sum reported in event summary table")
            print(f"event_start = {event_start}, event_end = {event_end}")

    # add data source columns to time series
    df_compound_event_tseries["wlevel_data_source"] = wlevel_data_source
    df_compound_event_tseries["rain_data_source"] = rain_data_source
    return df_compound_event_summary_stats, df_compound_event_tseries

def process_ncei_data(s_rainfall_ncei_in_og, target_res_min, years_to_exclude = []):
    # assign time zone (it's definitely local time, I think it's EST because there doesn't seem to be a change in the fall)
    s_rainfall_ncei_in = s_rainfall_ncei_in_og.copy()
    s_rainfall_ncei_in.index = s_rainfall_ncei_in.copy().index.tz_localize('EST')
    s_rainfall_ncei_daily_mm_per_tstep = s_rainfall_ncei_in*mm_per_inch
    og_tstep = pd.Series(s_rainfall_ncei_in.index).diff().mode()[0]
    hrs_per_tstep = pd.Series(s_rainfall_ncei_in.index).diff().mode()[0] / np.timedelta64(1, "h")
    s_rainfall_ncei_daily_mm_per_hr = s_rainfall_ncei_daily_mm_per_tstep / hrs_per_tstep

    # comnbert to target timestep for event selection
    ## add extra timestep at the end to avoid truncation
    s_rainfall_ncei_daily_mm_per_hr[(s_rainfall_ncei_daily_mm_per_tstep.index.max() + pd.Timedelta(hrs_per_tstep, "hr"))] = 0
    ## resample to target timestep
    s_rainfall_ncei_trgt_res_mm_per_hr = s_rainfall_ncei_daily_mm_per_hr.resample(f'{target_res_min:.0f}min').asfreq()
    ## in order to avoid forward filling in long swaths of missing data, need to do this by grouping according to original tstep
    s_rainfall_ncei_trgt_res_mm_per_hr = s_rainfall_ncei_trgt_res_mm_per_hr.groupby(pd.Grouper(level='DATE', freq=og_tstep)).ffill()
    # convert from eastern standard time to UT
    new_idx = s_rainfall_ncei_trgt_res_mm_per_hr.index.tz_convert('UTC')
    new_idx = new_idx.tz_localize(None)
    s_rainfall_ncei_trgt_res_mm_per_hr.index = new_idx
    # subset years to include
    s_rainfall_ncei_trgt_res_mm_per_hr = s_rainfall_ncei_trgt_res_mm_per_hr[~s_rainfall_ncei_trgt_res_mm_per_hr.index.year.isin(years_to_exclude)]
    s_rainfall_ncei_trgt_res_mm = s_rainfall_ncei_trgt_res_mm_per_hr * (target_res_min/60) # mm/hour * 5 min/tstep * 1/60 hr/min = mm per tstep
    s_rainfall_ncei_trgt_res_mm_per_hr.name = "mm_per_hr"
    s_rainfall_ncei_trgt_res_mm.name = "mm"
    return s_rainfall_ncei_trgt_res_mm_per_hr, s_rainfall_ncei_trgt_res_mm

def perform_statistical_test(test, x, y, alpha):
    res = test(x, y)
    pval = res.pvalue
    if res.pvalue < alpha:
        result = 'reject'
    else:
        result = 'fail_to_reject'
    return pval, result


def compare_stats(s1, s2, fig_title, plot = True, fname_savefig = None):
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
        txt_fig_cap_txt = "{}\n{}\n{}\n{}".format(txt_means, txt_meds, txt_cvm_results, txt_ranksum_results)
        fig.text(.5, -0.13, txt_fig_cap_txt, ha='center')
        fig.suptitle(fig_title)
        fig.tight_layout()
        if fname_savefig is not None:
            plt.savefig(fname_savefig, bbox_inches='tight')
            plt.clf()
    return cmv_result, ranksum_result


def classify_events(df_event_summary):
    for idx, row in df_event_summary.iterrows():
        if row["compound"] == True:
            e_type = "compound"
        elif row["surge_event"] == True:
            e_type = "surge"
        elif row["rain_event"] == True:
            e_type = "rain"
        df_event_summary.loc[idx, "event_type"] = e_type
    return df_event_summary

def compare_series_to_scalar(series, scalar):
    return (series == scalar) | (pd.isna(series) & pd.isna(scalar))

def combine_dataset_according_to_priority(col_target_data_source, lst_df_tseries_in_order_of_preferred_data_source,
                                           col_data, lst_df_summaries_in_order_of_preferred_data_source):
    # update data source for each event summary:
    for idx_smry, df_summary in enumerate(lst_df_summaries_in_order_of_preferred_data_source):
        # fill missing values for event time series data sources
        # if df_summary[col_target_data_source].isna().sum() > 0:
        #     print("warning: there are missing values for data source")
        #     df_summary[col_target_data_source] = df_summary.dropna()[col_target_data_source].iloc[0]
        # if df_summary[col_nontarget_data_source].isna().sum() > 0:
        #     print("warning: there are missing values for data source")
        #     df_summary[col_nontarget_data_source] = df_summary.dropna()[col_nontarget_data_source].iloc[0]
        lst_df_summaries_in_order_of_preferred_data_source[idx_smry] = df_summary
    # loop through keeping priority datasets and adding lower priority datasets as needed
    lst_dfs_lower_priority_events_to_keep = []
    df_higher_priority = lst_df_tseries_in_order_of_preferred_data_source[0]
    # if data source value is missing, fill missing values
    # if df_higher_priority[col_target_data_source].isna().sum() > 0:
    #     print("warning: there are missing values for data source")
    #     df_higher_priority[col_target_data_source] = df_higher_priority.dropna()[col_target_data_source].iloc[0]
    # if df_higher_priority[col_nontarget_data_source].isna().sum() > 0:
    #     print("warning: there are missing values for data source")
    #     df_higher_priority[col_nontarget_data_source] = df_higher_priority.dropna()[col_nontarget_data_source].iloc[0]

    for df_lower_priority in lst_df_tseries_in_order_of_preferred_data_source[1:]:
        # low_idx = high_idx+1
        # if data source value is missing, fill missing values
        # if df_lower_priority[col_target_data_source].isna().sum() > 0:
        #     print("warning: there are missing values for data source")
        #     df_lower_priority[col_target_data_source] = df_lower_priority.dropna()[col_target_data_source].iloc[0]
        # if df_lower_priority[col_nontarget_data_source].isna().sum() > 0:
        #     print("warning: there are missing values for data source")
        #     df_lower_priority[col_nontarget_data_source] = df_lower_priority.dropna()[col_nontarget_data_source].iloc[0]
        # df_lower_priority[col_target_data_source] = lst_corresponding_target_data_sources[low_idx]
        # df_lower_priority[col_nontarget_data_source] = lst_corresponding_nontarget_data_sources[low_idx]
        df_higher_priority_valid = df_higher_priority.loc[df_higher_priority[col_data].dropna().index, :]
        for e_id, df_lower_priority_event in df_lower_priority.groupby("event_id"):
            # is there valid data in the priority dataset overlapping with this event? (if so, do not include this event)
            filter_overlap = (df_higher_priority_valid["date_time"] >= df_lower_priority_event["date_time"].min()) & (df_higher_priority_valid["date_time"] <= df_lower_priority_event["date_time"].max())
            if filter_overlap.sum() > 0:
                continue
            # since there is no overlap, is there valid data from the target dataset in this time series? If so, include it
            df_lower_priority_event_valid = df_lower_priority_event.loc[df_lower_priority_event[col_data].dropna().index, :]
            if len(df_lower_priority_event_valid) > 0:
                lst_dfs_lower_priority_events_to_keep.append(df_lower_priority_event)
                # it is possible that this event needs to actually REPLACE a higher priority event if it's there because of an alternative threhsold selection process; if that's the case, drop that event from df_higher_priority
                filter_overlap = (df_higher_priority["date_time"] >= df_lower_priority_event["date_time"].min()) & (df_higher_priority["date_time"] <= df_lower_priority_event["date_time"].max())
                if filter_overlap.sum() > 0:
                    df_higher_priority = df_higher_priority.drop(df_higher_priority[filter_overlap].index)
        # update the df_higher_priority dataset
        lst_dfs_lower_priority_events_to_keep.append(df_higher_priority)
        df_higher_priority = pd.concat(lst_dfs_lower_priority_events_to_keep, ignore_index=True)

    # combine time series and event summary datasets ensuring matching indices
    df_all_summaries = pd.concat(lst_df_summaries_in_order_of_preferred_data_source, ignore_index=True)
    lst_df_tseries = []
    lst_s_summary_rows = []
    new_e_id = -1
    for idx_group, df_priority_event in df_higher_priority.groupby(["event_id", col_target_data_source]):
        new_e_id += 1
        e_id, d_src = idx_group
        # assign tseries event id
        df_priority_event["event_id"] = new_e_id
        # find corresponding summary row
        filter_smry = (df_all_summaries["event_id"] == e_id) & (df_all_summaries[col_target_data_source] == d_src) & (df_all_summaries["event_start"] == df_priority_event["date_time"].min()) #compare_series_to_scalar(df_all_summaries["precip_depth_mm"], df_priority_event["mm"].sum()) & compare_series_to_scalar(df_all_summaries["surge_peak_ft"], df_priority_event["surge_ft"].max())
        if (filter_smry.sum() > 1) or (filter_smry.sum() == 0):
            sys.exit("ERROR: CORRESPONDING EVENT SUMMARY NOT FOUND")

        s_summary_row = df_all_summaries[filter_smry].iloc[0]
        ## update event id to match
        s_summary_row["event_id"] = new_e_id
        s_summary_row.name = new_e_id
        lst_df_tseries.append(df_priority_event)
        lst_s_summary_rows.append(s_summary_row)

    df_combined_tseries = pd.concat(lst_df_tseries, ignore_index=True)
    df_combined_summaries = pd.concat(lst_s_summary_rows, ignore_index=True, axis = 1).T

    return df_combined_tseries, df_combined_summaries

# def classify_event(df_wlevel_rain_summary_stats_og):
#     df_wlevel_rain_summary_stats = df_wlevel_rain_summary_stats_og.copy()
#     s_data_sources = pd.Series(df_wlevel_rain_summary_stats.data_source.unique())
#     if 'event_selection_category' not in df_wlevel_rain_summary_stats.columns:
#         df_wlevel_rain_summary_stats.insert(2, 'event_selection_category', "dummy")

#     s_event_cat = df_wlevel_rain_summary_stats.event_selection_category.copy()

#     s_data_sources_both = s_data_sources[s_data_sources.str.contains("and")]
#     s_data_sources_remaining = s_data_sources[~s_data_sources.isin(s_data_sources_both)]
#     s_data_sources_surge = s_data_sources_remaining[s_data_sources_remaining.str.contains("surge")]
#     s_data_sources_remaining = s_data_sources_remaining[~s_data_sources_remaining.isin(s_data_sources_surge)]
#     s_data_sources_rain = s_data_sources_remaining

#     s_event_cat[df_wlevel_rain_summary_stats.data_source.isin(s_data_sources_both)] = "both"
#     s_event_cat[df_wlevel_rain_summary_stats.data_source.isin(s_data_sources_surge)] = "surge"
#     s_event_cat[df_wlevel_rain_summary_stats.data_source.isin(s_data_sources_rain)] = "rain"
#     df_wlevel_rain_summary_stats.loc[:, "event_selection_category"] = s_event_cat

#     return df_wlevel_rain_summary_stats


def calculate_plotting_positions(df_compound_event_summary_stats):
    """
    Plotting posiitons are calculated for each statistic after first dropping all missing values
    """
    # computing plotting positions
    lst_colnames = []
    lst_s_plting_positions = []
    for col, series in df_compound_event_summary_stats.items():
        series_no_na = series.dropna()
        # if a column has all missing values, skip it
        if len(series_no_na) == 0:
            print("all missing values encountered in column {}".format(col))
            continue
        idx_event_ids = series_no_na.index
        s_plting_positions = pd.Series(stats.mstats.plotting_positions(series_no_na))
        s_plting_positions.name = col
        s_plting_positions.index = idx_event_ids
        lst_s_plting_positions.append(s_plting_positions)
    # combine plotting position results into a single dataframe
    df_compound_event_summary_plotting_positions = pd.concat(lst_s_plting_positions, axis = 1)
    return df_compound_event_summary_plotting_positions

def calculate_quantile_correlations(df_compound_event_summary_stats):
    df_compound_event_summary_plotting_positions = calculate_plotting_positions(df_compound_event_summary_stats)
    # compute correlations between possible variables of interest
    df_corrs = df_compound_event_summary_plotting_positions.dropna().corr()
    # initialize lists to contain the variable names and their correlations
    lst_var1 = []
    lst_var2 = []
    lst_corr = []
    # lst_str_combos = []

    for var1, row in df_corrs.iterrows():
        for var2, val in row.items():
            if "surge" in var2: # only gonna house surge statistics as var1
                continue
            if var2 in lst_var1:
                continue
            unimportant = False
            var1_surge_related = False
            var2_surge_related = False
            # correlation_already_analyzed = False
            if "data_source" in [var1, var2]:
                unimportant = True
            if "event_selection_category" in [var1, var2]:
                unimportant = True
            if "tstep" in var1:
                unimportant = True
            if "tstep" in var2:
                unimportant = True
            if "start" in var1:
                unimportant = True
            if "start" in var2:
                unimportant = True
            if "end" in var1:
                unimportant = True
            if "end" in var2:
                unimportant = True
            if "surge" in var1:
                var1_surge_related = True
            if "surge" in var2:
                var2_surge_related = True
            if var1_surge_related and var2_surge_related:
                unimportant = True
            if sum([var1_surge_related, var2_surge_related])==0:
                unimportant = True
            # unimportant = False
            if unimportant:
                continue
            # s_combo_1 = "{}_{}_{}".format(var1, var2, val)
            # s_combo_2 = "{}_{}_{}".format(var2, var1, val)

            # if (s_combo_1 in lst_str_combos) or (s_combo_2 in lst_str_combos):
            #     unimportant = True
            else:
                lst_var1.append(var1)
                lst_var2.append(var2)
                lst_corr.append(df_corrs.loc[var1, var2])
                # lst_str_combos.append(s_combo_1)
                # lst_str_combos.append(s_combo_2)

    df_corrs_of_interest = pd.DataFrame(dict(
        var1 = lst_var1,
        var2 = lst_var2,
        corr = lst_corr
    ))

    # df_corrs_of_interest = df_corrs_of_interest.drop_duplicates()

    df_corrs_of_interest["abs_corr"] = df_corrs_of_interest["corr"].abs()

    # # remove redundant rows
    # lst_vars_inspected = []
    # lst_unique = []
    # for idx, row in df_corrs_of_interest.iterrows():
    #     df_dup_cors = df_corrs_of_interest[(df_corrs_of_interest.var1 == row.var2) & (df_corrs_of_interest.var2 == row.var1)]
    #     # lst_vars = [row.var1, row.var2]
    #     # lst_vars.sort()
    #     if len(df_dup_cors) > 0:
    #         lst_unique.append(False)
    #         print("duplicate encountered")
    #     else:
    #         # lst_vars_inspected.append(lst_vars)
    #         lst_unique.append(True)

    # df_corrs_of_interest = df_corrs_of_interest[lst_unique]
    return df_corrs_of_interest

def plot_event_statistic_relationships(df_variables_selected,
                                       df_compound_event_summary_stats, fig_title = None,
                                       s_fig_titles = None, s_savefig_fnames = None):
    
    df_compound_event_summary_plotting_positions = calculate_plotting_positions(df_compound_event_summary_stats)
    # df_corrs_of_interest = calculate_quantile_correlations(df_compound_event_summary_stats)
    count = -1
    for vars, cors in df_variables_selected.iterrows():
        count += 1
        fig, axes = plt.subplots(nrows = 1, ncols = 2, dpi=150, figsize = (8,4))
        df_plting_positions = df_compound_event_summary_plotting_positions.loc[:, [vars[0], vars[1]]]
        df_plting_positions.dropna().plot.scatter(vars[0], vars[1], ax = axes[0])
        df_realspace = df_compound_event_summary_stats.loc[:, [vars[0], vars[1]]]
        df_realspace.dropna().plot.scatter(vars[0], vars[1], ax = axes[1])
        fig_text1 = "Correlation of empirical quantiles (cunnane plotting position): {:.2f}".format(cors["corr"])
        txt_fig_cap_txt = "{}".format(fig_text1)
        fig.text(.5, -0.05, txt_fig_cap_txt, ha='center')
        if s_fig_titles is not None:
            fig.suptitle(s_fig_titles.iloc[count])
        if fig_title is not None:
            fig.suptitle(fig_title)
        fig.tight_layout()
        if s_savefig_fnames is not None:
            plt.savefig(s_savefig_fnames.iloc[count], bbox_inches='tight')
            plt.clf()

def analyze_correlations(df_corrs_of_interest, var_of_interest, corrs_of_interest, vars_to_filter_out=None):
    primary_selection = df_corrs_of_interest.var1.str.contains(var_of_interest, regex=True) | df_corrs_of_interest.var2.str.contains(var_of_interest, regex=True)
    secondary_selection = df_corrs_of_interest.var1.str.contains(corrs_of_interest, regex=True) | df_corrs_of_interest.var2.str.contains(corrs_of_interest, regex=True)
    if vars_to_filter_out is not None:
        filterout_selection = ~df_corrs_of_interest.var1.str.contains(vars_to_filter_out) & ~df_corrs_of_interest.var2.str.contains(vars_to_filter_out)
        selection = primary_selection & secondary_selection & filterout_selection
    else:
        selection = primary_selection & secondary_selection

    df_correlations_to_analyze = df_corrs_of_interest[selection].sort_values("abs_corr", ascending = False)

    return df_correlations_to_analyze


def append_list_of_selected_vars(var1_manal_selection, var2_manual_selection, lst_s_selections, df_correlations_to_analyze):
    s_variable_selection = df_correlations_to_analyze.set_index(['var1', "var2"]).loc[(var1_manal_selection,var2_manual_selection)]
    lst_s_selections.append(s_variable_selection)
    return lst_s_selections

def save_event_timeseries_plots(df_compound_event_tseries, df_compound_event_summary_stats,
                                df_compound_event_summary_plotting_positions):
    import matplotlib.gridspec as gridspec
    quant_low = 0.02
    quant_high = 0.98
    # surge plot lims
    surge_low = df_compound_event_tseries.surge_ft.min()
    surge_high = df_compound_event_tseries.surge_ft.max()
    surge_lim = (surge_low, surge_high)
    # surge plot lims
    wlevel_low = df_compound_event_tseries.waterlevel_ft.min()
    wlevel_high = df_compound_event_tseries.waterlevel_ft.max()
    wlevel_lim = (wlevel_low, wlevel_high)
    # rain plot lims
    rain_low = 0
    rain_high = df_compound_event_tseries.mm_per_hr.max()
    rain_lim = (rain_low, rain_high)
    for e_id in df_compound_event_tseries.index.get_level_values(0).unique():
        s_event_summary = df_compound_event_summary_stats.loc[e_id]
        s_event_plotting_positions = df_compound_event_summary_plotting_positions.loc[e_id]
        df_event_tseries = df_compound_event_tseries.loc[(e_id, slice(None))]
        fig = plt.figure(figsize=(6, 6))
        gs = gridspec.GridSpec(2, 1, height_ratios=[1, 1.5])
        # Plot y1 on the first y-axis
        ax1 = plt.subplot(gs[1])
        ax1.plot(df_event_tseries.index, df_event_tseries['surge_ft'], color='r')
        # ax1.xticks(rotation=45)
        ax1.set_xlabel('')
        ax1.set_ylabel('surge_ft', color='r')
        ax1.set_ylim(surge_lim)
        ax1b = ax1.twinx()
        ax1b.plot(df_event_tseries.index, df_event_tseries['waterlevel_ft'], color='green')
        ax1b.set_ylabel('water level (feet)', color='green')
        ax1b.set_ylim(wlevel_lim)

        # Create a second y-axis sharing the same x-axis
        ax2 = plt.subplot(gs[0])
        # ax2 = ax1.twinx()
        ax2.bar(df_event_tseries.index, df_event_tseries['mm_per_hr'], color='b', align='edge', width = pd.Series(df_event_tseries.index).diff().mode()[0])
        ax2.set_ylabel('mm_per_hr', color='b')
        ax2.set_xlabel('')
        ax2.set_xticks([])
        # Reverse the y-axis direction
        ax2.set_ylim(rain_lim)
        ax2.set_ylim(ax2.get_ylim()[::-1])
        
        t1 = "Peak water level (ft) = {:.1f}".format(df_event_tseries.waterlevel_ft.max())
        t2 = "Peak surge (ft) = {:.1f} (empirical quantile: {:.2f})".format(s_event_summary.surge_peak_ft,s_event_plotting_positions.surge_peak_ft)

        t3 = "Total rain depth (mm) = {:.1f} (empirical quantile: {:.2f})".format(s_event_summary.precip_depth_mm, s_event_plotting_positions.precip_depth_mm)
        t4 = "Peak rain intensity (mm/hr) = {:.1f}".format(s_event_summary.precip_max_intensity)
        if np.isnan(s_event_summary.precip_depth_mm):
            t3 = t4 = t5 = ""
        else:
            t5 = "Lag between peak surge and peak rain intensity:= {}".format(s_event_summary.surge_peak_after_rain_peak)
        
        txt_fig_cap_txt = "{}\n{}\n{}\n{}\n{}".format(t1, t2, t3, t4, t5)
        fig.text(.15, -0.10, txt_fig_cap_txt, ha='left')

        # fig.tight_layout()

        plt.savefig(dir_compound_events_plots + "event_{}.png".format(e_id), bbox_inches='tight')
        plt.clf()

