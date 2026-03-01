#%% import libararies
from _inputs import *
from _utils import *
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import shutil
import numpy as np
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
import dask.dataframe as dd
from tqdm import tqdm


# rain_intensity_stat = "max_4hr_mean_mm_per_hr"

# rain_cutoff_to_accept_0_precip_mm = 3
n_cutoff = 100
rtol = 0.01
atol = 0.1
Path(dir_storm_rescaling).mkdir(parents=True, exist_ok=True)
dir_over_sim = dir_storm_rescaling + "all_sims_rescaled/"
Path(dir_over_sim).mkdir(parents=True, exist_ok=True)
# load data
wlevel_data_source_for_rescaling = "6min_surge_data"
rain_data_source_for_rescaling = "mrms"

df_all_events_summary = pd.read_csv(f_combined_event_summaries, 
                                    parse_dates=["event_start", "event_end", "surge_peak_tstep",
                                                "precip_max_intensity_tstep"])

df_surge_events = df_all_events_summary[df_all_events_summary["event_type"] == "surge"]
df_rain_events = df_all_events_summary[df_all_events_summary["event_type"] == "rain"]
df_combo_events = df_all_events_summary[df_all_events_summary["event_type"] == "compound"]

df_obs_mrms = pd.read_csv(f_mrms_rainfall, parse_dates=True, index_col = "time")
df_obs_mrms = df_obs_mrms.drop(columns = ["bias_corrected"])

ds_event_tseries = xr.open_dataset(f_combined_event_tseries_netcdf)
#
# if len(pd.unique(ds_event_tseries.event_start.values)) != len(ds_event_tseries.event_start.values):
#     print("WARNING: NON-UNIQUE EVENT START DATES ENCOUNTERED; THIS WILL CAUSE PROBLEMS")

# ds_surge_events = xr.open_dataset(f_combined_event_summary_and_tseries_netcdf).sel(event_selection_category = "surge")
# ds_rain_events = xr.open_dataset(f_combined_event_summary_and_tseries_netcdf).sel(event_selection_category = "rain")
# ds_combo_events = xr.open_dataset(f_combined_event_summary_and_tseries_netcdf).sel(event_selection_category = "both")

dic_event_stats = {}

dic_event_summaries = {#"all_events":df_all_events_summary,
                       "compound_events":df_combo_events,
                       "rain_events":df_rain_events,
                       "surge_events":df_surge_events}

# dic_event_tseries = {#"all_events":df_all_events_summary,
#                        "compound_events":ds_combo_events,
#                        "rain_events":ds_rain_events,
#                        "surge_events":ds_surge_events}

dic_simulations = {}
for event_type_key in dic_event_summaries:
    dic_simulations[event_type_key] = pd.read_csv(f"{dir_simulate_from_copula}{event_type_key}_simulated_event_summaries.csv", 
                                                  index_col = 0)


non_event_data_cols = ["rain_event", "surge_event", "compound_event", "copula_source", "year", "event_id"]


# loop through and distribution event stats over time
# create series to make sure indices line up when relating NN selected events with observed events

# work
# event_type_key = "rain_events"
# sim_event_id = 183
# hydro_type = "surge"
# hydro_type = "rain"
# n_to_try = 5
verbose = False
n_neighbors = 7
n_cutoff = 50
# n_events_per_writing_intermediate_output = 10
lst_skip = []# ['compound_events', 'rain_events'] # these are ones that have been completed
clear_tseries_outputs = True
pick_up_where_left_off = False

#%% stochastic storm rescaling
lst_skip = ['compound_events', 'rain_events', "surge_events"]
for event_type_key in dic_event_summaries:
    modeled_stats = format_fitted_cdf_values_for_fitting_copula(event_type_key = event_type_key).columns
    rain_intensity_stat = None
    for col in modeled_stats:
        if col == "precip_depth_mm":
            continue
        if "surge" in col:
            continue
        else:
            rain_intensity_stat = col
    if event_type_key in lst_skip:
        print(f"Skipping {event_type_key} because it is in lst_skip")
        continue
    dir_tseries_outputs = f"{dir_over_sim}{event_type_key}_tseries/"
    fpath_sim_summaries = f"{dir_over_sim}{event_type_key}_simulation_summaries_all.csv"
    df_obs_event_summaries = dic_event_summaries[event_type_key]
    if event_type_key in ["rain_events", "surge_events"]:
        max_obs_strm_duration = max_strm_len  + np.timedelta64(timeseries_buffer_around_peaks_h*2, "h")# for rain and surge events, enforce maximum event duration defined in _inputs.py
    elif event_type_key == "compound_events":
        max_obs_strm_duration = (df_obs_event_summaries["event_end"] - df_obs_event_summaries["event_start"]).max()*compound_event_time_window_multiplier # allowing simulated time series to be longer than observed
    df_sim_smry = dic_simulations[event_type_key].drop(columns = non_event_data_cols)
    if pick_up_where_left_off and os.path.exists(fpath_sim_summaries):
        df_sim_event_summaries_previous = pd.read_csv(fpath_sim_summaries)
        df_sim_event_summaries_previous = df_sim_event_summaries_previous.set_index(["sim_event_id", "rescaling_attempt"])
        event_id_to_resume_with = df_sim_event_summaries_previous.index.max()[0]
        # exclude last event id in case it was incomplete
        df_sim_event_summaries_previous = df_sim_event_summaries_previous.loc[pd.IndexSlice[0:(event_id_to_resume_with-1), :], :]
        if event_id_to_resume_with == df_sim_smry.index.max():
            print(f"{event_type_key} have already been rescaled. Not rescaling since pick_up_where_left_off={pick_up_where_left_off}")
            continue
        # subset summary dataset 
        df_sim_smry = df_sim_smry.loc[event_id_to_resume_with:, :]
        print(f"Resuming rescaling {event_type_key} beginning with event id {event_id_to_resume_with}")
    else: # if not picking up where left off and clear outputs is true, clear outputs
        if clear_tseries_outputs:
            try:
                shutil.rmtree(dir_tseries_outputs)
                print(f"Deleteing and recreating folder {dir_tseries_outputs}")
            except:
                pass
    Path(dir_tseries_outputs).mkdir(parents=True, exist_ok=True)

    # initialize simulation event summary table
    cols_data = dic_simulations[event_type_key].drop(columns = non_event_data_cols).columns
    dic_col_rename = {}
    for dvar in cols_data:
        dic_col_rename[dvar] = f"{dvar}_target"
    df_sim_event_summaries = dic_simulations[event_type_key] 
    df_sim_event_summaries = df_sim_event_summaries.rename(columns = dic_col_rename)
    # df_sim_event_summaries.loc[:, cols_data] = np.nan
    # df_sim_event_summaries.loc[:, ["rain_rescaling_problems", "surge_rescaling_problems"]] = ""
    df_sim_event_summaries.index.name = "sim_event_id"
    lst_df_rain_event_summaries = []
    lst_df_surge_event_summaries = []

    # identify neighboring events to rescale for rainfall and storm surge time series
    s_cols = pd.Series(df_sim_smry.columns) # all simulation summary columns
    surge_data_cols = s_cols[s_cols.str.contains("surge") & ~s_cols.str.contains("after")].values
    # subset all events with nonnull peak surge coming from desired data source
    fltr_storms_to_rescale = (~df_all_events_summary["surge_peak_ft"].isna()) & (df_all_events_summary["wlevel_data_source"] == wlevel_data_source_for_rescaling)
    df_obs_target_source_surge = df_all_events_summary[fltr_storms_to_rescale]
    # subset all events with nonnull precip depths and coming from desired data source
    rain_data_cols = s_cols[~s_cols.str.contains("surge")].values
    fltr_storms_to_rescale = (~df_all_events_summary["precip_depth_mm"].isna()) & (df_all_events_summary["rain_data_source"] == rain_data_source_for_rescaling)
    df_obs_target_source_rain = df_all_events_summary[fltr_storms_to_rescale]
    sys.exit("I think i should combine all event statistics into a single nearest neighbors sample")
    df_neighbors_rain = return_df_of_neighbors_w_rank_weighted_slxn_prob(df_obs_target_source_rain, df_sim_smry, rain_data_cols, n_neighbors)
    df_neighbors_surge = return_df_of_neighbors_w_rank_weighted_slxn_prob(df_obs_target_source_surge, df_sim_smry, surge_data_cols, n_neighbors)
    nghbr_idx = df_neighbors_rain.columns # same for both
    # but just to make sure
    if (~(df_neighbors_rain.columns == df_neighbors_surge.columns)).sum() > 0:
        sys.exit("rain and surge neighbor dataframe do not have same column headers")
    # lst_tmp_tseries = []
    # lst_tmp_smries = []
    print(f"Rescaling {len(df_sim_smry)} {event_type_key}....")

    for n_events_processed, sim_event_id in tqdm(enumerate(df_sim_smry.index)):
        lst_df_surge_sim_tseries = []
        lst_df_rain_sim_tseries = []
        # work
        # n_events_processed, sim_event_id = 0, 0
        # n_events_processed, sim_event_id = 1, 1
        # lst_tmp_tseries = []
        # lst_tmp_smries = []
        # end work
        trial_count = 0
        for hydro_type in ["rain", "surge"]:
        # for sim_event_id, obs_event_id in tqdm(s_nearest_obs_event.items()):
            # create new row for final df_event_summary dataframe which will also be indexed on the rescaling attempt
            lst_df_event_summary_rescaling_attempts = []
            for i in np.arange(n_neighbors):
                df_event_summary_row = df_sim_event_summaries.loc[sim_event_id, :].to_frame().T.copy()
                df_event_summary_row.index.name = df_sim_event_summaries.index.name
                df_event_summary_row["rescaling_attempt"] = i
                df_event_summary_row = df_event_summary_row.reset_index().set_index(["sim_event_id", "rescaling_attempt"])
                lst_df_event_summary_rescaling_attempts.append(df_event_summary_row)
            df_event_summary_row = pd.concat(lst_df_event_summary_rescaling_attempts)

            s_n_problems = pd.Series(data = 0, index = nghbr_idx).astype(int)
            obs_event_selection_made = False
            n_rescaling_attempts = -1
            # work
            trial_count += 1
            # end work
            while obs_event_selection_made == False:                
                n_rescaling_attempts += 1
                # if the previous attempt had no problems or if all neighbors have at least 1 problem, break the loop
                prev_nbr_id = None
                if n_rescaling_attempts > 0:
                    prev_nbr_id = nghbr_idx[n_rescaling_attempts-1]
                # if previous rescaling attempt resulted in no problems or if all neighboring events have been exhausted, break the loop
                if ((prev_nbr_id is not None) and (s_n_problems.loc[prev_nbr_id] == 0)) or (n_rescaling_attempts >= len(nghbr_idx)):
                    if hydro_type == "rain":
                        lst_df_rain_event_summaries.append(df_event_summary_row)
                    if hydro_type == "surge":
                        lst_df_surge_event_summaries.append(df_event_summary_row)
                    obs_event_selection_made = True
                    break

                # break
                # distribute observed event over time
                # get the time series
                if hydro_type == "surge":
                    #
                    nbr_id, event_start, event_end, obs_event_id,\
                          row_obs_event, row_sim_event = retrieve_obs_and_sim_info_for_rescaling(surge_data_cols, df_neighbors_surge,
                                                                                                  sim_event_id, n_rescaling_attempts,
                                                                                                    df_all_events_summary, df_sim_smry)
                    #
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "surge_rescaling_problems"] = ""
                    obs_tseries_surge = ds_event_tseries.sel(date_time = slice(event_start,event_end))["surge_ft"].reset_coords(drop = True).to_dataframe()
                    if obs_tseries_surge.max().values != df_all_events_summary.loc[obs_event_id,'surge_peak_ft']:
                        sys.exit("ERROR: THE EVENT SUMMARY AND EVENT TIME SERIES STATS DON'T LINE UP")
                    surge_stat = "surge_peak_ft"
                    diff = row_sim_event[surge_stat] - row_obs_event[surge_stat]
                    sim_time_series = obs_tseries_surge + diff
                    if not np.isclose(sim_time_series.max().values, row_sim_event[surge_stat]):
                        sys.exit(F"ERROR: {surge_stat} FROM SIMULATED EVENT TIME SERIES DOES NOT EQUAL THE SIMULATED EVENT STATISTIC")
                    # re-index 
                    tstep = pd.Series(sim_time_series.index.diff()).mode().iloc[0]
                    sim_time_series.index = pd.timedelta_range(start = "0 hours", periods = len(sim_time_series), freq=tstep)
                    # sim_time_series.index.name = "timestep"
                    # lst_df_hydro_sims.append(sim_time_series)
                    if sim_time_series.index.max() > max_obs_strm_duration:
                        txt_problems = f"|WARNING: simulated water level tseries duration is longer than max observed storm length ({sim_time_series.index.max()} vs. {max_obs_strm_duration})"
                        df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "surge_rescaling_problems"] += txt_problems
                        s_n_problems.loc[nbr_id] += 1
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "sim_surge_duration"] = sim_time_series.index.max()
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "surge_peak_ft"] = (sim_time_series).max().values[0]
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "obs_surge_peak_ft"] = row_obs_event["surge_peak_ft"]
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], f"obs_event_id_{hydro_type}"] = obs_event_id
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], f"obs_event_start_{hydro_type}"] = event_start
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], f"obs_event_end_{hydro_type}"] = event_end
                    # add relevant identifiers for future indexing
                    sim_time_series.index.name = "timestep"
                    sim_time_series["sim_event_id"] = sim_event_id
                    sim_time_series["rescaling_attempt"] = n_rescaling_attempts
                    sim_time_series = sim_time_series.reset_index().set_index(["sim_event_id", "rescaling_attempt", "timestep"])
                    lst_df_surge_sim_tseries.append(sim_time_series)
                    # lst_tmp_smries.append(df_event_summary_row)
                    # lst_tmp_tseries.append(sim_time_series)
                elif hydro_type == "rain":
                    # data_cols, df_neighbors, sim_event_id, n_rescaling_attempts, df_all_events_summary, df_sim_smry = rain_data_cols, df_neighbors_rain, sim_event_id, n_rescaling_attempts, df_all_events_summary, df_sim_smry
                    nbr_id, event_start, event_end, obs_event_id,\
                          row_obs_event, row_sim_event = retrieve_obs_and_sim_info_for_rescaling(rain_data_cols, df_neighbors_rain,
                                                                                                  sim_event_id, n_rescaling_attempts,
                                                                                                    df_all_events_summary, df_sim_smry)
                    # load time series of selected neighbor
                    obs_tseries_mm_p_h = df_obs_mrms.loc[event_start:event_end, :]
                    obs_tseries_mm_p_h = obs_tseries_mm_p_h.rename(columns = dict(rain_mean = "mm_per_hr"))
                    tstep_tdelta = pd.Series(obs_tseries_mm_p_h.index).diff().mode().iloc[0]
                    # obs_tseries_mm_p_h = ds_event_tseries.sel(date_time = slice(event_start,event_end))["mm_per_hr"].reset_coords(drop = True).to_dataframe()
                    tstep_hr = pd.Series(obs_tseries_mm_p_h.index).diff().mode().iloc[0] / np.timedelta64(1, "h")
                    # trim leading and trailing zeros
                    if obs_tseries_mm_p_h["mm_per_hr"].values.sum()>0:
                        idx_last = obs_tseries_mm_p_h[obs_tseries_mm_p_h["mm_per_hr"] != 0].index.max()
                        idx_first = obs_tseries_mm_p_h[obs_tseries_mm_p_h["mm_per_hr"] != 0].index.min()
                        obs_tseries_mm_p_h = obs_tseries_mm_p_h.loc[idx_first:idx_last,:]
                    # initialize rescaling problems column
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "rain_rescaling_problems"] = ""
                    # check to make sure stats line up with event summaries
                    obs_tseries_mm = ds_event_tseries.sel(date_time = slice(event_start,event_end))["mm"].reset_coords(drop = True).to_dataframe()
                    if not np.isclose(obs_tseries_mm.sum().values, df_all_events_summary.loc[obs_event_id,'precip_depth_mm']):
                        sys.exit("ERROR: THE EVENT SUMMARY AND EVENT TIME SERIES STATS DON'T LINE UP")
                    # if the target depth is low and the intensity is such that a single timestep at that intensity would yield rainfall greater
                    # than the target rain depth, consider this a 0 precipitation event;
                    # also consider this a zero precipitation if the selected nearest neighbor has zero precipitation
                    min_rainfall_minutes = row_sim_event["precip_depth_mm"] / row_sim_event[rain_intensity_stat] * 60
                    target_tstep_min = pd.Series(obs_tseries_mm.index.diff()).mode().iloc[0] / np.timedelta64(1, "m")
                    if (min_rainfall_minutes < target_tstep_min) or (obs_tseries_mm_p_h["mm_per_hr"].values.sum() == 0): # consider this event to have no rainfall
                        sim_time_series = pd.DataFrame(columns=obs_tseries_mm_p_h.columns,
                                                           index = obs_tseries_mm_p_h.index - obs_tseries_mm_p_h.index.min())
                        # df_sim_time_series.index.name = "timestep"
                        # df_sim_time_series["sim_event_id"] = sim_event_id
                        # df_sim_time_series["rescaling_attempt"] = n_rescaling_attempts
                        sim_time_series.loc[:, :] = 0
                        # sim_time_series = df_sim_time_series["mm_per_hr"]
                        # lst_idx_sim_impossible_to_rescale.append(sim_event_id)
                        # txt_problems = f"|skipping rescaling event rainfall because it would require a shorter time series time step than {target_tstep_min} min to achieve target rain depth"
                        # df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "rain_rescaling_problems"] += txt_problems
                        # s_n_problems.loc[nbr_id] += 1
                        # print(txt_problem)
                        s_time_series_renameed = sim_time_series["mm_per_hr"]
                        s_time_series_renameed.name = "mm_per_hr"
                        sim_max_intensity = compute_sim_max_mean_rain_intensity(s_time_series_renameed.to_frame(), rain_intensity_stat)
                        sim_precip_depth_mm = (sim_time_series["mm_per_hr"]*tstep_hr).sum()
                    else: # otherweise, attempt rescaling
                        # verbose = False
                        sim_time_series, txt_problems = rescale_rainfall_timeseries(row_sim_event, obs_tseries_mm_p_h, max_obs_strm_duration, n_cutoff, rtol, atol, rain_intensity_stat=rain_intensity_stat, verbose = False)
                        # make sure that the timesteps match the target timestep
                        if (sim_time_series.index.diff().dropna() != tstep_tdelta).sum() > 0:
                            sys.exit("irregular timestep encountered")
                        if len(txt_problems) > 0:
                            df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "rain_rescaling_problems"] += txt_problems
                            s_n_problems.loc[nbr_id] += 2
                        sim_max_intensity = compute_sim_max_mean_rain_intensity(sim_time_series["mm_per_hr"].to_frame(), rain_intensity_stat)
                        sim_precip_depth_mm = (sim_time_series["mm_per_hr"]*tstep_hr).sum()
                        if not np.isclose(sim_max_intensity, row_sim_event[rain_intensity_stat], rtol=rtol, atol=atol):
                            txt_problems = f"|WARNING: simulated max 1 hr mean intensity is not within tolerance of target value ({sim_max_intensity:.3f} vs. {row_sim_event[rain_intensity_stat]:.3f})"
                            df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "rain_rescaling_problems"] += txt_problems
                            s_n_problems.loc[nbr_id] += 2
                            if verbose:
                                print(txt_problems)
                        if not np.isclose(sim_precip_depth_mm, row_sim_event['precip_depth_mm'], rtol=rtol, atol=atol):
                            txt_problems = f"|WARNING: simulated depth is not within tolerance of target value ({sim_precip_depth_mm:.3f} vs. {row_sim_event['precip_depth_mm']:.3f})"
                            df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "rain_rescaling_problems"] += txt_problems
                            s_n_problems.loc[nbr_id] += 2
                            if verbose:
                                print(txt_problems)
                        if sim_time_series.index.max() > max_obs_strm_duration:
                            txt_problems = f"|WARNING: simulated rainfall tseries duration is longer than max observed storm length ({sim_time_series.index.max()} vs. {max_obs_strm_duration})"
                            df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "rain_rescaling_problems"] += txt_problems
                            s_n_problems.loc[nbr_id] += 2
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "sim_rain_duration"] = sim_time_series.index.max()
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], rain_intensity_stat] = sim_max_intensity
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "precip_depth_mm"] = sim_precip_depth_mm
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "obs_rain_intensity_stat"] = row_obs_event[rain_intensity_stat]
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], "obs_precip_depth_mm"] = row_obs_event["precip_depth_mm"]
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], f"obs_event_id_{hydro_type}"] = obs_event_id
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], f"obs_event_start_{hydro_type}"] = event_start
                    df_event_summary_row.loc[pd.IndexSlice[sim_event_id, n_rescaling_attempts], f"obs_event_end_{hydro_type}"] = event_end
                    # add relevant identifiers for future indexing
                    sim_time_series.index.name = "timestep"
                    sim_time_series["sim_event_id"] = sim_event_id
                    sim_time_series["rescaling_attempt"] = n_rescaling_attempts
                    sim_time_series = sim_time_series.reset_index().set_index(["sim_event_id", "rescaling_attempt", "timestep"])
                    lst_df_rain_sim_tseries.append(sim_time_series)
                    # lst_tmp_smries.append(df_event_summary_row)
                    # lst_tmp_tseries.append(sim_time_series)
        # after processing event, write event-specific time series and all event summaries processed so far
        if len(lst_df_surge_sim_tseries) > 1:
            s_surge_tseries = pd.concat(lst_df_surge_sim_tseries)
        else:
            s_surge_tseries = lst_df_surge_sim_tseries[0]
        if len(lst_df_rain_sim_tseries) > 1:
            df_rain_tseries = pd.concat(lst_df_rain_sim_tseries)
        else:
            df_rain_tseries = lst_df_rain_sim_tseries[0]

        s_surge_tseries = s_surge_tseries.astype(float)
        df_rain_tseries = df_rain_tseries.astype(float)
        
        df_sim_event_tseries = pd.concat([s_surge_tseries, df_rain_tseries], axis = 1)
        ds_sim_event_tseries = df_sim_event_tseries.to_xarray()
        d_encoding = {}
        for da_name in ds_sim_event_tseries.data_vars:
            d_encoding[da_name] = {"zlib":True}
        ds_sim_event_tseries.to_netcdf(f"{dir_tseries_outputs}{event_type_key}_simid_{sim_event_id}_time_series.nc", encoding=d_encoding)

        # write simulated event summary outputs to file
        df_surge_smries = pd.concat(lst_df_surge_event_summaries)
        df_rain_smries = pd.concat(lst_df_rain_event_summaries)
        overlapping_cols = df_surge_smries.columns[df_surge_smries.columns.isin(df_rain_smries.columns)]
        if verbose:
            print(f"removing the following columns from surge rescaling event summary dataframe for joining with the rain rescaling event summary dataframe:\n{overlapping_cols}")
        df_surge_smries = df_surge_smries.drop(columns = overlapping_cols)
        df_sim_event_summaries_processed = pd.concat([df_rain_smries, df_surge_smries], axis = 1)
        # if either the rain and surge rescaling problems columns are NA, rescaling was not attempted because a previous attempt had succeeded
        fltr_rescaling_completed = ~df_sim_event_summaries_processed["surge_rescaling_problems"].isna() | ~df_sim_event_summaries_processed["rain_rescaling_problems"].isna()
        df_sim_event_summaries_processed = df_sim_event_summaries_processed[fltr_rescaling_completed]
        if pick_up_where_left_off: # append newly processed data to previous dataframe for writing
            # first make sure columns have same datatypes
            dtype_mapping = df_sim_event_summaries_processed.dtypes.to_dict()
            df_sim_event_summaries_previous = df_sim_event_summaries_previous.astype(dtype_mapping)
            df_sim_event_summaries_processed = pd.concat([df_sim_event_summaries_previous, df_sim_event_summaries_processed])
        df_sim_event_summaries_processed.to_csv(f"{dir_over_sim}{event_type_key}_simulation_summaries_all.csv")

        # if n_to_try is not None:
        #     if n_events_processed >= n_to_try:
        #         sys.exit("end work")

#%% adjust timesteps of time series to match simulated event statistics (and expand the surge dataset and add in from the observed period shifted randomly)
## compute stats that are unique to one or more event types to make sure they are included in event summaries
lst_all_modeled_stats = []
for event_type_key in dic_event_summaries:
    modeled_stats = format_fitted_cdf_values_for_fitting_copula(event_type_key = event_type_key).columns
    for col in modeled_stats:
        if col not in lst_all_modeled_stats:
            lst_all_modeled_stats.append(col)
lst_modeled_stats_not_in_all = []
for event_type_key in dic_event_summaries:
    modeled_stats = format_fitted_cdf_values_for_fitting_copula(event_type_key = event_type_key).columns
    for stat in lst_all_modeled_stats:
        if stat not in modeled_stats:
            if stat not in lst_modeled_stats_not_in_all:
                lst_modeled_stats_not_in_all.append(stat)


ds_6min_water_levels_mrms_res = xr.open_dataset(f_6min_wlevel_resampled_to_mrms_res_nc)
# n_years_to_simulate = 1000
verbose = False
df_occurences_sim = pd.read_csv(f"{dir_simulate_from_copula}simulated_event_counts_{n_years_of_weather_to_generate}yrs.csv", index_col = "year")

lst_skip = [] #["rain_events", 'compound_events'] # 
for event_type_key in dic_event_summaries:
    modeled_stats = format_fitted_cdf_values_for_fitting_copula(event_type_key = event_type_key).columns
    rain_intensity_stat = None
    for col in modeled_stats:
        if col == "precip_depth_mm":
            continue
        if "surge" in col:
            continue
        else:
            rain_intensity_stat = col
    if event_type_key in lst_skip:
        print(f"while script is in development, skipping {lst_skip}")
        continue
    print(f"Finalizing time series for {event_type_key}")
    #
    col_event_type = np.nan
    for col in df_occurences_sim.columns:
        if col in event_type_key:
            col_event_type = col
    s_counts_by_year = df_occurences_sim[col_event_type]
    n_events_to_simulate = s_counts_by_year.sum()
    df_obs_summaries = dic_event_summaries[event_type_key]
    # end work
    lst_s_summaries = []
    lst_df_tseries = []
    # work
    # event_type_key = "compound_events"
    
    df_raw_copula_event_sims = dic_simulations[event_type_key]
    # try:
    #     df_sim_event_tseries = pd.read_csv(f"{dir_over_sim}{event_type_key}_simulation_time_series.csv", converters={'timestep': pd.to_timedelta}, index_col=["sim_event_id", "rescaling_attempt", "timestep"])# , parse_dates = ["timestep"]
    # except:
    #     pass
    # "sim_rain_duration", "sim_surge_duration"
    df_sim_event_summaries = pd.read_csv(f"{dir_over_sim}{event_type_key}_simulation_summaries_all.csv",
                                          converters={"sim_rain_duration": pd.to_timedelta, "sim_surge_duration": pd.to_timedelta},
                                            parse_dates = ["obs_event_start_rain", "obs_event_end_rain", "obs_event_start_surge", "obs_event_end_surge"],
                                            index_col=["sim_event_id", "rescaling_attempt"])


    n_valid_events_to_extract = n_events_to_simulate # len(df_raw_copula_event_sims[df_raw_copula_event_sims["year"] < n_years_to_simulate])
    # return desired number of valid rainfall events (where rain rescaling is na AND the simulated depth is not na)
    df_sim_event_summaries_valid_rainfall = df_sim_event_summaries[(df_sim_event_summaries["rain_rescaling_problems"].isna()) & (~df_sim_event_summaries["precip_depth_mm"].isna())].iloc[0:n_valid_events_to_extract, :]
    idx_valid_rain_event = df_sim_event_summaries_valid_rainfall.reset_index()["sim_event_id"].unique()
    if len(idx_valid_rain_event) != n_valid_events_to_extract:
        sys.exit("ERROR: NUMBER OF UNIQUE VALID RAINFALL EVENTS DOES NOT EQUAL THE TARGET NUMBER")
    # return corresopnding surge for those events using the first valid rescaling attempt (not necessarily the same as the rescaling attempt used for rainfall)
    df_sim_event_summaries_subset = df_sim_event_summaries.loc[pd.IndexSlice[idx_valid_rain_event, :], :]
    df_sim_event_summaries_valid_surge = df_sim_event_summaries_subset[(df_sim_event_summaries_subset["surge_rescaling_problems"].isna()) & (~df_sim_event_summaries_subset["surge_peak_ft"].isna())]
    idx_surge_first_valid_rescaling = df_sim_event_summaries_valid_surge.reset_index().groupby("sim_event_id")["rescaling_attempt"].min()
    if len(idx_surge_first_valid_rescaling.index) != n_valid_events_to_extract:
        sys.exit("ERROR: NUMBER OF UNIQUE VALID SURGE EVENTS DOES NOT EQUAL THE TARGET NUMBER")
    # this ensures that the FIRST valid surge event (if there were multiple somehow) is selected for rescaling
    df_sim_event_summaries_valid_surge = df_sim_event_summaries_valid_surge.loc[pd.IndexSlice[idx_valid_rain_event, idx_surge_first_valid_rescaling.values],:]
    # loop through to extract the first valid surge sim for each event id
    # work
    # lst_tseries_nc = []
    # lst_tseries_df = []
    # loop through each year and event id
    iloc_sim_event_id = -1
    for year, n_events in tqdm(s_counts_by_year.items()):
        for event_id in np.arange(1, n_events+1):
            # if (year == 102) and (event_id == 1):
            #     sys.exit("work")
            iloc_sim_event_id += 1
            sim_event_idx = idx_valid_rain_event[iloc_sim_event_id]
            try:
                dir_tseries_outputs = f"{dir_over_sim}{event_type_key}_tseries/"
                ds_sim_event_tseries = xr.open_dataset(f"{dir_tseries_outputs}{event_type_key}_simid_{sim_event_idx}_time_series.nc")
                # lst_tseries_nc.append(ds_sim_event_tseries)
                df_sim_event_tseries = ds_sim_event_tseries.to_dataframe()
                og_surge_peak_idxmax = df_sim_event_tseries.surge_ft.idxmax()
                og_surge_peak = df_sim_event_tseries.surge_ft.max()
            except:
                pass

            # converting indicies from time deltas to datetimes with an arbitrary start date (so I don't have to worry about negative time deltas which I guess are fine but harder to wrap my head around)
            row_rain_event = df_sim_event_summaries_valid_rainfall.reset_index().set_index("sim_event_id").loc[sim_event_idx,:]
            row_surge_event = df_sim_event_summaries_valid_surge.reset_index().set_index("sim_event_id").loc[sim_event_idx,:]

            idx_targets = row_rain_event[row_rain_event.index.str.contains("target")].index
            if (row_surge_event[idx_targets] != row_rain_event[idx_targets]).sum() > 0:
                sys.exit("Rain and sim event statistic targets do not line up")
            if row_rain_event["rescaling_attempt"] != row_rain_event["rescaling_attempt"]:
                sys.exit("work")

            # sys.exit("work")
            if len(row_rain_event.shape) > 1:
                sys.exit(f"Sim event id {sim_event_idx} is returning multiple rescaling attempts for rain event")
            if len(row_surge_event.shape) > 1:
                sys.exit(f"Sim event id {sim_event_idx} is returning multiple rescaling attempts for surge event")

            col_rain = df_sim_event_tseries.filter(regex='^(?!.*surge)').columns
        
            df_tseries_sim_rain = df_sim_event_tseries.loc[pd.IndexSlice[sim_event_idx, row_rain_event["rescaling_attempt"], :], col_rain].droplevel(["sim_event_id", "rescaling_attempt"])
            s_tseries_sim_surge = df_sim_event_tseries.loc[pd.IndexSlice[sim_event_idx, row_surge_event["rescaling_attempt"], :], "surge_ft"].droplevel(["sim_event_id", "rescaling_attempt"])
            
            # assign arbitrary date
            df_tseries_sim_rain.index = df_tseries_sim_rain.index + arbirary_start_date
            s_tseries_sim_surge.index = s_tseries_sim_surge.index + arbirary_start_date

            s_tseries_sim_surge_filled, df_tseries_sim_rain_filled, shifted_obs_start, shifted_obs_end = fill_rain_and_extend_surge_series(df_tseries_sim_rain, s_tseries_sim_surge, ds_6min_water_levels_mrms_res,
                                                                                                                                            prev_obs_start = row_surge_event.loc["obs_event_start_surge"],
                                                                                                                                              row_surge_event = row_surge_event,
                                                                                                                                              arbirary_start_date = arbirary_start_date)
            tstep = pd.Series(df_tseries_sim_rain_filled.index.diff()).mode().iloc[0]
            # identify time delay statistic
            s_tdelay_stat = row_rain_event[row_rain_event.index.str.contains('after')]
            if len(s_tdelay_stat) > 1:
                sys.exit("ERROR: more than 1 time delay statistic has been identified; need to figure out what's going on")
            # shift time series to honor time delay statistic
            if s_tdelay_stat.index[0] == 'surge_peak_after_rain_peak_h_target':
                # s_tseries_sim_surge_filled, s_tseries_sim_rain_filled
                current_max_mm_per_hr_tstep = df_tseries_sim_rain_filled["mm_per_hr"].idxmax()
                current_max_surge_tstep = s_tseries_sim_surge_filled.idxmax()
                current_surge_after_rain_peak_h = (current_max_surge_tstep - current_max_mm_per_hr_tstep) / np.timedelta64(1, "h")
                shift_surge_h = s_tdelay_stat.iloc[0] - current_surge_after_rain_peak_h
                tdelt_shift_surge = pd.Timedelta(shift_surge_h, "h").round(tstep) 
                # shift dataset
                s_tseries_sim_surge_filled_shifted = s_tseries_sim_surge_filled.copy()
                s_tseries_sim_surge_filled_shifted.index = s_tseries_sim_surge_filled.index + tdelt_shift_surge
                new_max_surge_tstep = s_tseries_sim_surge_filled_shifted.idxmax()
                new_surge_after_rain_peak_h = (new_max_surge_tstep - current_max_mm_per_hr_tstep) / np.timedelta64(1, "h")
                # confirm that shift was achived
                if abs(new_surge_after_rain_peak_h - s_tdelay_stat.iloc[0]) > (tstep / np.timedelta64(1, "h")):
                    sys.exit("ERROR: MISCALCULATED SHIFT. DESIRED TIME DELAY NOT ACHIEVED IN SIMULATED TIME SERIES")
            else:
                tdelay_stat = s_tdelay_stat.index[0]
                maxmean_surge_dur = int(tdelay_stat.split("surge")[0].split("_")[-1].split("hr")[0])
                maxmean_rain_dur = int(tdelay_stat.split("after_")[1].split("_")[0].split("hr")[0])
                # compute the needed shift to the surge time series
                ## rain stat
                df_rain_summaries = compute_event_timeseries_statistics(df_tseries_sim_rain_filled["mm_per_hr"].to_frame(), lst_time_durations_h_to_analyze=[maxmean_rain_dur], varname=df_tseries_sim_rain_filled["mm_per_hr"].name,
                                                        agg_stat = "mean")
                current_max_mean_mm_per_hr_tstep = df_rain_summaries.iloc[0,1]
                ## surge stat
                df_stats_surge = compute_event_timeseries_statistics(s_tseries_sim_surge_filled.to_frame(), lst_time_durations_h_to_analyze=[maxmean_surge_dur], varname="surge",
                                                                    agg_stat = "mean")
                current_max_mean_surge_tstep = df_stats_surge.iloc[0,1]
                ## current diff
                current_surge_after_rain_peak_h = (current_max_mean_surge_tstep - current_max_mean_mm_per_hr_tstep) / np.timedelta64(1, "h")
                ## compute needed time shift and round to nearest timestep
                shift_surge_h = s_tdelay_stat.iloc[0] - current_surge_after_rain_peak_h
                tdelt_shift_surge = pd.Timedelta(shift_surge_h, "h").round(tstep) 
                # shift dataset
                s_tseries_sim_surge_filled_shifted = s_tseries_sim_surge_filled.copy()
                s_tseries_sim_surge_filled_shifted.index = s_tseries_sim_surge_filled.index + tdelt_shift_surge
                # verify target shift has been achieved
                df_stats_surge = compute_event_timeseries_statistics(s_tseries_sim_surge_filled_shifted.to_frame(), lst_time_durations_h_to_analyze=[maxmean_surge_dur], varname="surge",
                                                                    agg_stat = "mean")
                new_max_mean_surge_tstep = df_stats_surge.iloc[0,1]
                new_surge_after_rain_peak_h = (new_max_mean_surge_tstep - current_max_mean_mm_per_hr_tstep) / np.timedelta64(1, "h")
                # confirm that shift was achived
                if abs(new_surge_after_rain_peak_h - s_tdelay_stat.iloc[0]) > (tstep / np.timedelta64(1, "h")):
                    sys.exit("ERROR: MISCALCULATED SHIFT. DESIRED TIME DELAY NOT ACHIEVED IN SIMULATED TIME SERIES")
            # else:
            #     sys.exit(f"ERROR: I still need to code how to handle a time delay statistic of {s_tdelay_stat.index[0]}")
            ## newly calculated surge peak after rain peak
            s_event_summary = pd.Series().astype(object)
            s_event_summary.loc[s_tdelay_stat.index[0].split("_target")[0]] = new_surge_after_rain_peak_h

            # reindex rain and surge time series to the maximum upper and lower bounds
            df_tseries_combined = pd.concat([df_tseries_sim_rain_filled, s_tseries_sim_surge_filled_shifted], axis = 1)
            if len(pd.Series(df_tseries_combined.index.diff()).dropna().unique()) > 1:
                # if the gap between the rainfall and surge event is so large that no rainfall actually falls during the surge event, this can sometimes introduce a gap the time series
                # reindexing...
                idx_name = df_tseries_combined.index.name
                constant_tstep_index = pd.date_range(start = df_tseries_combined.index.min(), end = df_tseries_combined.index.max(), freq = tstep)
                df_tseries_combined = df_tseries_combined.reindex(constant_tstep_index)     
                df_tseries_combined.index.name = idx_name
            # update so that the start date is the arbitrary start date
            df_tseries_combined.index = df_tseries_combined.index - df_tseries_combined.index.min() + arbirary_start_date
            df_tseries_combined = reindex_to_buffer_significant_rain_and_surge(df_tseries_combined, arbirary_start_date)
            #
            s_tseries_sim_surge_filled, df_tseries_sim_rain_filled, shifted_obs_start, shifted_obs_end = fill_rain_and_extend_surge_series(df_tseries_sim_rain = df_tseries_combined[col_rain],
                                                                                                                                            s_tseries_sim_surge = df_tseries_combined["surge_ft"],
                                                                                                                                            ds_6min_water_levels_mrms_res = ds_6min_water_levels_mrms_res,
                                                                                                                                              prev_obs_start = shifted_obs_start,
                                                                                                                                                row_surge_event = row_surge_event,
                                                                                                                                                arbirary_start_date = arbirary_start_date)
            # generate a randomized tide component
            s_obs_tseries_tide_shifted, s_tseries_sim_wlevel, tide_shift_hrs = generate_randomized_tide_component(s_tseries_sim_surge_filled, ds_6min_water_levels_mrms_res, t_window_h_to_shift_tidal_tseries, shifted_obs_start, shifted_obs_end)
            df_tseries_combined = pd.concat([s_tseries_sim_wlevel, s_tseries_sim_surge_filled, s_obs_tseries_tide_shifted, df_tseries_sim_rain_filled], axis = 1)
            df_tseries_combined.index.name = "timestep"

            # combine rain and surge event information into single pandas series
            row_surge_event = row_surge_event.rename(index = dict(rescaling_attempt = "rescaling_attempt_surge"))
            row_rain_event = row_rain_event.rename(index = dict(rescaling_attempt = "rescaling_attempt_rain"))
            all_indices = pd.Series(data = (list(row_surge_event.index) + list(row_rain_event.index))).unique()
            overlapping_indices = row_surge_event[row_surge_event.index.isin(row_rain_event.index)].index
            overlapping_indices_non_equal = row_surge_event.loc[overlapping_indices][row_surge_event.loc[overlapping_indices] != row_rain_event.loc[overlapping_indices]].index
            for idx in all_indices:
                if idx in overlapping_indices_non_equal:
                    if "surge" in idx:
                        s_event_summary.loc[idx] = row_surge_event.loc[idx]
                    elif ("rain" in idx) or ("precip" in idx) or ("mm_per_hr"):
                        s_event_summary.loc[idx] = row_rain_event.loc[idx]
                elif idx in overlapping_indices: # the values are equal so it doesn't matter which row i pull it from
                    s_event_summary.loc[idx] = row_surge_event.loc[idx]
                elif idx in row_surge_event.index:
                    s_event_summary.loc[idx] = row_surge_event.loc[idx]
                elif idx in row_rain_event.index:
                    s_event_summary.loc[idx] = row_rain_event.loc[idx]

            # reindex on tiemstep
            df_tseries_combined_time_indexed = df_tseries_combined.copy()
            # update length of time series to honor the max duration of observed events for rain and surge 
            dur_obs_max = (df_obs_summaries["event_end"] - df_obs_summaries["event_start"]).max()
            dur_event = df_tseries_combined_time_indexed.index.max() - df_tseries_combined_time_indexed.index.min()
            # depending on the type of event, handle the event duration

            if dur_event > dur_obs_max:
                og_idx = df_tseries_combined_time_indexed.index
                if event_type_key == "rain_events":
                    # reindex based on rainfall alone
                    s_rain = df_tseries_combined_time_indexed["mm_per_hr"]
                    idx_with_rain = s_rain[s_rain>0].index
                    df_rain = s_rain.loc[idx_with_rain.min():idx_with_rain.max()].to_frame()
                    df_rain["surge_ft"] = np.nan
                    df_rain_reindexed = reindex_to_buffer_significant_rain_and_surge(df_rain, arbirary_start_date = None)
                    min_rain_index = df_rain_reindexed.index
                    dur_rain = min_rain_index.max() - min_rain_index.min()
                    # compute 3 possible indices with either the minimum rain index, the max preceding time, or the max proceeding time
                    print('I want to change this so that if the peak surge occurs outside the time window of the rainfall, that the timeseries is extended just as much as it needs to be')
                    sys.exit('Does it really make sense to use dur_obs_max here rather than max_strm_len? I guess this could allow this event to be converted into a compound event.')
                    max_extension_from_first_or_last_rain = dur_obs_max - dur_rain
                    min_possible_first_tstep = max(min_rain_index.min() - max_extension_from_first_or_last_rain, og_idx.min())
                    max_possible_last_tstep = min(min_rain_index.max() + max_extension_from_first_or_last_rain, og_idx.max())
                    # identify which index yields the highest peak surge
                    lst_possible_idxs = [min_rain_index, 
                                         pd.date_range(min_possible_first_tstep, min_rain_index.max(), freq = tstep),
                                         pd.date_range(min_rain_index.min(), max_possible_last_tstep, freq = tstep)]
                    iloc_idx_with_max_surge = greatest_peak_surge = -9999
                    for i, idx in enumerate(lst_possible_idxs):
                        peak_surge = df_tseries_combined_time_indexed.loc[idx]["surge_ft"].max()
                        if peak_surge > greatest_peak_surge:
                            greatest_peak_surge = peak_surge
                            iloc_idx_with_max_surge = i
                    # reindex the time series using the selected index
                    new_idx = lst_possible_idxs[iloc_idx_with_max_surge]
                    final_idx_selection = pd.date_range(max(new_idx.min(), og_idx.min()), min(new_idx.max(), og_idx.max()), freq = tstep)
                    df_tseries_combined = df_tseries_combined.loc[final_idx_selection, :]
                else:
                    if event_type_key == "surge_events":
                        dur_comp = max_strm_len
                    elif event_type_key == "compound_events":
                        dur_comp = (dur_obs_max*compound_event_time_window_multiplier).round(tstep)
                    # find the indices of significant surge time steps
                    s_surge = df_tseries_combined_time_indexed["surge_ft"]
                    idx_significant_surge = s_surge[s_surge>=surge_threshold_to_determine_event_limits].index
                    s_surge = s_surge.loc[idx_significant_surge.min():idx_significant_surge.max()]
                    # compute possible start and end times around significant surge
                    dur_significant_surge = idx_significant_surge.max() - idx_significant_surge.min()
                    if dur_significant_surge > dur_comp:
                        # choose the duration with the max moving average surge
                        s_surge_mean_rolling = s_surge.rolling(dur_comp, min_periods = int(dur_comp/tstep), closed = "left").mean().dropna()
                        new_upper_bound_index = s_surge_mean_rolling.idxmax()
                        s_surge = s_surge.loc[new_upper_bound_index-dur_comp:new_upper_bound_index]
                    # reindex to buffer around surge stats to compute the mininum index to capture the surge
                    if len(s_surge) > 0:
                        df_surge = s_surge.to_frame()
                        df_surge["mm_per_hr"] = np.nan
                        df_surge_reindexed = reindex_to_buffer_significant_rain_and_surge(df_surge, arbirary_start_date = None)
                        min_surge_index = df_surge_reindexed.index
                        max_extension_from_first_or_last_surge = dur_obs_max - dur_significant_surge
                        min_possible_first_tstep = max(min_surge_index.min() - max_extension_from_first_or_last_surge, og_idx.min())
                        max_possible_last_tstep = min(min_surge_index.max() + max_extension_from_first_or_last_surge, og_idx.max())
                    else:
                        min_possible_first_tstep = og_idx.min()
                        max_possible_last_tstep = og_idx.max()
                    # compute a rolling sum over the max storm length across the range of possible event indices
                    s_rain_depths = df_tseries_combined_time_indexed.loc[min_possible_first_tstep:max_possible_last_tstep, "mm_per_hr"] * (tstep/np.timedelta64(1, "h"))
                    if s_rain_depths.sum() > 0:
                        s_rain_depths_rolling = s_rain_depths.rolling(dur_comp, min_periods = 1, closed = "left").sum().dropna()
                        # find the timeframe that maximizes rain depth
                        timeframe_endtime = s_rain_depths_rolling[s_rain_depths_rolling == s_rain_depths_rolling.max()].index.min()
                        # generate new index that captures all the event characteristics
                        s_rain_depths_target_window = s_rain_depths[timeframe_endtime-dur_comp:timeframe_endtime]
                        rain_indices = s_rain_depths_target_window[s_rain_depths_target_window>0].index
                    else:
                        rain_indices = min_surge_index
                    new_idx = pd.date_range(min(rain_indices.min(), min_surge_index.min()), max(rain_indices.max(), min_surge_index.max()), freq = tstep)
                    final_idx_selection = pd.date_range(max(new_idx.min(), og_idx.min()), min(new_idx.max(), og_idx.max()), freq = tstep)
                    # buffer rainfall and storm surge
                    df_tseries_combined = reindex_to_buffer_significant_rain_and_surge(df_tseries_combined_time_indexed.loc[final_idx_selection, :], arbirary_start_date = None)
                    # df_tseries_combined = df_tseries_combined.loc[df_reindexed.index, :]
                # elif event_type_key == "compound_events": # leaving these alone for now
                #     if dur_event > dur_obs_max*2:
                #         sys.exit("Figure something out for this")
                #     pass
            # finally, buffer based on water level series
            df_tseries_combined = reindex_to_buffer_significant_rain_and_surge(df_tseries_combined, arbirary_start_date = None, wlevel_varname = "waterlevel_ft")
            if (df_tseries_combined_time_indexed.loc[df_tseries_combined.dropna().index, :] - df_tseries_combined.dropna()).sum().sum() != 0:
                sys.exit("The indexing is getting messed up when managing time series lengths")
            # verify time series alignment with observed indices
            s_obs_tseries_surge = ds_6min_water_levels_mrms_res.sel(date_time = slice(shifted_obs_start,shifted_obs_end))["surge_ft"].to_dataframe()["surge_ft"]
            surge_adjustement = row_surge_event["surge_peak_ft"] - row_surge_event["obs_surge_peak_ft"]
            s_obs_surge_shifted_tomatch_sim = s_obs_tseries_surge + surge_adjustement
            if (df_tseries_combined_time_indexed["surge_ft"].values - s_obs_surge_shifted_tomatch_sim.values).sum() != 0:
                sys.exit("The indexing is messed up with the observed start and end dates")
            if df_tseries_combined.isna().any().any():
                # sys.exit("sdfsdf;slkdjf")
                # if the data has already been gathered
                og_idx = df_tseries_combined_time_indexed.index
                new_idx = df_tseries_combined.index
                if (new_idx.min() >= og_idx.min()) and (new_idx.max() <= og_idx.max()):
                    df_tseries_combined = df_tseries_combined_time_indexed.loc[df_tseries_combined.index]
                else:
                    # hard code an event start time based on confirmed indices
                    tdiff = df_tseries_combined.index.min() - df_tseries_combined_time_indexed.index.min()
                    obs_start_hardcode = shifted_obs_start+tdiff
                    # if tdiff/np.timedelta64(1, "h") < 0:
                    #     obs_start_hardcode = shifted_obs_start+tdiff
                    # else:
                    #     obs_start_hardcode = shifted_obs_start+tdiff
                    # work
                    # sys.exit("working")
                    # print("Here")
                    df_tseries_sim_rain = df_tseries_combined[col_rain]
                    s_tseries_sim_surge = df_tseries_combined["surge_ft"]
                    ds_6min_water_levels_mrms_res = ds_6min_water_levels_mrms_res
                    row_surge_event = row_surge_event
                    arbirary_start_date = arbirary_start_date
                    # print("Working on extending final time serise...........")
                    # end work
                    try:
                        s_tseries_sim_surge_filled, df_tseries_sim_rain_filled, shifted_obs_start, shifted_obs_end = fill_rain_and_extend_surge_series(df_tseries_sim_rain = df_tseries_combined[col_rain],
                                                                                                                                                        s_tseries_sim_surge = df_tseries_combined["surge_ft"],
                                                                                                                                                        ds_6min_water_levels_mrms_res = ds_6min_water_levels_mrms_res,
                                                                                                                                                            obs_start_hardcode = obs_start_hardcode,
                                                                                                                                                            row_surge_event = row_surge_event,
                                                                                                                                                            arbirary_start_date = arbirary_start_date)
                    except:
                        sys.exit("ds;lfkjsd;lfkjs it's still not working")
                    # fill water level and tide
                    s_obs_tseries_tide_extended = ds_6min_water_levels_mrms_res.sel(date_time = slice(shifted_obs_start+tide_shift_hrs,shifted_obs_end+tide_shift_hrs))["tide_ft"].to_dataframe()["tide_ft"]
                    s_obs_tseries_tide_extended.index = s_tseries_sim_surge_filled.index # - s_tseries_sim_surge_filled.index.min() + arbirary_start_date
                    # make sure the extended time time series lines up
                    if pd.concat([df_tseries_combined_time_indexed["tide_ft"], s_obs_tseries_tide_extended], axis = 1).dropna().diff(axis = 1).dropna(axis = 1).max().iloc[0] > 0:
                        s_obs_tseries_tide_extended.name = "tide_ft_extended"
                        pd.concat([df_tseries_combined_time_indexed["tide_ft"], s_obs_tseries_tide_extended], axis = 1).dropna().plot()
                        sys.exit("error - the extended tide time series does not line up with original")
                    # compute extended water level time series
                    s_tseries_sim_wlevel = s_tseries_sim_surge_filled + s_obs_tseries_tide_extended
                    s_tseries_sim_wlevel.name = "waterlevel_ft"
                    # recreate the time series dataframe
                    df_tseries_combined = pd.concat([s_tseries_sim_wlevel, s_tseries_sim_surge_filled, s_obs_tseries_tide_extended, df_tseries_sim_rain_filled], axis = 1)
            df_tseries_combined.index = df_tseries_combined.index - df_tseries_combined.index.min() + arbirary_start_date
            df_tseries_combined.index.name = "timestep"
            df_tseries_combined["event_type"] = col_event_type
            df_tseries_combined["year"] = year
            df_tseries_combined["event_id"] = event_id
            # df_tseries_combined["waterlevel_ft"] = df_tseries_combined["surge_ft"] + df_tseries_combined["tide_ft"]
            dur_event = df_tseries_combined.index.max() - df_tseries_combined.index.min()
            if dur_event > (dur_obs_max + np.timedelta64(timeseries_buffer_around_peaks_h*3, "h")):
                if event_type_key == "compound_events": 
                    if dur_event > (dur_obs_max*compound_event_time_window_multiplier + np.timedelta64(timeseries_buffer_around_peaks_h+timeseries_buffer_before_first_rain_h, "h")):
                        sys.exit(f"what's going on here ({event_type_key})?")
                else:
                    sys.exit("what's going on here?")

            df_tseries_combined = df_tseries_combined.reset_index().set_index(["event_type", "year", "event_id", "timestep"])

            # if shifting and buffering time series changed event statistics, update them in the event summaries table
            # surge statistics
            tseries_val = df_tseries_combined["surge_ft"].max()
            og_smry_var = "surge_peak_ft"
            if tseries_val != s_event_summary.loc[og_smry_var]:
                s_event_summary.loc[og_smry_var] = tseries_val
                # print warning if the values aren't close
                if not np.isclose(tseries_val, s_event_summary.loc[og_smry_var], rtol = rtol, atol = atol):
                    sys.exit(f"WARNING: shifting the event in time led to a change in {og_smry_var} ({s_event_summary.loc[og_smry_var]} --> {tseries_val})")
            # update rain summmary stats
            ## precip depth mm
            tseries_val = (df_tseries_combined["mm_per_hr"] * (tstep / np.timedelta64(1, "h"))).sum()
            og_smry_var = "precip_depth_mm"
            if tseries_val != s_event_summary.loc[og_smry_var]:
                s_event_summary.loc[og_smry_var] = tseries_val
                # print warning if the values aren't close
                if not np.isclose(tseries_val, s_event_summary.loc[og_smry_var], rtol = rtol, atol = atol):
                    sys.exit(f"WARNING: shifting the event in time led to a change in {og_smry_var} ({s_event_summary.loc[og_smry_var]} --> {tseries_val})")
            ## max intensity
            maxmean_rain_dur = int(rain_intensity_stat.split("_")[1].split('hr')[0])
            df_rain_summaries = compute_event_timeseries_statistics(df_tseries_combined["mm_per_hr"].to_frame(), lst_time_durations_h_to_analyze=[maxmean_rain_dur], varname=df_tseries_combined["mm_per_hr"].name,
                                        agg_stat = "mean")
            og_smry_var = rain_intensity_stat
            tseries_val = df_rain_summaries.iloc[0,].loc[og_smry_var]
            if tseries_val != s_event_summary.loc[og_smry_var]:
                s_event_summary.loc[og_smry_var] = tseries_val
                # print warning if the values aren't close
                if not np.isclose(tseries_val, s_event_summary.loc[og_smry_var], rtol = rtol, atol = atol):
                    sys.exit(f"WARNING: shifting the event in time led to a change in {og_smry_var} ({s_event_summary.loc[og_smry_var]} --> {tseries_val})")
            # udpate time delay summary stats
            if s_tdelay_stat.index[0] == 'surge_peak_after_rain_peak_h_target':
                current_max_mm_per_hr_tstep = df_tseries_combined.reset_index().set_index("timestep")["mm_per_hr"].idxmax()
                current_max_surge_tstep = df_tseries_combined.reset_index().set_index("timestep")["surge_ft"].idxmax()
                current_surge_after_rain_peak_h = (current_max_surge_tstep - current_max_mm_per_hr_tstep) / np.timedelta64(1, "h")
                diff_from_target_state_h = s_tdelay_stat.iloc[0] - current_surge_after_rain_peak_h
                diff_from_target_state_h = pd.Timedelta(diff_from_target_state_h, "h").round(tstep)/np.timedelta64(1, "h")
            else: # s_tdelay_stat.index[0] == 'max_mean_16hrsurge_peak_after_16hrrain_peak_h_target':
                tdelay_stat = s_tdelay_stat.index[0]
                maxmean_surge_dur = int(tdelay_stat.split("surge")[0].split("_")[-1].split("hr")[0])
                maxmean_rain_dur = int(tdelay_stat.split("after_")[1].split("_")[0].split("hr")[0])
                # compute the needed shift to the surge time series
                ## rain stat
                df_rain_summaries = compute_event_timeseries_statistics(df_tseries_combined["mm_per_hr"].to_frame(), lst_time_durations_h_to_analyze=[maxmean_rain_dur], varname=df_tseries_combined["mm_per_hr"].name,
                                                        agg_stat = "mean")
                current_max_mean_mm_per_hr_tstep = df_rain_summaries.iloc[0,1]
                ## surge stat
                df_stats_surge = compute_event_timeseries_statistics(df_tseries_combined["surge_ft"].to_frame(), lst_time_durations_h_to_analyze=[maxmean_surge_dur], varname="surge",
                                                                    agg_stat = "mean")
                current_max_mean_surge_tstep = df_stats_surge.iloc[0,1]
                ## current diff
                current_surge_after_rain_peak_h = (current_max_mean_surge_tstep - current_max_mean_mm_per_hr_tstep) / np.timedelta64(1, "h")
                ## compute needed time shift and round to nearest timestep
                diff_from_target_state_h = s_tdelay_stat.iloc[0] - current_surge_after_rain_peak_h
                # tdelt_shift_surge = pd.Timedelta(shift_surge_h, "h").round(tstep) 
                diff_from_target_state_h = pd.Timedelta(diff_from_target_state_h, "h").round(tstep)/np.timedelta64(1, "h")
                # s_event_summary.loc[s_tdelay_stat.index[0].split("_target")[0]] = current_surge_after_rain_peak_h
            if s_event_summary.loc[s_tdelay_stat.index[0].split("_target")[0]] != current_surge_after_rain_peak_h:
                # print("Warning: Shifted and buffered time series no longer matches the targeted time delay statistic")
                # sys.exit("Work - let's see what's going on here")
                s_event_summary.loc[s_tdelay_stat.index[0].split("_target")[0]] = current_surge_after_rain_peak_h
                
            # add other summary statistics to the row

            ## new observed event surge time series start and end dates
            s_event_summary.loc["obs_event_start_surge_shifted"] = shifted_obs_start
            s_event_summary.loc["obs_event_end_surge_shifted"] = shifted_obs_end
            s_event_summary.loc["obs_wlevel_peak_ft"] = ds_6min_water_levels_mrms_res.sel(date_time = slice(shifted_obs_start,shifted_obs_end))["waterlevel_ft"].to_dataframe()["waterlevel_ft"].max()
            s_event_summary.loc["obs_tide_shift_hrs"] = tide_shift_hrs
            s_event_summary.loc["sim_event_idx"] = sim_event_idx
            s_event_summary.name = sim_event_idx
            s_event_summary.loc["event_id"] = event_id
            s_event_summary.loc["year"] = year
            s_event_summary.loc["event_type"] = col_event_type
            # make sure all modeled stats are included
            ## manually add surge peak after rain peak
            stat = "surge_peak_after_rain_peak"
            if stat not in s_event_summary.index:
                current_max_mm_per_hr_tstep = df_tseries_combined.reset_index().set_index("timestep")["mm_per_hr"].idxmax()
                current_max_surge_tstep = df_tseries_combined.reset_index().set_index("timestep")["surge_ft"].idxmax()
                current_surge_after_rain_peak_h = (current_max_surge_tstep - current_max_mm_per_hr_tstep) / np.timedelta64(1, "h")
                s_event_summary.loc[stat] = current_surge_after_rain_peak_h
            ## loop through all stats that weren't used in all event types and add them to the event summary table if they aren't included already
            for stat in lst_modeled_stats_not_in_all:
                if stat not in s_event_summary.index:
                    if "after" in stat:
                        maxmean_surge_dur = int(stat.split("surge")[0].split("_")[-1].split("hr")[0])
                        maxmean_rain_dur = int(stat.split("after_")[1].split("_")[0].split("hr")[0])
                        ## rain stat
                        df_rain_summaries = compute_event_timeseries_statistics(df_tseries_combined["mm_per_hr"].to_frame(), lst_time_durations_h_to_analyze=[maxmean_rain_dur], varname=df_tseries_combined["mm_per_hr"].name,
                                                                agg_stat = "mean")
                        current_max_mean_mm_per_hr_tstep = df_rain_summaries.iloc[0,1]
                        ## surge stat
                        df_stats_surge = compute_event_timeseries_statistics(df_tseries_combined["surge_ft"].to_frame(), lst_time_durations_h_to_analyze=[maxmean_surge_dur], varname="surge",
                                                                            agg_stat = "mean")
                        current_max_mean_surge_tstep = df_stats_surge.iloc[0,1]
                        ## current diff
                        current_surge_after_rain_peak_h = (current_max_mean_surge_tstep - current_max_mean_mm_per_hr_tstep) / np.timedelta64(1, "h")
                        s_event_summary.loc[stat] = current_surge_after_rain_peak_h
                    else:
                        sys.exit("need to code for this situation")
            lst_df_tseries.append(df_tseries_combined)
            lst_s_summaries.append(s_event_summary)
            
            if np.isnan(s_event_summary["precip_depth_mm"]) or np.isnan(s_event_summary["surge_peak_ft"]):
                sys.exit("found a problem event idx")


            # if there is missing precip or surge data, break the loop
    # write to file
    print("Writing event summary and time series files....")
    df_summaries = pd.concat(lst_s_summaries, axis = 1).T
    df_summaries = df_summaries.set_index(["event_type", "year", "event_id"])
    # write summary data frame
    df_summaries.to_csv(f"{dir_storm_rescaling}{event_type_key}_simulation_summaries.csv")
    
    # write time series dataframe to csv and to netcdf
    df_tseries = pd.concat(lst_df_tseries)
    df_tseries.to_csv(f"{dir_storm_rescaling}{event_type_key}_simulation_time_series.csv")
    # also write to netcdf in case it's more efficient
    ds_tseries = df_tseries.to_xarray()
    d_encoding = {}
    for da_name in ds_tseries.data_vars:
        d_encoding[da_name] = {"zlib":True}
    ds_tseries.to_netcdf(f"{dir_storm_rescaling}{event_type_key}_simulation_time_series.nc", encoding=d_encoding)
#%% consolidating into single summary dataframe and time series netcdf
df_occurences_sim = pd.read_csv(f"{dir_simulate_from_copula}simulated_event_counts_{n_years_of_weather_to_generate}yrs.csv", index_col = "year")
df_occurences_sim

lst_summaries = []
lst_ds_tseries = []
for event_type_key in dic_event_summaries:
    event_type = None
    for col in df_occurences_sim.columns:
        if col in event_type_key:
            event_type = col
    df_summaries = pd.read_csv(f"{dir_storm_rescaling}{event_type_key}_simulation_summaries.csv", index_col=["event_type", "year", "event_id"])

    ds_tseries = xr.open_dataset(f"{dir_storm_rescaling}{event_type_key}_simulation_time_series.nc", chunks = dict(timestep = "auto", year = "auto"))

    n_events_in_tseries = len(ds_tseries.isel(timestep = 0).to_dataframe().dropna())

    # make sure number of events is correct
    if (df_occurences_sim.sum()[event_type] != len(df_summaries)) or (df_occurences_sim.sum()[event_type] != n_events_in_tseries):
        sys.exit(f"ERROR: THE NUMBER OF SIMULATED {event_type_key} DOES NOT MATCH THE TARGETED NUMBER OF SIMULATIONS".upper())

    lst_summaries.append(df_summaries)
    lst_ds_tseries.append(ds_tseries)

#%% combine
event_type_order = ["rain", "surge", "compound"]
df_summary_combined = pd.concat(lst_summaries).sort_index().loc[pd.IndexSlice[event_type_order, :, :], :] # the indexing is to force order that index level

# convert surge units from feet to meters
for col_val_in_feet in [col for col in df_summary_combined.columns if ('_ft' in col)]:
    newname = col_val_in_feet.replace("_ft", "_m")
    dic_rename = {col_val_in_feet:newname}
    # sys.exit("work")
    df_summary_combined = df_summary_combined.rename(columns = dic_rename)
    df_summary_combined[newname] = df_summary_combined[newname] / feet_per_meter
    print(f"Converted {col_val_in_feet} to meters and renamed it to {newname}")

# compute all summary stats that are not shared


df_summary_combined.to_csv(f"{dir_storm_rescaling}combined_simulation_summaries.csv")


# ds_tseries_combined
ds_tseries_combined = xr.concat(lst_ds_tseries, dim = "event_type")

# convert water level to meters
ds_tseries_combined = ds_tseries_combined.rename(dict(waterlevel_ft = "waterlevel_m", surge_ft = "surge_m", tide_ft = "tide_m"))
ds_tseries_combined["waterlevel_m"] = ds_tseries_combined["waterlevel_m"] / feet_per_meter
ds_tseries_combined["surge_m"] = ds_tseries_combined["surge_m"] / feet_per_meter
ds_tseries_combined["tide_m"] = ds_tseries_combined["tide_m"] / feet_per_meter

d_encoding = {}
for da_name in ds_tseries_combined.data_vars:
    d_encoding[da_name] = {"zlib":True}

ds_tseries_combined.to_netcdf(f"{dir_storm_rescaling}combined_simulation_time_series.nc", encoding=d_encoding)

# final check that the total rainfall and peak surges line up
# compare rain depths
df_event_depths_from_tseries = (ds_tseries_combined["mm_per_hr"] * (tstep / np.timedelta64(1, "h"))).sum("timestep").to_dataframe()
df_event_depths_from_tseries = df_event_depths_from_tseries.rename(columns = dict(mm_per_hr = "precip_depth_mm_from_tseries"))

diffs = df_summary_combined["precip_depth_mm"].to_frame().join(df_event_depths_from_tseries).diff(axis = 1).iloc[:, 1].values

if (~np.isclose(diffs, 0)).sum() > 0:
    print("ERROR: rain depths do not line up")

# compare peak surge
tseries_var = "surge_m"
summary_var = "surge_peak_m"

df_from_tseries = ds_tseries_combined[tseries_var].max("timestep").to_dataframe()
df_from_tseries = df_from_tseries.rename(columns = {tseries_var:f"{summary_var}_from_tseries"})

diffs = df_summary_combined[summary_var].to_frame().join(df_from_tseries).diff(axis = 1).iloc[:, 1].values

if (~np.isclose(diffs, 0)).sum() > 0:
    print(f"ERROR: {summary_var} do not line up")