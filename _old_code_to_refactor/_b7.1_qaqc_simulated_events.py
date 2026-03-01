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
from matplotlib.gridspec import GridSpec
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
import dask.dataframe as dd
from tqdm import tqdm
dir_plot = f"{dir_storm_rescaling}plots/"
Path(dir_plot).mkdir(parents=True, exist_ok=True)
alpha=0.05


# plotting design storms for comparison
# df_rain_design_storms = pd.read_csv(f_csv_design_storms, parse_dates = ["datetime"], sep = "\t", index_col="datetime")*mm_per_inch
# fig, ax = plt.subplots(dpi = 300)
# trim_factor = 40
# df_rain_design_storms = df_rain_design_storms.iloc[trim_factor:-trim_factor, :]
# df_rain_design_storms.index = df_rain_design_storms.index - df_rain_design_storms.index.min()
# df_rain_design_storms.index = df_rain_design_storms.index / np.timedelta64(1, "h")
# df_rain_design_storms.loc[:, ["1_year", "10_year", "100_year"]].plot(ax=ax)
# ax.set_ylabel("mm per hour")
# ax.set_xlabel("time (hours)")

df_all_events_summary = pd.read_csv(f_combined_event_summaries, index_col="event_id",
                                    parse_dates=["event_start", "event_end", "surge_peak_tstep",
                                                "precip_max_intensity_tstep"])

# convert units of surge peak to meters
df_all_events_summary = df_all_events_summary.rename(columns = dict(surge_peak_ft = "surge_peak_m"))
df_all_events_summary["surge_peak_m"] = df_all_events_summary["surge_peak_m"] / feet_per_meter

ds_event_tseries = xr.open_dataset(f_combined_event_tseries_netcdf)
# convert units of water level and surge peak to meters
ds_event_tseries = ds_event_tseries.rename(dict(waterlevel_ft = "waterlevel_m", surge_ft = "surge_m"))
ds_event_tseries["waterlevel_m"] = ds_event_tseries["waterlevel_m"] / feet_per_meter
ds_event_tseries["surge_m"] = ds_event_tseries["surge_m"] / feet_per_meter
# ds_event_tseries["tide_m"] = ds_event_tseries["tide_m"] / feet_per_meter



df_surge_events = df_all_events_summary[df_all_events_summary["event_type"] == "surge"]
df_rain_events = df_all_events_summary[df_all_events_summary["event_type"] == "rain"]
df_combo_events = df_all_events_summary[df_all_events_summary["event_type"] == "compound"]

dic_event_summaries = {#"all_events":df_all_events_summary,
                       "compound_events":df_combo_events,
                       "rain_events":df_rain_events,
                       "surge_events":df_surge_events}

surge_event_threshold_m = df_surge_events.surge_peak_m.min()
rain_event_threshold_mm = df_rain_events.precip_depth_mm.min()

clear_plots = True
lst_skip = []# ['rain_events', 'compound_events']#,"surge_events" 'rain_events', 'compound_events']
lst_df_summaries = []
lst_ds_tseries = []


df_sim_summaries_all = pd.read_csv(f"{dir_storm_rescaling}combined_simulation_summaries.csv", index_col = ["event_type", "year", "event_id"])
ds_tseries_all = xr.open_dataset(f"{dir_storm_rescaling}combined_simulation_time_series.nc", chunks = dict(year = "auto"))

# reclassify events if necessary
df_sim_summaries_all["surge_event"] = df_sim_summaries_all["surge_peak_m"] >= surge_event_threshold_m
sys.exit("rain events are classified as rain events based on both the rain event threshold but also over a specific duration")
df_sim_summaries_all["rain_event"] = df_sim_summaries_all["precip_depth_mm"] >= rain_event_threshold_mm
df_sim_summaries_all["compound"] = df_sim_summaries_all["surge_event"] & df_sim_summaries_all["rain_event"]



df_sim_summaries_all = df_sim_summaries_all.sort_index()
df_sim_summaries_all = df_sim_summaries_all.reset_index().drop(columns = "event_type")
df_sim_summaries_all = classify_events(df_sim_summaries_all)


# idx_new_compound = df_sim_summaries_all[(df_sim_summaries_all["compound"] == True) & (df_sim_summaries_all["max_mean_16hrsurge_peak_after_16hrrain_peak_h"].isna())].index
# idx_no_longer_compound = df_sim_summaries_all[(df_sim_summaries_all["compound"] == False) & (~df_sim_summaries_all["max_mean_16hrsurge_peak_after_16hrrain_peak_h"].isna())].index



# def fill_timedelay_stats(ds_tseries_all, df_sim_summaries, idx_to_fill, delay_stat, dur_h):
#     for idx in idx_to_fill:
#         # sys.exit("work")
#         df_tseries = ds_tseries_all.sel(event_type = idx[0], year = idx[1], event_id = idx[2]).to_dataframe()
#         # compute time delay stat for compound events
#         df_rain_summaries = compute_event_timeseries_statistics(df_tseries["mm_per_hr"].to_frame(), lst_time_durations_h_to_analyze=[dur_h], varname=df_tseries["mm_per_hr"].name,
#                                                 agg_stat = "mean")
#         current_max_xhr_mean_mm_per_hr_tstep = df_rain_summaries.iloc[0,1]
#         ## surge stat
#         df_stats_surge = compute_event_timeseries_statistics(df_tseries["surge_m"].to_frame(), lst_time_durations_h_to_analyze=[dur_h], varname="surge",
#                                                             agg_stat = "mean")
#         current_max_xhr_mean_surge_tstep = df_stats_surge.iloc[0,1]
#         # diff
#         current_surge_after_rain_peak_h = (current_max_xhr_mean_surge_tstep - current_max_xhr_mean_mm_per_hr_tstep) / np.timedelta64(1, "h")
#         # add value for time diff column
#         df_sim_summaries.loc[idx, delay_stat] = current_surge_after_rain_peak_h
#     return df_sim_summaries

# # sys.exit()
# delay_stat, dur_h = "max_mean_16hrsurge_peak_after_16hrrain_peak_h", 16
# idx_to_fill = idx_new_compound
# df_sim_summaries_all = fill_timedelay_stats(ds_tseries_all, df_sim_summaries_all, idx_to_fill, delay_stat, dur_h)

# delay_stat, dur_h = "surge_peak_after_rain_peak_h", 1
# idx_to_fill = idx_no_longer_compound
# df_sim_summaries_all = fill_timedelay_stats(ds_tseries_all, df_sim_summaries_all, idx_to_fill, delay_stat, dur_h)



#%% create goodness of fit plots 
for event_type_key in dic_event_summaries:
    if event_type_key in lst_skip:
        print(f"while script is in development, skipping {lst_skip}")
        continue
    # extract event type for indexing summary dataframe and 
    event_type = ""
    for e_type in ds_tseries_all["event_type"].values:
        if e_type in event_type_key:
            event_type = e_type
    # make sure plot folder exists and clear it if specified
    dir_plot_events = f"{dir_plot}{event_type_key}/"
    if clear_plots:
        try:
            shutil.rmtree(dir_plot_events)
            print(f"Deleteing and recreating folder {dir_plot_events}")
        except:
            pass
    Path(dir_plot_events).mkdir(parents=True, exist_ok=True)
    
    df_sim_summaries = df_sim_summaries_all[df_sim_summaries_all["event_type"] == event_type]

    for which_rain_data in ["mrms", "aorc", "all"]:
        # create plot folder for subsets of the dataset
        dir_plot_events = f"{dir_plot}{event_type_key}/{which_rain_data}_rain_data/"
        if clear_plots:
            try:
                shutil.rmtree(dir_plot_events)
                print(f"Deleteing and recreating folder {dir_plot_events}")
            except:
                pass
        Path(dir_plot_events).mkdir(parents=True, exist_ok=True)

        df_obs_summary = dic_event_summaries[event_type_key]
        if which_rain_data == "all":
            pass
        else:
            df_obs_summary = df_obs_summary[df_obs_summary["rain_data_source"] == which_rain_data]
        
        # double check that the correct thresholds are met
        s_surge_unmet = df_sim_summaries["surge_peak_m"] < surge_event_threshold_m
        s_rain_unmet = df_sim_summaries["precip_depth_mm"] < rain_event_threshold_mm
        problem = False
        if event_type_key in ["compound_events", "rain_events"]:
            if s_rain_unmet.sum() > 0:
                problem = True
                sys.exit(f'WARNING 1: There are {s_rain_unmet.sum()} {event_type_key} that do not meet the rain event threshold. Dropping these from the analysis...')
                df_sim_summaries = df_sim_summaries[~s_rain_unmet]
        if event_type_key in ["compound_events", "surge_events"]:
            if s_surge_unmet.sum() > 0:
                problem = True
                sys.exit(f'WARNING 2: There are {s_surge_unmet.sum()} {event_type_key} that do not meet the surge event threshold.')
        s_surge_met = df_sim_summaries["surge_peak_m"] >= surge_event_threshold_m
        s_rain_met = df_sim_summaries["precip_depth_mm"] >= rain_event_threshold_mm
        if event_type_key == "surge_events":
            if s_rain_met.sum() > 0:
                problem = True
                sys.exit(f"WARNING 3: There are {s_rain_met.sum()} {event_type_key} that also meet the rain event threshold.")
        if event_type_key == "rain_events":
            if s_surge_met.sum() > 0:
                problem = True
                sys.exit(f"WARNING 4: There are {s_surge_met.sum()} {event_type_key} that also meet the surge event threshold.")
        # if problem:
        #     break
        # create plots comparing observed to simulated data
        df_cdf_obs = format_fitted_cdf_values_for_fitting_copula(event_type_key=event_type_key, verbose = False)
        # sys.exit('work')
        df_cdf_obs = df_cdf_obs.rename(columns = dict(surge_peak_ft = "surge_peak_m"))
        # Number of datasets and number of columns
        num_datasets = len(df_cdf_obs.columns)
        ncols = 2
        nbins = 20
        nrows = (num_datasets + ncols - 1) // ncols  # Calculate rows needed

        # Create a larger figure and adjust aspect ratio
        fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(18, 7 * nrows), dpi = 250)  # Adjust height for each row
        axes = axes.flatten()

        for ds_idx, dataset in enumerate(df_cdf_obs.columns):
            # Get the respective axes for plotting
            ax_plot = axes[ds_idx]

            sample1 = df_obs_summary[dataset].dropna()
            sample2 = df_sim_summaries[dataset]
            if (sample2.isna().any()):
                sys.exit(f"There are missing values for {dataset}")

            # Find the common range across both samples
            combined_min = min(sample1.min(), sample2.min())
            combined_max = max(sample1.max(), sample2.max())

            # Calculate common bin edges
            bin_edges = np.linspace(combined_min, combined_max, nbins + 1)

            # Plot histogram and KDE for sample1 and sample2
            ax_plot.hist(sample1, bins=bin_edges, color='blue', alpha=0.3, label='Observed Hist', density=True)
            ax_plot.hist(sample2, bins=bin_edges, color='green', alpha=0.3, label='Simulated Hist', density=True)
            sns.kdeplot(sample1, color='blue', label='Observed KDE', fill=False, ax=ax_plot)
            sns.kdeplot(sample2, color='green', label='Simulated KDE', fill=False, ax=ax_plot)

            # Add thresholds with text for surge and rain
            if dataset == "surge_peak_m":
                if event_type_key == "rain_events":
                    txt_offset = -1.5
                else:
                    txt_offset = 0.5
                ax_plot.axvline(surge_event_threshold_m, color='red', linestyle='--')
                ax_plot.text(surge_event_threshold_m + txt_offset, ax_plot.get_ylim()[1] * 0.9,
                            f'Surge\nevent\nthreshold (m) = {surge_event_threshold_m:.2f}', 
                            color='red', bbox=dict(facecolor='white', alpha=0.5))

            if dataset == "precip_depth_mm":
                ax_plot.axvline(rain_event_threshold_mm, color='red', linestyle='--')
                ax_plot.text(rain_event_threshold_mm + 0.5, ax_plot.get_ylim()[1] * 0.9,
                            f'Rain\nevent\nthreshold (mm) = {rain_event_threshold_mm:.2f}', 
                            color='red', bbox=dict(facecolor='white', alpha=0.5))

            # Set the title and labels for each subplot
            ax_plot.set_title(f'{dataset} (rain data source = {which_rain_data})')
            ax_plot.set_xlabel(dataset, fontsize=12)
            ax_plot.set_ylabel('Density', fontsize=12)
            ax_plot.legend()

            df_results = perform_two_sample_tests(sample1, sample2, alpha=0.05)
            txt = ''
            for result_idx, result_row in df_results.iterrows():
                txt += f'{result_row["Test"]}: {result_row["p-value"]:.2f} ({result_row["Conclusion"]})\n'
            ax_plot.text(0.2, -0.24, txt, ha='left', va='center', transform=ax_plot.transAxes, fontsize=8)
        # Adjust layout to prevent overlap
        fig.suptitle(f"{event_type_key} observed vs. simulated", fontsize=16)
        fig.tight_layout(rect=[0, 0, 1, 0.96])  # Leave space for suptitle
        plt.savefig(f"{dir_plot_events}simulated_vs_observed_{event_type_key}.png")
        plt.clf()

        # from scipy.stats import gaussian_kde, kendalltau
        fig_main_title=""
        cop_fitted = None
        plot = True
        n_for_montecarlo_tau_test = 1000
        alpha = 0.05
        idx_cols_of_interest = df_cdf_obs.columns
        df_obs_realspace = df_obs_summary[idx_cols_of_interest]
        df_simulated_realspace = df_sim_summaries[idx_cols_of_interest]

        # calculate cdf value of obs and simulated
        df_marginal_fits = pd.read_csv(f"{dir_fitting_maringals}{event_type_key}_distributions.csv")
        df_obs_cdf = pd.DataFrame(index = df_obs_realspace.index, columns = df_obs_realspace.columns).astype(float)
        df_simulated_cdf = pd.DataFrame(index = df_simulated_realspace.index, columns = df_simulated_realspace.columns).astype(float)
        for idx, fitted_row in df_marginal_fits.iterrows():
            s_fit = fitted_row
            dataset = fitted_row["data"]
            if dataset == "surge_peak_ft":
                mltply = feet_per_meter
                dataset = "surge_peak_m"
            else:
                mltply = 1
            df_obs_valid = df_obs_realspace[dataset].dropna()
            df_sim = df_simulated_realspace[dataset]
            # store indices for filling in dataframe
            sim_idx = df_sim.index
            obs_idx = df_obs_valid.index
            # transform according to fit
            s_data_sim = transform_data_given_transformations(df_sim*mltply, fitted_row)
            s_data_obs = transform_data_given_transformations(df_obs_valid*mltply, fitted_row)
            # compute cdf values for each observation
            n_params = fitted_row["n_params"]
            fx = getattr(stats, fitted_row.loc["distribution"])
            if n_params == 3:
                x_obs_cdf = pd.Series(fx.cdf(s_data_obs, fitted_row["shape"], fitted_row["loc"], fitted_row["scale"]), name = "x_obs_cdf_fromfit").astype(float)
                x_sim_cdf = pd.Series(fx.cdf(s_data_sim, fitted_row["shape"], fitted_row["loc"], fitted_row["scale"]), name = "x_sim_cdf_fromfit").astype(float)
            if n_params == 2:
                x_obs_cdf = pd.Series(fx.cdf(s_data_obs, fitted_row["loc"], fitted_row["scale"]), name = "x_obs_cdf_fromfit").astype(float)
                x_sim_cdf = pd.Series(fx.cdf(s_data_sim, fitted_row["loc"], fitted_row["scale"]), name = "x_sim_cdf_fromfit").astype(float)
            # populate dataframe of cdf values
            x_obs_cdf.index = obs_idx
            x_sim_cdf.index = sim_idx
            if not np.isnan(s_fit["upper_bound"]):
                print(f"Flipping cdf values for copula simluated {event_type_key} {dataset} because an upper bound transformation was applied to this dataset")
                x_sim_cdf_untransformed = x_sim_cdf.copy()
                x_obs_cdf_untransformed = x_obs_cdf.copy()
                x_sim_cdf = 1 - x_sim_cdf
                x_obs_cdf = 1 - x_obs_cdf

            df_simulated_cdf.loc[sim_idx,dataset] = x_sim_cdf
            df_obs_cdf.loc[obs_idx,dataset] = x_obs_cdf
        # col1_iloc = i = 0
        # col2_iloc = j = 1
        df_obs = df_obs_cdf
        cop_fitted=None
        df_simulated=df_simulated_cdf
        df_mc_results_combined = evaluate_copula_fit_based_on_comparing_kendalltaus_for_each_variable_pair(df_obs=df_obs, alpha=alpha,n_for_montecarlo_tau_test=n_for_montecarlo_tau_test, cop_fitted=None,
                                                                                                            df_simulated=df_simulated, plot = True, fldr_plots = dir_plot_events,
                                                                                                            fig_main_title=f"observed vs. stochastically simualted and rescaled {event_type_key} (rain data source = {which_rain_data})")



#%% plotting functions
import matplotlib
from matplotlib.cm import ScalarMappable
from matplotlib.colors import Normalize
# df_sim_smries_subset = df_sim_smries_using_most_rescaled_surge_event
# dataset = "surge_m"
def return_df_of_obs_tseries_reindexed_at_first_timestep_based_on_subset_of_sim_smry_df(df_sim_smries_subset, ds_tseries_all, dataset, reindex = True):
    lst_s_tseries_to_plot = []
    for row_idx, row_event in df_sim_smries_subset.iterrows():
        event_type = row_idx[0]
        year = row_idx[1]
        event_id = row_idx[2]
        s_tseries = ds_tseries_all.sel(event_type = event_type, year = year, event_id = event_id).to_dataframe().dropna()[dataset]
        s_tseries.name = f"{event_type}_{year}_{event_id}"
        # if rainfall, drop preceding and proceeding indices with zero rainfall; also fill na values with 0
        if dataset == "mm_per_hr":
            non_zero_indices = s_tseries[s_tseries>0].index
            s_tseries = s_tseries.loc[non_zero_indices.min():non_zero_indices.max()]
            s_tseries = s_tseries.fillna(0)
            s_tseries.index = s_tseries.index - s_tseries.index.min()
        # if surge, reindex so that the peak surge happens at index 0
        if dataset in ["surge_m", "tide_m", "waterlevel_m"]:
            # sys.exit("this ")
            # if not np.isnan(row_event["surge_peak_after_rain_peak_h_target"]):
            #     tdelay_stat_name = "surge_peak_after_rain_peak_h_target"
            #     tdelay_stat_target = row_event[tdelay_stat_name]
            # elif not np.isnan(row_event["max_mean_16hrsurge_peak_after_16hrrain_peak_h_target"]):
            #     tdelay_stat_name = "max_mean_16hrsurge_peak_after_16hrrain_peak_h_target"
            #     tdelay_stat_target = row_event[tdelay_stat_name]
            # row_event[tdelay_stat_name.split("_target")[0]]
            s_tseries_surge = ds_tseries_all.sel(event_type = event_type, year = year, event_id = event_id).to_dataframe().dropna()["surge_m"]
            idx_max_surge = s_tseries_surge.idxmax()
            new_idx = s_tseries_surge.index - idx_max_surge
            s_tseries.index = new_idx
        lst_s_tseries_to_plot.append(s_tseries)

    # combine; and for surge events, drop missing and reindex so that the first timestep is 0; this will not affect rain events
    df_tseries_combined = pd.concat(lst_s_tseries_to_plot, axis = 1)
    # df_tseries_combined.index = df_tseries_combined.index - df_tseries_combined.index.min()

    return df_tseries_combined


def plot_event_quantiles_over_time(df_tseries_combined, lst_s_to_plot, sep_plt_for_lines, ylab, txt_ax_title, add_cbar = False, fig=None, gs=None,
                                    ax1 = None, ax2 = None, x_tick_tstep_hr = "6 hours", fname_savefig=None, return_fig_and_ax = False):
    import matplotlib.gridspec as gridspec
    plot_dfq = pd.DataFrame(index = df_tseries_combined.columns, columns=(df_tseries_combined.index / np.timedelta64(1, "m")).astype(int)).astype(float)
    # plot_dfq = pd.DataFrame(index = df_tseries_combined.columns, columns=(df_tseries_combined.index).astype(float))

    for col_iloc, colname in enumerate(df_tseries_combined.index):
        col = df_tseries_combined.loc[colname, :].fillna(0).sort_values(ignore_index=True).copy()
        plot_dfq.iloc[:,col_iloc] = col

    quantiles=np.arange(1,len(plot_dfq)+1)
    normalized_quantiles=(quantiles-quantiles.min())/(quantiles.max()-quantiles.min())

    norm = Normalize(vmin=0, vmax=1)  # normalized_quantiles range from 0 to 1
    cmap = plt.get_cmap('coolwarm_r')
    mappable = ScalarMappable(norm=norm, cmap=cmap)

    if fig is None and gs is None:
        fig = plt.figure(figsize=(8, 10))  # Adjust the overall figure size
        gs = gridspec.GridSpec(2, 2, width_ratios=[1, 0.05], height_ratios=[1, 1])
    if ax1 is None:
        ax1 = fig.add_subplot(gs[0, 0])
    for i in range(len(plot_dfq)):
        # print(normalized_quantiles[i])
        color = cmap(normalized_quantiles[i])  # Ensure correct color mapping

        if i == 0:
            # ax1.fill_between(df_tseries_combined.index, min_value, df_tseries_combined.iloc[:, i],
            #                 color=color, alpha=0.8)  # First fill from baseline to first quantile
            pass
        else:
            ax1.fill_between(plot_dfq.columns, plot_dfq.iloc[i-1, :], plot_dfq.iloc[i, :],
                            color=color, alpha=1)
        # ax1.plot(plot_dfq.columns, plot_dfq.values[i], c=color)  # Apply color to line
        # if i > 0:
            # ax1.fill_between(plot_dfq.columns, plot_dfq.values[i-1], plot_dfq.values[i], color=color, alpha=0.5)  # Fill color between lines

    # Add color bar in its own space
    if add_cbar:
        nrows, ncols = gs.get_geometry()
        if nrows == 2:
            row = 0
        else:
            row = np.ceil((nrows-1)/2).astype(int)
        cbar_ax = fig.add_subplot(gs[row, 1])
        cbar = plt.colorbar(mappable, cax=cbar_ax)
        cbar.set_label('quantile')  # Set the label for the color bar
    ax1.set_title(txt_ax_title)

    if sep_plt_for_lines:
        if ax2 is None:
            ax2 = fig.add_subplot(gs[1, 0])
    else:
        ax2 = ax1
    for i, s_to_plot in enumerate(lst_s_to_plot):
        if s_to_plot.index.dtype != float:
            s_to_plot.index = s_to_plot.index / np.timedelta64(1, "m")
        color = None
        try:
            if s_to_plot.color == "lowest_cbar":
                color = cmap(norm(0))
            elif s_to_plot.color == "middle_cbar":
                color = "grey"
            elif s_to_plot.color == "highest_cbar":
                color = cmap(norm(1))
            else:
                color = s_to_plot.color
        except:
            sys.exit("color not defined for the passed series")
        ax2.plot(s_to_plot.index, s_to_plot.values, linewidth = 1, color = color, label = s_to_plot.name, alpha = 1)
        if sep_plt_for_lines:
            if s_to_plot.name.lower() == "original time series":
                ax1.plot(s_to_plot.index, s_to_plot.values, linewidth = 1, color = "black", label = s_to_plot.name, alpha = 1)
                ax1.legend()
        ax2.legend()

    if not sep_plt_for_lines:
        axes = [ax1]
    else:
        axes = [ax1, ax2]
    for ax in axes:
        ax.set_xlim((df_tseries_combined.min().min(), df_tseries_combined.max().max()*1.1))
        xticks_labs = (pd.Series(df_tseries_combined.index).dt.round(pd.to_timedelta(x_tick_tstep_hr)).unique() / np.timedelta64(1, "h")).astype(int)
        xticks = xticks_labs * 60
        ax.set_xticks(xticks)
        ax.set_xticklabels(xticks_labs,fontsize=10)
        ax.set_xlabel("hours")
        ax.set_ylabel(ylab)
    
    fig.tight_layout()

    if fname_savefig is not None:
        plt.savefig(fname_savefig, bbox_inches='tight')
        plt.clf()
    # else:
    #     plt.show()
    
    if return_fig_and_ax == True:
        return fig, axes


dpi = 100 # will probably want to increase this on the final pass through
dir_plot_eda = f"{dir_plot}/EDA/"
create_folder(dir_plot_eda, clear_folder=True)
import matplotlib.gridspec as gridspec
#%% combining surge and rain rescalings to look at variability of simulated events
df_sim_summaries_all = df_sim_summaries_all.set_index(["event_type", "year", "event_id"])
dir_plot_rain_and_surge_rescaling = f"{dir_plot}/rain_rescaling_and_surge/"
create_folder(dir_plot_rain_and_surge_rescaling, clear_folder=True)

df_sim_summaries_rain = df_sim_summaries_all[df_sim_summaries_all["precip_depth_mm"]>0]
obs_event_idx_rescaled_rain = df_sim_summaries_rain.loc[:, "obs_event_id_rain"].astype(int).unique()

for obs_event_idx in obs_event_idx_rescaled_rain:
    lst_s_to_plot = []
    df_sim_smries_using_rescaled_rain_event = df_sim_summaries_all[df_sim_summaries_all["obs_event_id_rain"] == obs_event_idx]


    # return time series for all simulated events with that observed rain dataset
    # sim_events_indices = df_sim_smries_using_rescaled_rain_event.reset_index()["sim_event_id"].values
    # ds_tseries_all_subset = ds_tseries_all.sel(sim_event_id = sim_events_indices)
    # create dataframe of time series shifted such that the first non-zero rainfall value is 0
    lst_df_tseries_combined = []
    for event_type, year, event_id in df_sim_smries_using_rescaled_rain_event.index:
        df_tseries_all_subset = ds_tseries_all.sel(event_type = event_type, year = year, event_id = event_id).to_dataframe()
        df_tseries_all_subset = df_tseries_all_subset.drop(columns = ["event_type", "year", "event_id"])
        # drop rows where all the values are na
        df_tseries_all_subset = df_tseries_all_subset[~df_tseries_all_subset.isna().all(axis = 1)]
        if df_tseries_all_subset["mm_per_hr"].sum() == 0:
            continue
        first_tstep_w_rain = df_tseries_all_subset[df_tseries_all_subset["mm_per_hr"] > 0].index[0]
        df_tseries_all_subset_reindexed = df_tseries_all_subset.copy()
        df_tseries_all_subset_reindexed.index = df_tseries_all_subset.index - first_tstep_w_rain
        df_tseries_all_subset_reindexed["event_info"] = f"{event_type}_{year}_{event_id}"
        df_tseries_all_subset_reindexed = df_tseries_all_subset_reindexed.reset_index().set_index(["event_info", "timestep"])
        lst_df_tseries_combined.append(df_tseries_all_subset_reindexed)
        # df_tseries_all_subset = df_tseries_all_subset[~df_tseries_all_subset.isna().all(axis = 1)]
        # reindex so that timestep 0 is first non-zero rainfall value
    
    df_tseries_combined = pd.concat(lst_df_tseries_combined)

    
    df_tseries_combined["surge_m"].unstack(level = "event_info")
    df_tseries_combined["waterlevel_m"].unstack(level = "event_info")



    # df_tseries_combined = return_df_of_obs_tseries_reindexed_at_first_timestep_based_on_subset_of_sim_smry_df(df_sim_smries_using_rescaled_rain_event, ds_tseries_all, dataset="mm_per_hr")
    
    n_rescalings = len(df_sim_smries_using_rescaled_rain_event)
    # fname_savefig = f"{dir_plot_rain_rescalings}rescaled_rain_event_{obs_event_idx}_with_surge_variation.png"
    row_w_target_event_info = df_sim_summaries_all[df_sim_summaries_all["obs_event_id_rain"] == obs_event_idx].iloc[0]
    obs_event_start = row_w_target_event_info["obs_event_start_rain"]
    obs_event_end = row_w_target_event_info["obs_event_end_rain"]
    df_obs = ds_event_tseries.sortby("date_time").sel(date_time = slice(obs_event_start,obs_event_end)).to_dataframe()
    # shift so that the first non-zero rain depth occurs at time 0
    df_obs.index = (df_obs.index - df_obs[df_obs["mm_per_hr"] > 0].index[0]) / np.timedelta64(1, "m")

    s_obs_rain_event = df_obs["mm_per_hr"]
    s_obs_rain_event.name = "observed rainfall"
    s_obs_rain_event.color = "black"
    # s_obs_event = s_obs_rain_event
    # include original event
    # s_obs_event_reindexed = s_obs_event.copy()
    # s_obs_event_reindexed.index = (s_obs_event.index - s_obs_event.index.min()) 
    
    lst_s_to_plot.append(s_obs_rain_event)
    # line_label = "original time series"
    ylab = "mm per hour"
    # extract observed event statistics for plot title
    obs_event_row = df_all_events_summary.loc[obs_event_idx, :]
    obs_event_type = obs_event_row["event_type"]
    obs_event_precip_depth = obs_event_row["precip_depth_mm"]
    obs_event_precip_max_intnsty = obs_event_row["precip_max_intensity"]
    obs_event_peak_surge = obs_event_row["surge_peak_m"]

    txt_ax_title = f"Rescaled and shifted rainfall and storm surge around obs rain event id {obs_event_idx} (n_rescalings = {n_rescalings})\nObserved event type = {obs_event_type} | rain depth (mm) = {obs_event_precip_depth:.1f}\npeak intensity (mm per hr) = {obs_event_precip_max_intnsty:.1f} | peak surge (m) = {obs_event_peak_surge:.1f}"

    fname_savefig = None
    return_fig_and_ax = True
    sep_plt_for_lines = False
    fig = plt.figure(figsize=(10, 12), dpi = 300)  # Adjust the overall figure size
    gs = gridspec.GridSpec(3, 2, width_ratios=[1, 0.03], height_ratios=[.8, 1, 1])
    ax1 = fig.add_subplot(gs[0, 0])
    plot_event_quantiles_over_time(df_tseries_combined=df_tseries_combined["mm_per_hr"].unstack(level = "event_info"), lst_s_to_plot=lst_s_to_plot,
                                    sep_plt_for_lines = sep_plt_for_lines,
                                      ylab=ylab, txt_ax_title=txt_ax_title, add_cbar = False, fig=fig, gs=gs, ax1 = ax1, x_tick_tstep_hr = "12 hours",
                                        fname_savefig=fname_savefig)
    # sys.exit()

    lst_s_to_plot = []
    s_obs_event = df_obs["surge_m"]
    s_obs_event.name = "observed surge"
    s_obs_event.color = "black"
    lst_s_to_plot.append(s_obs_event)
    # line_label = "original time series"
    ylab = "surge (m)"
    txt_ax_title = f""
    # fname_savefig  = None
    sep_plt_for_lines = False

    ax2 = fig.add_subplot(gs[1, 0])
    plot_event_quantiles_over_time(df_tseries_combined=df_tseries_combined["surge_m"].unstack(level = "event_info"), lst_s_to_plot=lst_s_to_plot,
                                    sep_plt_for_lines = sep_plt_for_lines,
                                      ylab=ylab, txt_ax_title=txt_ax_title, add_cbar = True, fig=fig, gs=gs, ax1 = ax2, x_tick_tstep_hr = "12 hours",
                                      fname_savefig=None)
    

    lst_s_to_plot = []
    s_obs_event = df_obs["waterlevel_m"]
    s_obs_event.name = "observed water level"
    s_obs_event.color = "black"
    lst_s_to_plot.append(s_obs_event)
    # line_label = "original time series"
    ylab = "water level (surge + tide) (m)"
    txt_ax_title = f""
    # fname_savefig  = None
    sep_plt_for_lines = False

    ax3 = fig.add_subplot(gs[2, 0])
    plot_event_quantiles_over_time(df_tseries_combined=df_tseries_combined["waterlevel_m"].unstack(level = "event_info"), lst_s_to_plot=lst_s_to_plot,
                                    sep_plt_for_lines = sep_plt_for_lines,
                                      ylab=ylab, txt_ax_title=txt_ax_title, add_cbar = False, fig=fig, gs=gs, ax1 = ax3, x_tick_tstep_hr = "12 hours",
                                      fname_savefig=None)
    fname_savefig = f"{dir_plot_rain_and_surge_rescaling}rain_event_{obs_event_idx}_rescaled_w_surge.png"
    plt.savefig(fname_savefig, bbox_inches='tight')
    plt.clf()
    # sys.exit()




#%% inspecting rain rescalings
df_sim_summaries_all
ds_tseries_all

dir_plot_rain_rescalings = f"{dir_plot}/rain_rescaling/"
create_folder(dir_plot_rain_rescalings, clear_folder=True)

obs_event_idx_rescaled_rain = df_sim_summaries_all[df_sim_summaries_all["precip_depth_mm"]>0].loc[:, "obs_event_id_rain"].astype(int).unique()

for obs_event_idx in obs_event_idx_rescaled_rain:
    lst_s_to_plot = []
    df_sim_smries_using_rescaled_rain_event = df_sim_summaries_all[df_sim_summaries_all["obs_event_id_rain"] == obs_event_idx]
    df_tseries_combined = return_df_of_obs_tseries_reindexed_at_first_timestep_based_on_subset_of_sim_smry_df(df_sim_smries_using_rescaled_rain_event, ds_tseries_all, dataset="mm_per_hr")
    
    s_longest = df_tseries_combined.dropna(axis = 1).iloc[:, 0]
    s_longest.name = "longest rescaling"
    s_longest.color = "lowest_cbar"
    lst_s_to_plot.append(s_longest)

    s_highest_intensity = df_tseries_combined.loc[:, df_tseries_combined.max().idxmax()]
    s_highest_intensity.name = "most intense rescaling"
    s_highest_intensity.color = "highest_cbar"
    lst_s_to_plot.append(s_highest_intensity)

    

    n_rescalings = len(df_sim_smries_using_rescaled_rain_event)
    fname_savefig = f"{dir_plot_rain_rescalings}n{n_rescalings}_rescaled_rainevent_{obs_event_idx}.png"
    row_w_target_event_info = df_sim_summaries_all[df_sim_summaries_all["obs_event_id_rain"] == obs_event_idx].iloc[0]

    obs_event_start = row_w_target_event_info["obs_event_start_rain"]
    obs_event_end = row_w_target_event_info["obs_event_end_rain"]
    s_obs_rain_event = ds_event_tseries.sortby("date_time").sel(date_time = slice(obs_event_start,obs_event_end)).to_dataframe()["mm_per_hr"]
    s_obs_rain_event.name = "original time series"
    
    s_obs_event = s_obs_rain_event
    # include original event
    s_obs_event_reindexed = s_obs_event.copy()
    s_obs_event_reindexed.index = (s_obs_event.index - s_obs_event.index.min()) / np.timedelta64(1, "m")
    s_obs_event_reindexed.color = "middle_cbar"
    lst_s_to_plot.append(s_obs_event_reindexed)
    # line_label = "original time series"
    ylab = "mm per hour"
    txt_ax_title = f"Rescaled and shifted rainfall based on obs event id {obs_event_idx}\ntimes rescaled = {n_rescalings}"
    plot_event_quantiles_over_time(df_tseries_combined=df_tseries_combined, lst_s_to_plot=lst_s_to_plot,
                                    sep_plt_for_lines = True, add_cbar = True,
                                      ylab=ylab, txt_ax_title=txt_ax_title, fname_savefig=fname_savefig)

#%% investigate surge rescalings
dir_plot_surge_rescalings = f"{dir_plot}/surge_rescaling/"
create_folder(dir_plot_surge_rescalings, clear_folder=True)

obs_event_idx_rescaled_surge = df_sim_summaries_all[df_sim_summaries_all["surge_peak_m"]>0].loc[:, "obs_event_id_surge"].astype(int).unique()

for obs_event_idx in obs_event_idx_rescaled_surge:
    lst_s_to_plot = []
    df_sim_smries_using_rescaled_surge_event = df_sim_summaries_all[df_sim_summaries_all["obs_event_id_surge"] == obs_event_idx]
    df_tseries_combined = return_df_of_obs_tseries_reindexed_at_first_timestep_based_on_subset_of_sim_smry_df(df_sim_smries_using_rescaled_surge_event, ds_tseries_all, dataset="surge_m")
    df_tseries_combined = df_tseries_combined.dropna()
    
    # s_longest = df_tseries_combined.dropna(axis = 1).iloc[:, 0]
    # s_longest.name = "longest rescaling"
    # s_longest.color = "lowest_cbar"
    # lst_s_to_plot.append(s_longest)

    # s_highest_intensity = df_tseries_combined.loc[:, df_tseries_combined.max().idxmax()]
    # s_highest_intensity.name = "most intense rescaling"
    # s_highest_intensity.color = "highest_cbar"
    # lst_s_to_plot.append(s_highest_intensity)

    n_rescalings = len(df_sim_smries_using_rescaled_surge_event)
    fname_savefig = f"{dir_plot_surge_rescalings}n{n_rescalings}_rescaled_surge_event_{obs_event_idx}.png"
    row_w_target_event_info = df_sim_summaries_all[df_sim_summaries_all["obs_event_id_surge"] == obs_event_idx].iloc[0]

    obs_event_start = row_w_target_event_info["obs_event_start_surge"]
    obs_event_end = row_w_target_event_info["obs_event_end_surge"]
    s_obs_surge_event = ds_event_tseries.sortby("date_time").sel(date_time = slice(obs_event_start,obs_event_end)).to_dataframe()["surge_m"]
    s_obs_surge_event.name = "original time series"
    
    s_obs_event = s_obs_surge_event
    # include original event
    s_obs_event_reindexed = s_obs_event.copy()

    idx_max_surge = s_obs_event_reindexed.idxmax()
    new_idx = s_obs_event_reindexed.index - idx_max_surge
    s_obs_event_reindexed.index = new_idx / np.timedelta64(1, "m")

    s_obs_event_reindexed.color = "black"
    lst_s_to_plot.append(s_obs_event_reindexed)
    # line_label = "original time series"
    ylab = "surge (m)"
    txt_ax_title = f"Rescaled and shifted surge based on obs event id {obs_event_idx}\ntimes rescaled = {n_rescalings}"
    # fname_savefig  = None
    sep_plt_for_lines = False
    plot_event_quantiles_over_time(df_tseries_combined=df_tseries_combined, lst_s_to_plot=lst_s_to_plot,
                                    sep_plt_for_lines = sep_plt_for_lines,
                                      ylab=ylab, txt_ax_title=txt_ax_title, fname_savefig=fname_savefig)







#%% looking into scaled versions of rescaled rainfall time series

fig, ax = plt.subplots(dpi=dpi)
df_counts_rain_event_rescaling = df_sim_summaries_all.obs_event_id_rain.astype(int).value_counts()
df_counts_rain_event_rescaling.plot.bar(ax=ax)
n_rain_events_rescaled = len(df_sim_summaries_all.obs_event_id_rain.value_counts())
n_rain_events_observed = len(df_all_events_summary[~df_all_events_summary["precip_depth_mm"].isna()])
ax.set_title(f"# of times rain event was rescaled\n({n_rain_events_rescaled} unique observed events rescaled out of \n{n_rain_events_observed} observed rain time series across\nsurge, rain, and compound events)")
fig.tight_layout()
plt.savefig(f"{dir_plot_eda}bar_count_rain_event_rescaling.png", bbox_inches='tight')

#%% inspecting the rain event that was most rescaled
obs_event_id_rain_rescaled_most_times = df_counts_rain_event_rescaling.idxmax()

row_w_target_event_info = df_sim_summaries_all[df_sim_summaries_all["obs_event_id_rain"] == obs_event_id_rain_rescaled_most_times].iloc[0]
obs_event_start = row_w_target_event_info["obs_event_start_rain"]
obs_event_end = row_w_target_event_info["obs_event_end_rain"]

row_obs_event_smry = df_all_events_summary.loc[obs_event_id_rain_rescaled_most_times,:]
s_obs_rain_event = ds_event_tseries.sortby("date_time").sel(date_time = slice(obs_event_start,obs_event_end)).to_dataframe()["mm_per_hr"]
fig, ax = plt.subplots(dpi=dpi)
s_obs_rain_event.plot(ax=ax)
ax.set_title(f"Rainfall from event id {obs_event_id_rain_rescaled_most_times}\n(depth = {row_obs_event_smry['precip_depth_mm']:.2f} mm, max intensity = {row_obs_event_smry['precip_max_intensity']:.2f} mm per hour)")
fig.tight_layout()
plt.savefig(f"{dir_plot_eda}tseries_obs_rainevent_most_rescaled.png", bbox_inches='tight')




#%% similar analysis but for surge events
fig, ax = plt.subplots(dpi=dpi)
df_counts_surge_event_rescaling = df_sim_summaries_all.obs_event_id_surge.astype(int).value_counts()
df_counts_surge_event_rescaling.plot.bar(ax=ax)
n_surge_events_rescaled = len(df_sim_summaries_all.obs_event_id_surge.value_counts())
n_surge_events_observed = len(df_all_events_summary[~df_all_events_summary["precip_depth_mm"].isna()])
ax.set_title(f"# of times surge event was rescaled\n({n_surge_events_rescaled} unique observed events rescaled out of \n{n_surge_events_observed} observed surge time series across\nrain, surge, and compound events)")
fig.tight_layout()
plt.savefig(f"{dir_plot_eda}bar_count_surge_event_rescaling.png", bbox_inches='tight')

obs_event_id_surge_rescaled_most_times = df_counts_surge_event_rescaling.idxmax()

row_w_target_event_info = df_sim_summaries_all[df_sim_summaries_all["obs_event_id_surge"] == obs_event_id_surge_rescaled_most_times].iloc[0]
obs_event_start = row_w_target_event_info["obs_event_start_surge"]
obs_event_end = row_w_target_event_info["obs_event_end_surge"]

row_obs_event_smry = df_all_events_summary.loc[obs_event_id_surge_rescaled_most_times,:]
s_obs_surge_event = ds_event_tseries.sel(date_time = slice(obs_event_start,obs_event_end)).to_dataframe()["surge_m"]
fig, ax = plt.subplots(dpi=dpi)
s_obs_surge_event.plot(ax=ax)
ax.set_title(f"Surge from event id {obs_event_id_surge_rescaled_most_times}\n(peak surge (m) = {row_obs_event_smry['surge_peak_m']:.2f})")
fig.tight_layout()
plt.savefig(f"{dir_plot_eda}tseries_obs_surge_event_most_rescaled.png", bbox_inches='tight')






#%%

df_sim_smries_using_most_rescaled_surge_event = df_sim_summaries_all[df_sim_summaries_all["obs_event_id_surge"] == obs_event_id_surge_rescaled_most_times]
df_tseries_combined = return_df_of_obs_tseries_reindexed_at_first_timestep_based_on_subset_of_sim_smry_df(df_sim_smries_using_most_rescaled_surge_event, ds_tseries_all, dataset="surge_m")
df_tseries_combined = df_tseries_combined.dropna()
fname_savefig = f"{dir_plot_eda}tseries_rescaled_versions_of_obs_surge_event_most_rescaled.png"
s_obs_event = s_obs_surge_event

s_obs_event_reindexed = s_obs_event.copy()
idx_max_surge = s_obs_event_reindexed.idxmax()
new_idx = s_obs_event_reindexed.index - idx_max_surge
s_obs_event_reindexed.index = new_idx / np.timedelta64(1, "m")
# s_obs_event_reindexed.index = (s_obs_event_reindexed.index - s_obs_event_reindexed.index.min()) / np.timedelta64(1, "m")


line_label = "original time series"
ylab = "surge (m)"
txt_ax_title = f"Rescaled and shifted surge based on obs event id {obs_event_id_surge_rescaled_most_times}"
plot_event_quantiles_over_time(df_tseries_combined, s_obs_event_reindexed, line_label, ylab, txt_ax_title)