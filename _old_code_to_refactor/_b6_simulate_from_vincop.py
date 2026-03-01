#%%
from tqdm import tqdm
import pyvinecopulib as pv
from _inputs import *
from _utils import *
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import probplot
from scipy import stats
import matplotlib.patches as mpatches
import shutil
from matplotlib.lines import Line2D
from scipy.stats import gaussian_kde, kendalltau
from scipy.stats import poisson
n_years_to_simulate = np.ceil(1.5 * n_years_of_weather_to_generate).astype(int) # i over simulate by some amount in case there are invalid simulated events
alpha = 0.05
random_seed_np = np.random.RandomState(234)

# Set the random seed for reproducibility
random_seed = 24

dir_plot = f"{dir_simulate_from_copula}plots/"
Path(dir_plot).mkdir(parents=True, exist_ok=True)

df_all_events_summary = pd.read_csv(f_combined_event_summaries, 
                                    parse_dates=["event_start", "event_end", "surge_peak_tstep",
                                                "precip_max_intensity_tstep"])
df_surge_events = df_all_events_summary[df_all_events_summary["event_type"] == "surge"]
df_rain_events = df_all_events_summary[df_all_events_summary["event_type"] == "rain"]
df_combo_events = df_all_events_summary[df_all_events_summary["event_type"] == "compound"]

lst_all_event_stats = []
lst_surge_event_stats = []
lst_rain_event_stats = []
lst_compound_event_stats = []

dic_event_stats = {}

dic_event_summaries = {#"all_events":df_all_events_summary,
                       "compound_events":df_combo_events,
                       "rain_events":df_rain_events,
                       "surge_events":df_surge_events}

# load event time series
df_combined_tseries = pd.read_csv(f_combined_event_tseries_csv, parse_dates = ["date_time"])
# load original mrms time series
df_rain_tseries = pd.read_csv(f_mrms_rainfall, index_col="time", parse_dates = ["time"])
# load water level data
ds_6min_water_levels_trgt_res = xr.open_dataset(f_6min_wlevel_resampled_to_mrms_res_nc)

# load copulas
dic_copulas = {}
for key in dic_event_summaries:
    cop_json = f"{dir_vinecop}vinecop_params_{key}.json"
    with open(cop_json, "r") as f:
        json_str = f.read()
    copula_from_json = pv.Vinecop.from_json(json_str)
    # copula_from_json = pv.Vinecop()
    dic_copulas[key] = copula_from_json

# load raw hydro data
# ncei hourly
# df_rainfall_ncei_hourly_in = pd.read_csv(f_hourlyprecip_airport, parse_dates=["DATE"])
# s_rainfall_ncei_hourly_in = df_rainfall_ncei_hourly_in.loc[:, ["DATE", "HPCP"]].drop_duplicates().set_index("DATE").HPCP
# mrms
df_rainfall = pd.read_csv(f_mrms_rainfall, parse_dates=True, index_col = "time")
df_rainfall.index.name = "date_time"
s_mrms_rainfall_trgt_res_mm_per_hr = df_rainfall.rain_mean
s_mrms_rainfall_trgt_res_mm_per_hr.name = "mm_per_hr"
# 6-min water level data
df_6min_water_levels = pd.read_csv(f_water_level_storm_surge, parse_dates=True, index_col="date_time")

# determine return rates for any event, compound event, surge event, and rain event using mrms and 6 min ncei data
# ncei_rain_dates = pd.Series(data = "rain", index=pd.Series(s_rainfall_ncei_hourly_in.index.date).unique())
# ncei_rain_dates.name = "ncei_rain_data"

mrms_rain_dates = pd.Series(data = "rain", index=pd.Series(s_mrms_rainfall_trgt_res_mm_per_hr.index.date).unique())
mrms_rain_dates.name = "mrms_rain_data"

df_aorc_rainfall = pd.read_csv(f_aorc_rainfall, parse_dates=True, index_col = "time")
df_aorc_rainfall.index.name = "date_time"
s_aorc_rainfall_trgt_res_mm_per_hr = df_aorc_rainfall["rain_mean"]
aorc_rain_dates = pd.Series(data = "rain", index=pd.Series(s_aorc_rainfall_trgt_res_mm_per_hr.index.date).unique())
aorc_rain_dates.name = "aorc_rain_data"


idx_valid_rainfall = pd.concat([mrms_rain_dates, aorc_rain_dates]).index.unique().sort_values()
all_rain_dates = pd.Series(data = "rain", index=pd.Series(idx_valid_rainfall))
all_rain_dates.name = "all_rain_data"

wlevel_dates = pd.Series(data = "wlevel", index=pd.Series(df_6min_water_levels.index.date).unique())
wlevel_dates.name = "wlevel_data"

idx_valid_dates = pd.concat([all_rain_dates, wlevel_dates], join = "inner", axis = 1).index
idx_valid_dates = pd.to_datetime(idx_valid_dates)
df_valid_dates = pd.concat([all_rain_dates, wlevel_dates], axis = 1)
# df_valid_dates = pd.concat([mrms_rain_dates, wlevel_dates], join = "outer", axis = 1).index
#%% build model of # of events per year using mrms data
def create_df_of_obs_event_occurences(idx_valid_dates, df_all_events_summary, df_valid_dates, fixed_start_date, lst_event_type = ["rain", "surge", "compound"]):
    # t_window_d = 364
    start_date = idx_valid_dates.min()
    end_date = idx_valid_dates.max()
    if fixed_start_date is not None:
        start_date = pd.to_datetime(f"{start_date.year}-{fixed_start_date}")
        end_date = (pd.to_datetime(f"{end_date.year+1}-{fixed_start_date}") - np.timedelta64(24, "h")).date()
    idx_full_daterange = pd.date_range(start_date, end_date)
    fltr_valid = []
    for __, row in df_all_events_summary.iterrows():
        valid_event = False
        # for valid_date in idx_valid_dates:
        if ((idx_valid_dates >= row["event_start"].date()) & (idx_valid_dates <= row["event_start"].date())).sum()> 0:
            valid_event = True
        fltr_valid.append(valid_event)
    df_all_events_summary_subset = df_all_events_summary[fltr_valid]
    lst_s_event_occurences = []
    for event_type in lst_event_type:
        s_event_start_dates = df_all_events_summary_subset[df_all_events_summary_subset["event_type"] == event_type]["event_start"].dt.date
        s_event_occurred = pd.Series(data = True, index = s_event_start_dates)
        s_event_occurred = s_event_occurred.reindex(idx_full_daterange, fill_value=False)
        s_event_occurred.name = event_type
        lst_s_event_occurences.append(s_event_occurred)
    df_obs_annual_event_occurence = pd.concat(lst_s_event_occurences, axis = 1)
    # if fixed_start_date is not None: # create full-year groups
    s_groups = pd.Series(index = df_obs_annual_event_occurence.index, dtype=int)
    group = 0
    timeleft = True
    group_start_date = df_obs_annual_event_occurence.index.min()
    while timeleft == True:
        group_end_date = pd.to_datetime((pd.to_datetime(f"{group_start_date.year+1}-{fixed_start_date}") - np.timedelta64(24, "h")).date())
        # creat groups
        if len(s_groups.loc[group_start_date:group_end_date]) == 0:
            timeleft = False
            break
        s_groups.loc[group_start_date:group_end_date] = int(group)
        group += 1
        group_start_date = pd.to_datetime((pd.to_datetime(f"{group_start_date.year+1}-{fixed_start_date}")).date())
    s_groups = s_groups.astype(int)
    # df_obs_annual_event_counts = df_obs_annual_event_occurence.reset_index(drop = True).rolling(window=t_window_d).sum().iloc[::t_window_d].dropna()
    df_obs_annual_event_counts = df_obs_annual_event_occurence.groupby(s_groups).sum()
    df_obs_annual_event_counts.index.name = "group"
    s_groups.index.name = "date"
    s_groups.name = "group"
    s_start_dates = s_groups.to_frame().reset_index().groupby("group").min()["date"]
    s_start_dates.name = "begin_date"
    s_end_dates = s_groups.to_frame().reset_index().groupby("group").max()["date"]
    s_end_dates.name = "end_date"
    s_date_ranges = pd.concat([s_start_dates, s_end_dates], axis = 1)
    df_obs_annual_event_counts = df_obs_annual_event_counts.join(s_date_ranges)
    # compute statistics
    df_obs_annual_event_counts["any"] = df_obs_annual_event_counts.loc[:, lst_event_type].sum(axis = 1)
    # col_idx_adjust = len(df_obs_annual_event_counts.columns)
    # df_obs_annual_event_counts["ppct_quant"] = np.nan
    # df_obs_annual_event_counts["ppct_result"] = ""
    s_valid = pd.Series(data = True, index = idx_valid_dates).reindex(idx_full_daterange, fill_value=False)
    n_valid_days_per_group = s_valid.groupby(s_groups).sum()
    n_days_per_group = df_obs_annual_event_counts["end_date"] - df_obs_annual_event_counts["begin_date"] + np.timedelta64(24, "h")
    n_days_per_group = n_days_per_group / np.timedelta64(1, "D")
    df_obs_annual_event_counts["n_missing"] = (n_days_per_group - n_valid_days_per_group).astype(int)
    for grp_idx, row in df_obs_annual_event_counts.iterrows():
        n_days_in_group = n_days_per_group.loc[grp_idx]
        if row["end_date"].date() >= df_valid_dates.index.max():
            df_valid_dates_group = df_valid_dates.loc[row["begin_date"].date():].copy()
            n_additional_missing = (row["end_date"].date() - df_valid_dates_group.index.max()) / np.timedelta64(1, "D")
        else:
            df_valid_dates_group = df_valid_dates.loc[row["begin_date"].date():row["end_date"].date()].copy()
            n_additional_missing = 0
        s_days_missing_group = n_days_in_group - (~df_valid_dates_group.isna()).sum() + n_additional_missing
        df_obs_annual_event_counts.loc[grp_idx, "n_missing_rain"] = s_days_missing_group.loc["all_rain_data"]
        df_obs_annual_event_counts.loc[grp_idx, "n_missing_wlevel"] = s_days_missing_group.loc["wlevel_data"]
    # else: # do a rolling sum
    #     df_obs_annual_event_counts = df_obs_annual_event_counts.reset_index(drop = True).rolling(window=t_window_d).sum().iloc[::t_window_d].dropna()
    #     df_obs_annual_event_counts["begin_date"] = s_event_occurred.index.min()
    #     df_obs_annual_event_counts["end_date"] = s_event_occurred.index.min()
    #     # analyze missing days in observed event series
    #     iloc_idx_event_counts = df_obs_annual_event_counts.index
    #     loc_event_count_end_dates = s_event_occurred.iloc[iloc_idx_event_counts].index
    #     # ppcf
    #     low = 1
    #     high = 365
    #     # low = 1
    #     # high = 12
    #     mc_samps_for_dist = 1000
    #     df_obs_annual_event_counts["any"] = df_obs_annual_event_counts.loc[:, lst_event_type].sum(axis = 1)
    #     col_idx_adjust = len(df_obs_annual_event_counts.columns)
    #     df_obs_annual_event_counts["n_missing"] = np.nan
    #     df_obs_annual_event_counts["ppct_quant"] = np.nan
    #     df_obs_annual_event_counts["ppct_result"] = ""
    # #     # lst_n_missing = []
    # prev_enddate = s_event_occurred.index.min()
    # for i, enddate in tqdm(enumerate(loc_event_count_end_dates)):
    #     s_event_occurred_reindexed = s_event_occurred.reindex(idx_full_daterange, fill_value=np.nan).loc[prev_enddate:enddate]
    #     s_event_occurred_reindexed_missing = s_event_occurred_reindexed[s_event_occurred_reindexed.isna()]
    #     s_event_occurred_reindexed_present = s_event_occurred_reindexed[~s_event_occurred_reindexed.isna()]
    #     s_monthly_date_counts = pd.Series(s_event_occurred_reindexed_present.index.month).value_counts().sort_index()
    #     # probability plot correlation test: uniform distribution
    #     ## create series of days included in the observational dataset replacing 366 (on leap years) with 29
    #     s_obs = pd.Series(s_event_occurred_reindexed_present.index.day_of_year).replace(366, 29)
    #     # s_obs = pd.Series(s_event_occurred_reindexed_present.index.month)
    #     quant, fig, ax = ppcf_randint(s_obs, mc_samps_for_dist, low, high, plot = False, alpha = alpha)
    #     if (quant < alpha):
    #         result = "reject"
    #     else:
    #         result = "fail_to_reject"
    #     df_obs_annual_event_counts.iloc[i, col_idx_adjust+3] = quant
    #     df_obs_annual_event_counts.iloc[i, col_idx_adjust+4] = result
    #     # if result == "reject":
    #     #     quant, fig, ax = ppcf_randint(s_obs, mc_samps_for_dist, low, high, plot = True, alpha = 0.05)
    #     #     ax.set_title(F"Probability Plot Correlation Test\nObserved day-of-year valid dates in 365-day period vs.\nsimulated from randint(low={low}, high={high}) distribution")

    #     df_obs_annual_event_counts.iloc[i, col_idx_adjust] = len(s_event_occurred_reindexed_missing)
    #     df_obs_annual_event_counts.iloc[i, col_idx_adjust+1] = prev_enddate
    #     df_obs_annual_event_counts.iloc[i, col_idx_adjust+2] = enddate
    #     prev_enddate = enddate
    return df_obs_annual_event_counts



idx_valid_allrain_and_wlevel_dates = pd.concat([all_rain_dates, wlevel_dates], join = "inner", axis = 1).index
idx_valid_allrain_and_wlevel_dates = pd.to_datetime(idx_valid_allrain_and_wlevel_dates).date
idx_valid_dates = idx_valid_allrain_and_wlevel_dates
fixed_start_date = "01-01"
df_obs_annual_event_counts = create_df_of_obs_event_occurences(idx_valid_dates, df_all_events_summary, df_valid_dates, fixed_start_date, lst_event_type = ["rain", "surge", "compound"])

print("Mean annual occurence rate before removing observations")
print(df_obs_annual_event_counts.loc[:, ["rain", "surge", "compound"]].mean())


df_obs_annual_event_counts_for_fitting = df_obs_annual_event_counts[df_obs_annual_event_counts.n_missing < 10]
print("Mean annual occurence rate after removing observations where n_missing > 10")
print(df_obs_annual_event_counts_for_fitting.loc[:, ["rain", "surge", "compound"]].mean())
df_occurences_obs = df_obs_annual_event_counts_for_fitting.loc[:, ["any", "rain", "surge", "compound"]]
#%% create observed event time series for all years with complete observations with mrms and 6 minute water level data
# create dataframe with observed events that occurred during years with complete observations
valid_rows = []
for idx, row in df_all_events_summary.iterrows():
    filter_in_complete_year = (pd.to_datetime(row.event_start.date()) >= df_obs_annual_event_counts_for_fitting["begin_date"]) & (pd.to_datetime(row.event_start.date()) <= df_obs_annual_event_counts_for_fitting["end_date"])
    if filter_in_complete_year.sum() == 1:
        row.loc["year"] = filter_in_complete_year.reset_index(drop = True).idxmax()
        valid_rows.append(row)
    elif filter_in_complete_year.sum() > 1:
        sys.exit("ERROR: an event falls into more than 1 year")
    else:
        continue

df_events_in_complete_years = pd.concat(valid_rows, axis = 1).T

# only keep years that use 6 min water level data and mrms rainfall data exclusively
valid_years = []
for year, df_year in df_events_in_complete_years.groupby("year"):
    if df_year.wlevel_data_source.unique() != "6min_surge_data":
        continue
    if len(df_year.rain_data_source.unique()) > 1:
        continue
    if df_year.rain_data_source.unique() != "mrms":
        continue
    valid_years.append(year)

df_events_in_complete_years = df_events_in_complete_years[df_events_in_complete_years.year.isin(valid_years)]

# reindex event summaries and time series to match the structure of the simulated event time series (so event ids in ascending order for each event type, organized by year)
lst_valid_smry_dfs_reindexed = []
lst_valid_tseries_dfs_reindexed = []
new_year = -1
for year, df_year in df_events_in_complete_years.groupby("year"):
    new_year += 1
    df_year['year'] = new_year
    for event_type, df_year_type in df_year.groupby("event_type"):
        og_event_ids = df_year_type["event_id"]
        df_year_type["event_id"] = np.arange(1, len(df_year_type)+1)
        lst_valid_smry_dfs_reindexed.append(df_year_type)

        df_tseries_subset = df_combined_tseries[df_combined_tseries.event_id.isin(og_event_ids.values)].copy()
        # e_id lookup
        d_re_index_events = {}
        for idx, og_e_id in og_event_ids.items():
            new_e_id = df_year_type["event_id"].loc[idx]
            d_re_index_events[og_e_id] = new_e_id

        new_tseries_eid = df_tseries_subset.loc[:, "event_id"].replace(d_re_index_events)
        df_tseries_subset.loc[:, "event_id"] = new_tseries_eid
        df_tseries_subset.loc[:, "year"] = new_year
        df_tseries_subset.loc[:, "event_type"] = event_type
        lst_valid_tseries_dfs_reindexed.append(df_tseries_subset)
        # sys.exit()

df_events_in_complete_years = pd.concat(lst_valid_smry_dfs_reindexed)
df_events_in_complete_years.set_index(["year", "event_type", 'event_id']).to_csv(f_obs_event_summaries_from_continuous_years)

df_combined_tseries_mrms_6min_surge = pd.concat(lst_valid_tseries_dfs_reindexed)

# extract mrms rain fall intensity column names
col_rain = []
for colname in df_rain_tseries.columns:
    if "rain_mean" != colname:
        col_rain.append(str(colname))
df_rain_tseries = df_rain_tseries.loc[:, col_rain]
# col_rain.append("mm_per_hr")

# join original spatially distributed mrms data to the event time series
df_combined_tseries_mrms_6min_surge = df_combined_tseries_mrms_6min_surge.set_index("date_time")
df_rain_tseries.index.name = "date_time"
df_observed_events_to_simulate = df_combined_tseries_mrms_6min_surge.join(df_rain_tseries, how = "left")
df_observed_events_to_simulate = df_observed_events_to_simulate.drop(columns = ["rain_data_source", "wlevel_data_source", "mm"]).reset_index().set_index(["year", "event_type", "event_id", 'date_time'])

s_first_tstep_w_rainfall = df_observed_events_to_simulate[df_observed_events_to_simulate["mm_per_hr"]>0].reset_index().groupby(["year", "event_type", "event_id"])["date_time"].min()
s_first_tstep_w_rainfall.name = "first_obs_tstep_w_rainfall"
df_first_tstep_w_rainfall = s_first_tstep_w_rainfall.to_frame()
# reindex the observed time series to follow same time buffer rules as the final simulated product
lst_df_reindexed = []
for grp_id, df_grp in df_observed_events_to_simulate.groupby(level = ["year", "event_type", "event_id"]):
    year, event_type, e_id = grp_id
    # sys.exit()
    df_tseries_combined = df_grp.reset_index()
    df_tseries_combined = df_tseries_combined.set_index("date_time")
    df_tseries_combined = reindex_to_buffer_significant_rain_and_surge(df_tseries_combined)
    df_extended_wlevel_tseries = ds_6min_water_levels_trgt_res.sel(date_time = slice(df_tseries_combined.index.min(),df_tseries_combined.index.max())).to_dataframe()
    df_extended_wlevel_tseries.index.name = "timestep"
    # fill rainfall with 0 and join with extended waterlevel data
    df_tseries_combined = df_tseries_combined.drop(columns = ["surge_ft", "waterlevel_ft", "event_id"]).fillna(0).join(df_extended_wlevel_tseries, how = "inner")
    df_tseries_combined["event_id"] = e_id
    df_tseries_combined["event_type"] = event_type
    df_tseries_combined["year"] = year
    # reindex
    df_tseries_combined.index = df_tseries_combined.index - df_tseries_combined.index.min() + arbirary_start_date
    df_tseries_combined.index.name = "timestep"
    lst_df_reindexed.append(df_tseries_combined.reset_index().set_index(["year", "event_type", 'event_id', 'timestep']))

# create dataframe with all the re-indexed observed events for simulating
df_obs_events = pd.concat(lst_df_reindexed)


# convert surge units from feet to meters
for col_val_in_feet in [col for col in df_obs_events.columns if ('_ft' in col)]:
    newname = col_val_in_feet.replace("_ft", "_m")
    dic_rename = {col_val_in_feet:newname}
    # sys.exit("work")
    df_obs_events = df_obs_events.rename(columns = dic_rename)
    df_obs_events[newname] = df_obs_events[newname] / feet_per_meter
    print(f"Converted {col_val_in_feet} to meters and renamed it to {newname}")


df_obs_events = df_obs_events.drop(columns = ["bias_corrected"])

ds_obs_events = df_obs_events.to_xarray()
# sys.exit("work")
ds_first_tstep_w_rainfall = df_first_tstep_w_rainfall.to_xarray()

ds_obs_events = xr.merge([ds_obs_events, ds_first_tstep_w_rainfall])

comp = dict(zlib=True, complevel=to_netcdf_compression_level)
encoding = {var: comp for var in ds_obs_events.data_vars}
ds_obs_events.to_netcdf(f_obs_event_tseries_from_continuous_years, encoding=encoding, engine = "h5netcdf")

#%% analyze missing periods
dir_plots_ommitted_time_periods = f"{dir_plot}event_occurence_simulations/"
Path(dir_plots_ommitted_time_periods).mkdir(parents=True, exist_ok=True)

lst_all_valid_dates_in_rejected_samples = []
for idx, row in df_obs_annual_event_counts.iterrows():
    if row["n_missing"] < 10:
        continue
    
    idx_all_dates_over_period = pd.date_range(row["begin_date"], row["end_date"])
    idx_valid_dates_over_period = idx_valid_dates[(idx_valid_dates >= row["begin_date"].date()) & (idx_valid_dates <= row["end_date"].date())]

    s_valid_dates = pd.Series(data = True, index = idx_valid_dates_over_period)
    s_valid_dates.name = "valid_date"
    s_valid_dates = s_valid_dates.reindex(idx_all_dates_over_period, fill_value=False)
    fig, ax = plt.subplots()
    s_valid_dates.groupby(s_valid_dates.index.month).sum().plot.bar(ax=ax)
    lst_all_valid_dates_in_rejected_samples.append(s_valid_dates)
    ax.set_title(f"Valid Date Observation Count by Month\n{row['begin_date'].date()} to {row['end_date'].date()} (n_missing = {row['n_missing']})")
    plt.savefig(f"{dir_plots_ommitted_time_periods}rejected_time_periods_{row['begin_date'].date()}_to_{row['end_date'].date()}.png", bbox_inches='tight')
    plt.clf()
#%%
fig, ax = plt.subplots()
s_all_valid_dates_of_rejected_samples = pd.concat(lst_all_valid_dates_in_rejected_samples)
s_all_valid_dates_of_rejected_samples.groupby(s_all_valid_dates_of_rejected_samples.index.month).sum().plot.bar(ax=ax)
ax.set_title(f"Valid Date Observation Count by Month\nfor all rejected time periods")
plt.savefig(f"{dir_plots_ommitted_time_periods}rejected_time_periods_all.png", bbox_inches='tight')
plt.clf()
#%% (OBSOLETE - NOT USING POISSON) generate event occurences from poisson distribution
def estimate_poisson_lambda(df_obs_annual_event_counts, idx_valid_dates, event_type):
    s_obs_annual_event_counts = df_obs_annual_event_counts[event_type]
    s_obs_annual_event_counts = s_obs_annual_event_counts.reset_index(drop = True)
    n_years = len(idx_valid_dates)/365
    n_observations = s_obs_annual_event_counts.sum()
    lbda = n_observations/n_years
    print(f"Computed a lambda for {event_type} events of {lbda:.2f} events per year based on a {n_years:.2f} year dataset which included {n_observations} observations of this event type.")
    return lbda

def simulate_from_poisson(lbda, n_years_to_simulate, random_seed, event_type):
    n_per_year_sim = poisson.rvs(lbda, size=n_years_to_simulate, random_state = random_seed)
    s_sim = pd.Series(n_per_year_sim)
    s_sim.name = event_type
    return s_sim


## compound
event_type="compound"
lbda_compound = estimate_poisson_lambda(df_obs_annual_event_counts_for_fitting, idx_valid_dates, event_type)

## rain only
event_type="rain"
lbda_rain = estimate_poisson_lambda(df_obs_annual_event_counts_for_fitting, idx_valid_dates, event_type)

## surge only
event_type="surge"
lbda_surge = estimate_poisson_lambda(df_obs_annual_event_counts_for_fitting, idx_valid_dates, event_type)

# ## simulate from poisson distribution
# n_per_year_sim_compound = simulate_from_poisson(lbda_compound, n_years_to_simulate, random_seed, "compound")
# n_per_year_sim_rain = simulate_from_poisson(lbda_rain, n_years_to_simulate, random_seed, "rain")
# n_per_year_sim_surge = simulate_from_poisson(lbda_surge, n_years_to_simulate, random_seed, "surge")

# # df_occurences_obs = pd.concat([s_obs_annual_event_counts_compound, s_obs_annual_event_rain, s_obs_annual_event_counts_surge], axis = 1)
# # df_occurences_obs["any"] = df_occurences_obs.sum(axis = 1, skipna = False)

# df_occurences_sim = pd.concat([n_per_year_sim_compound, n_per_year_sim_rain, n_per_year_sim_surge], axis = 1)
# df_occurences_sim["any"] = df_occurences_sim.sum(axis = 1)

# plot simulations
# txt_fig_caption = f"This is based on the number of 365-day observational periods on record"
# savefig_filename = f"{dir_plot}event_occurences_observed.png"
# plot_event_occurances(df_occurences_obs, txt_fig_caption=txt_fig_caption, savefig_filename=savefig_filename, plot_n_count = True)
# 

# savefig_filename = None
# txt_fig_caption = f"n={n_years_of_weather_to_generate} years of event occurences simulated using 3 poissons distributions for rainfall, storm surge, and compound events"
# compare_sim_vs_obs_event_occurances(df_occurences_sim.iloc[0:n_years_of_weather_to_generate, :], df_occurences_obs, txt_fig_caption = txt_fig_caption, savefig_filename=savefig_filename, plot_n_count = True)

# n_bootstrap = 200
# alpha = 0.05
# dir_savefig = f"{dir_plot}occurence_sim_vs_obs_gof/"
# try:
#     shutil.rmtree(dir_savefig)
# except:
#     pass
# Path(dir_savefig).mkdir(parents=True, exist_ok=True)
# figtitle_supplementary_text = ""
# for dataset in df_occurences_sim.columns:
#     fig_fname_prefix = f"_event_occurence_gof_"
#     s_dataset1 = df_occurences_sim[dataset]
#     s_dataset1.name = "simulated"
#     s_dataset2 = df_occurences_obs[dataset].dropna()
#     s_dataset2.name = "observed"
#     n_per_sample = len(s_dataset2)
#     compare_2_distributions(s_dataset1, s_dataset2, dataset, n_bootstrap, n_per_sample, alpha, bins = 30, include_kde = True,
#                                         dir_savefig = dir_savefig, figtitle_supplementary_text = figtitle_supplementary_text,
#                                             fig_fname_prefix = fig_fname_prefix)
    
# print("not using Poisson")

#%% looking at monthly event occurence rates
fig, axes = plt.subplots(1, 3, figsize = (12, 4))
months = pd.Series(df_all_events_summary.event_start.dt.month.unique()).sort_values().values

s_event_counts = df_all_events_summary[df_all_events_summary["event_type"] == "compound"].event_start.dt.month.value_counts()
s_event_counts.reindex(months, fill_value=0).sort_index().plot.bar(ax=axes[0])
axes[0].set_title("Compound Events")
axes[0].set_xlabel("Month")


s_event_counts = df_all_events_summary[df_all_events_summary["event_type"] == "surge"].event_start.dt.month.value_counts()
s_event_counts.reindex(months, fill_value=0).sort_index().plot.bar(ax=axes[1])
axes[1].set_title("Surge Events")
axes[1].set_xlabel("Month")


s_event_counts = df_all_events_summary[df_all_events_summary["event_type"] == "rain"].event_start.dt.month.value_counts()
s_event_counts.reindex(months, fill_value=0).sort_index().plot.bar(ax=axes[2])
axes[2].set_title("Rain Events")
axes[2].set_xlabel("Month")

plt.savefig(f"{dir_plots_ommitted_time_periods}event_occurance_counts_by_month.png", bbox_inches='tight')
plt.clf()
#%% generating event occurenaces by bootstrapping with replacement
df_occurences_sim = df_occurences_obs.sample(n=n_years_to_simulate, replace = True, random_state = random_seed).reset_index(drop = True)
df_occurences_sim = df_occurences_sim.astype(int)
savefig_filename = f"{dir_plot}event_occurences_simulated_{n_years_of_weather_to_generate}yrs.png"
txt_fig_caption = f"n={n_years_of_weather_to_generate} years of event occurences simulated by sampling with replacement {len(df_occurences_obs)} continuous 365-day event counts"
compare_sim_vs_obs_event_occurances(df_occurences_sim.iloc[0:n_years_of_weather_to_generate, :], df_occurences_obs, txt_fig_caption = txt_fig_caption, savefig_filename=savefig_filename, plot_n_count = True)

df_occurences_sim.index.name = "year"
df_occurences_sim.iloc[0:n_years_of_weather_to_generate, :].to_csv(f"{dir_simulate_from_copula}simulated_event_counts_{n_years_of_weather_to_generate}yrs.csv")

#%%  sampling of surge, rain, and combo events
# define surge event and rain event thresholds from event time series
surge_event_threshold = df_surge_events.surge_peak_ft.min()
rain_event_threshold = df_rain_events.precip_depth_mm.min()

n_surge_events = df_occurences_sim["surge"].sum()
n_rain_events = df_occurences_sim["rain"].sum()
n_combo_events = df_occurences_sim["compound"].sum()
n_total_events = df_occurences_sim["any"].sum()

dic_event_category = {"compound_events":"compound",
                "rain_events":"rain",
                "surge_events":"surge"}

df_marginals_compound = f"{dir_fitting_maringals}compound_events_distributions.csv"
df_marginals_rain = f"{dir_fitting_maringals}rain_events_distributions.csv"
df_marginals_surge = f"{dir_fitting_maringals}surge_events_distributions.csv"

n_to_oversimulate = n_years_to_simulate * 10
dic_n_to_simulate = {"compound_events":n_combo_events,
                     "rain_events":n_rain_events,
                     "surge_events":n_surge_events}

lst_sim_to_skip = []#, "compound_events", "rain_events"]

n_oversimulate_multiplier = 2
lst_df_simulated_realspace = []
dic_frac_invalid = {}
dic_full_over_sim = {}
from __ref_ams_functions import *
for event_type_key in dic_copulas:
    if event_type_key in lst_sim_to_skip: # not modeling all events at the same time since the marginal fit qualities were poor
        continue 
    # sys.exit("work")
    cop_fitted = dic_copulas[event_type_key]
    df_event_summaries = dic_event_summaries[event_type_key]
    n_to_simulate = dic_n_to_simulate[event_type_key] * n_oversimulate_multiplier
    df_marginal_fits = pd.read_csv(f"{dir_fitting_maringals}{event_type_key}_distributions.csv")
    df_obs = format_fitted_cdf_values_for_fitting_copula(event_type_key=event_type_key)
    # simulate from copula
    df_simulated_cdf = simulate_from_copula(df_obs=df_obs, n=n_to_simulate, cop_fitted=cop_fitted, seeds = random_seed)
    df_simulated_realspace = pd.DataFrame(index = df_simulated_cdf.index, columns = df_simulated_cdf.columns)
    for idx, fitted_row in df_marginal_fits.iterrows():
        dataset = fitted_row["data"]
        s_data_sim = df_simulated_cdf[dataset]
        # if there is an upper bound, need to flip the simulated cdf values before transforming back to real space
        if not np.isnan(fitted_row["upper_bound"]):
            print(f"Flipping cdf values for copula simluated {event_type_key} {dataset} because an upper bound transformation was applied to this dataset")
            s_data_sim_untransformed = s_data_sim.copy()
            s_data_sim = 1 - s_data_sim
        #     sys.exit("work")
        n_params = fitted_row["n_params"]
        fx = getattr(stats, fitted_row.loc["distribution"])
        if n_params == 3:
            x_sim = pd.Series(fx.ppf(s_data_sim, fitted_row["shape"], fitted_row["loc"], fitted_row["scale"]), name = "x_fit_at_empirical_cdf_val")
        if n_params == 2:
            x_sim = pd.Series(fx.ppf(s_data_sim, fitted_row["loc"], fitted_row["scale"]), name = "x_fit_at_empirical_cdf_val")
        # only has an effect if a transformation was applied
        x_sim_realspace = backtransform_data(x_sim, fitted_row)
        df_simulated_realspace.loc[:,dataset] = x_sim_realspace
    # classify events
    filter_surge_event = df_simulated_realspace["surge_peak_ft"] >= surge_event_threshold
    filter_rain_event = df_simulated_realspace["precip_depth_mm"] >= rain_event_threshold
    filter_compound_event = filter_surge_event & filter_rain_event
    df_simulated_realspace["rain_event"] = filter_rain_event
    df_simulated_realspace["surge_event"] = filter_surge_event
    df_simulated_realspace["compound_event"] = filter_compound_event
    df_simulated_realspace["copula_source"] = event_type_key
    dic_full_over_sim[event_type_key] = df_simulated_realspace.copy()
    df_realspace_valid = df_simulated_realspace[~df_simulated_realspace.isna().any(axis = 1)]
    # compute frac invalid as a performance metric
    n_invalid = len(df_simulated_realspace) - len(df_realspace_valid)
    frac_invalid = n_invalid / len(df_simulated_realspace)
    dic_frac_invalid[event_type_key] = frac_invalid
    if event_type_key == 'surge_events':
        df_realspace_valid = df_realspace_valid[(df_realspace_valid["surge_event"] == True) & df_realspace_valid["compound_event"] == False]
        df_simulated_return = df_realspace_valid.sample(dic_n_to_simulate[event_type_key])
    if event_type_key == "rain_events":
        df_realspace_valid = df_realspace_valid[(df_realspace_valid["rain_event"] == True) & df_realspace_valid["compound_event"] == False]
        df_simulated_return = df_realspace_valid.sample(dic_n_to_simulate[event_type_key])
    if event_type_key == "compound_events":
        df_realspace_valid = df_realspace_valid[df_realspace_valid["compound_event"] == True]
        df_simulated_return = df_realspace_valid.sample(dic_n_to_simulate[event_type_key])
    # if frac_invalid > 0:
    #     print(f"{frac_invalid} is the fraction of {event_type_key} simulations that were invalid either due to thresholds not being properly met or there being na values after the realspace transformation.")

    # df_simulated_realspace = pd.concat([df_compound_events, df_non_compound_events])
    lst_df_simulated_realspace.append(df_simulated_return)
    # plt.scatter(df_simulated_return["max_1hr_mean_mm_per_hr"], df_simulated_return["precip_depth_mm"])
    # sys.exit("work")
    
df_simulated_realspace = pd.concat(lst_df_simulated_realspace)

for key in dic_frac_invalid.keys():
    if dic_frac_invalid[key] > 0:
        print(f"For {key}, {100*dic_frac_invalid[key]:.5f}% of simulated events are invalid and were rejected (probably due to na values in the transformations).")
        print(f"see dic_full_over_sim['{key}'].isna().sum() to see which variables were the problem going on")

# double check that the total number of events line up
chk1 = (n_total_events == len(df_simulated_realspace))
chk2 = (n_surge_events == ((df_simulated_realspace["compound_event"] == False) & (df_simulated_realspace["surge_event"] == True)).sum())
chk3 = (n_rain_events == ((df_simulated_realspace["compound_event"] == False) & (df_simulated_realspace["rain_event"] == True)).sum())
chk4 = (n_combo_events == ((df_simulated_realspace["compound_event"] == True).sum()))

if not (chk1 and chk2 and chk3 and chk4):
    print("WARNING: THE TOTAL NUMBER OF SIMULATED EVENTS DOES NOT ALIGN WITH THE EXPECTED NUMBER")


#%% check to make sure that compound event statistics generated from surge copula are same as those generated from rain copula
dic_simulations = {}
for event_type in df_simulated_realspace.copula_source.unique():
    df_simulated_realspace_subset = df_simulated_realspace[df_simulated_realspace["copula_source"] == event_type]
    df_simulated_realspace_overlapping_vars = df_simulated_realspace_subset.dropna(axis = 1)
    dic_simulations[event_type] = df_simulated_realspace_overlapping_vars.reset_index(drop = True)
    df_event_summaries = dic_event_summaries[event_type]
    lst_vars_in_common = []
    for var in df_simulated_realspace_overlapping_vars.columns:
        if var not in ["rain_event", "surge_event", "compound_event", "copula_source"]:
            lst_vars_in_common.append(var)

    alpha = 0.05
    n_bootstrap = 1000
    dir_savefig = dir_plot + f"sim_vs_obs_{event_type}/"
    figtitle_supplementary_text = f""
    fig_fname_prefix = f"{event_type}_comparison_"
    cols_to_compare = lst_vars_in_common
    for dataset in cols_to_compare:
        s_dataset1 = df_event_summaries[dataset].dropna()
        s_dataset1.name = "observed"
        s_dataset2 = df_simulated_realspace_overlapping_vars[dataset]
        s_dataset2.name = "simulated"
        n_per_sample = len(s_dataset1)
        compare_2_distributions(s_dataset1, s_dataset2, dataset, n_bootstrap, n_per_sample, alpha, bins = 30, include_kde = True,
                                    dir_savefig = dir_savefig, figtitle_supplementary_text = figtitle_supplementary_text,
                                        fig_fname_prefix = fig_fname_prefix)



#%% distributing events over time
event_id = 0
surge_event_idx = 0
rain_event_idx = 0
compound_event_idx = 0
for year, row in df_occurences_sim.iterrows():
    for idx in np.arange(row["surge"]):
        dic_simulations["surge_events"].loc[surge_event_idx, "year"] = year
        dic_simulations["surge_events"].loc[surge_event_idx, "event_id"] = event_id
        event_id += 1
        surge_event_idx += 1
    for idx in np.arange(row["rain"]):
        dic_simulations["rain_events"].loc[rain_event_idx, "year"] = year
        dic_simulations["rain_events"].loc[rain_event_idx, "event_id"] = event_id
        event_id += 1
        rain_event_idx += 1
    for idx in np.arange(row["compound"]):
        dic_simulations["compound_events"].loc[compound_event_idx, "year"] = year
        dic_simulations["compound_events"].loc[compound_event_idx, "event_id"] = event_id
        event_id += 1
        compound_event_idx += 1

# export to file and plot
from matplotlib.gridspec import GridSpec
alpha=0.05
for event_type_key in dic_simulations:
    df_event_sims = dic_simulations[event_type_key]
    # update date types 
    df_event_sims["year"] = df_event_sims["year"].astype(int)
    df_event_sims["event_id"] = df_event_sims["event_id"].astype(int)
    # create multi-index
    df_event_sims.reset_index(drop = True, inplace = True)
    df_event_sims.set_index(["year", "event_id"])
    df_event_sims.to_csv(f"{dir_simulate_from_copula}{event_type_key}_simulated_event_summaries.csv")
    # double check that the correct thresholds are met
    s_surge_unmet = df_event_sims["surge_peak_ft"] < surge_event_threshold
    s_rain_unmet = df_event_sims["precip_depth_mm"] < rain_event_threshold
    problem = False
    if event_type_key in ["compound_events", "rain_events"]:
        if s_rain_unmet.sum() > 0:
            problem = True
            print(f'WARNING: There are {s_rain_unmet.sum()} {event_type_key} that do not meet the rain event threshold.')
    if event_type_key in ["compound_events", "surge_events"]:
        if s_surge_unmet.sum() > 0:
            problem = True
            print(f'WARNING: There are {s_surge_unmet.sum()} {event_type_key} that do not meet the surge event threshold.')
    s_surge_met = df_event_sims["surge_peak_ft"] >= surge_event_threshold
    s_rain_met = df_event_sims["precip_depth_mm"] >= rain_event_threshold
    if event_type_key == "surge_events":
        if s_rain_met.sum() > 0:
            problem = True
            print(f"WARNING: There are {s_rain_met.sum()} {event_type_key} that also meet the rain event threshold.")
    if event_type_key == "rain_events":
        if s_surge_met.sum() > 0:
            problem = True
            print(f"WARNING: There are {s_surge_met.sum()} {event_type_key} that also meet the surge event threshold.")
    if problem:
        break
    # create plots comparing observed to simulated data
    df_cdf_obs = format_fitted_cdf_values_for_fitting_copula(event_type_key=event_type_key)
    df_observed = dic_event_summaries[event_type_key]
    # Number of datasets and number of columns
    num_datasets = len(df_cdf_obs.columns)
    ncols = 3
    nbins = 15
    nrows = (num_datasets + ncols - 1) // ncols  # Calculate rows needed

    # Create a larger figure and adjust aspect ratio
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(18, 7 * nrows), dpi = 250)  # Adjust height for each row
    axes = axes.flatten()

    for ds_idx, dataset in enumerate(df_cdf_obs.columns):
        # Get the respective axes for plotting
        ax_plot = axes[ds_idx]

        sample1 = df_observed[dataset].dropna()
        sample2 = df_event_sims[dataset]

        # Plot histogram and KDE for sample1 and sample2
        ax_plot.hist(sample1, bins=nbins, color='blue', alpha=0.3, label='Observed Hist', density=True)
        ax_plot.hist(sample2, bins=nbins, color='green', alpha=0.3, label='Simulated Hist', density=True)
        sns.kdeplot(sample1, color='blue', label='Observed KDE', fill=False, ax=ax_plot)
        sns.kdeplot(sample2, color='green', label='Simulated KDE', fill=False, ax=ax_plot)

        # Add thresholds with text for surge and rain
        if dataset == "surge_peak_ft":
            if event_type_key == "rain_events":
                txt_offset = -1.5
            else:
                txt_offset = 0.5
            ax_plot.axvline(surge_event_threshold, color='red', linestyle='--')
            ax_plot.text(surge_event_threshold + txt_offset, ax_plot.get_ylim()[1] * 0.9,
                        f'Surge\nevent\nthreshold (ft) = {surge_event_threshold:.2f}', 
                        color='red', bbox=dict(facecolor='white', alpha=0.5))

        if dataset == "precip_depth_mm":
            ax_plot.axvline(rain_event_threshold, color='red', linestyle='--')
            ax_plot.text(rain_event_threshold + 0.5, ax_plot.get_ylim()[1] * 0.9,
                        f'Rain\nevent\nthreshold (mm) = {rain_event_threshold:.2f}', 
                        color='red', bbox=dict(facecolor='white', alpha=0.5))

        # Set the title and labels for each subplot
        ax_plot.set_title(f'{dataset}')
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
    plt.savefig(f"{dir_plot}simulated_vs_observed_{event_type_key}.png")
    plt.clf()
    # break

#%% make sure relationships between real space variables makes sense

for event_type_key in dic_copulas:
    if event_type_key in lst_sim_to_skip: # not modeling all events at the same time since the marginal fit qualities were poor
        continue 

    df_simulated_realspace_subset = df_simulated_realspace[df_simulated_realspace["copula_source"] == event_type_key]
    df_simulated_realspace_overlapping_vars = df_simulated_realspace_subset.dropna(axis = 1)
    # sys.exit("work")
    # cop_fitted = dic_copulas[event_type_key]
    df_event_summaries = dic_event_summaries[event_type_key]

    # df_marginal_fits = pd.read_csv(f"{dir_fitting_maringals}{event_type_key}_distributions.csv")
    df_obs = format_fitted_cdf_values_for_fitting_copula(event_type_key=event_type_key)

    dir_plot_events = f"{dir_plot}pairwise_realspace_comp_{event_type_key}/"
    Path(dir_plot_events).mkdir(parents=True, exist_ok=True)

    n_for_montecarlo_tau_test = 1000
    df_obs = df_event_summaries.loc[:, df_obs.columns].astype(float)
    cop_fitted=None
    df_simulated= df_simulated_realspace_overlapping_vars.loc[:, df_obs.columns].astype(float)
    df_mc_results_combined = evaluate_copula_fit_based_on_comparing_kendalltaus_for_each_variable_pair(df_obs=df_obs, alpha=alpha,n_for_montecarlo_tau_test=n_for_montecarlo_tau_test, cop_fitted=None,
                                                                                                        df_simulated=df_simulated, plot = True, fldr_plots = dir_plot_events,
                                                                                                        fig_main_title=f"observed vs. stochastically simualted {event_type_key}")