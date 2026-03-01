#%% import libraries
from pathlib import Path
import os
import pandas as pd


#%% User inputs
n_years_of_weather_to_generate = 1000
# constantsdef_inputs_for_a
mm_per_inch = 25.4
feet_per_meter = 3.28084
rain_event_threshold_mm_selected = 52
surge_event_threshold_ft_selected = 2.5

# script a
begin_year = 1928

# minimum record length to use an NCEI dataset
min_record_length = 30

# SST stuff
nrealizations = 2
sst_tstep = 5
sst_event_duration = 72 # hours
sst_storms_per_year = 5
start_date = "2020-09-01" # start date for each of the time series in SWMM

threshold_varname_rain_events = "precip_depth_mm"
threshold_varname_surge_events = "surge_peak_ft"

# stochastic storm rescaling time buffers
timeseries_buffer_around_peaks_h = 3 # amount to extend time series to buffer peak surge by 3 hours before and after, and the last significant rainfall plug by 3 hours
timeseries_buffer_before_first_rain_h = 2 # amount to extend time series to ensure at least 2 hours of simulation before first rainfall timestep
buffer_window_raindepth_fraction_to_trigger_tserties_extension = 0.1 # if this fraction of rainfall or greater happens in a upsampled same with timestep timeseries_buffer_around_peaks_h, extend tseries by timeseries_buffer_around_peaks_h
compound_event_time_window_multiplier = 1.5 # this times the max observed compound event duration determines the max duration of a stochastically generated time series (the max observed event duration is used for rain and surge events)
arbirary_start_date = pd.to_datetime("2025-8-31") # for final time series
#%% Directories
fldr_nrflk = str(Path(os.getcwd()).parents[1]) + "/"
fldr_stormy = fldr_nrflk + "stormy/"
dir_sst = fldr_stormy + "stochastic_storm_transposition/"
fldr_ssr = fldr_stormy + "stochastic_storm_rescaling/"
fldr_mrms_processing = fldr_nrflk + "/highres-radar-rainfall-processing/data/"
fldr_mrms_at_gages = fldr_mrms_processing + "mrms_zarr_preciprate_fullres_yearlyfiles_atgages/"
dir_ssr_outputs = fldr_ssr + "outputs/"

# SWMM stuff
fldr_swmm = fldr_stormy + "swmm/"
fldr_swmm_features = fldr_swmm + "hague/swmm_model/exported_layers/"
fldr_swmm_tseries = fldr_stormy + "swmm/hague/swmm_timeseries/"
f_csv_design_storms = f"{fldr_swmm_tseries}design_storms.csv"
f_swmm_model = fldr_swmm + "hague_V1_using_norfolk_data.inp"
f_shp_swmm_subs = fldr_swmm_features + "subcatchments.shp"

# NCEI Data
fldr_NCEI = fldr_stormy + "data/climate/NCEI/"
f_daily_summaries = fldr_NCEI + "2023-1-4_NCEI_daily summaries_download.csv"
f_hourlyprecip_airport = fldr_NCEI + "2024-5-16_NCEI_airport_hourly.csv"
f_daily_summaries_airport = fldr_NCEI + "2024-5-16_NCEI_airport_daily.csv"

dir_noaa_water_levels = dir_ssr_outputs + "a_NOAA_water_levels/"
f_out_a_meta = dir_noaa_water_levels + 'sewells_pt_water_level_metadatap.json'
f_water_level_storm_surge = dir_noaa_water_levels + "a_water-lev_tide_surge.csv"
f_water_level_storm_surge_hrly = dir_noaa_water_levels + "a_water-lev_tide_surge_hrly.csv"

#
dir_aorc_rainfall = dir_ssr_outputs + "a2_aorc_data/"
f_aorc_data_pre_mrms = dir_aorc_rainfall + "aorc_pre_mrms.zarr"
f_aorc_data = dir_aorc_rainfall + "aorc.zarr"
f_aorc_data_missing_mrms_fill = dir_aorc_rainfall + "aorc_2012-2014.zarr"


# dcl work 
f_water_level_storm_surge_event_summaries = dir_noaa_water_levels + "a1a_surge_event_summary.csv"
f_water_level_storm_surge_event_tseries = dir_noaa_water_levels + "a1b_surge_event_tseries.csv"
f_water_level_hourly_storm_surge_event_summaries = dir_noaa_water_levels + "a2a_hourly_surge_event_summary.csv"
f_water_level_hourly_storm_surge_event_tseries = dir_noaa_water_levels + "a2b_hourly_surge_event_tseries.csv"
# end dcl work
f_out_a_shp = dir_noaa_water_levels + "sewells_pt.shp"
f_out_swmm_waterlevel = fldr_swmm_tseries + "a_water_levels_ft.dat"

# event selection
# min_interevent_time = 6 # hour
# max_event_length = 72 # hours
# min_event_threshold_in = 0.5 # inches of total rainfall

dir_storm_rescaling = dir_ssr_outputs + "b7_storm_rescaling/"

dir_event_selection = dir_ssr_outputs + "b2_event_selection/"

f_6min_wlevel_resampled_to_mrms_res = dir_event_selection + "6min_wlevel_rsmpld_to_5_min.csv"
f_6min_wlevel_resampled_to_mrms_res_nc = dir_event_selection + "6min_wlevel_rsmpld_to_5_min.nc"

dir_surge_events = dir_event_selection + "b2_surge_events/"
f_6min_surge_event_summaries = dir_surge_events + "surge_6min_event_summaries.csv"
f_6min_surge_event_timeseries = dir_surge_events + "surge_6min_event_timeseries.csv"
f_hrly_surge_event_summaries = dir_surge_events + "surge_hrly_event_summaries.csv"
f_hrly_surge_event_timeseries = dir_surge_events + "surge_hrly_event_timeseries.csv"

dir_compound_events = dir_event_selection + "b2_compound_events/"

dir_selecting_maringals = dir_ssr_outputs + "b3_selecting_marginals/"

dir_fitting_maringals = dir_ssr_outputs + "b4_fitting_marginals/"

dir_vinecop = dir_ssr_outputs + "b5_vinecop_modeling/"

dir_simulate_from_copula = dir_ssr_outputs + "b6_simulating_from_copula/"
f_obs_event_summaries_from_continuous_years = f"{dir_simulate_from_copula}obs_event_summaries_from_yrs_with_complete_coverage.csv"
f_obs_event_tseries_from_continuous_years = f"{dir_simulate_from_copula}obs_event_tseries_from_yrs_with_complete_coverage.nc"
# 6 min water level and mrms
f_6m_wlevel_mrms_event_summaries = dir_compound_events + "6m_wlevel_mrms_event_summaries.csv"
f_6m_wlevel_mrms_event_timeseries = dir_compound_events + "6m_wlevel_mrms_event_timeseries.csv"
# 6 min water level and aorc
f_6m_wlevel_aorc_event_summaries = dir_compound_events + "6m_wlevel_aorc_event_summaries.csv"
f_6m_wlevel_aorc_event_timeseries = dir_compound_events + "6m_wlevel_aorc_event_timeseries.csv"
# hourly water level and mrms
f_hrly_wlevel_mrms_event_summaries = dir_compound_events + "hrly_wlevel_mrms_event_summaries.csv"
f_hrly_wlevel_mrms_event_timeseries = dir_compound_events + "hrly_wlevel_mrms_event_timeseries.csv"
# 6 min water level and daily NCEI
f_6min_wlevel_dly_ncei_event_summaries = dir_compound_events + "6min_wlevel_dly_ncei_event_summaries.csv"
f_6min_wlevel_dly_ncei_event_timeseries = dir_compound_events + "6min_wlevel_dly_ncei_event_timeseries.csv"
# hourly water level and daily NCEI
f_hrly_wlevel_dly_ncei_event_summaries = dir_compound_events + "hrly_wlevel_dly_ncei_event_summaries.csv"
f_hrly_wlevel_dly_ncei_event_timeseries = dir_compound_events + "hrly_wlevel_dly_ncei_event_timeseries.csv"
# 6 min water level and hourly NCEI
f_6min_wlevel_hrly_ncei_event_summaries = dir_compound_events + "6min_wlevel_hrly_ncei_event_summaries.csv"
f_6min_wlevel_hrly_ncei_event_timeseries = dir_compound_events + "6min_wlevel_hrly_ncei_event_timeseries.csv"
# hourly water level and hourly NCEI
f_hrly_wlevel_hrly_ncei_event_summaries = dir_compound_events + "hrly_wlevel_hrly_ncei_event_summaries.csv"
f_hrly_wlevel_hrly_ncei_event_timeseries = dir_compound_events + "hrly_wlevel_hrly_ncei_event_timeseries.csv"

# for combined event summmaries (that uses 6-min water level and MRMS where available, hourly surge and NCEI hourly everywhere else)
f_combined_event_summaries = dir_compound_events + "compound_event_summaries.csv"
f_combined_event_tseries_csv = dir_compound_events + "compound_event_tseries.csv"
f_combined_event_tseries_netcdf = dir_compound_events + "compound_event_tseries.nc"
to_netcdf_compression_level = 5

dir_compound_events_plots = dir_compound_events + "event_tseries_plots/"
dir_event_slxn_validing_distributions = dir_compound_events + "plots_mrms_vs_ncei_comparison/"
dir_compound_events_stats_plots = dir_compound_events + "compound_event_statistics_plots/"

dir_mrms_events = dir_event_selection + "b2_mrms_events/"
f_mrms_event_summaries = dir_mrms_events + "mrms_event_summaries.csv"
f_mrms_event_timeseries = dir_mrms_events + "mrms_event_timeseries.csv"
dir_aorc_events = dir_event_selection + "b2_aorc_events/"
f_aorc_event_summaries = dir_aorc_events + "aorc_event_summaries.csv"
f_aorc_event_timeseries = dir_aorc_events + "aorc_event_timeseries.csv"

dir_ncei_events = dir_event_selection + "b2_ncei_events/"
f_ncei_hrly_event_summaries = dir_ncei_events + "ncei_hrly_event_summaries.csv"
f_ncei_hrly_event_timeseries = dir_ncei_events + "ncei_hrly_event_timeseries.csv"
f_ncei_daily_event_summaries = dir_ncei_events + "ncei_daily_event_summaries.csv"
f_ncei_daily_event_timeseries = dir_ncei_events + "ncei_daily_event_timeseries.csv"
# f_observed_wlevel_rainfall_tseries = dir_mrms_events + "observed_compound_event_timeseries.csv"
# f_observed_compound_event_summaries = dir_mrms_events + "observed_compound_event_summaries.csv"
# f_waterlevels_same_tstep_as_sst = dir_mrms_events + "observed_waterlevels_sst_timestep.csv"

dir_swmm_model = fldr_stormy + "swmm/hague/"
dir_swmm_sst_scenarios = dir_swmm_model + "swmm_scenarios_sst/"
dir_scenario_weather = dir_swmm_sst_scenarios + "weather/"
f_rain_realizations = dir_scenario_weather + "rainfall.nc"

fld_out_b = dir_ssr_outputs + "b_precip_time_series_at_gages/"
f_in_b_nc = fldr_mrms_processing+"mrms_nc_preciprate_fullres_atgages.nc"
f_mrms_rainfall = fld_out_b + "mrms_rainfall.csv"
f_aorc_rainfall = fld_out_b + "aorc_rainfall.csv"
f_out_b_csv_subs_w_mrms_grid = fld_out_b + "b_sub_ids_and_mrms_rain_col.csv"
f_out_swmm_rainfall = fldr_swmm_tseries + "b_mrms_rainfall_in_per_hr_{}.dat"

f_realizations_hrly = fldr_swmm + "hague/swmm_scenarios_sst_hourly/_combined_realizations.nc"

dir_swmm_sst_scenarios_hrly = fldr_swmm + "hague/swmm_scenarios_sst_hourly/"
dir_time_series_hrly = dir_swmm_sst_scenarios_hrly + "time_series/"
f_key_subnames_gridind = dir_swmm_sst_scenarios_hrly + "_key_subnames_and_grid-indices.csv"

sst_hrly_tstep_min = 60 # number of minutes per tstep

time_buffer = 6 # hours; this is the amount of time before either the start of rain or the peak storm surge and AFTER the end of rain or peak storm surge

wlevel_threshold = 0.5 # i don't want simulated time series that are 50% above or below the min and max observed waterlevel since 2000

# unique to script c
fld_out_c_plts = fldr_NCEI + "qaqc_plots/"
fld_out_c_processed_data = fldr_NCEI + "processed_data/"

# script b2 - compound event selection
## compound event statistical summary time groupings
lst_time_durations_h_to_analyze = [1, 4, 8, 16, 24]
target_events_per_year = 5
min_interevent_time = 12
max_event_duration_h = 48 # hours, for surge and rain events
surge_threshold_to_determine_event_limits = 2 # feet
min_interevent_time = pd.Timedelta('{} hours'.format(min_interevent_time))
max_strm_len = pd.Timedelta(max_event_duration_h, "hours")
rainyday_storm_duration = pd.Timedelta('{} hours'.format(max_event_duration_h))
rainfall_tstep = pd.Timedelta('{} minutes'.format(5))

t_window_h_to_shift_tidal_tseries = 2*7*24 # amount of time to shift tidal time series from the observed datetime (2 weeks = 2*7*24 hours)
n_years_to_look_back_for_tidal_tseries = 4 # uses the most recent 4 years of tidal time series to add to surge time series
#%% HRSD data
dir_hrsd = "D:/Dropbox/_GradSchool/_norfolk/norfolk_ffa/data/processed/precip/gage_hrsd/"
shp_hrsd_gages = dir_hrsd + "rain_gages.shp"
f_csv_hrsd_rainfall = "processed_hrsd_rain_data.csv"