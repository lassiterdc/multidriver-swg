#%% loading packages
from __ref_ams_functions import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# from matplotlib.lines import Line2D
# import scipy
from _utils import *
from scipy import stats
import sys
from tqdm import tqdm
from scipy.stats import bootstrap
import xarray as xr
from _inputs import *
import shutil
from pathlib import Path
from tqdm import tqdm
from glob import glob

alpha = 0.05 # this is the cvm and ks p value threshold for fit selection
cvm_cutoff = ks_cutoff = 0.2

#%% load and process data data
df_all_events_summary = pd.read_csv(f_combined_event_summaries, 
                                    parse_dates=["event_start", "event_end", "surge_peak_tstep",
                                                "precip_max_intensity_tstep"])
df_surge_events = df_all_events_summary[df_all_events_summary["event_type"] == "surge"]
df_rain_events = df_all_events_summary[df_all_events_summary["event_type"] == "rain"]
df_combo_events = df_all_events_summary[df_all_events_summary["event_type"] == "compound"]

surge_threshold_ft = df_surge_events[threshold_varname_surge_events].min()
rain_threshold_mm = df_rain_events[threshold_varname_rain_events].min()


lst_all_event_stats = []
lst_surge_event_stats = []
lst_rain_event_stats = []
lst_compound_event_stats = []

dic_event_stats = {}

dic_event_summaries = {#"all_events":df_all_events_summary,
                       "compound_events":df_combo_events,
                       "rain_events":df_rain_events,
                       "surge_events":df_surge_events}

for f_chosen_event_stats in glob(f"{dir_selecting_maringals}*.csv"):
    df_event_stats = pd.read_csv(f_chosen_event_stats)
    lst_stats = list(pd.Series(list(df_event_stats.var1) + list(df_event_stats.var2)).unique())
    # if "all_event" in f_chosen_event_stats:
    #     dic_event_stats["all_events"] = lst_stats
    if "compound" in f_chosen_event_stats:
        dic_event_stats["compound_events"] = lst_stats
    elif "rain_event" in f_chosen_event_stats:
        dic_event_stats["rain_events"] = lst_stats
    elif "surge_event" in f_chosen_event_stats:
        dic_event_stats["surge_events"] = lst_stats
    else:
        prnt = f"file {f_chosen_event_stats} not recognized"
        print(prnt)

dir_plots_all_fitted_marginals = dir_fitting_maringals + "fitted_distributions_all/"
Path(dir_plots_all_fitted_marginals).mkdir(parents=True, exist_ok=True)

# vars_with_0_lb = ["max_1hr_mean_mm_per_hr"]
#%% fit all possible pdfs and transformations for each variable
sys.exit("I really need to update the variables I'm modeling to include the duration used for threshold-based event selection")

# create list of functions and variables
fxs = [gev, weibull_min_dist, weibull_max_dist, gumbel_right, gumbel_left,
       norm_dist, lognormal, student_t, genpareto_dist, chi_dist, gamma_dist, p3, sep]
# not doing normalization or box cox transformations on the first pass
lst_normalize = [False]
lst_boxcox = [False, True]
lst_fix_floc_to_min = [True, False]

count = -1
#% work
# key = "surge_events"
# v = "precip_depth_mm"
# df_event_summary = dic_event_summaries[key]
# s_data = df_event_summary[v]
# upper_bound = rain_threshold_mm
# scalar_shift = 0
# normalize = False
# boxcox = False
# fix_loc_to_min = True
# f = genpareto_dist
# fx_name = f["args"]["fx"].name
# fx = f['args']["fx"]
# n_params = f['args']["n_params"]
# s_fit, df_emp_vs_fitted = fit_dist(s_data, **f['args'], upper_bound = upper_bound, fix_loc_to_min = fix_loc_to_min, scalar_shift = scalar_shift, normalize = normalize, boxcox = boxcox)
#% end work
for key in dic_event_summaries:
    lst_s_fits = []
    lst_s_failed_fits = []
    df_event_summary = dic_event_summaries[key]
    vars_all = dic_event_stats[key]
    # create folder for plots
    f_pdf_performance = f"{dir_plots_all_fitted_marginals}{key}.csv"
    f_failed_fits = f"{dir_plots_all_fitted_marginals}{key}_failed.csv"
    lst_df_perf = []
    df_perf = pd.DataFrame()
    ind = -1
    for v in tqdm(vars_all):
        surge_upper_bound = False
        rain_upper_bound = False
        s_data = df_event_summary[v].dropna()
        # shift statistics used for setting thresholds so that the lowest values is at zero
        # for distributions with a lower bound of zero, this will enforce a lower bound equal to the threshold
        lst_scalar_shifts = [0]
        lst_upper_bound = [np.nan]
        if key == "rain_events":
            # lower bound
            if v == threshold_varname_rain_events:
                lst_scalar_shifts.append(-rain_threshold_mm)
            # upper bound
            if v == threshold_varname_surge_events:
                # ((df_event_summary[threshold_varname_surge_events] - surge_threshold_ft)*-1).hist()
                lst_upper_bound.append(surge_threshold_ft)
        if key == "surge_events":
            # lower bound
            if v == threshold_varname_surge_events:
                lst_scalar_shifts.append(-surge_threshold_ft)
            # upper bound
            if v == threshold_varname_rain_events:
                lst_upper_bound.append(rain_threshold_mm)
        if key == "compound_events":
            # lower bound
            if v == threshold_varname_rain_events:
                lst_scalar_shifts.append(-rain_threshold_mm)
            # lower bound
            if v == threshold_varname_surge_events:
                lst_scalar_shifts.append(-surge_threshold_ft)
        # upper_bound = lst_upper_bound[1]
        # scalar_shift = 0
        # normalize = False
        # boxcox = False
        for f in fxs:
            fx_name = f["args"]["fx"].name
            fx = f['args']["fx"]
            n_params = f['args']["n_params"]
            for upper_bound in lst_upper_bound:
                for scalar_shift in lst_scalar_shifts:
                    for normalize in lst_normalize:
                        for boxcox in lst_boxcox:
                            for fix_loc_to_min in lst_fix_floc_to_min:
                                if (n_params == 2) and (fix_loc_to_min == True):
                                    continue
                                s_failed_fit = pd.Series(index = ["data", "distribution", "n_params","scalar_shift",
                                                                "upper_bound", "fix_loc_to_min", "normalize", "boxcox",
                                                                "problem"],
                                                    dtype=object)
                                s_failed_fit.loc["data"] = s_data.name
                                s_failed_fit.loc["distribution"] = fx_name
                                s_failed_fit.loc["n_params"] = n_params
                                s_failed_fit.loc["scalar_shift"] = scalar_shift
                                s_failed_fit.loc["normalize"] = normalize
                                s_failed_fit.loc["boxcox"] = boxcox
                                s_failed_fit.loc["upper_bound"] = upper_bound
                                s_failed_fit.loc["fix_loc_to_min"] = fix_loc_to_min
                                s_failed_fit.loc["problem"] = ""
                                try:
                                    s_fit, df_emp_vs_fitted = fit_dist(s_data, **f['args'], upper_bound = upper_bound, fix_loc_to_min = fix_loc_to_min, scalar_shift = scalar_shift, normalize = normalize, boxcox = boxcox)
                                    if s_fit is not None:
                                        s_fit = pd.concat([pd.Series([key], index = ["dataset"]), s_fit])
                                        lst_s_fits.append(s_fit)
                                        count += 1
                                    else:
                                        s_failed_fit.loc["problem"] = "return None conditional triggered during distribution fitting"
                                        lst_s_failed_fits.append(s_failed_fit)
                                except Exception as e:
                                    s_failed_fit.loc["problem"] = f"Error: {e}"
                                    lst_s_failed_fits.append(s_failed_fit)
                                    pass
    df_perf = pd.concat(lst_s_fits, axis = 1, ignore_index=True).T
    df_perf.to_csv(f_pdf_performance, index = False)
    print(f"exported {f_pdf_performance}")
    df_failed = pd.concat(lst_s_failed_fits, axis = 1, ignore_index=True).T
    df_failed.to_csv(f_failed_fits, index = False)

#%% plot all distribution fits
mode = "all"
subdir_name = None
return_fit = True

plot_subset_of_fits(dic_event_summaries, dic_event_stats, dir_plots_all_fitted_marginals,
                     cvm_cutoff, ks_cutoff, mode = mode, subdir_name = subdir_name, return_fit = return_fit)
#%% coarse fit selection
subdir_name = "slxn_coarse"
# surge_events
which_event_key = "surge_events"
print(f"Plotting {which_event_key} distributions.")

lst_fname_of_interest = ["surge_peak_ft_cvm-0.89_trns_1_shift_dist-gamma",
                         "surge_peak_ft_cvm-0.86_trns_1_shift_dist-weibull_min_fixed_loc",
                         "surge_peak_ft_cvm-0.86_notrns_dist-weibull_min_fixed_loc",
                         "precip_depth_mm_cvm-0.72_trns_1_ub_dist-genpareto",
                         "precip_depth_mm_cvm-0.38_trns_1_ub_dist-genpareto_fixed_loc",
                         "max_mean_1hrsurge_peak_after_4hrrain_peak_h_cvm-0.85_notrns_dist-gumbel_l",
                         "max_4hr_mean_mm_per_hr_cvm-0.91_notrns_dist-genpareto_fixed_loc",
                         "max_4hr_mean_mm_per_hr_cvm-0.76_notrns_dist-genpareto",
                         "max_4hr_mean_mm_per_hr_cvm-0.51_notrns_dist-weibull_min_fixed_loc"
                         ]

s_vars_accounted_for = reproduce_select_plots_and_verify_all_vars_accounted_for(dic_event_stats, which_event_key, lst_fname_of_interest, dic_event_summaries,
                                                                                dir_plots_all_fitted_marginals, cvm_cutoff, ks_cutoff)
print(f"{which_event_key} variables accounted for:\n{s_vars_accounted_for}")
#%% rain_events
which_event_key = "rain_events"
print(f"Plotting {which_event_key} distributions.")

lst_fname_of_interest = ["surge_peak_ft_cvm-0.96_trns_1_ub_dist-exponpow",
                         "precip_depth_mm_cvm-0.96_notrns_dist-weibull_min_fixed_loc",
                         "max_mean_1hrsurge_peak_after_16hrrain_peak_h_cvm-0.98_notrns_dist-t",
                         "max_4hr_mean_mm_per_hr_cvm-0.96_notrns_dist-chi"
                         ]

s_vars_accounted_for = reproduce_select_plots_and_verify_all_vars_accounted_for(dic_event_stats, which_event_key, lst_fname_of_interest, dic_event_summaries,
                                                                                dir_plots_all_fitted_marginals, cvm_cutoff, ks_cutoff)
print(f"{which_event_key} variables accounted for:\n{s_vars_accounted_for}")
#%% compound_events
which_event_key = "compound_events"
print(f"Plotting {which_event_key} distributions.")

lst_fname_of_interest = ["surge_peak_ft_cvm-0.96_trns_1_bxcx_dist-weibull_min_fixed_loc",
                         "surge_peak_ft_cvm-0.94_trns_2_shift_bxcx_dist-weibull_min_fixed_loc",
                         "surge_peak_ft_cvm-0.94_trns_1_shift_dist-weibull_min_fixed_loc",
                         "surge_peak_ft_cvm-0.94_notrns_dist-weibull_min_fixed_loc",
                         "precip_depth_mm_cvm-0.91_trns_1_bxcx_dist-weibull_min_fixed_loc",
                         "precip_depth_mm_cvm-0.86_notrns_dist-genpareto",
                         "precip_depth_mm_cvm-0.85_trns_1_shift_dist-weibull_min_fixed_loc",
                         "precip_depth_mm_cvm-0.85_notrns_dist-weibull_min_fixed_loc",
                         "precip_depth_mm_cvm-0.83_trns_1_shift_dist-genpareto",
                         "max_mean_16hrsurge_peak_after_24hrrain_peak_h_cvm-0.94_notrns_dist-weibull_min",
                         "max_mean_16hrsurge_peak_after_24hrrain_peak_h_cvm-0.93_notrns_dist-genextreme",
                         "max_4hr_mean_mm_per_hr_cvm-0.97_trns_1_bxcx_dist-t",
                         "max_4hr_mean_mm_per_hr_cvm-0.67_trns_1_bxcx_dist-weibull_min_fixed_loc",
                         "max_4hr_mean_mm_per_hr_cvm-0.81_notrns_dist-genextreme",
                         "max_4hr_mean_mm_per_hr_cvm-0.75_notrns_dist-lognorm",
                         "max_4hr_mean_mm_per_hr_cvm-0.65_notrns_dist-gamma",
                         "max_4hr_mean_mm_per_hr_cvm-0.7_trns_1_bxcx_dist-weibull_min",
                         "max_4hr_mean_mm_per_hr_cvm-0.5_notrns_dist-weibull_min_fixed_loc"
                         ]
s_vars_accounted_for = reproduce_select_plots_and_verify_all_vars_accounted_for(dic_event_stats, which_event_key, lst_fname_of_interest, dic_event_summaries,
                                                                                dir_plots_all_fitted_marginals, cvm_cutoff, ks_cutoff)
print(f"{which_event_key} variables accounted for:\n{s_vars_accounted_for}")


#%% all_events
# which_event_key = "all_events"

# lst_fname_of_interest = ["surge_peak_ft_2_notransformation_cvmpval-0.23_aic-362.8_dist-weibull_min",
# "surge_peak_ft_1_transformation_cvmpval-0.36_aic-293.3_dist-exponpow",
# "surge_peak_after_rain_peak_h_2_notransformation_cvmpval-0.28_aic-832.6_dist-t",
# "precip_depth_mm_2_notransformation_cvmpval-0.49_aic-1124.3_dist-gumbel_r",
# "precip_depth_mm_2_notransformation_cvmpval-0.34_aic-1121.2_dist-genextreme",
# "precip_depth_mm_1_transformation_cvmpval-0.49_aic-258.9_dist-gumbel_r",
# "precip_depth_mm_1_transformation_cvmpval-0.32_aic-285.3_dist-pearson3",
# "max_24hr_mean_surge_1_transformation_cvmpval-0.09_aic-289.1_dist-exponpow",
# "max_1hr_mean_mm_per_hr_2_notransformation_cvmpval-0.88_aic-776.1_dist-exponpow",
# "max_1hr_mean_mm_per_hr_2_notransformation_cvmpval-0.65_aic-822.3_dist-weibull_min",
# "max_1hr_mean_mm_per_hr_1_transformation_cvmpval-0.88_aic-516.8_dist-weibull_min"]

# for fname_of_interest in lst_fname_of_interest:
#     plot_subset_of_fits(mode = "from_fname", which_event_key = which_event_key, fname_of_interest = fname_of_interest, subdir_name = "slxn_coarse")

#%% MAKING FINAL SELECTION
which_event_key = "surge_events"
df_event_summaries = dic_event_summaries[which_event_key]
print(f"Plotting {which_event_key} distributions.")
lst_fname_of_interest = ["surge_peak_ft_cvm-0.86_notrns_dist-weibull_min_fixed_loc",
                         "precip_depth_mm_cvm-0.72_trns_1_ub_dist-genpareto",
                         "max_mean_1hrsurge_peak_after_4hrrain_peak_h_cvm-0.85_notrns_dist-gumbel_l",
                         "max_4hr_mean_mm_per_hr_cvm-0.91_notrns_dist-genpareto_fixed_loc"
                         ]

final_selection_plot_and_write_csv(lst_fname_of_interest, which_event_key, dic_event_stats, dic_event_summaries,
                                   dir_plots_all_fitted_marginals, cvm_cutoff, ks_cutoff)

which_event_key = "rain_events"
df_event_summaries = dic_event_summaries[which_event_key]
print(f"Plotting {which_event_key} distributions.")
lst_fname_of_interest = ["surge_peak_ft_cvm-0.96_trns_1_ub_dist-exponpow",
                         "precip_depth_mm_cvm-0.96_notrns_dist-weibull_min_fixed_loc",
                         "max_mean_1hrsurge_peak_after_16hrrain_peak_h_cvm-0.98_notrns_dist-t",
                         "max_4hr_mean_mm_per_hr_cvm-0.96_notrns_dist-chi"
                         ]

final_selection_plot_and_write_csv(lst_fname_of_interest, which_event_key, dic_event_stats, dic_event_summaries, 
                                   dir_plots_all_fitted_marginals, cvm_cutoff, ks_cutoff)

which_event_key = "compound_events"
df_event_summaries = dic_event_summaries[which_event_key]
print(f"Plotting {which_event_key} distributions.")
lst_fname_of_interest = ["surge_peak_ft_cvm-0.94_notrns_dist-weibull_min_fixed_loc",
                         "precip_depth_mm_cvm-0.86_notrns_dist-genpareto",
                         "max_mean_16hrsurge_peak_after_24hrrain_peak_h_cvm-0.94_notrns_dist-weibull_min",
                         "max_4hr_mean_mm_per_hr_cvm-0.97_trns_1_bxcx_dist-t"
                         ]

final_selection_plot_and_write_csv(lst_fname_of_interest, which_event_key, dic_event_stats, dic_event_summaries, 
                                   dir_plots_all_fitted_marginals, cvm_cutoff, ks_cutoff)

# which_event_key = "all_events"
# df_event_summaries = dic_event_summaries[which_event_key]
# print(f"Plotting {which_event_key} distributions.")
# lst_fname_of_interest = ["surge_peak_ft_cvm-0.76_notrns_dist-gamma",
#                          "surge_peak_after_rain_peak_h_cvm-1.0_notrns_dist-t",
#                          "precip_depth_mm_cvm-1.0_trns_1_shift_dist-exponpow",
#                          "max_mean_4hrsurge_peak_after_1hrrain_peak_h_cvm-0.97_notrns_dist-t",
#                          "max_24hr_mean_surge_cvm-1.0_trns_1_bxcx_dist-weibull_min",
#                          "max_1hr_mean_mm_per_hr_cvm-0.89_trns_1_bxcx_dist-chi"]

# final_selection_plot_and_write_csv(lst_fname_of_interest, which_event_key, dic_event_stats, dic_event_summaries, 
#                                    dir_plots_all_fitted_marginals, cvm_cutoff, ks_cutoff)
