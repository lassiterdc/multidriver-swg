#%%
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
import textwrap
#%% load data
cntrl_selection_alpha = 0.1

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

#%% function for performing two sample statistical tests comparing observed and simulated data
def evaluate_copula_fit_based_on_comparing_simulated_marginals_with_obervations(df_obs, cop_fitted, n_samps, fldr_plots, alpha):
    n_vars = len(df_obs.columns)
    for col_iloc in range(n_vars):
        s_obs = df_obs.iloc[:, col_iloc]
        lst_results = []
        for samp_id in np.arange(n_samps):
            df_simulated_to_compare = simulate_from_copula(df_obs=df_obs, n=len(df_obs), cop_fitted=cop_fitted)
            s_sim = df_simulated_to_compare.iloc[:, col_iloc]
            # Perform tests
            sys.exit('work: I think i should bootstrap the observations for each test; also, the rejection rate is just a test statistic; I need to createa  null distribution of the rejection rate by repeating this process with two samples from the vine copula repeated the same number of times.')
            df_results = perform_two_sample_tests(s_obs, s_sim, alpha=alpha)
            df_results["samp_id"] = samp_id
            lst_results.append(df_results)

        df_mc_results = pd.concat(lst_results).reset_index(drop = True)
        fig_title = f"{s_obs.name}\n2-sample test results comparing observed and simulated data\nn-per-sample: {len(df_obs)} | n samples: {n_samps}"
        plot_pvalue_distributions(df_mc_results, alpha=alpha,
                    fig_title = fig_title,
                    fname_savefig = f"{fldr_plots}obs_vs_sim_2samp_test_results_{s_obs.name}.png")


def empirical_multivariate_cdf(df_samples):
    n_samples = len(df_samples)
    s_emp_cdf = pd.Series(index = df_samples.index).astype(float)
    for emp_cdf_index, sample_values in df_samples.iterrows():
        # Compare each column in df_samples to the current sample values
        df_exceedance = (df_samples <= sample_values)
        # Check where all variables are less than or equal to the sample
        n_all_lessthan_or_equal_to = df_exceedance.all(axis=1).sum()
        # Calculate the empirical CDF value
        emp_cdf_val = n_all_lessthan_or_equal_to / (n_samples+1)
        s_emp_cdf.loc[emp_cdf_index] = emp_cdf_val
    return s_emp_cdf

def compute_cvm_criterion(s_sample, s_fitted):
    cvm_stat = ((s_sample - s_fitted)**2).sum()
    return cvm_stat

def compare_emp_cvm_criterion_to_dist_from_copula_sims(n_samps_for_dist, df_obs, cop_fitted, alpha):
    sys.exit("i need to update this function so that it is a one-sided test")
    s_emp_cdf_obs = empirical_multivariate_cdf(df_obs)
    s_cop_cdf_obs = pd.Series(cop_fitted.cdf(df_obs), index = df_obs.index)
    cvm_crit_obs = compute_cvm_criterion(s_emp_cdf_obs, s_cop_cdf_obs)
    s_cvm_crit_sim = pd.Series(index = np.arange(n_samps_for_dist))
    for sim_idx in np.arange(n_samps_for_dist):
        df_simulated = simulate_from_copula(df_obs=df_obs, n=len(df_obs), cop_fitted=cop_fitted)
        s_emp_cdf_sim = empirical_multivariate_cdf(df_simulated)
        s_cop_cdf_sim = pd.Series(cop_fitted.cdf(df_simulated), index = df_simulated.index)
        cvm_crit_sim = compute_cvm_criterion(s_emp_cdf_sim, s_cop_cdf_sim)
        s_cvm_crit_sim.loc[sim_idx] = cvm_crit_sim
    pvalue = get_quantile_from_value(s_cvm_crit_sim, cvm_crit_obs)
    if pvalue < alpha/2:
        mc_result = "reject"
    elif pvalue > (1-(alpha/2)):
        mc_result = "reject"
    else:
        mc_result = "fail to reject"
    return mc_result, pvalue, cvm_crit_obs, s_cvm_crit_sim



def create_vinecop_gof_plots(df_obs, cop_fitted, n_for_density_plot, n_for_marginal_cdf_comparison, n_for_montecarlo_tau_test, n_for_montecarlo_cvm_test,
                            alpha, fldr_plots, event_key, clearfigs = True):
    df_simulated = simulate_from_copula(df_obs=df_obs, n=n_for_density_plot, cop_fitted=cop_fitted)
    # Create contour plots for each pair of variables
    n_vars = df_obs.shape[1]

    # plot cvm test statistic results
    if n_for_montecarlo_cvm_test is not None:
        mc_result, pvalue, cvm_crit_obs, s_cvm_crit_sim = compare_emp_cvm_criterion_to_dist_from_copula_sims(n_samps_for_dist=n_for_montecarlo_cvm_test, df_obs=df_obs, cop_fitted=cop_fitted, alpha=alpha)
        varname_test="CVM Statistic"
        f_savefig = f'{fldr_plots}{varname_test}_monte_carlo.png'
        plot_obs_vs_montecarlo_simulated_statstic(varname_test=varname_test, s_mc_simulated_stat = s_cvm_crit_sim,
                                                   val_obs_stat = cvm_crit_obs, pvalue = pvalue ,string_two_sided_test_result = mc_result, alpha=alpha,
                                                            n_obs = len(df_obs), n_mc_sims=n_for_montecarlo_cvm_test,
                                                              fig_maintitle = event_key, ax = None, fig = None, clearfigs = True, f_savefig = f_savefig)

    evaluate_copula_fit_based_on_comparing_simulated_marginals_with_obervations(df_obs, cop_fitted=cop_fitted, n_samps = n_for_marginal_cdf_comparison, fldr_plots = fldr_plots, alpha=alpha)
    evaluate_copula_fit_based_on_comparing_kendalltaus_for_each_variable_pair(df_obs, alpha, n_for_montecarlo_tau_test, cop_fitted,
                                                                                    df_simulated=df_simulated, plot = True, fldr_plots = fldr_plots,
                                                                                    fig_main_title = event_key)

def compute_obs_cvm_stat(df_obs, vinecop):
    s_emp_cdf_obs = empirical_multivariate_cdf(df_obs)
    s_cop_cdf_obs = pd.Series(vinecop.cdf(df_obs), index = df_obs.index)
    cvm_crit_obs = compute_cvm_criterion(s_emp_cdf_obs, s_cop_cdf_obs)
    return cvm_crit_obs


#%% try a bunch of different fit controls to find all the combos that get a good CVM statistic p value
import itertools
from tqdm import tqdm
n_for_density_plot = 1000
n_for_montecarlo_cvm_test = 50
n_for_montecarlo_tau_test = 200
lst_selection_criterion = ["loglik", "aic", "bic"]
lst_parametric_method = ["mle", "itau"]
lst_tree_criterion = ["tau", "hoeffd", "rho", "mcor"]
lst_select_trunc_lvl = [True, False]
alpha = 0.05


for key in dic_event_summaries:
    if key == "surge_events":
        continue # done
    if key == "rain_events":
        continue # done
    if key == "compound_events":
        continue # done
    fldr_cntrl_sensitivity = dir_vinecop + f"plots_{key}_control_testing/"
    try:
        shutil.rmtree(fldr_cntrl_sensitivity)        
    except:
        pass
    Path(fldr_cntrl_sensitivity).mkdir(parents=True, exist_ok=True)
    # load observations
    df_obs = format_fitted_cdf_values_for_fitting_copula(event_type_key = key)

    # Create all combinations of the values in the lists
    combinations = list(itertools.product(lst_selection_criterion, lst_parametric_method, lst_tree_criterion, lst_select_trunc_lvl))

    # Create a pandas DataFrame with the combinations
    df_controls = pd.DataFrame(combinations, columns=['selection_criterion', 'parametric_method', 'tree_criterion', 'select_trunc_lvl'])

    dic_simulated_cvms = dict(control_set_index = [],
                            simulated_cvm_stats = [])
    df_results = pd.DataFrame(index = df_controls.index, columns = ["pvalue", "result", "cvm_obs", "nsims", "cvm_sims_lower_bound",
                                                                    "cvm_sims_upper_bound", "cvm_sims_median", "f_vinecop_params",
                                                                    "cvm_obs_before_exporting", "cvm_obs_after_loading_from_json",
                                                                    "problems"])
    for cntrl_set_idx, row_cntrl in tqdm(df_controls.iterrows()):
        fitcontrols = pv.FitControlsVinecop()
        fitcontrols.selection_criterion = row_cntrl["selection_criterion"]
        fitcontrols.parametric_method = row_cntrl["parametric_method"]
        fitcontrols.threshold=0.01
        fitcontrols.tree_criterion=row_cntrl["tree_criterion"]
        fitcontrols.select_trunc_lvl=row_cntrl["select_trunc_lvl"]
        try:
            vinecop = pv.Vinecop(data=df_obs, controls = fitcontrols)
            vinecop.select(data=df_obs, controls = fitcontrols)
        except Exception as e:
            df_results.loc[cntrl_set_idx, "problems"] = e
            continue
        # create strings for figure and saving to json
        txt_test_id = ""
        txt_for_fname_fig = ""
        for control_var, control_val in row_cntrl.items():
            txt_test_id += f"{control_var}: {str(control_val)} |"
            txt_for_fname_fig += f"{str(control_val)}_"
        txt_test_id += f"trunc_lvl: {vinecop.trunc_lvl}"
        varname_test="CVM_stat"
        # save to json
        df_results.loc[cntrl_set_idx, "f_vinecop_params"] = f"{fldr_cntrl_sensitivity}cntrl_idx{cntrl_set_idx}_vinecop_params_{txt_for_fname_fig}{vinecop.trunc_lvl}.json"
        vinecop.to_json(df_results.loc[cntrl_set_idx, "f_vinecop_params"])
        cvm_crit_obs = compute_obs_cvm_stat(df_obs, vinecop)
        df_results.loc[cntrl_set_idx, "cvm_obs_before_exporting"] = cvm_crit_obs
        vinecop = pv.Vinecop(df_results.loc[cntrl_set_idx, "f_vinecop_params"])
        cvm_crit_obs_after_reloading_from_json = compute_obs_cvm_stat(df_obs, vinecop)
        df_results.loc[cntrl_set_idx, "cvm_obs_after_loading_from_json"] = cvm_crit_obs_after_reloading_from_json
        if not np.isclose(cvm_crit_obs_after_reloading_from_json, cvm_crit_obs, atol=1e-2):
            print("WARNING: FOR SOME REASON THE COPULA IS REPORTING DIFFERENT CDF VALUES AFTER RE-LOADING FROM JSON")
        # running tests
        ## cvm values
        mc_result, pvalue, cvm_crit_obs, s_cvm_crit_sim = compare_emp_cvm_criterion_to_dist_from_copula_sims(n_for_montecarlo_cvm_test, df_obs, vinecop, alpha)
        ## kendall tau
        # df_simulated = simulate_from_copula(df_obs=df_obs, n=n_for_density_plot, cop_fitted=vinecop)
        df_kendalltau_fits = evaluate_copula_fit_based_on_comparing_kendalltaus_for_each_variable_pair(df_obs, alpha,
                                                                                                        n_for_montecarlo_tau_test, cop_fitted = vinecop,
                                                                                                          plot = False)#, df_simulated=df_simulated, plot = True)
        # create column values to add to the results dataframe
        for tau_idx, tau_row in df_kendalltau_fits.iterrows():
            colname = f"{tau_row.var1}_vs_{tau_row.var2}_kendall_tau_pvalue"
            val = tau_row.mc_pvalue
            df_results.loc[cntrl_set_idx, colname] = val
            colname = f"{tau_row.var1}_vs_{tau_row.var2}_kendall_tau_mc_result"
            val = tau_row.mc_result
            df_results.loc[cntrl_set_idx, colname] = val

        dic_simulated_cvms["control_set_index"].append(cntrl_set_idx)
        dic_simulated_cvms["simulated_cvm_stats"].append(s_cvm_crit_sim)
        df_results.loc[cntrl_set_idx, "pvalue"] = pvalue
        df_results.loc[cntrl_set_idx, "result"] = mc_result
        df_results.loc[cntrl_set_idx, "cvm_obs"] = cvm_crit_obs
        df_results.loc[cntrl_set_idx, "nsims"] = n_for_montecarlo_cvm_test
        df_results.loc[cntrl_set_idx, "cvm_sims_lower_bound"] = s_cvm_crit_sim.quantile(alpha/2)
        df_results.loc[cntrl_set_idx, "cvm_sims_upper_bound"] = s_cvm_crit_sim.quantile(1-alpha/2)
        df_results.loc[cntrl_set_idx, "cvm_sims_median"] = s_cvm_crit_sim.quantile(.5)
        df_results.loc[cntrl_set_idx, "trunc_lvl"] = vinecop.trunc_lvl

        f_savefig = f'{fldr_cntrl_sensitivity}cntrl_idx{cntrl_set_idx}_{mc_result}_p{pvalue:.2f}_{txt_for_fname_fig}{vinecop.trunc_lvl}.png'

        fig_maintitle = f"({key})\n{txt_test_id}"
        plot_obs_vs_montecarlo_simulated_statstic(varname_test=varname_test, s_mc_simulated_stat = s_cvm_crit_sim,
                                                    val_obs_stat = cvm_crit_obs, pvalue = pvalue, string_two_sided_test_result = mc_result, alpha=alpha,
                                                            n_obs = len(df_obs), n_mc_sims=n_for_montecarlo_cvm_test,
                                                                fig_maintitle = fig_maintitle, ax = None, clearfigs = True, f_savefig = f_savefig)
   
    df_controls_and_results = df_results.join(df_controls).sort_values("pvalue")
    df_controls_and_results.index.name = "control_idx"

    df_controls_and_results.to_csv(f"{fldr_cntrl_sensitivity}copula_fit_sensitivity_analysis_results.csv")

#%% if needed, load and loop back through the fits
cntrl_selection_alpha_alt = 0.05
for key in dic_event_summaries:
    fldr_cntrl_sensitivity = dir_vinecop + f"plots_{key}_control_testing/"
    if key in ["compound_events", "surge_events"]: # ["rain_events", "compound_events"]:
        continue
    df_controls_and_results = pd.read_csv(f"{fldr_cntrl_sensitivity}copula_fit_sensitivity_analysis_results.csv")
    filter_best_kendall_tau_ps = ((df_controls_and_results.filter(like="kendall_tau_pvalue")>=cntrl_selection_alpha_alt) & (df_controls_and_results.filter(like="kendall_tau_pvalue")<=(1-cntrl_selection_alpha_alt))).all(axis = 1)
    df_controls_and_results_best = df_controls_and_results[filter_best_kendall_tau_ps]
    df_controls_and_results_best  = df_controls_and_results_best[(df_controls_and_results_best.pvalue>=cntrl_selection_alpha_alt) & (df_controls_and_results_best.pvalue<=(1-cntrl_selection_alpha_alt))]
    df_controls_and_results_best.to_csv(f"{fldr_cntrl_sensitivity}copula_fit_sensitivity_analysis_results_best.csv")

#%% looking closer at some of the well-performing control configurations
n_for_density_plot = n_for_marginal_cdf_comparison = 1000
n_for_montecarlo_tau_test = 500
n_for_montecarlo_cvm_test = 500
alpha = 0.05

# manually select control indices that resulted in a defensible fit
lst_surge_control_idx = [0, 37, 1]
lst_rain_control_idx = [7,5,1]
lst_compound_control_idx = [1,3,4,0]

for key in dic_event_summaries:
    fldr_cntrl_sensitivity = dir_vinecop + f"plots_{key}_control_testing/"
    if key == "surge_events":
        # continue # done
        lst_control_idx = lst_surge_control_idx
    if key == "rain_events":
        # continue # done
        lst_control_idx = lst_rain_control_idx
    if key == "compound_events":
        # continue # done
        lst_control_idx = lst_compound_control_idx
    df_controls_and_results = pd.read_csv(f"{fldr_cntrl_sensitivity}copula_fit_sensitivity_analysis_results.csv", index_col = "control_idx")
    for control_idx in lst_control_idx:
        print(f"Plotting copula fits for {key} for copula control idx {control_idx}")
        # df_fits = pd.read_csv(f"{dir_fitting_maringals}{key}_distributions.csv")
        fldr_plots = f"{fldr_cntrl_sensitivity}cntrl_idx{control_idx}/"
        try:
            shutil.rmtree(fldr_plots)
        except:
            pass
        Path(fldr_plots).mkdir(parents=True, exist_ok=True)
        # load vine copula from json
        df_obs = format_fitted_cdf_values_for_fitting_copula(event_type_key = key)
        cop_json = df_controls_and_results.loc[control_idx, "f_vinecop_params"]
        vinecop = pv.Vinecop(cop_json)
        create_vinecop_gof_plots(df_obs=df_obs, cop_fitted=vinecop, n_for_density_plot=n_for_density_plot, n_for_marginal_cdf_comparison=n_for_marginal_cdf_comparison, n_for_montecarlo_tau_test=n_for_montecarlo_tau_test, n_for_montecarlo_cvm_test=n_for_montecarlo_cvm_test,
                                alpha=alpha, fldr_plots=fldr_plots, event_key=key)

#%% final selection
control_idx_selected_rain = 1
control_idx_selected_surge = 0
control_idx_selected_compound = 3 # all tests passed

#%% create plots to evaluate fit
n_for_density_plot = n_for_marginal_cdf_comparison = 1000
n_for_montecarlo_cvm_test = n_for_montecarlo_tau_test = 500
alpha = 0.05
for key in dic_event_summaries:
    fldr_cntrl_sensitivity = dir_vinecop + f"plots_{key}_control_testing/"
    if key == "surge_events":
        continue # done
        control_idx = control_idx_selected_surge
    if key == "rain_events":
        continue # done
        control_idx = control_idx_selected_rain
    if key == "compound_events":
        continue # done
        control_idx = control_idx_selected_compound
    df_controls_and_results = pd.read_csv(f"{fldr_cntrl_sensitivity}copula_fit_sensitivity_analysis_results.csv", index_col = "control_idx")
    fldr_plots = dir_vinecop + f"plots_{key}/"
    try:
        shutil.rmtree(fldr_plots)
    except:
        pass
    Path(fldr_plots).mkdir(parents=True, exist_ok=True)
    # building vine copula directly from data
    df_obs = format_fitted_cdf_values_for_fitting_copula(event_type_key = key)    
    cop_json = f"{dir_vinecop}vinecop_params_{key}.json"
    # copy json to the main folder
    shutil.copy(df_controls_and_results.loc[control_idx, "f_vinecop_params"], cop_json)
    # load copula fit
    vinecop = pv.Vinecop(cop_json)
    print(f"Plotting copula fits for {key}")
    create_vinecop_gof_plots(df_obs=df_obs, cop_fitted=vinecop, n_for_density_plot=n_for_density_plot, n_for_marginal_cdf_comparison=n_for_marginal_cdf_comparison, n_for_montecarlo_tau_test=n_for_montecarlo_tau_test, n_for_montecarlo_cvm_test=n_for_montecarlo_cvm_test,
                            alpha=alpha, fldr_plots=fldr_plots, event_key=key)