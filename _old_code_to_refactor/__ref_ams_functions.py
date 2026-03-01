#%% loading packages
import pandas as pd
import scipy
from scipy import stats
from scipy.stats import genextreme
from scipy.stats import weibull_min
from scipy.stats import weibull_max
from scipy.stats import gumbel_r
from scipy.stats import gumbel_l
from scipy.stats import norm
from scipy.stats import lognorm
from scipy.stats import pearson3
from scipy.stats import t
from scipy.stats import chi
from scipy.stats import genpareto
from scipy.stats import gamma
from scipy.stats import exponpow
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys

#%% functions
def boxcox_transformation(s_data):
    scalar_shift_for_boxcox = 0
    if s_data.min() <= 0:
        scalar_shift_for_boxcox = .0001+abs(1.0001*s_data.min())
        s_data += scalar_shift_for_boxcox
    s_data_transformed, lmbda = stats.boxcox(s_data)
    return s_data_transformed, lmbda, scalar_shift_for_boxcox

def inverse_boxcox(s_data_transformed, lmbda, scalar_shift_for_boxcox):
    if lmbda == 0:
        s_data_untransformed = np.exp(s_data_transformed)
    else:
        s_data_untransformed = np.power((s_data_transformed * lmbda) + 1, 1 / lmbda)
    s_data_untransformed = s_data_untransformed - scalar_shift_for_boxcox
    return s_data_untransformed

def normalize_data(s_data):
    mean, std = np.mean(s_data), np.std(s_data)
    s_data_nrmlzd = (s_data - mean)/std
    return s_data_nrmlzd, mean, std

def inverse_normalize(s_data_transformed, mean, std):
    s_data_untransformed = s_data_transformed*std + mean
    return s_data_untransformed

def shift_data(s_data, scalar_shift):
    s_data_shifted = s_data + scalar_shift
    return s_data_shifted

def unshift_data(s_data_shifted, scalar_shift):
    s_data_unshifted = s_data_shifted - scalar_shift
    return s_data_unshifted

def transform_data_given_transformations(s_data_realspace, s_fit):
    s_data_transformed = s_data_realspace.copy()
    # apply upper bound
    if not np.isnan(s_fit["upper_bound"]):
        s_data_transformed = (s_data_transformed - s_fit["upper_bound"])*-1
    # apply shift
    s_data_transformed = shift_data(s_data_transformed, s_fit["scalar_shift"])
    # normalize
    if s_fit["normalize"] == True:
        s_data_transformed = (s_data_transformed - s_fit["mean"])/s_fit["std"]
    # boxcox
    if s_fit["boxcox"] == True:
        s_data_transformed = stats.boxcox(s_data_transformed+s_fit["scalar_shift_for_boxcox"], lmbda=s_fit["lambda"])
    return s_data_transformed

def transform_data_for_fitting(s_data_untransformed, upper_bound = np.nan, scalar_shift = 0, normalize=False, boxcox=False):
    s_data_for_fitting = s_data_untransformed.copy()
    # apply upper bound
    if not np.isnan(upper_bound):
        s_data_for_fitting = (s_data_for_fitting - upper_bound)*-1
    # apply shift
    s_data_for_fitting = shift_data(s_data_for_fitting, scalar_shift)
    # normalize
    mean = std = np.nan
    if normalize == True:
        s_data_for_fitting, mean, std = normalize_data(s_data_for_fitting)
    # boxcox
    scalar_shift_for_boxcox = np.nan
    lmbda = np.nan
    if boxcox == True:
        s_data_for_fitting, lmbda, scalar_shift_for_boxcox = boxcox_transformation(s_data_for_fitting)
    return pd.Series(s_data_for_fitting), scalar_shift_for_boxcox, lmbda, mean, std

def backtransform_data(s_data_transformed, s_fit):
    s_data_untransformed = s_data_transformed.copy()
    # in reverse order from function transform_data_for_fitting 
    if s_fit["boxcox"] == True:
        s_data_untransformed = inverse_boxcox(s_data_untransformed, s_fit["lambda"], s_fit["scalar_shift_for_boxcox"])
    if s_fit["normalize"] == True:
        s_data_untransformed = inverse_normalize(s_data_untransformed, s_fit["mean"], s_fit["std"])
    # if s_fit["log"] == True:
        # s_data_untransformed = np.exp(s_data_untransformed)
        # sys.exit("I am not coding to perform log transformations (doing boxcox instead) so this is probably a mistake if log is set to True")
    s_data_untransformed = unshift_data(s_data_untransformed,  s_fit["scalar_shift"])
    # reverse upper bound application
    if not np.isnan(s_fit["upper_bound"]):
        s_data_untransformed = s_data_untransformed*-1 + s_fit["upper_bound"]
    return pd.Series(s_data_untransformed)

def comp_msdi(empirical, fitted):
    return sum(((empirical - fitted)/empirical)**2)/len(empirical)

def comp_madi(empirical, fitted):
    return sum(abs((empirical - fitted)/empirical))/len(empirical)


def fit_dist(s_data, fx, n_params, upper_bound, fix_loc_to_min, scalar_shift, normalize, boxcox, plot = False, fname_savefig = None):
    dist = fx.name
    data_variable = s_data.name
    s_data = s_data.sort_values()
    og_index = s_data.index
    s_data_for_fitting = s_data.reset_index(drop=True).copy()
    s_data_untransformed = s_data_for_fitting.copy()
    s_fit = pd.Series(index = ["data", "distribution", "n_params", "shape", "loc", "scale", "cvm_pval", "ks_pval", "aic", "msdi", "madi",
                                "log", "scalar_shift", "upper_bound", "fix_loc_to_min", "normalize", "boxcox", "scalar_shift_for_boxcox"],
                        dtype=object)
    s_fit.loc["data"] = data_variable
    s_fit.loc["distribution"] = dist
    s_fit.loc["n_params"] = n_params
    s_fit.loc["scalar_shift"] = scalar_shift
    s_fit.loc["normalize"] = normalize
    s_fit.loc["boxcox"] = boxcox
    s_fit.loc["upper_bound"] = upper_bound
    s_fit.loc["fix_loc_to_min"] = fix_loc_to_min
    # perform transformations
    s_data_for_fitting, scalar_shift_for_boxcox, lmbda, mean, std = transform_data_for_fitting(s_data_untransformed = s_data_untransformed,
                                                                                               upper_bound = upper_bound,
                                                                                                scalar_shift=scalar_shift,
                                                                                                  normalize=normalize,
                                                                                                    boxcox=boxcox)
    s_fit.loc["scalar_shift_for_boxcox"] = scalar_shift_for_boxcox
    s_fit.loc["lambda"] = lmbda
    s_fit.loc["mean"] = mean
    s_fit.loc["std"] = std

    any_transformation = (not np.isnan(s_fit.loc["upper_bound"])) or (s_fit.loc["scalar_shift"]!=0) or s_fit["normalize"] or s_fit["boxcox"]
    # if "log" in s_fit.loc["distribution"]:
    #     any_transformation = True
    s_fit["any_transformation"] = any_transformation
    
    s_emp = pd.Series(scipy.stats.mstats.plotting_positions(s_data_for_fitting, 0.44,0.44), name = 'cdf_emp') # gringerton plotting position

    if n_params == 3:
        if fix_loc_to_min == True:
            shape, loc, scale = params = fx.fit(s_data_for_fitting, floc = s_data_for_fitting.min())
        else:
            shape, loc, scale = params = fx.fit(s_data_for_fitting)
        s_fit.loc["shape"] = shape
        x_fit = pd.Series(fx.ppf(s_emp, shape, loc, scale), name = "x_fit_at_empirical_cdf_val")
        y_cdf = pd.Series(fx.cdf(s_data_for_fitting, shape, loc, scale), name = 'cdf_fit')
        y_pdf = pd.Series(fx.pdf(x_fit, shape, loc, scale), name = "y_pdf_fit")
        log_likelihood = np.log(np.prod(fx.pdf(s_data_for_fitting,shape, loc, scale)))
    if n_params == 2:
        loc, scale = params = fx.fit(s_data_for_fitting)   
        x_fit = pd.Series(fx.ppf(s_emp, loc, scale), name = "x_fit_at_empirical_cdf_val")
        y_cdf = pd.Series(fx.cdf(s_data_for_fitting, loc, scale), name = 'cdf_fit')
        y_pdf = pd.Series(fx.pdf(x_fit, loc, scale), name = "y_pdf_fit")
        log_likelihood = np.log(np.prod(fx.pdf(s_data_for_fitting, loc, scale)))
    # compute the lower and upper bounds of the fitted distribution
    dist = fx(*params)
    support_lb, support_ub = dist.support()

    # if a scalar shift was applied or an upper bound was provided, throw an error
    lower_or_upper_bound_applied = (not np.isnan(s_fit["upper_bound"])) or (s_fit["scalar_shift"] != 0)
    if lower_or_upper_bound_applied and (np.isinf(support_lb)):
        # if the lower boundary is less than 0, do not use
        return None, None
    if (s_data_for_fitting.min() != support_lb) and fix_loc_to_min:
        return None, None

    s_fit.loc["support_lb"] = support_lb
    s_fit.loc["support_ub"] = support_ub
    s_fit.loc["loc"] = loc
    s_fit.loc["scale"] = scale
    # back transform fitted data (in reverse order as transformation)
    s_data_transformed = x_fit.copy()
    x_fit_untransformed = backtransform_data(x_fit, s_fit)

    aic = 2 * n_params - 2 * log_likelihood
    
    df_emp_vs_fitted = pd.concat([s_data_for_fitting, x_fit, s_data_untransformed, x_fit_untransformed, y_pdf, y_cdf, s_emp], axis = 1, ignore_index=True)
    df_emp_vs_fitted.columns = ["observations_transformed", f"{x_fit.name}_transformed", "observations_realspace", f"{x_fit.name}_realspace", y_pdf.name, y_cdf.name, s_emp.name]
    df_emp_vs_fitted.sort_values(s_emp.name, inplace=True)
    df_emp_vs_fitted.index = og_index

    # if not any_transformation:
    #     df_emp_vs_fitted["observations_transformed"] = np.nan
    #     df_emp_vs_fitted[f"{x_fit.name}_transformed"] = np.nan

    msdi = comp_msdi(s_data_untransformed, x_fit_untransformed)
    madi = comp_madi(s_data_untransformed, x_fit_untransformed)
    
    stat, ks_pval = stats.ks_2samp(s_data_for_fitting, x_fit)
    if n_params == 3:
        cvm_output = stats.cramervonmises(s_data_for_fitting, fx.name, args = (shape, loc, scale))
    if n_params == 2:
        cvm_output = stats.cramervonmises(s_data_for_fitting, fx.name, args = (loc, scale))
    cvm_pval = cvm_output.pvalue

    s_fit.loc["cvm_pval"] = cvm_pval
    s_fit.loc["ks_pval"] = ks_pval
    s_fit.loc["aic"] = aic
    s_fit.loc["msdi"] = msdi
    s_fit.loc["madi"] = madi
    s_fit.name = s_data.name
    if plot:
        plot_fitted_distribution(s_fit, df_emp_vs_fitted, n_params, fname_savefig)
    return s_fit, df_emp_vs_fitted

def plot_fitted_distribution(s_fit, df_emp_vs_fitted, n_params, fname_savefig):
    # update xlab name to include any transformations
    xlab = s_fit.name
    transformation = xlab
    if not np.isnan(s_fit["upper_bound"]):
        transformation = "({} - {:.2f})*-1".format(transformation, s_fit["upper_bound"])
    if s_fit["scalar_shift"] != 0:
        transformation = "(({}) - {:.2f})".format(transformation, -s_fit["scalar_shift"])
    if s_fit["normalize"] == True:
        n_params += 2 # estiamted mean and standard deviation
        transformation = "normalized({})".format(transformation)
    if s_fit["boxcox"] == True:
        transformation = transformation + " + {}".format(str(round(s_fit.scalar_shift_for_boxcox, 2)))
        n_params += 1 # estimated lambda
        transformation = "boxcox({})".format(transformation)

    fig, axes = plt.subplots(2,2, dpi=400, figsize=[10, 9])

    fit_color = "red"
    obs_color = "blue"

    axes[1,1].hist(df_emp_vs_fitted["cdf_fit"], bins=30, alpha=0.3, label='CDF Fit', color=fit_color, density=True)
    sns.kdeplot(df_emp_vs_fitted["cdf_fit"], ax=axes[1,1], color=fit_color, fill=False, label="CDF Fit",
                bw_adjust = 0.4)
    axes[1,1].hist(df_emp_vs_fitted["cdf_emp"], bins=30, alpha=0.3, label='CDF Empirical', color=obs_color, density=True)
    sns.kdeplot(df_emp_vs_fitted["cdf_emp"], ax=axes[1,1], color=obs_color, fill=False, label="CDF Empirical",
                bw_adjust = 0.4)
    axes[1,1].set_title("Density of Empirical (Gringerton) vs. Fitted CDF Values")
    axes[1,1].set_ylabel("Probability Density")
    axes[1,1].set_xlabel("CDF Value Operated at Observation")
    axes[1,1].set_xlim((-0.1, 1.1))
    # add cvm text
    # cvm_uniform = stats.cramervonmises(df_emp_vs_fitted["cdf_fit"], 'uniform')
    # ax_txt = "CVM P-value\nof fitted CDF\nvalues vs.\nuniform dist.:\n{:.3f}".format(cvm_uniform.pvalue)
    # # Add text to the upper left of axes[1,1]
    # axes[1,1].text(0.05, 0.95, ax_txt, transform=axes[1,1].transAxes,
    #             fontsize=10, verticalalignment='top', bbox=dict(facecolor='white', alpha=0.5))


    # plot empirical pdfs   
    axes[0,0].hist(df_emp_vs_fitted["observations_transformed"], density=True, histtype="stepfilled",
            alpha=0.2, label=" empirical", color = obs_color, bins=20)
    sns.kdeplot(df_emp_vs_fitted["observations_transformed"], ax=axes[0,0], color=obs_color, fill=False, label="empirical",
                bw_adjust = 0.6)
    # plot the fitted pdf
    distribution = getattr(stats, s_fit.loc["distribution"])
    shape, loc, scale = s_fit.loc["shape"], s_fit.loc["loc"], s_fit.loc["scale"]
    ## define plot extents 
    data_min = df_emp_vs_fitted["observations_transformed"].min()
    data_max = df_emp_vs_fitted["observations_transformed"].max()
    range_extension = 0.35 * (data_max - data_min)  # Adjust this factor as needed
    xlims = (df_emp_vs_fitted["observations_transformed"].min()-range_extension, df_emp_vs_fitted["observations_transformed"].max()+range_extension)
    x_dist = np.linspace(data_min-range_extension*2, data_max+range_extension*2, 1000)
    if np.isnan(s_fit.loc["shape"]): # 2 param
        y_dist = distribution.pdf(x_dist, loc, scale)
    else: # 3 param
        y_dist = distribution.pdf(x_dist, shape, loc, scale)
    axes[0,0].plot(x_dist, y_dist, label = "fitted", color=fit_color)
    # axes[0,0].plot(df_emp_vs_fitted["x_fitted"], df_emp_vs_fitted.y_pdf_fit, label = "fitted", color=fit_color)
    # axes[0,0].legend()
    axes[0,0].set_xlim(xlims)
    axes[0,0].set_ylabel("Probability Density")
    axes[0,0].set_xlabel(transformation)
    axes[0,0].set_title("Probability Density Function")
    axes[0,1].plot(df_emp_vs_fitted["observations_realspace"], df_emp_vs_fitted.cdf_emp, label = "empirical", color = obs_color)
    axes[0,1].plot(df_emp_vs_fitted["observations_realspace"], df_emp_vs_fitted.cdf_fit, label = "fitted", color=fit_color, ls="--")
    axes[0,1].legend()
    axes[0,1].set_ylabel("Cumulative Probability")
    axes[0,1].set_title("Cumulative Density Function")
    # axes[0,1].set_xlabel(xlab)
    axes[0,1].set_xlabel(xlab)
    if np.isnan(s_fit.loc["shape"]): # 2 param
        stats.probplot(df_emp_vs_fitted["observations_transformed"], sparams = (loc, scale), dist = s_fit.loc["distribution"], plot = axes[1,0])
    else: # 3 param
        stats.probplot(df_emp_vs_fitted["observations_transformed"], sparams = (shape, loc, scale), dist = s_fit.loc["distribution"], plot = axes[1,0])

    # create figure title
    perf_summary = ": AIC = {} | CVM P = {} | KS P = {}".format(round(s_fit.loc["aic"],1),
                                                                  round(s_fit.loc["cvm_pval"],3),
                                                                  round(s_fit.loc["ks_pval"],3))
    txt_fig_title = f'{s_fit.loc["distribution"]+perf_summary}\n{s_fit.loc["support_lb"]:.2f}<={transformation}<={s_fit.loc["support_ub"]:.2f}\n'
    if transformation == xlab:
        transformation = xlab
    else:
        # back transform the support params
        s_bounds = pd.Series(data=[s_fit.loc["support_lb"], s_fit.loc["support_ub"]], index = ["support_lb", "support_ub"])
        s_bounds_untrs = backtransform_data(s_bounds, s_fit)
        # if the lower and upper bounds are infinit, assign the same value for the upper and lower bounds
        if np.isinf(s_fit.loc["support_lb"]):
            s_bounds_untrs.loc["support_lb"] = s_fit.loc["support_lb"]
        if np.isinf(s_fit.loc["support_ub"]):
            s_bounds_untrs.loc["support_ub"] = s_fit.loc["support_ub"]
        # if upper bound is applied, reverse the upper and lower bounds
        if not np.isnan(s_fit["upper_bound"]):
            old_lb = s_bounds_untrs.loc["support_lb"]
            s_bounds_untrs.loc["support_lb"] = -1*s_bounds_untrs.loc["support_ub"]
            s_bounds_untrs.loc["support_ub"] = old_lb

        txt_fig_title += f'{s_bounds_untrs.loc["support_lb"]:.2f}<={xlab}<={s_bounds_untrs.loc["support_ub"]:.2f}\n'
    if s_fit.loc["fix_loc_to_min"]:
        txt_fig_title += f'loc param fixed to min of {transformation}'
    fig.suptitle(txt_fig_title)
    # fig.text(.5, 1, s_fit.loc["distribution"]+perf_summary+transformation, ha='center')

    fig.set_tight_layout(True)

    if fname_savefig is not None: # save
        plt.savefig(fname_savefig, dpi = 400, bbox_inches='tight')
    return fig, axes


# create dictionaries with the arguments to the fitting functions
gev = {
       "args":{"fx":genextreme, "n_params":3}}

# loggev = { 
#        "args":{"fx":genextreme, "log":True, "n_params":3}}

weibull_min_dist = { 
           "args":{"fx":weibull_min, "n_params":3}}

# logwweibull_min_dist = { 
#            "args":{"fx":weibull_min, "log":True, "n_params":3}}

weibull_max_dist = { 
           "args":{"fx":weibull_max, "n_params":3}}

# logwweibull_max_dist = { 
#            "args":{"fx":weibull_max, "log":True, "n_params":3}}

gumbel_right = { 
          "args":{"fx":gumbel_r, "n_params":2}} # has lower bound of zero

gumbel_left = { 
          "args":{"fx":gumbel_l, "n_params":2}}

# loggumbel = { 
#           "args":{"fx":gumbel_r, "log":True, "n_params":3}}

norm_dist = { 
             "args":{"fx":norm, "n_params":2}}

lognormal = { 
             "args":{"fx":lognorm, "n_params":3}}

student_t = { 
             "args":{"fx":t, "n_params":3}}

genpareto_dist = { 
             "args":{"fx":genpareto, "n_params":3}}

chi_dist = { 
             "args":{"fx":chi, "n_params":3}}

gamma_dist = { 
             "args":{"fx":gamma, "n_params":3}}

p3 = { 
      "args":{"fx":pearson3, "n_params":3}}

# lp3 = { 
#       "args":{"fx":pearson3, "log":True, "n_params":3}}

sep = { 
      "args":{"fx":exponpow, "n_params":3}}

fxs = [gev, weibull_min_dist, weibull_max_dist, gumbel_right, gumbel_left,
       norm_dist, lognormal, student_t, genpareto_dist, chi_dist, gamma_dist, p3, sep]

# fxs = [gev, loggev, weibull_min_dist, logwweibull_min_dist, weibull_max_dist, logwweibull_max_dist,
#        gumbel_right, gumbel_left, loggumbel, norm_dist, lognormal, student_t, genpareto_dist, chi_dist, gamma_dist, p3, lp3, sep]
# %%
