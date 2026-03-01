#%% load libraries
from pathlib import Path
import os
from _inputs import *
import xarray as xr
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from glob import glob
from tqdm import tqdm
# f_in_b_nc, f_shp_swmm_subs, f_mrms_rainfall, f_out_b_csv_subs_w_mrms_grid, f_out_swmm_rainfall, mm_per_inch = def_inputs_for_b()
#%% funciotns
# define functions for computing whether points are within 10% of 1 to 1 line
def comp_dist_to_1to1_line(df, xvar, yvar):
    # https://en.wikipedia.org/wiki/Distance_from_a_point_to_a_line
    b = 1
    a = -1*b
    c = 0
    x0 = df[xvar]
    y0 = df[yvar]

    dist = np.abs(a * x0 + b * y0 + c) / np.sqrt(a**2 + b**2)
    nearest_x = (b * (b*x0 - a * y0) - a * c) / (a**2 + b**2)
    nearest_y = (a*(-b*x0 + a*y0) - b*c)/(a**2 + b**2)

    dist.name = "distance_to_1to1_line"
    nearest_x.name = "x_coord_of_nearest_pt_on_1to1_line"
    nearest_y.name = "y_coord_of_nearest_pt_on_1to1_line"

    return dist, nearest_x, nearest_y

def compute_frac_within_tolerance_of_1to1_line(dist, nearest_x, frac_tol):
    s_within_tol = (dist <= nearest_x*(frac_tol/2))
    return s_within_tol.sum()/len(s_within_tol)

# define hexbin plot function
def plt_hexbin(df, frac_tol = 0.1, col1="mrms_biascorrected_daily_totals_mm", col2="mrms_nonbiascorrected_daily_totals_mm",
                refcol ="ref_daily_totals_mm",logcount = True, gridcnt = 40, fig_fpath = None,
                fig_title = None):
    fig, (ax0, ax1) = plt.subplots(ncols=2, figsize=(10, 4), dpi = 300)

    ylim = xlim = (0, df.loc[:,refcol ].max())
    nx = gridcnt
    ny = int(round(nx / np.sqrt(3),0))
    extent = xlim[0], xlim[1], ylim[0], ylim[1]
    # xlim = (0, max(df.loc[:, col1].max(), df.loc[:, col2].max()))
    ax0.set(xlim=xlim, ylim=ylim)
    ax1.set(xlim=xlim, ylim=ylim)

    if logcount:
        hb1 = ax0.hexbin(df.loc[:, col1], df.loc[:,refcol ], bins='log', cmap='inferno',mincnt=5,gridsize=(nx, ny), extent = extent)
        hb2 = ax1.hexbin(df.loc[:, col2], df.loc[:,refcol ], bins='log', cmap='inferno',mincnt=5,gridsize=(nx, ny), extent = extent)
    else:
        hb1 = ax0.hexbin(df.loc[:, col1], df.loc[:,refcol ], cmap='inferno', mincnt=5,gridsize=(nx, ny), extent = extent)
        hb2 = ax1.hexbin(df.loc[:, col2], df.loc[:,refcol ], cmap='inferno', mincnt=5,gridsize=(nx, ny), extent = extent)

    # compute frac of points within tolerance
    dist1, nearest_x1, nearest_y1 = comp_dist_to_1to1_line(df, col1, refcol)
    frac_pts_in_tol1 = compute_frac_within_tolerance_of_1to1_line(dist1, nearest_x1, frac_tol)
    dist2, nearest_x2, nearest_y2 = comp_dist_to_1to1_line(df, col2, refcol)
    frac_pts_in_tol2 = compute_frac_within_tolerance_of_1to1_line(dist2, nearest_x2, frac_tol)

    ax0.set_xlabel(df.loc[:, col1].name)
    ax0.set_ylabel(df.loc[:,refcol ].name)
    ax1.set_xlabel(df.loc[:, col2].name)
    ax1.set_ylabel(df.loc[:,refcol ].name)

    ax0.set_title("Percent of observations within {}% of the 1:1 line: {}%".format(int(frac_tol*100), round(frac_pts_in_tol1*100, 2)),
                  fontsize = 9)
    ax1.set_title("Percent of observations within {}% of the 1:1 line: {}%".format(int(frac_tol*100), round(frac_pts_in_tol2*100, 2)),
                  fontsize = 9)
    # add lines showing tolerance threshold
    x0 = y0 = np.linspace(0, xlim, 500)
    dist = x0 * frac_tol/2
    # based on a^2 + b^2 = c^2, assuming 1:1 line so a=b
    x_upper = x0 - np.sqrt(dist**2/2)
    y_upper = y0 + np.sqrt(dist**2/2)
    x_lower = x0 + np.sqrt(dist**2/2)
    y_lower = y0 - np.sqrt(dist**2/2)

    ax0.plot(x_upper, y_upper, label = "upper bound", c = "cyan", ls = "--", linewidth = 0.7, alpha = 0.8)
    ax0.plot(x_lower, y_lower, label = "lower bound", c = "cyan", ls = "--", linewidth = 0.7, alpha = 0.8)

    ax1.plot(x_upper, y_upper, label = "upper bound", c = "cyan", ls = "--", linewidth = 0.7, alpha = 0.8)
    ax1.plot(x_lower, y_lower, label = "lower bound", c = "cyan", ls = "--", linewidth = 0.7, alpha = 0.8)

    # ax1.set_title("With a log color scale")
    cb1 = fig.colorbar(hb1, ax=ax0, label='')
    cb2 = fig.colorbar(hb2, ax=ax1, label='')
    ax0.axline((0, 0), slope=1, c = "cyan", ls = "--", linewidth = 1.2)
    ax1.axline((0, 0), slope=1, c = "cyan", ls = "--", linewidth = 1.2)
    # define lines based on upper and lower bounds of defined tolerance
    pts = nearest_y1[nearest_y1>0].quantile([0.1, 0.9])
    # create folder if it doesn't already exist
    Path(fig_fpath).parent.mkdir(parents=True, exist_ok=True)
    if fig_title is not None:
        fig.suptitle(fig_title)
    # fig.tight_layout()
    if fig_fpath is not None:
        plt.savefig(fig_fpath)
    plt.close()
    return
#%% load data
# ds_mrms = xr.open_dataset(f_in_b_nc)
lst_fs_mrms_at_gages = glob(fldr_mrms_at_gages + "*.zarr")
lst_ds_qaqc = []
lst_ds_rainfall = []
for f in lst_fs_mrms_at_gages:
    try:
        del ds["spatial_ref"]
    except:
        pass
    if "_qaqc.zarr" in f:
        ds = xr.open_dataset(f, engine = "zarr",chunks = "auto")
        lst_ds_qaqc.append(ds)
    else:
        ds = xr.open_dataset(f, engine = "zarr",chunks = "auto")
        lst_ds_rainfall.append(ds)

ds_mrms = xr.concat(lst_ds_rainfall, dim = "time")
ds_qaqc = xr.concat(lst_ds_qaqc, dim = "date")
ds_aorc = xr.open_dataset(f_aorc_data, engine = "zarr",chunks = "auto")

# transform to normal coordiante system
def transform_to_4326(ds):
    if ds.longitude.to_series().max() > 180:
        ds["longitude"] = ds["longitude"] - 360
    return ds

ds_mrms = transform_to_4326(ds_mrms)
ds_qaqc = transform_to_4326(ds_qaqc)
ds_aorc = transform_to_4326(ds_aorc)
# ds_aorc = xr.open_mfdataset([f_aorc_data_pre_mrms, f_aorc_data_missing_mrms_fill], engine = "zarr", concat_dim = "time", combine = "nested")

#%% create hexplots comparing bias and non-bias-corrected daily totals
ds_subset = ds_qaqc[["mrms_biascorrected_daily_totals_mm", "mrms_nonbiascorrected_daily_totals_mm","ref_daily_totals_mm"]]
ds_subset.coords["year"] = ds_subset['date'].dt.strftime('%Y')
# ds_subset.coords["year_month"] = ds_subset['date'].dt.strftime('%Y-%m')

# ds_subset_yearly_totals = ds_subset.groupby("year").sum()
# ds_subset_yearly_totals = ds_subset_yearly_totals.load()
# # subset only where the mrms and reference data have non zero totals
# condition = (ds_subset_yearly_totals["mrms_nonbiascorrected_daily_totals_mm"] > 0) & \
#             (ds_subset_yearly_totals["ref_daily_totals_mm"] > 0)
# if condition.sum() > 0:
#     ds_subset_yearly_totals = ds_subset_yearly_totals.where(condition, drop=True)
# else:
#     print("No valid data points matching the condition.")

years = pd.unique(ds_subset.date.dt.year.values)

df_qaqc_daily = ds_subset.to_dataframe()

plt_hexbin(df_qaqc_daily, fig_fpath = f"{fld_out_b}plots/daily_totals_hexbin.png", fig_title = "daily total comparison")

for yr in ds_subset.year.to_series().unique():
    ds_sub_yr = ds_subset.sel(date = yr)
    df_yearly_totals = ds_sub_yr.to_dataframe().dropna()
    plt_hexbin(df_yearly_totals, fig_fpath = "{}plots/daily_totals_by_year/year_{}_dailly_tots.png".format(fld_out_b,yr), fig_title = "year {} daily total comparison".format(yr))

#%%
gdf_subs = gpd.read_file(f_shp_swmm_subs)


#%% associate each sub with the closest grid coord

def return_gridcells_nearest_subcatchments(ds_rain, gdf_subs):
    gdf_sub_centroid = gpd.GeoDataFrame(geometry=gdf_subs.centroid)

    x,y = np.meshgrid(ds_rain.longitude.values, ds_rain.latitude.values, indexing="ij")
    grid_length = x.shape[0] * x.shape[1]
    x = x.reshape(grid_length)
    y = y.reshape(grid_length)

    df_rain_coords = pd.DataFrame({"x_lon":x, "y_lat":y})

    # create geopandas dataframe for rain grid
    gdf_rain = gpd.GeoDataFrame(geometry=gpd.points_from_xy(x=df_rain_coords.x_lon, y=df_rain_coords.y_lat), crs="EPSG:4326")


    gdf_rain_state_plane = gdf_rain.to_crs("EPSG:2284")

    # join subcatchment centroids with the closest MRMS point
    gdf_matching_subs_and_rain = gpd.sjoin_nearest(gdf_sub_centroid, gdf_rain_state_plane, how='left')

    idx = gdf_matching_subs_and_rain.index_right.values

    df_rain_at_subs = df_rain_coords.iloc[idx, :]

    # unique gridcells
    df_rain_at_subs_unique = df_rain_at_subs.drop_duplicates()
    return df_rain_at_subs_unique, df_rain_at_subs


df_mrms_at_subs_unique, df_mrms_at_subs = return_gridcells_nearest_subcatchments(ds_mrms, gdf_subs)

# find the aorc gridcells nearest the mrms gridcells
# df_aorc_at_subs_unique, ___ = return_gridcells_nearest_subcatchments(ds_aorc, gdf_subs)

gdf_mrms = gpd.GeoDataFrame(geometry=gpd.points_from_xy(x=df_mrms_at_subs_unique.x_lon, y=df_mrms_at_subs_unique.y_lat), crs="EPSG:4326")
gdf_mrms_state_plane = gdf_mrms.to_crs("EPSG:2284")

df_aorc_locs = ds_aorc.isel(time = 0).to_dataframe().reset_index()[["latitude", "longitude"]]
gdf_aorc = gpd.GeoDataFrame(geometry=gpd.points_from_xy(x=df_aorc_locs.longitude, y=df_aorc_locs.latitude), crs="EPSG:4326")
gdf_aorc_state_plane = gdf_aorc.to_crs("EPSG:2284")

gdf_aorc_nearest_mrms_idx = gpd.sjoin_nearest(gdf_mrms_state_plane, gdf_aorc_state_plane, how='left')

# gdf_aorc_nearest_mrms = gdf_aorc_state_plane.loc[gdf_aorc_nearest_mrms_idx["index_right"]]

gdf_aorc_nearest_mrms = gdf_aorc.loc[gdf_aorc_nearest_mrms_idx["index_right"]]

df_aorc_at_subs_unique = pd.DataFrame(index = gdf_aorc_nearest_mrms.index, data = dict(x_lon = gdf_aorc_nearest_mrms.geometry.x, y_lat = gdf_aorc_nearest_mrms.geometry.y))

if df_aorc_at_subs_unique.duplicated().sum() > 0:
    sys.exit("there are duplicate aorc values in a dataframe that should only have unique values")
# lst_s_aorc_nearest_to_mrms = []
# for idx, row in df_mrms_at_subs_unique.iterrows():
#     min_dist = np.inf
#     s_nearest_point = pd.Series(dtype=float)
    
#     for idx2, row2 in df_aorc_at_subs_unique.iterrows():
#         dist = ((row.x_lon - row2.x_lon)**2 + (row.y_lat - row2.y_lat)**2)**0.5
#         if dist < min_dist:
#             s_nearest_point.loc["id"] = idx2
#             s_nearest_point.loc["x_lon"] = row2.x_lon
#             s_nearest_point.loc["y_lat"] = row2.y_lat
#     lst_s_aorc_nearest_to_mrms.append(s_nearest_point)


#%%
fig, ax = plt.subplots()
gdf_subs.to_crs("EPSG:4326").plot(ax = ax)
df_mrms_at_subs_unique.plot.scatter(x = "x_lon", y = "y_lat", ax = ax, label = "mrms", color = "red")
df_aorc_at_subs_unique.plot.scatter(x = "x_lon", y = "y_lat", ax = ax, label = "aorc", color = "orange")
ax.legend()
#%% append subcatchment geodataframe with associated rain time series
raingage_id = df_mrms_at_subs.index.values
sub_name = gdf_subs.NAME.values

df_subs_raingages = pd.DataFrame({"subcatchment_id":sub_name, "raingage_id":raingage_id})
df_subs_raingages.to_csv(f_out_b_csv_subs_w_mrms_grid)

#%% create time series files
import sys
def create_time_series(ds_rain, df_rain_at_subs_unique):
    # ds_rain, df_rain_at_subs_unique = ds_mrms, df_mrms_at_subs_unique
    lst_s_tseries = []
    lst_s_bias_corrected = []
    for id, coords in tqdm(df_rain_at_subs_unique.iterrows()):
        idx = dict(latitude = coords.y_lat, longitude = coords.x_lon)
        # ds_rain_subset = ds_rain.sel(idx)
        da_rain_subset = ds_rain["rainrate"].sel(idx).load()
        df_rain_subset = da_rain_subset.to_dataframe()
        df_rain_subset_nona = df_rain_subset.rainrate.dropna().reset_index().drop_duplicates().set_index("time").sort_index()
        s_rain = df_rain_subset_nona["rainrate"]
        s_rain.name = id
        lst_s_tseries.append(s_rain)
        if "bias_corrected" in ds_rain.coords:
            s_bias_corrected = df_rain_subset_nona["bias_corrected"]
            s_bias_corrected.name = id
            lst_s_bias_corrected.append(s_bias_corrected)
    # create rainfall time series
    df_rain_tseries = pd.concat(lst_s_tseries, axis = 1)
    # df_rain_tseries = pd.DataFrame(data).set_index('time')
    df_rain_tseries['rain_mean'] = df_rain_tseries.mean(axis=1, skipna=True)
    if "bias_corrected" in ds_rain.coords:
        df_bias_corrected_tseries = pd.concat(lst_s_bias_corrected, axis = 1)
        for id, tseries in df_bias_corrected_tseries.T.iterrows():
            for id2, tseries2 in df_bias_corrected_tseries.T.iterrows():
                if id == id2:
                    continue
                else:
                    if (tseries != tseries2).sum() != 0:
                        sys.exit("there is a misalignment in bias correction status")
        df_rain_tseries["bias_corrected"] = df_bias_corrected_tseries.iloc[:,0]
    return df_rain_tseries


#%% export csv files
df_aorc_tseries = create_time_series(ds_aorc, df_aorc_at_subs_unique)
df_aorc_tseries.to_csv(f_aorc_rainfall)
print(f"Created {f_aorc_rainfall}")

#%% mrms (takes a while, 10-15 minutes)
df_mrms_tseries = create_time_series(ds_mrms, df_mrms_at_subs_unique)
df_mrms_tseries.to_csv(f_mrms_rainfall)
print(f"Created {f_mrms_rainfall}")




#%% creating .dat files for swmmm
"""
From SWMM Manual Version 5.1:
"a standard user-prepared format where each line of the file contains
the station ID, year, month, day, hour, minute, and non-zero precipitation
reading, all separated by one or more spaces."
Also
"""

gage_ids = df_mrms_tseries.columns
df_rain_tseries = df_mrms_tseries.reset_index()
df_rain_tseries['date'] = df_rain_tseries.time.dt.strftime('%m/%d/%Y')
df_rain_tseries['time'] = df_rain_tseries.time.dt.time

# station ID has a ';' which is the comment symbol in SWMM
df_long = pd.melt(df_rain_tseries, id_vars = ["date", "time"], var_name="station_id", value_name="precip")

# df_long["year"] = df_long.time.dt.year
# df_long["month"] = df_long.time.dt.month
# df_long["day"] = df_long.time.dt.day
# df_long["hour"] = df_long.time.dt.hour
# df_long["minute"] = df_long.time.dt.minute
# df_long["time"] = df_long.time.dt.time
df_long["precip_in"] = df_long.precip / mm_per_inch
df_long = df_long[["station_id", "date", "time", "precip_in"]]

# remove non-zero values (swmm assumes zeros if there is no data at a timestep)
df_long = df_long[df_long['precip_in'] > 0]
#%% export to file
for g_id in gage_ids:
    # initialize file with proper header
    with open(f_out_swmm_rainfall.format(g_id), "w+") as file:
        file.write(";;MRMS Precipitation Data\n")
        file.write(";;Rainfall (in/hr)\n")
    df_long_subset = df_long[df_long['station_id'] == g_id]
    df_long_subset = df_long_subset.drop(["station_id"], axis=1)
    df_long_subset.to_csv(f_out_swmm_rainfall.format(g_id), sep = '\t', index = False, header = False, mode="a")
    

