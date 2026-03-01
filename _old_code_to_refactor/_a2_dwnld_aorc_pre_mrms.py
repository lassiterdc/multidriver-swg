#%%
from pathlib import Path
import os
from _inputs import *
import xarray as xr
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import numpy as np
import pandas as pd
from glob import glob
import rioxarray
import xarray as xr
import fsspec
import numpy as np
import s3fs
import zarr
from _utils import *
import dask

current_year = 2024

base_url = f's3://noaa-nws-aorc-v1-1-1km'
#%% load dask client
import dask
from dask.distributed import Client
# client = Client()
# client = Client(processes=False, n_workers=2, threads_per_worker=2)
client = Client(processes=False, n_workers=4, threads_per_worker=2, memory_limit="4GB")
client
#%% single year example
# year = '1979'
# single_year_url = f'{base_url}/{year}.zarr/'

# ds_single = xr.open_zarr(fsspec.get_mapper(single_year_url, anon=True), consolidated=True)

# var = "APCP_surface"
# ds_single[var]

# print(f'Variable size: {ds_single[var].nbytes/1e12:.1f} TB')

#%% load multiple years

def load_aorc_rain_data(start_year, end_year, shp_clip):
    dataset_years = list(range(start_year,end_year))

    s3_out = s3fs.S3FileSystem(anon=True)
    fileset = [s3fs.S3Map(
                root=f"s3://{base_url}/{dataset_year}.zarr", s3=s3_out, check=False
            ) for dataset_year in dataset_years]

    var='APCP_surface'
    ds_aorc = xr.open_mfdataset(fileset, engine='zarr')
    da_aorc_rainfall = ds_aorc[var]
    da_aorc_rainfall.name = "rainrate" # rename like mrms
    ds_aorc_rainfall = da_aorc_rainfall.to_dataset()
    gdf_clip = gpd.read_file(shp_clip).buffer(2500).to_crs(ds_aorc_rainfall.rio.crs)
    minx, miny, maxx, maxy = gdf_clip.total_bounds
    subset = ds_aorc_rainfall.rio.clip_box(minx, miny, maxx, maxy)
    if subset.longitude.values.min() < 0:
        subset["longitude"] = subset.longitude + 360
    return subset

ds_aorc_rainfall_1979_to_now = load_aorc_rain_data(1979, current_year, shp_hrsd_gages)

#%% downloading data from 1979 to present
encoding = define_zarr_compression(ds_aorc_rainfall_1979_to_now)
chunks = dict(time = 24*30, latitude = 2, longitude = 3)
ds_aorc_rainfall_1979_to_now.chunk(chunks).to_zarr(f_aorc_data, mode="w", encoding=encoding, consolidated=True)

#%%
# ds_aorc_rainfall_1979_to_2000 = load_aorc_rain_data(1979, 2000, shp_hrsd_gages)
# ds_aorc_rainfall_2012_to_2014 = load_aorc_rain_data(2012, 2014, shp_hrsd_gages)
#%% downloading data from 1979 to 2000
# encoding = define_zarr_compression(ds_aorc_rainfall_1979_to_2000)
# chunks = dict(time = 24*30, latitude = 2, longitude = 3)
# ds_aorc_rainfall_1979_to_2000.chunk(chunks).to_zarr(f_aorc_data_pre_mrms, mode="w", encoding=encoding, consolidated=True)

#%% downloading data from 2012-2014
# encoding = define_zarr_compression(ds_aorc_rainfall_2012_to_2014)
# chunks = dict(time = 24*30, latitude = 2, longitude = 3)
# ds_aorc_rainfall_2012_to_2014.chunk(chunks).to_zarr(f_aorc_data_missing_mrms_fill, mode="w", encoding=encoding, consolidated=True)

#%% to silence warning (did not work to silence warning)
# from dask import delayed
# @delayed
# def save_chunk(data, filename):
#     data.to_zarr(filename, mode="w", encoding=encoding, consolidated=True)

# # Apply save_chunk to the dataset
# save_task = save_chunk(subset.chunk(chunks), f_aorc_data_pre_mrms)
# save_task.compute()

#%% 
ds_aorc_rain = xr.open_dataset(f_aorc_data_pre_mrms, engine = "zarr")
ds_aorc_rain = xr.open_dataset(f_aorc_data_missing_mrms_fill, engine = "zarr")