'''
Quick test of timeseries.py
'''
from netcdf_tools.timeseries import load_timeseries

# --- Specify test inputs ---
data_dir  = "/mnt/c/Users/clmbn/NMT_PhD/data/SWANN/UA_NSIDC/grid_0125deg/"
variable  = "SWE"           # variable name inside the netCDF files
latitude  = 35.0             # target latitude
longitude = -106.5           # target longitude

df = load_timeseries(data_dir, variable, latitude, longitude)

print(df)
print(f"\nRows: {len(df)},  Column: {df.columns[0]}")
