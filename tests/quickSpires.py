'''
Quick test spires loading
'''
from datetime import date
from netcdf_tools.regrid.spires import load_spires

#spires_path = "/mnt/c/Users/clmbn/NMT_PhD/data/MODIS/SPIRES/raw/"
spires_path = "/home/chriscox/Data/MODIS/SPIRES/"

ds = load_spires(spires_path, date(2025, 2, 1), west=-125.0, east=-100.0, south=30.0, north=50.0)
print(ds)
