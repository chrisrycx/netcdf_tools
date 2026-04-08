'''
Quick test spires loading
'''
from datetime import date
from netcdf_tools.regrid.spires import load_spires

spires_path = "/mnt/c/Users/clmbn/NMT_PhD/data/MODIS/SPIRES/raw/"

ds = load_spires(spires_path, date(2025, 2, 1), ["h09v04", "h09v05"])
print(ds)
