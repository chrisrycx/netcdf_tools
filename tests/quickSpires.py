'''
Quick test spires loading
'''
from datetime import date
from netcdf_tools.regrid.spires import load_spires

ds = load_spires("/path/to/data", date(2020, 3, 1), ["h09v04", "h09v05"])
print(ds)
