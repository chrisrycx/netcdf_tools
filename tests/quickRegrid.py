'''
Quick test of regrid pipeline using SPIRES data.
'''
from datetime import date
from netcdf_tools.regrid.regrid import regrid

spires_path = "/mnt/c/Users/clmbn/NMT_PhD/data/MODIS/SPIRES/raw/"
#spires_path = "/home/chriscox/Data/MODIS/SPIRES/"
grid_json = "./grids/e3sm0125.json"
output_file = "./tests/spires_regridded_test.nc"

regrid(
    input_file=spires_path,
    grid_json=grid_json,
    output_file=output_file,
    data_type="spires",
    target_date=date(2025, 2, 1),
)
