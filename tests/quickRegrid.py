'''
Quick test of regrid pipeline using SPIRES data.
'''
import os
from datetime import date
from netcdf_tools.regrid.regrid import regrid
from dotenv import load_dotenv
load_dotenv()

spires_path = os.getenv('SPIRES_PATH')
grid_json = "./grids/e3sm0125.json"
output_file = "./tests/spires_regridded_test.nc"

regrid(
    input_file=spires_path,
    grid_json=grid_json,
    output_file=output_file,
    data_type="spires",
    target_date=date(2024, 2, 1),
)
