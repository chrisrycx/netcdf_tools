'''
Quick test spires loading
'''
import os
from datetime import date
from netcdf_tools.regrid.spires import load_spires
from dotenv import load_dotenv
load_dotenv()

spires_path = os.getenv('SPIRES_PATH')

ds = load_spires(spires_path, date(2025, 2, 1), west=-125.0, east=-100.0, south=30.0, north=50.0)
print(ds)
