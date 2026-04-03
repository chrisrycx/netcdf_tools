"""
netcdf_tools — utilities for inspecting, extracting, and processing NetCDF files.
"""

from .inspect import inspect_netcdf
from .extract import extract_variables
from .timeseries import load_timeseries
from .regrid import regrid

__all__ = [
    "inspect_netcdf",
    "extract_variables",
    "load_timeseries",
    "regrid",
]
