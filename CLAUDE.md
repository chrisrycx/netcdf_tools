# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Installation & Setup
```bash
# Install package in development mode
pip install -e .

# Install with optional dependencies for regridding
pip install -e ".[regrid]"

# Install with SPIRES support
pip install -e ".[spires]"
```

### Testing
No formal test framework is configured. The package uses quick test scripts in the `tests/` directory:
```bash
# Run individual test scripts
python tests/quickRegrid.py
python tests/quickSpires.py
python tests/quickTimeseries.py
```

### CLI Tools
The package provides several command-line tools (defined in pyproject.toml):
- `nct-inspect` - Inspect NetCDF file structure
- `nct-extract` - Extract specific variables from NetCDF files
- `nct-timeseries` - Extract time series at specific lat/lon points
- `nct-regrid` - Regrid NetCDF data using conservative interpolation
- `nct-spires` - Process SPIRES (MODIS) satellite data
- `nct-getgrid` - Grid utility functions

## Architecture

### Core Modules Structure
- `src/netcdf_tools/` - Main package
  - `inspect.py` - NetCDF file inspection utilities
  - `extract.py` - Variable extraction from NetCDF files
  - `timeseries.py` - Time series extraction at specific coordinates
  - `regrid/` - Regridding functionality
    - `regrid.py` - Main regridding interface with xesmf
    - `spires.py` - SPIRES/MODIS data processing
    - `getgrid.py` - Grid specification utilities

### Key Design Patterns

**Date-based Processing**: The architecture is designed around processing one date at a time for cluster computing efficiency. The `regrid.py` module accepts `target_date` parameters to process single dates independently.

**Data Type Dispatch**: The `load_data()` function in `regrid.py` dispatches to different loaders based on `data_type` parameter (currently supports "spires").

**Grid Specifications**: Uses JSON grid specification files (see `grids/e3sm0125.json`) to define target grids for regridding operations.

### Dependencies
- Core: `netCDF4`, `numpy`, `pandas`, `xarray`, `cftime`
- Regridding: `xesmf` (optional)
- SPIRES processing: `rioxarray`, `rasterio` (optional)

### Utilities Directory
Contains standalone utility scripts and Jupyter notebooks for development and testing. These are not part of the main package but provide examples and validation workflows.

## Processing Workflow
The recommended workflow for large-scale processing:
1. Process one file per date using the regrid module
2. Submit parallel jobs (one per date) to cluster scheduler
3. Merge final outputs using `xr.open_mfdataset()` or `ncrcat`

This pattern supports embarrassingly parallel processing and provides fault tolerance for cluster computing environments.