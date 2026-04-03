#!/usr/bin/env python3
"""
spires — Load SPIReS historical snow cover NetCDF4 tiles for the western US.

Mosaics MODIS sinusoidal tiles by date, reprojects to EPSG:4326, and returns
an xarray Dataset ready for regridding.

Install extra dependencies with: pip install netcdf_tools[spires]
"""

import logging
import re
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import numpy as np

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FILENAME_RE = re.compile(
    r"SPIRES_HIST_h(?P<hh>\d{2})v(?P<vv>\d{2})_MOD09GA061_"
    r"(?P<date>\d{8})_V\d+\.\d+\.nc$"
)

# Western US bounding box (cell edges) used for clipping after reprojection
WEST_LON = -125.0
EAST_LON = -100.0
SOUTH_LAT = 30.0
NORTH_LAT = 50.0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(filename: str) -> Optional[date]:
    """Return the date encoded in a SPIReS filename, or None if no match."""
    m = FILENAME_RE.search(Path(filename).name)
    if m is None:
        return None
    return datetime.strptime(m.group("date"), "%Y%m%d").date()


def _group_files_by_date(input_dir: Path) -> dict[date, list[Path]]:
    """Scan *input_dir* and return {date: [path, ...]} for all matching files."""
    groups: dict[date, list[Path]] = {}
    for path in sorted(input_dir.glob("SPIRES_HIST_*.nc")):
        d = _parse_date(path.name)
        if d is not None:
            groups.setdefault(d, []).append(path)
    return groups


def _read_tile(path: Path, variable: str):
    """
    Open one SPIReS tile and return the requested variable as a float32
    DataArray with the sinusoidal CRS attached and fill values as NaN.
    """
    try:
        import xarray as xr
        import rioxarray  # noqa: F401
    except ImportError:
        raise ImportError(
            "rioxarray is required for SPIReS processing. "
            "Install with: pip install netcdf_tools[spires]"
        )

    ds = xr.open_dataset(path, masked=True)

    if variable not in ds:
        raise KeyError(
            f"Variable '{variable}' not found in {path.name}. "
            f"Available: {list(ds.data_vars)}"
        )

    da = ds[variable].squeeze(drop=True)
    da.encoding["dtype"] = da.encoding.get("dtype") or str(da.dtype)
    da = da.astype(np.float32)

    proj4 = str(ds["crs"].attrs["proj4"])
    da = da.rio.write_crs(proj4)
    da = da.rio.set_spatial_dims(x_dim="x", y_dim="y")

    ds.close()
    return da


def _load_day(tile_paths: list[Path], variable: str):
    """
    Mosaic all tiles for one day and reproject to EPSG:4326.

    Returns a DataArray with dims (time, lat, lon).
    """
    try:
        from rioxarray.merge import merge_arrays
        from rasterio.enums import Resampling
    except ImportError:
        raise ImportError(
            "rioxarray and rasterio are required for SPIReS processing. "
            "Install with: pip install netcdf_tools[spires]"
        )

    day = _parse_date(tile_paths[0].name)
    log.info("Loading %s (%d tile(s))", day, len(tile_paths))

    arrays = [_read_tile(p, variable) for p in tile_paths]
    mosaic = merge_arrays(arrays, method="first") if len(arrays) > 1 else arrays[0]

    buffer = 2.0
    mosaic_4326 = mosaic.rio.reproject(
        "EPSG:4326",
        resampling=Resampling.average,
        nodata=np.nan,
    )
    mosaic_4326 = mosaic_4326.rio.clip_box(
        minx=WEST_LON - buffer,
        maxx=EAST_LON + buffer,
        miny=SOUTH_LAT - buffer,
        maxy=NORTH_LAT + buffer,
    )

    mosaic_4326 = mosaic_4326.rename({"x": "lon", "y": "lat"})

    time_val = np.array([np.datetime64(day.isoformat(), "ns")])
    return mosaic_4326.expand_dims(dim={"time": time_val})


# ---------------------------------------------------------------------------
# Public loader
# ---------------------------------------------------------------------------

def load_spires(
    input_dir,
    variable: str = "snow_fraction",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
):
    """
    Load SPIReS tiles, mosaic by date, and reproject to EPSG:4326.

    Parameters
    ----------
    input_dir : str or Path
        Directory containing SPIRES_HIST_*.nc tile files.
    variable : str
        Data variable to extract (default: 'snow_fraction').
    start_date, end_date : date, optional
        Inclusive date range filter.

    Returns
    -------
    xarray.Dataset
        Dataset with dimensions (time, lat, lon) in EPSG:4326,
        ready for regridding to a target grid.
    """
    import xarray as xr

    groups = _group_files_by_date(Path(input_dir))
    if not groups:
        raise ValueError(f"No matching SPIRES files found in {input_dir}")

    if start_date:
        groups = {d: v for d, v in groups.items() if d >= start_date}
    if end_date:
        groups = {d: v for d, v in groups.items() if d <= end_date}
    if not groups:
        raise ValueError("No files remain after applying date filter.")

    log.info("Loading %d date(s).", len(groups))

    daily_arrays = [
        _load_day(paths, variable)
        for _, paths in sorted(groups.items())
    ]

    combined = xr.concat(daily_arrays, dim="time")
    return combined.to_dataset(name=variable)
