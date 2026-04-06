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


def _find_tile_files(input_dir: Path, tiles: list[str], target_date: date) -> list[Path]:
    """
    Search each tile subdirectory for the file matching *target_date*.

    Parameters
    ----------
    input_dir : Path
        Root directory containing one subdirectory per tile (e.g. 'h09v04/').
    tiles : list of str
        Tile identifiers to search (e.g. ['h09v04', 'h09v05']).
    target_date : date
        Date to look for.

    Returns
    -------
    list of Path
        One path per tile that has a matching file. Logs a warning for any
        tile where no file is found.
    """
    date_str = target_date.strftime("%Y%m%d")
    found: list[Path] = []
    for tile in tiles:
        tile_dir = input_dir / tile
        matches = sorted(tile_dir.glob(f"SPIRES_HIST_*_{date_str}_V*.nc"))
        if not matches:
            log.warning("No file found for tile %s on %s in %s", tile, target_date, tile_dir)
        else:
            found.append(matches[0])
    return found


def _read_tile(path: Path):
    """
    Open one SPIReS tile and return all data variables as a float32 Dataset
    with the sinusoidal CRS attached and fill values as NaN.
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
    proj4 = str(ds["crs"].attrs["proj4"])

    arrays = {}
    for var in ds.data_vars:
        if var == "crs":
            continue
        da = ds[var].squeeze(drop=True)
        da.encoding["dtype"] = da.encoding.get("dtype") or str(da.dtype)
        arrays[var] = da.astype(np.float32)

    ds.close()

    ds_out = xr.Dataset(arrays)
    ds_out = ds_out.rio.write_crs(proj4)
    ds_out = ds_out.rio.set_spatial_dims(x_dim="x", y_dim="y")
    return ds_out


def _load_day(tile_paths: list[Path]):
    """
    Mosaic all tiles for one day and reproject to EPSG:4326.

    Returns a Dataset with dims (time, lat, lon).
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

    tile_datasets = [_read_tile(p) for p in tile_paths]

    if len(tile_datasets) == 1:
        mosaic = tile_datasets[0]
    else:
        crs = tile_datasets[0].rio.crs
        variables = list(tile_datasets[0].data_vars)
        mosaicked = {
            var: merge_arrays([ds[var] for ds in tile_datasets], method="first")
            for var in variables
        }
        import xarray as xr
        mosaic = xr.Dataset(mosaicked)
        mosaic = mosaic.rio.write_crs(crs)
        mosaic = mosaic.rio.set_spatial_dims(x_dim="x", y_dim="y")

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
    target_date: date,
    tiles: list[str],
):
    """
    Load SPIReS tiles for a single date, mosaic, and reproject to EPSG:4326.

    Parameters
    ----------
    input_dir : str or Path
        Root directory containing one subdirectory per tile (e.g. 'h09v04/').
    target_date : date
        The date to load.
    tiles : list of str
        Tile identifiers to load (e.g. ['h09v04', 'h09v05']).

    Returns
    -------
    xarray.Dataset
        Dataset with dimensions (time, lat, lon) in EPSG:4326,
        ready for regridding to a target grid.
    """
    tile_paths = _find_tile_files(Path(input_dir), tiles, target_date)
    if not tile_paths:
        raise ValueError(
            f"No SPIRES files found for date {target_date} "
            f"in any of the requested tiles: {tiles}"
        )

    return _load_day(tile_paths)
