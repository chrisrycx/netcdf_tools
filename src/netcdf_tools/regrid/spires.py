#!/usr/bin/env python3
"""
spires — Load SPIReS historical snow cover NetCDF4 tiles for the western US.

Mosaics MODIS sinusoidal tiles by date, reprojects to EPSG:4326, and returns
an xarray Dataset ready for regridding.

Install extra dependencies with: pip install netcdf_tools[spires]
"""

import logging
import math
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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(filename: str) -> Optional[date]:
    """Return the date encoded in a SPIReS filename, or None if no match."""
    m = FILENAME_RE.search(Path(filename).name)
    if m is None:
        return None
    return datetime.strptime(m.group("date"), "%Y%m%d").date()


def _tiles_for_bounds(west: float, east: float, south: float, north: float) -> list[str]:
    """
    Return MODIS sinusoidal tile names that overlap the given lat/lon bounding box.

    The MODIS grid has 36 columns (h00–h35) and 18 rows (v00–v17). Each tile
    spans 10° in the sinusoidal y-axis (latitude). In geographic lon, the tile
    width widens toward the poles, so we check both the northern and southern
    edge of each row to conservatively capture all overlapping tiles.
    """
    # v_min: northernmost row whose southern edge is above our bbox south boundary.
    # floor((90-north)/10) gives the first v row whose northern edge <= north.
    v_min = max(0, math.floor((90.0 - north) / 10.0))
    # v_max: southernmost row whose northern edge is strictly above our bbox south boundary.
    # ceil((90-south)/10) - 1 avoids including a row that only touches south at its northern edge.
    v_max = min(17, math.ceil((90.0 - south) / 10.0) - 1)

    tiles = []
    for v in range(v_min, v_max + 1):
        lat_n = 90.0 - v * 10.0
        lat_s = 90.0 - (v + 1) * 10.0
        # Avoid division by zero at the poles
        cos_n = max(math.cos(math.radians(lat_n)), 1e-9)
        cos_s = max(math.cos(math.radians(lat_s)), 1e-9)

        for h in range(36):
            # Geographic lon of this tile's left/right edges at north and south latitudes.
            # Sinusoidal formula: lon = (h * 10 - 180) / cos(lat)
            tile_west = min((h * 10.0 - 180.0) / cos_n, (h * 10.0 - 180.0) / cos_s)
            tile_east = max(((h + 1) * 10.0 - 180.0) / cos_n, ((h + 1) * 10.0 - 180.0) / cos_s)
            if tile_west <= east and tile_east >= west:
                tiles.append(f"h{h:02d}v{v:02d}")

    return tiles


def _find_tile_files(input_dir: Path, tiles: list[str], target_date: date) -> list[Path]:
    """
    Search each tile subdirectory for the file matching *target_date*.

    Returns one path per tile found. Logs a warning for any tile with no file.
    """
    date_str = target_date.strftime("%Y%m%d")
    found: list[Path] = []
    for tile in tiles:
        tile_dir = input_dir / tile / str(target_date.year)
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

    ds = xr.open_dataset(path, mask_and_scale=True)
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


def _load_day(tile_paths: list[Path], west: float, east: float, south: float, north: float):
    """
    Mosaic all tiles for one day, reproject to EPSG:4326, and clip to bounds.

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
        minx=west - buffer,
        maxx=east + buffer,
        miny=south - buffer,
        maxy=north + buffer,
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
    west: float,
    east: float,
    south: float,
    north: float,
):
    """
    Load SPIReS tiles for a single date, mosaic, and reproject to EPSG:4326.

    Parameters
    ----------
    input_dir : str or Path
        Root directory containing one subdirectory per tile (e.g. 'h09v04/').
    target_date : date
        The date to load.
    west, east : float
        Longitude bounds in degrees (e.g. -125.0, -100.0).
    south, north : float
        Latitude bounds in degrees (e.g. 30.0, 50.0).

    Returns
    -------
    xarray.Dataset
        Dataset with dimensions (time, lat, lon) in EPSG:4326,
        clipped to the requested bounds and ready for regridding.
    """
    tiles = _tiles_for_bounds(west, east, south, north)
    log.info("Bounds (W=%.1f E=%.1f S=%.1f N=%.1f) → tiles: %s", west, east, south, north, tiles)

    tile_paths = _find_tile_files(Path(input_dir), tiles, target_date)
    if not tile_paths:
        raise ValueError(
            f"No SPIRES files found for date {target_date} "
            f"within bounds [{west}, {east}, {south}, {north}]"
            f" Path: {input_dir}"
        )

    return _load_day(tile_paths, west, east, south, north)
