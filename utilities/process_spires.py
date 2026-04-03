#!/usr/bin/env python3
"""
process_spires.py — Process SPIReS historical snow cover NetCDF4 files for the western US.

Pipeline:
  1. Group input files by date (parsed from filename).
  2. For each date, mosaic all MODIS sinusoidal tiles with rioxarray.merge.merge_arrays.
  3. Reproject mosaic to EPSG:4326 using Resampling.average.
  4. Resample to a regular 0.125° grid over the western US.
  5. Assign time coordinate and write a compressed daily NetCDF4 file.
  6. Concatenate daily files into a single output with xr.open_mfdataset.

Usage:
  python process_spires.py -i /data/spires -o output.nc -v snow_fraction
  python process_spires.py -i /data/spires -o output.nc -v snow_fraction \
      --start-date 20000301 --end-date 20000331 --temp-dir /scratch/tmp
"""

import argparse
import logging
import re
import tempfile
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import numpy as np
import xarray as xr
import rioxarray  # noqa: F401  — activates the .rio accessor
from rioxarray.merge import merge_arrays
from rasterio.enums import Resampling


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FILENAME_RE = re.compile(
    r"SPIRES_HIST_h(?P<hh>\d{2})v(?P<vv>\d{2})_MOD09GA061_"
    r"(?P<date>\d{8})_V\d+\.\d+\.nc$"
)

# Western US bounding box (cell edges) and resolution
WEST_LON = -125.0
EAST_LON = -100.0
SOUTH_LAT = 30.0
NORTH_LAT = 50.0
RESOLUTION = 0.125  # degrees

# Target grid cell *centers* at half-resolution offset from edges
TARGET_LONS = np.arange(WEST_LON + RESOLUTION / 2, EAST_LON, RESOLUTION)
TARGET_LATS = np.arange(SOUTH_LAT + RESOLUTION / 2, NORTH_LAT, RESOLUTION)

# CF time encoding origin
TIME_UNITS = "days since 2000-01-01"
TIME_CALENDAR = "standard"

# Compression
ZLIB_LEVEL = 4

log = logging.getLogger(__name__)


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


def _encoding_for_variable(da: xr.DataArray) -> dict:
    """
    Build xarray encoding for *da* that matches the original integer dtype,
    recovering scale_factor / add_offset so values span the dtype range.

    uint8  → _FillValue=255,  valid range [0,254]
    uint16 → _FillValue=65535, valid range [0,65534]
    """
    orig_dtype = da.encoding.get("dtype") or da.attrs.get("_orig_dtype")
    if orig_dtype is not None and np.dtype(orig_dtype) == np.dtype("uint16"):
        fill = 65535
        out_dtype = "uint16"
        n_steps = 65534
    else:
        fill = 255
        out_dtype = "uint8"
        n_steps = 254

    # Determine physical range from data (ignore NaN)
    valid = da.values[np.isfinite(da.values)]
    if valid.size == 0:
        vmin, vmax = 0.0, 1.0
    else:
        vmin = float(np.nanmin(valid))
        vmax = float(np.nanmax(valid))
        if vmin == vmax:
            vmax = vmin + 1.0

    scale = (vmax - vmin) / n_steps
    offset = vmin

    return {
        "dtype": out_dtype,
        "_FillValue": fill,
        "scale_factor": np.float32(scale),
        "add_offset": np.float32(offset),
        "zlib": True,
        "complevel": ZLIB_LEVEL,
    }


def _read_tile(path: Path, variable: str) -> xr.DataArray:
    """
    Open one SPIReS tile file and return the requested variable as a
    float32 DataArray with the sinusoidal CRS written to its .rio accessor
    and NaN where data was masked/fill.
    """
    ds = xr.open_dataset(path, masked=True)

    if variable not in ds:
        raise KeyError(
            f"Variable '{variable}' not found in {path.name}. "
            f"Available: {list(ds.data_vars)}"
        )

    da = ds[variable].squeeze(drop=True)  # drop singleton time dim if present

    # Preserve original dtype for re-encoding later
    da.encoding["dtype"] = da.encoding.get("dtype") or str(da.dtype)

    # Convert to float32; masked values become NaN
    da = da.astype(np.float32)

    # Write CRS from the file's crs variable
    proj4 = str(ds["crs"].attrs["proj4"])
    da = da.rio.write_crs(proj4)
    da = da.rio.set_spatial_dims(x_dim="x", y_dim="y")

    ds.close()
    return da


# ---------------------------------------------------------------------------
# Per-day processing (parallelisation unit)
# ---------------------------------------------------------------------------


def process_day(
    tile_paths: list[Path],
    variable: str,
    output_path: Path,
) -> None:
    """
    Process all tiles for a single date and write a compressed daily NetCDF4.

    Parameters
    ----------
    tile_paths:
        All SPIReS tile files for this date.
    variable:
        Name of the data variable to extract.
    output_path:
        Destination .nc file for this day.
    """
    day = _parse_date(tile_paths[0].name)
    log.info("Processing %s (%d tile(s))", day, len(tile_paths))

    # --- 1. Read tiles -------------------------------------------------------
    arrays = [_read_tile(p, variable) for p in tile_paths]

    # --- 2. Mosaic in sinusoidal space ---------------------------------------
    if len(arrays) == 1:
        mosaic = arrays[0]
    else:
        mosaic = merge_arrays(arrays, method="first")

    # --- 3. Reproject to EPSG:4326 ------------------------------------------
    # Clip to western-US extent plus a small buffer before reprojection to
    # keep the operation lightweight.
    buffer = 2.0  # degrees — account for sinusoidal distortion at edges
    mosaic_4326 = mosaic.rio.reproject(
        "EPSG:4326",
        resampling=Resampling.average,
        nodata=np.nan,
    )

    # --- 4. Clip to western US + buffer, then resample to target grid --------
    mosaic_4326 = mosaic_4326.rio.clip_box(
        minx=WEST_LON - buffer,
        maxx=EAST_LON + buffer,
        miny=SOUTH_LAT - buffer,
        maxy=NORTH_LAT + buffer,
    )

    # Resample to the exact 0.125° target grid using interp (nearest for speed;
    # the heavy averaging was already done in reproject above).
    regridded = mosaic_4326.interp(
        x=TARGET_LONS,
        y=TARGET_LATS,
        method="nearest",
        kwargs={"fill_value": np.nan},
    )

    # Rename spatial dims to CF-standard names
    regridded = regridded.rename({"x": "lon", "y": "lat"})

    # --- 5. Assign time coordinate -------------------------------------------
    time_val = np.array([np.datetime64(day.isoformat(), "ns")])
    regridded = regridded.expand_dims(dim={"time": time_val})

    # --- 6. Build Dataset and write -----------------------------------------
    ds_out = regridded.to_dataset(name=variable)

    # Propagate original dtype hint into the output DataArray
    ds_out[variable].encoding["dtype"] = arrays[0].encoding.get("dtype", "uint8")

    # Coordinate encodings
    time_enc = {
        "units": TIME_UNITS,
        "calendar": TIME_CALENDAR,
        "dtype": "int32",
    }
    coord_enc = {"dtype": "float64", "zlib": False}

    # Grid-mapping attribute
    ds_out[variable].attrs["grid_mapping"] = "crs"

    # Write a CRS variable (WGS84)
    crs_var = xr.DataArray(
        np.int32(0),
        attrs={
            "grid_mapping_name": "latitude_longitude",
            "longitude_of_prime_meridian": 0.0,
            "semi_major_axis": 6378137.0,
            "inverse_flattening": 298.257223563,
            "crs_wkt": (
                'GEOGCS["WGS 84",DATUM["WGS_1984",'
                'SPHEROID["WGS 84",6378137,298.257223563]],'
                'PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
            ),
        },
    )
    ds_out["crs"] = crs_var

    encoding = {
        variable: _encoding_for_variable(ds_out[variable]),
        "time": time_enc,
        "lat": coord_enc,
        "lon": coord_enc,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    ds_out.to_netcdf(output_path, encoding=encoding, format="NETCDF4")
    log.info("  → wrote %s", output_path)


# ---------------------------------------------------------------------------
# Final assembly
# ---------------------------------------------------------------------------


def assemble_output(daily_files: list[Path], output_path: Path, variable: str) -> None:
    """Concatenate sorted daily files into a single CF-compliant NetCDF4."""
    log.info("Assembling %d daily files → %s", len(daily_files), output_path)

    ds = xr.open_mfdataset(
        sorted(daily_files),
        combine="by_coords",
        decode_times=True,
    )

    # Global attributes
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    ds.attrs.update(
        {
            "Conventions": "CF-1.10",
            "title": "SPIReS Historical Snow Cover — Western US",
            "source": "SPIRES_HIST MOD09GA061 V1.0",
            "variable_processed": variable,
            "spatial_extent": (
                f"lon [{WEST_LON}, {EAST_LON}], lat [{SOUTH_LAT}, {NORTH_LAT}]"
            ),
            "grid_resolution_degrees": str(RESOLUTION),
            "date_processed": now,
        }
    )

    encoding: dict = {
        "time": {"units": TIME_UNITS, "calendar": TIME_CALENDAR, "dtype": "int32"},
        "lat": {"dtype": "float64"},
        "lon": {"dtype": "float64"},
        variable: {
            "zlib": True,
            "complevel": ZLIB_LEVEL,
        },
    }

    # Pick up dtype / scale from one of the daily files
    sample_enc = ds[variable].encoding
    if "dtype" in sample_enc:
        encoding[variable]["dtype"] = sample_enc["dtype"]
    if "scale_factor" in sample_enc:
        encoding[variable]["scale_factor"] = sample_enc["scale_factor"]
    if "add_offset" in sample_enc:
        encoding[variable]["add_offset"] = sample_enc["add_offset"]
    if "_FillValue" in sample_enc:
        encoding[variable]["_FillValue"] = sample_enc["_FillValue"]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(output_path, encoding=encoding, format="NETCDF4")
    ds.close()
    log.info("Done → %s", output_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process SPIReS historical snow cover tiles for the western US.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "-i", "--input-dir",
        required=True,
        type=Path,
        help="Directory containing SPIReS_HIST_*.nc tile files.",
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        type=Path,
        help="Path for the final output NetCDF4 file.",
    )
    parser.add_argument(
        "-v", "--variable",
        default="snow_fraction",
        help="Variable name to process (default: snow_fraction).",
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y%m%d").date(),
        metavar="YYYYMMDD",
        help="First date to include (inclusive).",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y%m%d").date(),
        metavar="YYYYMMDD",
        help="Last date to include (inclusive).",
    )
    parser.add_argument(
        "--temp-dir",
        type=Path,
        default=None,
        help="Directory for temporary daily files (default: system temp).",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Do not delete temporary daily files after assembly.",
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Suppress INFO messages.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    # --- Discover and filter files ------------------------------------------
    groups = _group_files_by_date(args.input_dir)
    if not groups:
        raise SystemExit(f"No matching files found in {args.input_dir}")

    if args.start_date:
        groups = {d: v for d, v in groups.items() if d >= args.start_date}
    if args.end_date:
        groups = {d: v for d, v in groups.items() if d <= args.end_date}
    if not groups:
        raise SystemExit("No files remain after applying date filter.")

    log.info("Found %d date(s) to process.", len(groups))

    # --- Process each day into a temporary file ------------------------------
    use_tmp_dir = args.temp_dir is None
    tmp_ctx = (
        tempfile.TemporaryDirectory()
        if use_tmp_dir and not args.keep_temp
        else None
    )

    if tmp_ctx is not None:
        tmp_root = Path(tmp_ctx.name)
    elif args.temp_dir is not None:
        tmp_root = args.temp_dir
        tmp_root.mkdir(parents=True, exist_ok=True)
    else:
        tmp_root = args.output.parent / "_spires_daily"
        tmp_root.mkdir(parents=True, exist_ok=True)

    daily_files: list[Path] = []
    for day, paths in sorted(groups.items()):
        daily_nc = tmp_root / f"spires_{args.variable}_{day.strftime('%Y%m%d')}.nc"
        process_day(paths, args.variable, daily_nc)
        daily_files.append(daily_nc)

    # --- Assemble final output -----------------------------------------------
    assemble_output(daily_files, args.output, args.variable)

    if tmp_ctx is not None:
        tmp_ctx.cleanup()


if __name__ == "__main__":
    main()
