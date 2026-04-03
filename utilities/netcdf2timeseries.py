#!/usr/bin/env python3
"""
Extract a time series from a directory of netCDF files at a given lat/lon point.

Usage:
    python netcdf2timeseries.py <directory> --variable VAR --latitude LAT --longitude LON
    python netcdf2timeseries.py <directory> --variable VAR --latitude LAT --longitude LON --output out.csv
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import netCDF4 as nc


def find_nearest_index(array, value):
    """Return the index of the element in array closest to value."""
    return int(np.argmin(np.abs(array - value)))


def normalize_lon(lons, target):
    """
    Return a copy of lons and target_lon both in the same 0-360 or -180-180 convention
    so that find_nearest_index works regardless of how the file stores longitude.
    """
    lons = np.asarray(lons, dtype=float)
    # Detect convention from the data
    if lons.max() > 180:
        # Data is 0-360; convert target if needed
        if target < 0:
            target = target + 360.0
    else:
        # Data is -180-180; convert target if needed
        if target > 180:
            target = target - 360.0
    return lons, target


def decode_time(time_var):
    """Decode a netCDF time variable to an array of datetime objects."""
    try:
        import cftime
        times = nc.num2date(time_var[:], units=time_var.units,
                            calendar=getattr(time_var, 'calendar', 'standard'))
        return pd.to_datetime([t.isoformat() for t in times])
    except Exception as e:
        raise RuntimeError(f"Could not decode time variable: {e}")


def extract_from_file(filepath, variable, lat_idx, lon_idx, lat_dim, lon_dim):
    """
    Open a single netCDF file and return (dates, values) at the given grid indices.

    Supports dimension orders: (time, lat, lon) and (time, lon, lat).
    """
    with nc.Dataset(filepath, 'r') as ds:
        if variable not in ds.variables:
            raise KeyError(f"Variable '{variable}' not found in {filepath}. "
                           f"Available: {list(ds.variables.keys())}")

        var = ds.variables[variable]
        dims = var.dimensions

        # Locate time dimension
        time_dim = next((d for d in dims if d not in (lat_dim, lon_dim)), None)
        if time_dim is None:
            raise ValueError(f"Cannot identify time dimension in {dims}")

        time_var = ds.variables[time_dim]
        dates = decode_time(time_var)

        # Build indexer based on dimension order
        dim_order = list(dims)
        indexer = [slice(None)] * len(dim_order)
        indexer[dim_order.index(lat_dim)] = lat_idx
        indexer[dim_order.index(lon_dim)] = lon_idx
        # Use set_auto_maskandscale so we control masking ourselves,
        # avoiding fill_value dtype-mismatch errors in some netCDF4 versions.
        ds.set_auto_maskandscale(False)
        raw = var[tuple(indexer)]

        # Apply scale/offset if present
        scale  = getattr(var, 'scale_factor', None)
        offset = getattr(var, 'add_offset',   None)
        values = np.asarray(raw, dtype=float)
        if scale  is not None: values = values * float(scale)
        if offset is not None: values = values + float(offset)

        # Mask fill values
        fill = getattr(var, '_FillValue', None)
        if fill is not None:
            values[raw == fill] = np.nan
        # Also mask any surviving ±inf
        values = np.where(np.isfinite(values), values, np.nan)

        return dates, np.asarray(values, dtype=float)


LAT_ALIASES = {"lat", "latitude", "Latitude", "LAT", "y"}
LON_ALIASES = {"lon", "longitude", "Longitude", "LON", "x"}


def load_timeseries(directory, variable, latitude, longitude,
                    lat_name=None, lon_name=None):
    """
    Extract a time series from all netCDF files in a directory at the nearest
    grid point to (latitude, longitude).

    Parameters
    ----------
    directory : str or Path
        Directory containing netCDF files.
    variable : str
        Name of the data variable to extract.
    latitude : float
        Target latitude.
    longitude : float
        Target longitude.
    lat_name : str, optional
        Latitude dimension name (auto-detected if omitted).
    lon_name : str, optional
        Longitude dimension name (auto-detected if omitted).

    Returns
    -------
    pd.DataFrame
        Single-column DataFrame with a DatetimeIndex named 'date'.
        Column is named '<variable> (<units>)' if units are present,
        otherwise '<variable>'.
    """
    directory = Path(directory)
    nc_files = sorted(directory.glob("*.nc"))
    if not nc_files:
        nc_files = sorted(directory.glob("*.nc4"))
    if not nc_files:
        raise FileNotFoundError(f"No netCDF files found in '{directory}'.")

    with nc.Dataset(nc_files[0], 'r') as ds:
        var_names = set(ds.variables.keys())

        lat_dim = lat_name or next((n for n in LAT_ALIASES if n in var_names), None)
        lon_dim = lon_name or next((n for n in LON_ALIASES if n in var_names), None)

        if lat_dim is None or lon_dim is None:
            raise ValueError(
                f"Could not auto-detect lat/lon variables in {nc_files[0]}. "
                f"Available: {list(var_names)}. Use lat_name/lon_name to specify."
            )

        lats = ds.variables[lat_dim][:]
        lons = ds.variables[lon_dim][:]
        units = getattr(ds.variables.get(variable), 'units', None)

    lons, target_lon = normalize_lon(lons, longitude)
    lat_idx = find_nearest_index(lats, latitude)
    lon_idx = find_nearest_index(lons, target_lon)

    all_dates, all_values = [], []
    for filepath in nc_files:
        dates, values = extract_from_file(
            filepath, variable, lat_idx, lon_idx, lat_dim, lon_dim)
        all_dates.extend(dates)
        all_values.extend(values)

    col_name = f"{variable} ({units})" if units else variable
    df = pd.DataFrame(
        {col_name: all_values},
        index=pd.DatetimeIndex(all_dates, name='date')
    )
    df.sort_index(inplace=True)
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Extract a lat/lon time series from a directory of netCDF files."
    )
    parser.add_argument("directory", help="Directory containing netCDF files")
    parser.add_argument("--variable", required=True, help="Name of the data variable")
    parser.add_argument("--latitude", type=float, required=True, help="Target latitude")
    parser.add_argument("--longitude", type=float, required=True, help="Target longitude")
    parser.add_argument("--output", default=None,
                        help="Output CSV file path (default: <variable>_timeseries.csv)")
    parser.add_argument("--lat-name", default=None,
                        help="Latitude dimension/variable name (auto-detected if omitted)")
    parser.add_argument("--lon-name", default=None,
                        help="Longitude dimension/variable name (auto-detected if omitted)")
    args = parser.parse_args()

    directory = Path(args.directory)
    if not directory.is_dir():
        sys.exit(f"Error: '{directory}' is not a directory.")

    df = load_timeseries(
        args.directory, args.variable, args.latitude, args.longitude,
        lat_name=args.lat_name, lon_name=args.lon_name
    )

    output_path = Path(args.output) if args.output else Path(f"{args.variable}_timeseries.csv")
    df_out = df.reset_index()
    df_out['date'] = df_out['date'].dt.strftime('%Y-%m-%d')
    df_out.to_csv(output_path, index=False)
    print(f"\nTime series written to: {output_path}  ({len(df_out)} rows)")


if __name__ == "__main__":
    main()
