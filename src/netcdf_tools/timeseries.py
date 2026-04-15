#!/usr/bin/env python3
"""
Extract a time series from a directory of netCDF files at a given lat/lon point.
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import netCDF4 as nc


LAT_ALIASES = {"lat", "latitude", "Latitude", "LAT", "y"}
LON_ALIASES = {"lon", "longitude", "Longitude", "LON", "x"}


def find_nearest_index(array, value):
    """Return the index of the element in array closest to value."""
    return int(np.argmin(np.abs(array - value)))


def normalize_lon(lons, target):
    """
    Return lons and target both in the same 0-360 or -180-180 convention
    so that find_nearest_index works regardless of how the file stores longitude.
    """
    lons = np.asarray(lons, dtype=float)
    if lons.max() > 180:
        if target < 0:
            target = target + 360.0
    else:
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


def extract_from_file(filepath, variables, lat_idx, lon_idx, lat_dim, lon_dim):
    """
    Open a single netCDF file and return (dates, values_dict) at the given grid indices.

    Supports dimension orders: (time, lat, lon) and (time, lon, lat).

    Parameters
    ----------
    variables : list of str
        Variable names to extract.

    Returns
    -------
    dates : pd.DatetimeIndex
    values_dict : dict mapping variable name -> np.ndarray
    """
    with nc.Dataset(filepath, 'r') as ds:
        ds.set_auto_maskandscale(False)

        # Decode time once per file
        # Find the time dimension from the first variable
        first_var = ds.variables.get(variables[0])
        if first_var is None:
            raise KeyError(f"Variable '{variables[0]}' not found in {filepath}. "
                           f"Available: {list(ds.variables.keys())}")
        dims = first_var.dimensions
        time_dim = next((d for d in dims if d not in (lat_dim, lon_dim)), None)
        if time_dim is None:
            raise ValueError(f"Cannot identify time dimension in {dims}")

        time_var = ds.variables[time_dim]
        dates = decode_time(time_var)

        values_dict = {}
        for variable in variables:
            if variable not in ds.variables:
                raise KeyError(f"Variable '{variable}' not found in {filepath}. "
                               f"Available: {list(ds.variables.keys())}")

            var = ds.variables[variable]
            dim_order = list(var.dimensions)
            indexer = [slice(None)] * len(dim_order)
            indexer[dim_order.index(lat_dim)] = lat_idx
            indexer[dim_order.index(lon_dim)] = lon_idx
            raw = var[tuple(indexer)]

            scale  = getattr(var, 'scale_factor', None)
            offset = getattr(var, 'add_offset',   None)
            values = np.asarray(raw, dtype=float)
            if scale  is not None: values = values * float(scale)
            if offset is not None: values = values + float(offset)

            fill = getattr(var, '_FillValue', None)
            if fill is not None:
                values[raw == fill] = np.nan
            values = np.where(np.isfinite(values), values, np.nan)

            values_dict[variable] = np.asarray(values, dtype=float)

        return dates, values_dict


def load_timeseries(directory, variables, latitude, longitude,
                    lat_name=None, lon_name=None):
    """
    Extract a time series from all netCDF files in a directory at the nearest
    grid point to (latitude, longitude).

    Parameters
    ----------
    directory : str or Path
        Directory containing netCDF files.
    variables : str or list of str
        Name(s) of the data variable(s) to extract.
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
        DataFrame with a DatetimeIndex named 'date' and one column per variable.
        Column names are '<variable> (<units>)' if units are present,
        otherwise '<variable>'.
    """
    if isinstance(variables, str):
        variables = [variables]

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

        units = {
            v: getattr(ds.variables.get(v), 'units', None)
            for v in variables
        }

    lons, target_lon = normalize_lon(lons, longitude)
    lat_idx = find_nearest_index(lats, latitude)
    lon_idx = find_nearest_index(lons, target_lon)

    all_dates = []
    all_values = {v: [] for v in variables}

    for filepath in nc_files:
        dates, values_dict = extract_from_file(
            filepath, variables, lat_idx, lon_idx, lat_dim, lon_dim)
        all_dates.extend(dates)
        for v in variables:
            all_values[v].extend(values_dict[v])

    col_names = {
        v: (f"{v} ({units[v]})" if units[v] else v)
        for v in variables
    }
    df = pd.DataFrame(
        {col_names[v]: all_values[v] for v in variables},
        index=pd.DatetimeIndex(all_dates, name='date'),
    )
    df.sort_index(inplace=True)
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Extract a lat/lon time series from a directory of netCDF files."
    )
    parser.add_argument("directory", help="Directory containing netCDF files")
    parser.add_argument("--variable", required=True, nargs='+',
                        help="Name(s) of the data variable(s) to extract (space-separated)")
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
        lat_name=args.lat_name, lon_name=args.lon_name,
    )

    var_stem = "_".join(args.variable)
    output_path = Path(args.output) if args.output else Path(f"{var_stem}_timeseries.csv")
    df_out = df.reset_index()
    df_out['date'] = df_out['date'].dt.strftime('%Y-%m-%d')
    df_out.to_csv(output_path, index=False)
    print(f"\nTime series written to: {output_path}  ({len(df_out)} rows)")


if __name__ == "__main__":
    main()
