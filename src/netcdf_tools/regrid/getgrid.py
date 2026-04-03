#!/usr/bin/env python3
"""
nct-getgrid — Extract a regular grid specification from a NetCDF file.

Runs nct-inspect on the file so the user can identify the relevant variables,
then reads the specified lat/lon center variables and computes the bounding box
and resolution. Writes a compact JSON grid-spec that can be committed alongside
a project and loaded by nct-regrid.

Usage:
    nct-getgrid <input_file> <output_file>

Example:
    nct-getgrid domain_WUS_0125.nc grids/WUS_0125.json
"""

import json
import sys
from pathlib import Path

import netCDF4 as nc
import numpy as np

from netcdf_tools.inspect import inspect_netcdf


def _check_uniform(arr, name, tol=1e-6):
    """Raise if spacing in arr is not uniform. Returns the spacing."""
    diffs = np.diff(arr)
    if np.max(diffs) - np.min(diffs) > tol:
        raise ValueError(
            f"'{name}' spacing is not uniform "
            f"(min={np.min(diffs):.6g}, max={np.max(diffs):.6g}). "
            "This script only supports regular grids."
        )
    return float(np.round(np.mean(diffs), 10))


def getgrid(input_file, output_file):
    # Show full inspection so the user can identify variable names
    print("\n" + "=" * 60)
    print("FILE METADATA")
    print("=" * 60)
    inspect_netcdf(input_file)

    print("\n" + "=" * 60)
    print("VARIABLE SELECTION")
    print("=" * 60)
    lat_var = input("  Enter latitude center variable name:  ").strip()
    lon_var = input("  Enter longitude center variable name: ").strip()

    ds = nc.Dataset(input_file, "r")

    for var in (lat_var, lon_var):
        if var not in ds.variables:
            raise SystemExit(f"Error: variable '{var}' not found in {input_file}")

    lat_data = np.array(ds.variables[lat_var][:], dtype=np.float64)
    lon_data = np.array(ds.variables[lon_var][:], dtype=np.float64)
    ds.close()

    # Collapse 2-D arrays that are rectilinear to 1-D
    if lat_data.ndim == 2:
        lat_data = lat_data[:, 0]
    if lon_data.ndim == 2:
        lon_data = lon_data[0, :]

    if lat_data.ndim != 1 or lon_data.ndim != 1:
        raise SystemExit("Error: lat/lon variables must be 1-D or 2-D rectilinear.")

    lat_res = _check_uniform(lat_data, lat_var)
    lon_res = _check_uniform(lon_data, lon_var)

    if abs(lat_res - lon_res) > 1e-6:
        raise SystemExit(
            f"Error: lat and lon resolutions differ "
            f"(lat={lat_res:.6g}°, lon={lon_res:.6g}°). "
            "Only square-cell grids are supported."
        )

    resolution = round((lat_res + lon_res) / 2, 10)
    half = resolution / 2

    spec = {
        "source_file": str(Path(input_file).name),
        "grid_type": "regular",
        "south": round(float(lat_data[0])  - half, 6),
        "north": round(float(lat_data[-1]) + half, 6),
        "west":  round(float(lon_data[0])  - half, 6),
        "east":  round(float(lon_data[-1]) + half, 6),
        "resolution": resolution,
    }

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    Path(output_file).write_text(json.dumps(spec, indent=2) + "\n")

    print("\n" + "=" * 60)
    print("GRID SPEC")
    print("=" * 60)
    print(json.dumps(spec, indent=2))
    print(f"\nWritten to: {output_file}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Extract a regular grid specification from a NetCDF file. "
            "Runs nct-inspect first so you can confirm variable names, "
            "then writes a JSON grid-spec from the specified lat/lon variables."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("input_file",  help="Source NetCDF file")
    parser.add_argument("output_file", help="Output JSON file (e.g. grids/WUS_0125.json)")
    args = parser.parse_args()

    if not Path(args.input_file).exists():
        print(f"Error: File not found: {args.input_file}")
        sys.exit(1)

    getgrid(args.input_file, args.output_file)


if __name__ == "__main__":
    main()
