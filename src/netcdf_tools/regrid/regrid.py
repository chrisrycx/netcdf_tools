#!/usr/bin/env python3
"""
Conservative Regridding Utility
Regrids variables from an input NetCDF file (or all NetCDF files in a directory)
to the lat/lon grid defined by a JSON grid spec using xesmf.

Install extra dependencies with: pip install netcdf_tools[regrid]
"""

import json
import sys
from pathlib import Path

import numpy as np
import xarray as xr


def load_data(input_path, data_type, target_date=None, tiles=None):
    """
    Load input data for regridding, dispatching on data_type.

    Parameters
    ----------
    input_path : str or Path
    data_type : str
        'spires' — load using the SPIReS tile pipeline.
        Any other value returns an empty Dataset (placeholder).
    target_date : datetime.date, optional
        Required for data types that load a single date (e.g. 'spires').
    tiles : list of str, optional
        Tile identifiers required for 'spires' (e.g. ['h09v04', 'h09v05']).

    Returns
    -------
    xarray.Dataset
    """
    if data_type == "spires":
        from netcdf_tools.regrid.spires import load_spires
        if target_date is None:
            raise ValueError("target_date is required for data_type='spires'")
        if tiles is None:
            raise ValueError("tiles is required for data_type='spires'")
        return load_spires(input_path, target_date, tiles)

    import xarray as xr
    return xr.Dataset()


def load_grid_spec(grid_json_path):
    """
    Load a JSON grid spec and return a Dataset with lat/lon center coordinates.

    Parameters
    ----------
    grid_json_path : str or Path
        Path to a JSON file with keys: south, north, west, east, resolution.

    Returns
    -------
    xarray.Dataset
        Dataset with 1-D 'lat' and 'lon' coordinate arrays (cell centers).
    """
    import xarray as xr

    spec = json.loads(Path(grid_json_path).read_text())
    res = spec["resolution"]
    lat_centers = np.arange(spec["south"] + res / 2, spec["north"], res)
    lon_centers = np.arange(spec["west"]  + res / 2, spec["east"],  res)
    return xr.Dataset({"lat": lat_centers, "lon": lon_centers})


def regrid(input_file, grid_json, output_file, data_type=None, variables=None, method="conservative", target_date=None, tiles=None):
    """
    Regrid variables from input_file to the grid defined in grid_json.

    Parameters
    ----------
    input_file : str or Path
        Path to the input data (file or directory, depending on data_type).
    grid_json : str or Path
        Path to the JSON grid-spec file (e.g. grids/e3sm0125.json).
    output_file : str or Path
        Path to write the regridded NetCDF file.
    data_type : str, optional
        Dataset type passed to load_data (e.g. 'spires').
    variables : list of str, optional
        Variables to regrid. If None, all non-coordinate data variables are regridded.
    method : str
        xesmf regridding method. Default is "conservative".
    target_date : datetime.date, optional
        Single date to load; required for 'spires'.
    tiles : list of str, optional
        Tile identifiers to load; required for 'spires' (e.g. ['h09v04', 'h09v05']).
    """
    try:
        import xesmf as xe
    except ImportError:
        raise ImportError(
            "xesmf is required for regridding. "
            "Install with: pip install netcdf_tools[regrid]\n"
            "Note: xesmf may require conda — see utilities/readme.MD for details."
        )

    print(f"Input file:  {input_file}")
    print(f"Target grid: {grid_json}")
    print(f"Output file: {output_file}")
    print(f"Method:      {method}")

    ds_in = load_data(input_file, data_type, target_date=target_date, tiles=tiles)
    ds_out = load_grid_spec(grid_json)

    print(f"\nInput grid:  {len(ds_in.lat)} lat x {len(ds_in.lon)} lon")
    print(f"Output grid: {len(ds_out['lat'])} lat x {len(ds_out['lon'])} lon")

    if variables is None:
        variables = list(ds_in.data_vars)

    skip_vars = [v for v in variables if v in ("crs", "time_str")]
    regrid_vars = [
        v for v in variables
        if v not in skip_vars
        and set(ds_in[v].dims).issuperset({"lat", "lon"})
    ]
    passthrough_vars = [v for v in variables if v not in skip_vars and v not in regrid_vars]

    print(f"Variables to regrid:      {regrid_vars}")
    print(f"Variables passed through: {passthrough_vars}")
    print(f"Variables skipped:        {skip_vars}")

    regridder = xe.Regridder(ds_in, ds_out, method)

    ds_regridded: xr.Dataset = regridder(ds_in[regrid_vars])  # type: ignore[assignment]

    for v in regrid_vars:
        ds_regridded[v].attrs = ds_in[v].attrs

    for v in passthrough_vars:
        ds_regridded[v] = ds_in[v]

    for v in skip_vars:
        ds_regridded[v] = ds_in[v]

    ds_regridded.attrs = ds_in.attrs
    ds_regridded.attrs["regrid_method"] = f"xesmf {method}"
    ds_regridded.attrs["input"] = str(Path(input_file).name)

    ds_regridded.to_netcdf(output_file)
    print(f"\nDone. Written to {output_file}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Conservative regridding utility. Input can be a single file or a directory of NetCDF files."
    )
    parser.add_argument("input", help="Input NetCDF file or directory of NetCDF files")
    parser.add_argument("grid_json", help="JSON grid-spec file (e.g. grids/e3sm0125.json)")
    parser.add_argument("output_dir", help="Directory to write regridded output files")
    parser.add_argument(
        "--suffix",
        default="_regridded",
        help="String to append to each input filename (before extension) for the output filename (default: '_regridded')",
    )
    parser.add_argument("--variables", nargs="*", help="Variables to regrid (default: all)")
    parser.add_argument("--data-type", default=None, help="Dataset type for loading (e.g. 'spires')")
    args = parser.parse_args()

    input_path = Path(args.input)
    grid_json = Path(args.grid_json)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        print(f"Error: Input not found: {input_path}")
        sys.exit(1)
    if not grid_json.exists():
        print(f"Error: Grid spec not found: {grid_json}")
        sys.exit(1)

    if input_path.is_dir():
        input_files = sorted(input_path.glob("*.nc"))
        if not input_files:
            print(f"Error: No .nc files found in {input_path}")
            sys.exit(1)
    else:
        input_files = [input_path]

    output_dir.mkdir(parents=True, exist_ok=True)

    for input_file in input_files:
        output_file = output_dir / (input_file.stem + args.suffix + input_file.suffix)
        if output_file.exists():
            response = input(f"Output file '{output_file}' already exists. Overwrite? (y/N): ")
            if response.lower() != "y":
                print(f"Skipping {input_file.name}")
                continue
        regrid(input_file, grid_json, output_file, args.data_type, args.variables)


if __name__ == "__main__":
    main()
