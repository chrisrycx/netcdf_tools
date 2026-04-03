#!/usr/bin/env python3
"""
Conservative Regridding Utility
Regrids variables from an input NetCDF file (or all NetCDF files in a directory)
to the lat/lon grid of a target NetCDF using xesmf.

Install extra dependencies with: pip install netcdf_tools[regrid]
"""

import sys
from pathlib import Path


def regrid(input_file, target_file, output_file, variables=None, method="conservative"):
    """
    Regrid variables from input_file to the grid defined in target_file.

    Parameters
    ----------
    input_file : str or Path
        Path to the input NetCDF file.
    target_file : str or Path
        Path to the NetCDF file whose lat/lon grid defines the output grid.
    output_file : str or Path
        Path to write the regridded NetCDF file.
    variables : list of str, optional
        Variables to regrid. If None, all non-coordinate data variables are regridded.
    method : str
        xesmf regridding method. Default is "conservative".
    """
    try:
        import xarray as xr
        import xesmf as xe
    except ImportError:
        raise ImportError(
            "xesmf is required for regridding. "
            "Install with: pip install netcdf_tools[regrid]\n"
            "Note: xesmf may require conda — see utilities/readme.MD for details."
        )

    print(f"Input file:  {input_file}")
    print(f"Target grid: {target_file}")
    print(f"Output file: {output_file}")
    print(f"Method:      {method}")

    ds_in = xr.open_dataset(input_file)
    ds_target = xr.open_dataset(target_file)

    ds_out = xr.Dataset({
        "lat": ds_target["lat"],
        "lon": ds_target["lon"],
    })

    print(f"\nInput grid:  {len(ds_in.lat)} lat x {len(ds_in.lon)} lon")
    print(f"Output grid: {len(ds_out.lat)} lat x {len(ds_out.lon)} lon")

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

    ds_regridded = regridder(ds_in[regrid_vars])

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
    parser.add_argument("target_file", help="NetCDF file whose lat/lon grid defines the output grid")
    parser.add_argument("output_dir", help="Directory to write regridded output files")
    parser.add_argument(
        "--suffix",
        default="_regridded",
        help="String to append to each input filename (before extension) for the output filename (default: '_regridded')",
    )
    parser.add_argument("--variables", nargs="*", help="Variables to regrid (default: all)")
    args = parser.parse_args()

    input_path = Path(args.input)
    target_file = args.target_file
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        print(f"Error: Input not found: {input_path}")
        sys.exit(1)
    if not Path(target_file).exists():
        print(f"Error: Target file not found: {target_file}")
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
        regrid(input_file, target_file, output_file, args.variables)


if __name__ == "__main__":
    main()
