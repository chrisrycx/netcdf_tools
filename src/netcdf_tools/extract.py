#!/usr/bin/env python3
"""
NetCDF Variable Extractor
Extracts specific variables from a NetCDF file and creates a new file with only those variables.
"""

import netCDF4 as nc
import sys
from pathlib import Path


def extract_variables(input_file, output_file, variables_list):
    """
    Extract specific variables from a NetCDF file.

    Parameters
    ----------
    input_file : str
        Path to input NetCDF file.
    output_file : str
        Path to output NetCDF file.
    variables_list : list of str
        Variable names to extract.

    Returns
    -------
    bool
        True on success, False on failure.
    """
    try:
        src = nc.Dataset(input_file, 'r')

        print(f"Reading from: {input_file}")
        print(f"Available variables: {list(src.variables.keys())}")

        missing_vars = [var for var in variables_list if var not in src.variables]
        if missing_vars:
            print(f"Error: Variables not found: {missing_vars}")
            src.close()
            return False

        dst = nc.Dataset(output_file, 'w', format=src.file_format)

        print(f"Extracting variables: {variables_list}")

        for attr in src.ncattrs():
            setattr(dst, attr, getattr(src, attr))

        dims_needed = set()
        for var_name in variables_list:
            var = src.variables[var_name]
            dims_needed.update(var.dimensions)

        print(f"Dimensions needed: {list(dims_needed)}")

        for dim_name in dims_needed:
            dim = src.dimensions[dim_name]
            size = len(dim) if not dim.isunlimited() else None
            dst.createDimension(dim_name, size)

        coord_vars = [var for var in dims_needed if var in src.variables]
        all_vars_to_copy = list(set(variables_list + coord_vars))

        print(f"Copying variables (including coordinates): {all_vars_to_copy}")

        for var_name in all_vars_to_copy:
            src_var = src.variables[var_name]

            fill_value = None
            if hasattr(src_var, '_FillValue'):
                fill_value = src_var._FillValue

            dst_var = dst.createVariable(
                var_name,
                src_var.dtype,
                src_var.dimensions,
                zlib=True,
                complevel=4,
                fill_value=fill_value,
            )

            for attr in src_var.ncattrs():
                if attr != '_FillValue':
                    setattr(dst_var, attr, getattr(src_var, attr))

            dst_var[:] = src_var[:]
            print(f"  Copied {var_name}: {src_var.shape}")

        src.close()
        dst.close()

        input_size = Path(input_file).stat().st_size / (1024 * 1024)
        output_size = Path(output_file).stat().st_size / (1024 * 1024)

        print(f"\nSuccess!")
        print(f"Input file size:  {input_size:.1f} MB")
        print(f"Output file size: {output_size:.1f} MB")

        return True

    except Exception as e:
        print(f"Error: {e}")
        return False


def main():
    if len(sys.argv) < 4:
        print("Usage: nct-extract <input_file> <output_file> <variable1> [variable2] ...")
        print("")
        print("Examples:")
        print("  nct-extract data.nc subset.nc temperature")
        print("  nct-extract data.nc subset.nc temperature precipitation wind_speed")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    variables_list = sys.argv[3:]

    if not Path(input_file).exists():
        print(f"Error: Input file '{input_file}' not found")
        sys.exit(1)

    if Path(output_file).exists():
        response = input(f"Output file '{output_file}' already exists. Overwrite? (y/N): ")
        if response.lower() != 'y':
            print("Operation cancelled")
            sys.exit(0)

    success = extract_variables(input_file, output_file, variables_list)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
