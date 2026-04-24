#!/usr/bin/env python3
"""
Parallel yearly regridding script using multiprocessing with temporary storage.
Processes each date in parallel and merges into a single yearly output file.
"""

import os
import tempfile
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import xarray as xr
from netcdf_tools.regrid.regrid import regrid


def process_single_date(args):
    """
    Process a single date with regridding.

    Parameters
    ----------
    args : tuple
        (date_obj, temp_dir, input_path, grid_json)

    Returns
    -------
    str
        Path to the output file for this date
    """
    date_obj, temp_dir, input_path, grid_json = args

    # Create temporary output file
    output_file = os.path.join(temp_dir, f"regridded_{date_obj.strftime('%Y%m%d')}.nc")

    try:
        regrid(
            input_file=input_path,
            grid_json=grid_json,
            output_file=output_file,
            data_type="spires",
            target_date=date_obj,
        )
        print(f"✓ Completed {date_obj}")
        return output_file
    except Exception as e:
        print(f"✗ Failed {date_obj}: {e}")
        return None


def regrid_yearly_parallel(
    input_path,
    grid_json,
    output_file,
    year,
    max_workers=None
):
    """
    Regrid a full year of data using parallel processing with temporary storage.

    Parameters
    ----------
    input_path : str
        Path to input SPIRES data directory
    grid_json : str
        Path to grid specification JSON file
    output_file : str
        Path for final yearly output file
    year : int
        Year to process
    max_workers : int, optional
        Number of parallel workers (defaults to CPU count)
    """

    # Generate date range for the year
    start_date = date(year, 1, 1)
    end_date = date(year, 12, 31)
    dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    print(f"Processing {len(dates)} dates for year {year}")
    print(f"Using {max_workers or os.cpu_count()} parallel workers")

    # Create temporary directory for intermediate files
    with tempfile.TemporaryDirectory(prefix="regrid_temp_") as temp_dir:
        print(f"Using temporary directory: {temp_dir}")

        # Prepare arguments for each date
        args_list = [
            (date_obj, temp_dir, input_path, grid_json)
            for date_obj in dates
        ]

        # Process dates in parallel
        successful_files = []
        failed_dates = []

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_date = {
                executor.submit(process_single_date, args): args[0]
                for args in args_list
            }

            # Collect results as they complete
            for future in as_completed(future_to_date):
                date_obj = future_to_date[future]
                try:
                    result = future.result()
                    if result:
                        successful_files.append(result)
                    else:
                        failed_dates.append(date_obj)
                except Exception as e:
                    print(f"✗ Exception processing {date_obj}: {e}")
                    failed_dates.append(date_obj)

        # Report results
        print(f"\nProcessing complete:")
        print(f"  Successful: {len(successful_files)}")
        print(f"  Failed: {len(failed_dates)}")

        if failed_dates:
            print(f"  Failed dates: {[d.strftime('%Y-%m-%d') for d in failed_dates[:10]]}...")

        if not successful_files:
            raise RuntimeError("No dates processed successfully!")

        # Merge all successful files into yearly output
        print(f"\nMerging {len(successful_files)} files into {output_file}")

        # Sort files by date to ensure proper temporal ordering
        successful_files.sort()

        # Open and concatenate all files
        ds = xr.open_mfdataset(
            successful_files,
            combine='by_coords',
            coords='minimal',
            compat='override'
        )

        # Save merged dataset
        ds.to_netcdf(output_file)
        ds.close()

        print(f"✓ Yearly file saved to: {output_file}")
        print(f"✓ Temporary files automatically cleaned up")


if __name__ == "__main__":
    # Example usage - modify these paths and parameters for your setup

    # Configuration
    spires_path = "/mnt/c/Users/clmbn/NMT_PhD/data/MODIS/SPIRES/raw"
    grid_json = "./grids/e3sm0125.json"
    output_file = "./yearly_regridded_2025.nc"

    # Process year 2025 with 4 workers (adjust as needed)
    regrid_yearly_parallel(
        input_path=spires_path,
        grid_json=grid_json,
        output_file=output_file,
        year=2025,
        max_workers=1  # Use 4 cores, adjust based on your machine
    )
