# netcdf_tools
A bunch of tools for working with netcdf_files

CLI Use:
"nct-inspect /path/to/netcdf"

Recommendation: One File Per Date, Merge After
This is the right call for cluster computing, and here's why it fits your architecture:

The natural parallelization unit is one date. Each date's job is independent: load tiles → mosaic → reproject → regrid → write. Submit N jobs to the scheduler (SLURM/PBS), one per date or one per month-chunk, and merge at the end with xr.open_mfdataset (lazy, no memory issue) or ncrcat.

Concretely, the package should work like this:


regrid.py main()
  └── for each date in range:
        load_data(input_dir, date)   ← loads ONE day
        regrid(ds_in, ds_out_grid)
        write to spires_YYYYMMDD.nc

# separately, after all jobs finish:
xr.open_mfdataset("spires_*.nc") → combined output

What needs to change in the package:

load_spires() in regrid/spires.py currently loads all dates — it needs a single-date mode (the _load_day() function already exists and does the right thing, it just isn't exposed as the primary path)
regrid() in regrid/regrid.py needs a date loop (or be called in a loop by a driver script that writes per-date files)
Add an assemble step — the assemble_output() function in process_spires.py is already a working template
For the cluster specifically: keep the regrid function signature taking a single date, then your job submission script becomes trivially parallelizable:


# SLURM array job — one task per date
python -m netcdf_tools.regrid.regrid --date $DATE --input $DIR --output daily/
Then one final merge job runs after all array tasks complete.

Summary
Concern	One file/date	All-at-once
Memory	Constant (1 day)	Grows with range
Cluster parallelism	Embarrassingly parallel	Sequential
Fault tolerance	Restart one day	Restart everything
Final merge	open_mfdataset (lazy)	Already done
The utility script already validated this pattern. The main architectural work is aligning the package's regrid/ modules to match it — specifically making load_spires() operate on one date and adding a merge step to the CLI.