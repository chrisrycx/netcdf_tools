#!/usr/bin/env python3
"""
NetCDF File Inspector
Explores the structure, dimensions, variables, and attributes of a NetCDF file
"""

import netCDF4 as nc
import numpy as np
import sys
from pathlib import Path

def inspect_netcdf(file_path):
    """
    Comprehensive inspection of a NetCDF file
    """
    try:
        # Open the NetCDF file
        dataset = nc.Dataset(file_path, 'r')
        
        print("="*60)
        print(f"NETCDF FILE INSPECTION: {Path(file_path).name}")
        print("="*60)
        
        # File format and basic info
        print(f"\nFile Format: {dataset.file_format}")
        print(f"Data Model: {dataset.data_model}")
        
        # Global attributes
        print(f"\n{'GLOBAL ATTRIBUTES':-<40}")
        if dataset.ncattrs():
            for attr in dataset.ncattrs():
                print(f"  {attr}: {getattr(dataset, attr)}")
        else:
            print("  No global attributes found")
        
        # Dimensions
        print(f"\n{'DIMENSIONS':-<40}")
        print(f"{'Name':<20} {'Size':<10} {'Unlimited'}")
        print("-" * 40)
        for dim_name, dim in dataset.dimensions.items():
            unlimited = "Yes" if dim.isunlimited() else "No"
            print(f"{dim_name:<20} {len(dim):<10} {unlimited}")
        
        # Variables
        print(f"\n{'VARIABLES':-<40}")
        for var_name, var in dataset.variables.items():
            print(f"\nVariable: {var_name}")
            print(f"  Dimensions: {var.dimensions}")
            print(f"  Shape: {var.shape}")
            print(f"  Data type: {var.dtype}")
            
            # Variable attributes
            if var.ncattrs():
                print("  Attributes:")
                for attr in var.ncattrs():
                    attr_val = getattr(var, attr)
                    # Truncate long attribute values
                    if isinstance(attr_val, str) and len(attr_val) > 100:
                        attr_val = attr_val[:97] + "..."
                    print(f"    {attr}: {attr_val}")
            
            # Show data range for numeric variables (first 1000 values for efficiency)
            if var.size > 0 and np.issubdtype(var.dtype, np.number):
                try:
                    if var.size <= 1000:
                        data_sample = var[:]
                    else:
                        # Sample data efficiently for large arrays
                        if len(var.shape) == 1:
                            data_sample = var[:1000]
                        else:
                            # For multi-dimensional arrays, take a slice
                            slices = tuple(slice(0, min(10, s)) for s in var.shape)
                            data_sample = var[slices]
                    
                    # Remove masked values if present
                    if hasattr(data_sample, 'mask'):
                        data_sample = data_sample[~data_sample.mask]
                    
                    if len(data_sample) > 0:
                        print(f"  Data range: {np.min(data_sample):.6g} to {np.max(data_sample):.6g}")
                        print(f"  Sample values: {data_sample.flat[:5]}")
                except:
                    print("  Data range: Unable to compute (data access error)")
        
        # Groups (if any)
        if dataset.groups:
            print(f"\n{'GROUPS':-<40}")
            for group_name, group in dataset.groups.items():
                print(f"Group: {group_name}")
                print(f"  Variables: {list(group.variables.keys())}")
                print(f"  Dimensions: {list(group.dimensions.keys())}")
        
        # Summary
        print(f"\n{'SUMMARY':-<40}")
        print(f"Total dimensions: {len(dataset.dimensions)}")
        print(f"Total variables: {len(dataset.variables)}")
        print(f"Total groups: {len(dataset.groups)}")
        
        # Coordinate variables (typically have same name as dimension)
        coord_vars = [var for var in dataset.variables 
                     if var in dataset.dimensions]
        if coord_vars:
            print(f"Coordinate variables: {', '.join(coord_vars)}")
        
        # Data variables (non-coordinate variables)
        data_vars = [var for var in dataset.variables 
                    if var not in dataset.dimensions]
        if data_vars:
            print(f"Data variables: {', '.join(data_vars)}")
        
        dataset.close()
        
    except Exception as e:
        print(f"Error reading NetCDF file: {e}")
        return False
    
    return True

def main():
    """
    Main function to run the NetCDF inspector
    """
    if len(sys.argv) != 2:
        print("Usage: python netcdf_inspector.py <netcdf_file>")
        print("Example: python netcdf_inspector.py data.nc")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    if not Path(file_path).exists():
        print(f"Error: File '{file_path}' not found")
        sys.exit(1)
    
    success = inspect_netcdf(file_path)
    
    if success:
        print(f"\n{'NEXT STEPS':-<40}")
        print("Based on this inspection, you can now:")
        print("1. Identify which variables you want to extract")
        print("2. Note the dimension names and coordinate variables")
        print("3. Check variable attributes for units, descriptions, etc.")
        print("4. Use the extraction script to subset your data")

if __name__ == "__main__":
    main()