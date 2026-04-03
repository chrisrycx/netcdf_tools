#!/bin/bash
# ============================================================
# NSIDC Earthdata Programmatic Download Script
# Downloads netCDF files for a range of years via wget.
#
# PREREQUISITES — one-time setup before running this script:
# ============================================================
#
# 1. Create a NASA Earthdata Login account at:
#    https://urs.earthdata.nasa.gov
#
# 2. Add your credentials to ~/.netrc (create the file if needed):
#
#      echo 'machine urs.earthdata.nasa.gov login <uid> password <password>' >> ~/.netrc
#      chmod 0600 ~/.netrc
#
#    Replace <uid> and <password> with your Earthdata username and password.
#    The chmod command restricts access to your account only (required).
#
# 3. Create an empty cookies file (required by wget for session handling):
#
#      touch ~/.urs_cookies
#
# ============================================================
# CONFIGURATION — edit these variables before running:
# ============================================================

# Base URL of the dataset directory on NSIDC (no trailing slash).
# This is the URL of the folder containing the files you want to download.
# Example: https://daacdata.apps.nsidc.org/pub/DATASETS/nsidc0719_GOES_SWE_v1/data
BASE_URL="https://daacdata.apps.nsidc.org/pub/DATASETS/nsidc0719_SWE_Snow_Depth_v1/"

# File name pattern — use {YEAR} as a placeholder for the year.
# The script substitutes the actual year for {YEAR} on each iteration.
# Using {YEAR} avoids ambiguity when "Y" or "YY" appear adjacent to the
# year digits in the filename (e.g. a "WY" water-year prefix like "WYYYYY"
# would incorrectly match a bare YYYY placeholder).
# Examples:
#   "MOD10A1_{YEAR}_v2.nc"
#   "4km_SWE_Depth_WY{YEAR}_v01.nc"
FILE_PATTERN="4km_SWE_Depth_WY{YEAR}_v01.nc"

# Year range to download (inclusive)
START_YEAR=2001
END_YEAR=2023

# Directory to save downloaded files (will be created if it doesn't exist)
OUTPUT_DIR="/global/cfs/cdirs/m4986/data/snow/UA/"

# ============================================================
# Script — no edits needed below this line
# ============================================================

COOKIES_FILE=~/.urs_cookies

mkdir -p "$OUTPUT_DIR"

echo "Downloading years ${START_YEAR}-${END_YEAR} from:"
echo "  ${BASE_URL}"
echo "Saving to: ${OUTPUT_DIR}"
echo ""

for YEAR in $(seq "$START_YEAR" "$END_YEAR"); do
    PATTERN="${FILE_PATTERN//\{YEAR\}/$YEAR}"
    URL="${BASE_URL}/${PATTERN}"

    echo "Fetching year ${YEAR}: ${URL}"

    wget \
        --load-cookies "$COOKIES_FILE" \
        --save-cookies "$COOKIES_FILE" \
        --keep-session-cookies \
        --auth-no-challenge=on \
        --no-check-certificate \
        -e robots=off \
        -np \
        --content-disposition \
        -P "$OUTPUT_DIR" \
        "$URL"

    if [ $? -eq 0 ]; then
        echo "  -> OK"
    else
        echo "  -> FAILED (year ${YEAR})"
    fi
done

echo ""
echo "Done."
