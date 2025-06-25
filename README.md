# bdc-hitchhiker

This repository contains sample code for downloading and processing the [FCC Broadband Data Collection datasets](https://broadbandmap.fcc.gov/data-download/nationwide-data).

## Instructions

The code made available in this repository can be used to download the complete set of fixed availability and challenge data from the National Broadband Map portal. Furthermore, it can be used to generate challenge summaries at the Broadband-Serviceable Location (BSL), Census Block, Census Block Group, Census Tract, County, and State (or Territory) levels.

The two main requirements for this code are (1) a Python environment configured with the minimal [requirements](requirements.txt) and (2) an FCC Broadband Map API key. To obtain an API key, first register an FCC account at https://broadbandmap.fcc.gov/login. Once registered and signed in, click on the user email and click on "Manage API Access". In the new page, generate an API access token. Once you have the access token, create a copy of the file `code/sample_credentials.py` named `code/credentials.py`. In `code/credentials.py`, `BDC_USERNAME` should be filled with your FCC account email, while `BDC_HASH_VALUE` should be filled with your token hash value string. Further documentation on the token generation process and available APIs can be found at https://us-fcc.app.box.com/v/bdc-public-data-api-spec.

Once the environment and credentials are configured, the complete data downloading and processing pipeline can be run with the simple terminal command:

```
bash download-and-process-data.sh
```

## Outputs

As a result of running the pipeline, several files will be stored in the `data` directory.

The `data/raw/bdc` directory stores the original files containing availability and challenge records downloaded from the National Broadband Map portal.
For availability, files are organized following a three-level hierarchy of _as of date_, state, and access technology.
For challenge, files are organized in a two-level hierarchy of _as of date_ and state.
All files are stored in their original ZIP format.

The `data/processed/bdc` directory stores files resulting from data processing.
For availability, a single Comma-Separated Value (CSV) file is created for each pair of _as of date_ and state consolidating all of its respective records.
An additional CSV file, `data/processed/bdc/availability/fixed/bsl_geolocation.csv`, is also created pairing each unique Broadband-Serviceable Location (BSL) in the data with its geolocation (represented by Census Block, Census Block Group, Census Tract, County, and State [GEOIDs](https://www.census.gov/programs-surveys/geography/guidance/geo-identifiers.html)).
For challenge, a single CSV file is created consolidating all challenges resolved to date.
In addition to those files, a series of summary CSV files are created for both availability and challenge data in their respective subdirectory.
Finally, merged CSV files joining availability and challenge summaries are created for each summary level.
