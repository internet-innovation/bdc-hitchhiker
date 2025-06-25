import json
import os

from pathlib import Path

import pandas as pd

from utils import AVAILABILITY_DTYPES


def determine_bsl_geolocation_from_availability(source, destination):
    """Determines the geolocation (i.e., block_geoid) of each unique
    Broadband-Serviceable Location (BSL) in the availability data in the source
    directory and saves this information to a file 'bsl_geolocation.csv' in the
    destination directory.

    Args:
        source: Directory where the availability data is stored.

        destination: Directory to save the BSL information.
    """
    # Check and abort in case file exists
    destination_fn = f"{destination}/bsl_geolocation.csv"
    if Path(destination_fn).is_file():
        print("Consolidated file already exists. Nothing to do.")
        return
    # Determine As of Dates in the availability data
    try:
        with open(f'{source}/metadata.json') as f:
            aods_md = json.load(f)
    except FileNotFoundError:
        print("Could not find the As of Dates metadata file.")
        return
    as_of_dates = sorted(aods_md['as_of_dates'])
    # Create a dict that maps all available As of Dates for each State
    states_aods = {}
    # For each as of date
    for as_of_date in as_of_dates:
        # Determine the States with data available for the current As of Date
        aod_path = f"{source}/{as_of_date}/"
        try:
            with open(f"{aod_path}/metadata.json") as f:
                aod_md = json.load(f)
        except FileNotFoundError:
            print("Could not find the metadata file for as of date"
                  f"{as_of_date}.")
            return
        aod_states = aod_md['states']
        # Update the dict for each state to contina the current As of Date
        for state_id in aod_states:
            if state_id not in states_aods:
                states_aods[state_id] = []
            states_aods[state_id].append(as_of_date)
    # Read the availability data for each technology, for each as of date, and
    # for each state and determine the unique BSLs in the state
    GEOS = ['state', 'county', 'tract', 'block_group', 'block']
    BSL_COLS = ['location_id'] + [f"{geo}_geoid" for geo in GEOS]
    # For each state
    state_dfs = []
    for state_id, state_aods in sorted(states_aods.items()):
        print(f"State: {state_id}")
        # For each as of date associated to the state
        aod_dfs = []
        for as_of_date in sorted(state_aods):
            print(end=f"    As of Date: {as_of_date}", flush=True)
            aod_path = f"{source}/{as_of_date}/"
            state_fn = f"{aod_path}/{state_id}.csv"
            # Load availability data for the pair <as_of_date, state>
            aod_df = pd.read_csv(
                state_fn,
                dtype=AVAILABILITY_DTYPES,
                usecols=BSL_COLS,
            )
            print(end=".", flush=True)
            # > Each BSL location_id may appear multiple times for a state,
            # each for a different provider and technology. Drop complete row
            # duplicates.
            aod_df = aod_df.drop_duplicates(subset=BSL_COLS)
            print(end=".", flush=True)
            # > Assert that each location_id only appears once after dropping
            # complete row duplicates.
            assert aod_df['location_id'].is_unique, \
                   "Geospatial data is not consistent within a file."
            print(end=".", flush=True)
            # Add new column with As of Date information
            aod_df['as_of_date'] = as_of_date
            # Add to the list of as of date dataframes
            aod_dfs.append(aod_df)
            print("done", flush=True)
            # break
        # Determine the unique BSLs across all as of dates for the state
        print(end="    Determining unique BSLs across as of dates...",
              flush=True)
        state_df = pd.concat(aod_dfs, ignore_index=True)
        aod_dfs = None  # Help free up memory
        # Keep only the most up-to-date location information. Drop more recent
        # location data that exactly matches older data (first occurence).
        state_df = state_df.drop_duplicates(subset=BSL_COLS, keep='first')
        # Drop older location data for a BSL that does not match the most
        # up-to-date data (last occurence).
        state_df = state_df.drop_duplicates(subset='location_id', keep='last')
        # Add to the list of state dataframes
        state_dfs.append(state_df)
        print("done", flush=True)
        # break
    # Determine the unique BSLs across all states
    print(end="Determining unique BSLs across states...", flush=True)
    bsl_df = pd.concat(state_dfs, ignore_index=True)
    # A BSL may 'move' between (neighbooring) states across As of Dates. We
    # must keep only the most up-to-date location information, whatever the
    # state.
    # > Start by making sure the records are sorted increasing by As of Date
    bsl_df = bsl_df.sort_values(by='as_of_date')
    # > Drop location_id duplicates keeping the last record (latest As of Date)
    bsl_df = bsl_df.drop_duplicates(subset='location_id', keep='last')
    print("done", flush=True)
    # Write consolidate data to file
    print(end="Writing consolidated data to file...", flush=True)
    # > Create destination directory
    os.makedirs(destination, exist_ok=True)
    bsl_df.to_csv(destination_fn, index=False)
    print("done")


if __name__ == "__main__":
    source = "data/processed/bdc/availability/fixed/"
    destination = "data/processed/bdc/availability/fixed/"

    determine_bsl_geolocation_from_availability(source=source,
                                                destination=destination)
