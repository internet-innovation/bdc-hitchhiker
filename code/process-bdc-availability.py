import json
import os

from pathlib import Path

import pandas as pd

from utils import AVAILABILITY_DTYPES, RELIABLE_TECHNOLOGY_CODES


def consolidate_and_agument_availability_data(source, destination):
    """First, consolidates availability data across technology files for each
    <as_of_date, state> pair. Second, augments availability data with geoIDs at
    different geographic levels (e.g., states, counties) and with the service
    status (i.e., unserved, underserved, served).

    Args:
        source: Directory where the availability data is stored.

        destination: Directory to save the consolidated and augmented
        availability data.
    """
    # Create destination directory
    os.makedirs(destination, exist_ok=True)
    # Define auxiliary variables
    GEOS = ['state', 'county', 'tract', 'block_group']
    GEOID_LENS = [2, 5, 11, 12]
    # Determine As of Dates in the availability data
    try:
        with open(f'{source}/metadata.json') as f:
            aods_md = json.load(f)
    except FileNotFoundError:
        print("Could not find the As of Dates metadata file.")
        return
    as_of_dates = sorted(aods_md['as_of_dates'])
    # Save availability data metadata to destination directory
    with open(f"{destination}/metadata.json", 'w') as f:
        json.dump(aods_md, f, indent=4)
    # Consolidate data for each as_of_date separately
    for as_of_date in as_of_dates:
        print(f"As of Date: {as_of_date}")
        aod_path = f"{source}/{as_of_date}/"
        aod_save_path = f"{destination}/{as_of_date}/"
        # Create destination directory for As of Date
        os.makedirs(aod_save_path, exist_ok=True)
        # Determine States in the As of Date
        try:
            with open(f"{aod_path}/metadata.json") as f:
                aod_md = json.load(f)
        except FileNotFoundError:
            print("Could not find the metadata file for as_of_date"
                  f"{as_of_date}.")
            return
        states = sorted(aod_md['states'])
        # Save as_of_date metadata to destination directory
        with open(f"{aod_save_path}/metadata.json", "w") as f:
            json.dump(aod_md, f, indent=4)
        # Check and skip in case consolidated files already exist for the
        # as_of_date
        skip = False
        for state in states:
            if Path(f"{aod_save_path}/{state}.csv").is_file():
                skip = True
                break
        if skip:
            print("One or more consolidated files already exist. Skipping")
            continue
        # Consolidate data for each pair <as_of_date, state> in a separate file
        for state_id in states:
            print(f"    State: {state_id}")
            state_path = f"{aod_path}/{state_id}/"
            state_save_fn = f"{aod_save_path}/{state_id}.csv"
            # Determine technology files in the state
            try:
                with open(f"{state_path}/metadata.json") as f:
                    smd = json.load(f)
            except FileNotFoundError:
                print(f"Could not find the metadata file for {state_id}.")
                return
            files = smd['files']
            # Consolidate and augment data on a per-file basis
            for fmd in files:
                print(end=f"        File: {fmd['file_name']}", flush=True)
                file_path = f"{state_path}/{fmd['file_name']}.zip"
                # Load dataframe from file
                file_df = pd.read_csv(file_path, dtype=AVAILABILITY_DTYPES)
                print(end=".", flush=True)
                # Determine GeoIDs at different geographic levels
                for geo, geoid_len in zip(GEOS, GEOID_LENS):
                    if geo not in ["block_geoid"]:
                        file_df[f"{geo}_geoid"] = file_df['block_geoid'].apply(
                            lambda x: str(x)[:geoid_len] if x else x
                        )
                print(end=".", flush=True)
                # Determine the service statuses of each availability record
                unserved_index = \
                    (~file_df.technology.isin(RELIABLE_TECHNOLOGY_CODES)) | \
                    (file_df.business_residential_code.isin(['B'])) | \
                    (file_df.max_advertised_download_speed < 25) | \
                    (file_df.max_advertised_upload_speed < 3) | \
                    (file_df.low_latency == 0)
                underserved_index = \
                    ~unserved_index & (
                        (file_df.max_advertised_download_speed < 100) |
                        (file_df.max_advertised_upload_speed < 20) |
                        (file_df.low_latency == 0)
                    )
                served_index = ~unserved_index & ~underserved_index
                file_df.loc[unserved_index, 'status'] = 2  # Unserved
                file_df.loc[underserved_index, 'status'] = 1  # Underserved
                file_df.loc[served_index, 'status'] = 0  # Served
                print(end=".", flush=True)
                # Write (partial) augmented data to file
                file_df.to_csv(
                    state_save_fn,
                    index=False,
                    mode='a',
                    header=not Path(state_save_fn).is_file(),
                )
                print("done")
                # break
            # break
        # break


if __name__ == "__main__":
    source = "data/raw/bdc/availability/fixed/"
    destination = "data/processed/bdc/availability/fixed/"

    consolidate_and_agument_availability_data(
        source=source,
        destination=destination,
    )
