import json
import os

from pathlib import Path

import pandas as pd

from utils import AVAILABILITY_DTYPES


def extract_challenging_bsl_availability(availability_source,
                                         challenge_source_fn,
                                         destination):
    """Extracts availability records for all BSLs engaged in at least one
    challenge. Records are augmented to contain service status ratings and as
    of dates relative to the records.

    Args:
        availability_source: Directory where the availability data is stored.

        challenge_source_fn: Name of file containing the consolidated challenge
        data.

        destination: Directory to save the filtered and consolidated engaged
        BSL availability data.
    """
    # Load consolidated challenge file
    print(end="Loading challenge data...", flush=True)
    c_df = pd.read_csv(challenge_source_fn, usecols=["location_id"], dtype=str)
    print("done")
    # Build set of unique engaged BSL location_ids
    cbsl_ids = set(c_df.location_id.unique())
    # Determine As of Dates in the availability data
    try:
        with open(f'{availability_source}/metadata.json') as f:
            aods_md = json.load(f)
    except FileNotFoundError:
        print("Could not find the As of Dates metadata file.")
        return
    as_of_dates = sorted(aods_md['as_of_dates'])
    # Extract records from each As of Date individually
    for as_of_date in as_of_dates:
        print(f"As of Date: {as_of_date}")
        aod_path = f"{availability_source}/{as_of_date}/"
        aod_save_path = f"{destination}/{as_of_date}/"
        save_fn = f"{aod_save_path}/cbsl.csv"
        # Create destination directory for As of Date
        os.makedirs(aod_save_path, exist_ok=True)
        # Check and skip in case summary files already exist for the as of date
        if Path(save_fn).is_file():
            print("Consolidated file already exists. Skipping.")
            continue
        # Determine States in the As of Date
        try:
            with open(f"{aod_path}/metadata.json") as f:
                aod_md = json.load(f)
        except FileNotFoundError:
            print("Could not find the metadata file for as of date"
                  f" {as_of_date}.")
            return
        states = sorted(aod_md['states'])
        # Extract records from each state individually
        for state_id in states:
            print(end=f"    State: {state_id}", flush=True)
            state_fn = f"{aod_path}/{state_id}.csv"
            # Load availability data for the pair <as_of_date, state>
            a_df = pd.read_csv(
                state_fn,
                dtype=AVAILABILITY_DTYPES,
            )
            print(end=".", flush=True)
            # Filter out records from non-challenging BSLs
            a_df = a_df[a_df.location_id.isin(cbsl_ids)]
            print(end=".", flush=True)
            # Write (partial) challenging BSL data to file
            a_df.to_csv(
                save_fn,
                index=False,
                mode='a',
                header=not Path(save_fn).is_file(),
            )
            print("done")
            # break
        # break


if __name__ == "__main__":
    availability_source = "data/processed/bdc/availability/fixed/"
    challenge_source_fn = "data/processed/bdc/challenge/fixed_resolved/" \
                          "challenge.csv"
    destination = "data/processed/bdc/availability/fixed/"

    extract_challenging_bsl_availability(
        availability_source=availability_source,
        challenge_source_fn=challenge_source_fn,
        destination=destination
    )
