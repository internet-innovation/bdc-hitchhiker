import json
import os

from pathlib import Path

import pandas as pd


def merge_challenge_and_availability_summaries(challenge_source,
                                               availability_source,
                                               destination):
    """Merge challenge and availability summaries across multiple geographical
    levels and at the BSL level.

    Args:
        challenge_source: Directory where the challenge summaries are stored.

        availability_source: Directory where the availability summaries are
        stored.

        destination: Directory to save the merged summary data.
    """
    # Create destination directory
    os.makedirs(destination, exist_ok=True)
    # Define auxiliary variables
    LEVELS = [
        {"desc": "nation", "key": "geoid"},
        {"desc": "state", "key": "geoid"},
        {"desc": "county", "key": "geoid"},
        {"desc": "tract", "key": "geoid"},
        {"desc": "block_group", "key": "geoid"},
        {"desc": "block", "key": "geoid"},
        {"desc": "bsl", "key": "location_id", "desc_a": "cbsl"},
    ]
    DTYPES = {"geoid": str, "location_id": str}
    # Determine As of Dates in the availability data
    try:
        with open(f'{availability_source}/metadata.json') as f:
            aods_md = json.load(f)
    except FileNotFoundError:
        print("Could not find the As of Dates metadata file.")
        return
    # Save availability data metadata to destination directory
    with open(f"{destination}/metadata.json", 'w') as f:
        json.dump(aods_md, f, indent=4)
    as_of_dates = sorted(aods_md['as_of_dates'])
    # Merge summaries on a per-level
    for level in LEVELS:
        lvl_desc = level["desc"]
        lvl_desc_a = level["desc_a"] if "desc_a" in level else lvl_desc
        lvl_key = level["key"]
        print(f"Level: {lvl_desc}")
        # Load challenge summary for the current level
        print(end="    Loading challenge data...", flush=True)
        c_fn = f"{challenge_source}/{lvl_desc}_summary.csv"
        c_df = pd.read_csv(c_fn, dtype=DTYPES)
        c_df = c_df.rename(
            columns={
                col: f"c_{col}"
                for col in c_df.columns
                if col not in ['geoid', 'location_id']
            }
        )
        print("done")
        # Merge summaries on a per-availability-as-of-date basis
        for aod in as_of_dates:
            print(f"    As of Date: {aod}")
            # Create destination directory
            aod_destination = f"{destination}/{aod}/"
            m_fn = f"{aod_destination}/{lvl_desc}_summary.csv"
            os.makedirs(aod_destination, exist_ok=True)
            # Check and skip as-of-date if file already exists
            if Path(m_fn).is_file():
                print("        Merge file already exists. Skipping.")
                continue
            # Load availability summary for the current level and as-of-date
            print(end="        Loading availability data...", flush=True)
            a_fn = f"{availability_source}/{aod}/{lvl_desc_a}_summary.csv"
            a_df = pd.read_csv(a_fn, dtype=DTYPES)
            a_df = a_df.rename(
                columns={
                    col: f"a_{col}"
                    for col in a_df.columns
                    if col not in ['geoid', 'location_id']
                }
            )
            print("done")
            # Merge dataframes for the current level and as-of-date
            print(end="        Merging summary dataframes...", flush=True)
            m_df = pd.merge(
                left=c_df,
                right=a_df,
                how="outer",
                on=lvl_key,
                suffixes=("_c", "_a")
            )
            m_df = m_df.fillna(0)
            # Make sure numeric columns have dtype int
            for col in m_df.columns:
                if col not in ['geoid', 'location_id'] \
                        and m_df[col].dtype != int:
                    m_df[col] = m_df[col].astype(int)
            print("done")
            # Save merged dataframe to file
            print(end="        Saving merged dataframe to file...", flush=True)
            m_df.to_csv(m_fn, index=False)
            print("done")


if __name__ == "__main__":
    challenge_source = "data/processed/bdc/challenge/fixed_resolved/"
    availability_source = "data/processed/bdc/availability/fixed/"
    destination = "data/processed/bdc/challenge_availability/fixed/"

    merge_challenge_and_availability_summaries(
        challenge_source=challenge_source,
        availability_source=availability_source,
        destination=destination,
    )
