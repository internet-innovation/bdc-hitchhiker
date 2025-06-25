import json
import os

from pathlib import Path

import numpy as np
import pandas as pd
import us


def consolidate_and_augment_challenge_data(challenge_source,
                                           bsl_source,
                                           destination):
    """First, consolidates challenge data across As of Datas present in the
    'source' directory combining challenges from all states into a single CSV
    file in the 'destination' directory. Second, augments challenge data with
    geospatial location data (GEOID and H3).

    Args:
        challenge_source: Directory where the challenge data is stored.

        bsl_source: Directory where the BSL data is stored.

        destination: Directory to save the consolidated and augmented challenge
        data.
    """
    if Path(f"{destination}/challenge.csv").is_file():
        print("Consolidated file already exists. Nothing to do.")
        return
    # CONSOLIDATE
    print(end="Consolidating challenge data")
    try:
        with open(f'{challenge_source}/metadata.json') as f:
            aods_md = json.load(f)
    except FileNotFoundError:
        print("Could not find the As of Dates metadata file.")
        return

    # Load every challenge file and concatenate into a single dataframe.
    challenges = pd.DataFrame()
    as_of_dates = sorted(aods_md['as_of_dates'])
    for as_of_date in as_of_dates:
        print(f"As of Date: {as_of_date}")
        aod_path = f"{challenge_source}/{as_of_date}/"
        try:
            with open(f"{aod_path}/metadata.json") as f:
                aod_md = json.load(f)
        except FileNotFoundError:
            print(f"Could not find the metadata file for as of date"
                  f"{as_of_date}.")
            return

        states = sorted(aod_md['states'])
        for state_id in states:
            print(f"    State: {state_id}")
            state_filename = f"{aod_path}/{state_id}.zip"
            if Path(state_filename).is_file():
                state_df = pd.read_csv(
                    state_filename,
                    dtype=str,
                    low_memory=False,
                )
                challenges = pd.concat([challenges, state_df],
                                       ignore_index=True)
            else:
                print(f"Could not find the data file for state {state_id}.")
                return
    # Remove challenge duplicates, keeping the entry with the latest adj date
    challenges = challenges.sort_values(by='adjudication_date')
    challenges = challenges.drop_duplicates(subset='challenge_id', keep='last')
    # AUGMENT
    # Make simpler to determine challenge outcome with outcome code column
    upheld_oc = [
        'Challenge Upheld - Provider Conceded',
        'Challenge Upheld - Service Change',
        'Challenge Upheld - Adjudicated by FCC',
    ]
    overturned_oc = ['Challenge Overturned']
    withdrawn_oc = ['Challenge Withdrawn']
    upheld_index = challenges['outcome'].isin(upheld_oc)
    challenges.loc[upheld_index, 'outcome_code'] = 0  # Upheld
    overturned_index = challenges['outcome'].isin(overturned_oc)
    challenges.loc[overturned_index, 'outcome_code'] = 1  # Overtuned
    withdrawn_index = challenges['outcome'].isin(withdrawn_oc)
    challenges.loc[withdrawn_index, 'outcome_code'] = 2  # Withdrawn
    challenges['outcome_code'] = challenges['outcome_code'].astype(np.int8)
    # Load BSL location data
    print(end="Loading consolidated BSL location data...")
    filename = f"{bsl_source}/bsl_geolocation.csv"
    bsls = pd.read_csv(filename, dtype=str, low_memory=False)
    print("done")
    # Join dataframes preserving location_id keys on challenge data
    print(end="Merging data on location_id...")
    n_challenges = challenges.shape[0]
    challenges = pd.merge(
        left=challenges,
        right=bsls,
        how="left",
        on="location_id",
    )
    # Determine state-, county-, tract-, and block-group-level GeoIDs
    geos = ['state', 'county', 'tract', 'block_group']
    lens = [2, 5, 11, 12]
    for geo, length in zip(geos, lens):
        if f"{geo}_geoid" not in list(challenges.columns):
            challenges[f"{geo}_geoid"] = challenges['block_geoid'].apply(
                lambda x: str(x)[:length] if x else x
            )
    # Add state_geoid to challenges without block_geoid based on the
    # location_state state abbreviation
    STATES = us.STATES_AND_TERRITORIES + [us.states.DC]
    abbr2fips = us.states.mapping('abbr', 'fips', states=STATES)
    challenges.loc[challenges.block_geoid.isna(), 'state_geoid'] = \
        challenges[challenges.block_geoid.isna()].location_state.apply(
            lambda x: abbr2fips[x]
        )
    print("done")
    # Update location_state from state_geoid
    fips2abbr = us.states.mapping('fips', 'abbr', states=STATES)
    challenges.location_state = challenges.state_geoid.apply(
        lambda x: fips2abbr[x]
    )
    # Assert
    assert challenges.shape[0] == n_challenges, \
        "There are more challenges in the augmented data than in the" \
        " original data."
    # Save augmented dataframe to file
    print(end="Saving the consolidated and augmented dataframe to file...")
    filename = f"{destination}/challenge.csv"
    os.makedirs(destination, exist_ok=True)
    challenges.to_csv(filename, index=False)
    print("done")


if __name__ == "__main__":
    challenge_source = "data/raw/bdc/challenge/fixed_resolved/"
    bsl_source = "data/processed/bdc/availability/fixed/"
    destination = "data/processed/bdc/challenge/fixed_resolved/"

    consolidate_and_augment_challenge_data(
        challenge_source=challenge_source,
        bsl_source=bsl_source,
        destination=destination,
    )
