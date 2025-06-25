import json
import os

from pathlib import Path

import pandas as pd

from utils import AVAILABILITY_DTYPES, TECHNOLOGY_CODES, STATUS_CODES


def summarize_availability_per_geographic_unit(source, destination):
    """Summarizes the availability data for each As of Date across geographic
    units. The geographic levels under consideration are nation, states/
    territories/DC, counties, and census tracts. The summary data consists of
    counters on the number of availability records and BSLs overall as well as
    for different access technologies for each geography unit.

    Args:
        source: Directory where the availability data is stored.

        destination: Directory to save the summary data files.
    """
    # Create destination directory
    os.makedirs(destination, exist_ok=True)
    # Define auxiliary variables
    GEOS = ['state', 'county', 'tract', 'block_group', 'block']
    AVAILABILITY_COLS = [
        'location_id',
        'technology',
        'status',
    ]
    AVAILABILITY_COLS = AVAILABILITY_COLS + [f"{geo}_geoid" for geo in GEOS]
    # Determine As of Dates in the availability data
    try:
        with open(f'{source}/metadata.json') as f:
            aods_md = json.load(f)
    except FileNotFoundError:
        print("Could not find the As of Dates metadata file.")
        return
    as_of_dates = sorted(aods_md['as_of_dates'])
    # Summarize each As of Date individually
    for as_of_date in as_of_dates:
        print(f"As of Date: {as_of_date}")
        aod_path = f"{source}/{as_of_date}/"
        aod_save_path = f"{destination}/{as_of_date}/"
        # Create destination directory for As of Date
        os.makedirs(aod_save_path, exist_ok=True)
        # Check and skip in case summary files already exist for the as of date
        skip = False
        for geo in GEOS:
            if Path(f"{aod_save_path}/{geo}_summary.csv").is_file():
                skip = True
                break
        if skip:
            print("One or more summary files already exist. Skipping")
            continue
        # Determine States in the As of Date
        try:
            with open(f"{aod_path}/metadata.json") as f:
                aod_md = json.load(f)
        except FileNotFoundError:
            print("Could not find the metadata file for as of date"
                  f"{as_of_date}.")
            return
        states = sorted(aod_md['states'])
        # Summarize on a per State-basis
        aod_state_dfs = []
        for state_id in states:
            print(f"    State: {state_id}")
            state_fn = f"{aod_path}/{state_id}.csv"
            # Load availability data for the pair <as_of_date, state>
            print(end="        Loading availability data...",
                  flush=True)
            a_df = pd.read_csv(
                state_fn,
                dtype=AVAILABILITY_DTYPES,
                usecols=AVAILABILITY_COLS,
            )
            # Order records by service statuses
            a_df = a_df.sort_values(by="status", ascending=True)
            # Determine the service status of each BSL
            bsl_df = a_df.drop(columns='technology').drop_duplicates(
                subset='location_id', keep='first'
            )
            # Determine unique availability records for each pair of
            # location_id and technology (keeping the record with the best
            # service status for each pair)
            unique_pairs_df = a_df.drop_duplicates(
                subset=['location_id', 'technology'],
                keep='first',
            )
            print("done")
            # Compute Sand write (partial) summary data to file
            print(end="        Computing and writing summary data to files",
                  flush=True)
            # > For each geography level
            for geo in GEOS:
                # Compute summary data at this geography level
                group1_dl = []
                for geoid, geoid_df in a_df.groupby(f"{geo}_geoid"):
                    geoid_dict = {'geoid': geoid}
                    # > Identification and total counts for the unit
                    geoid_dict['total_records'] = \
                        int(geoid_df.shape[0])
                    geoid_dict['total_bsls'] = \
                        geoid_df.location_id.nunique()
                    # > Per-service-level record counts
                    # >> Compute counts
                    for sc, recs in geoid_df.status.value_counts().items():
                        col = f"s{int(sc)}_records"
                        geoid_dict[col] = recs
                    # > Per-technology and per-technology+service-level record
                    #   counts
                    # >> Compute counts
                    for tc, tc_df in geoid_df.groupby('technology'):
                        # >>> Technology counts
                        geoid_dict[f"t{tc}_records"] = tc_df.shape[0]
                        # >>> Technology+service-level counts
                        for sc, recs in tc_df.status.value_counts().items():
                            col = f"t{tc}_s{int(sc)}_records"
                            geoid_dict[col] = recs
                    # Add to group1 summary dict list
                    group1_dl.append(geoid_dict)

                # > Per-service-level BSL counts
                group2_dl = []
                for geoid, geoid_df in bsl_df.groupby(f"{geo}_geoid"):
                    geoid_dict = {'geoid': geoid}
                    # >> Compute counts
                    for sc, bsls in geoid_df.status.value_counts().items():
                        geoid_dict[f"s{int(sc)}_bsls"] = bsls
                    # Add to group2 summary dict list
                    group2_dl.append(geoid_dict)

                # > Per-technology and per-technology+service-level BSL counts
                group3_dl = []
                for geoid, geoid_df in unique_pairs_df.groupby(f"{geo}_geoid"):
                    geoid_dict = {'geoid': geoid}
                    # >> Compute counts
                    for tc, tc_df in geoid_df.groupby('technology'):
                        # >>> Technology counts
                        tcol = f"t{tc}_bsls"
                        geoid_dict[tcol] = tc_df.shape[0]
                        # >>> Technology+service-level counts
                        for sc, bsls in tc_df.status.value_counts().items():
                            col = f"t{tc}_s{int(sc)}_bsls"
                            geoid_dict[col] = bsls
                    # Add to group3 summary dict list
                    group3_dl.append(geoid_dict)

                # Create dataframe for each group and merge columns
                group1_df = pd.DataFrame().from_dict(group1_dl)
                group2_df = pd.DataFrame().from_dict(group2_dl)
                group3_df = pd.DataFrame().from_dict(group3_dl)
                summary_df = pd.merge(
                    left=group1_df,
                    right=group2_df,
                    on="geoid",
                    how="outer",
                )
                summary_df = pd.merge(
                    left=summary_df,
                    right=group3_df,
                    on="geoid",
                    how="outer",
                )
                # Make sure all columns are present
                summary_cols = ['geoid', 'total_records', 'total_bsls']
                for sc in STATUS_CODES:
                    for cnt in ['records', 'bsls']:
                        value_lbl = f"s{int(sc)}_{cnt}"
                        summary_cols.append(value_lbl)
                        if value_lbl not in summary_df.columns:
                            summary_df[value_lbl] = 0
                for tc in TECHNOLOGY_CODES:
                    for cnt in ['records', 'bsls']:
                        value_lbl = f"t{int(tc)}_{cnt}"
                        summary_cols.append(value_lbl)
                        if value_lbl not in summary_df.columns:
                            summary_df[value_lbl] = 0
                for tc in TECHNOLOGY_CODES:
                    for sc in STATUS_CODES:
                        for cnt in ['records', 'bsls']:
                            value_lbl = f"t{tc}_s{int(sc)}_{cnt}"
                            summary_cols.append(value_lbl)
                            if value_lbl not in summary_df.columns:
                                summary_df[value_lbl] = 0
                # Sort columns by clear order
                summary_df = summary_df.sort_index(
                    axis='columns',
                    key=lambda x: pd.Index([summary_cols.index(y) for y in x])
                )
                # Replace NaN values (i.e., COI value not present for the
                # geographic unit) with zeros
                summary_df = summary_df.fillna(0)
                # Change numeric column dtypes to int
                for col in summary_df.columns:
                    if col not in ['geoid']:
                        summary_df[col] = summary_df[col].astype(int)
                # Write (partial) summary data to file
                summary_fn = f"{aod_save_path}/{geo}_summary.csv"
                summary_df.to_csv(
                    summary_fn,
                    index=False,
                    mode='a',
                    header=not Path(summary_fn).is_file(),
                )
                # If geo is state add data for nation-wise summarization
                if geo == "state":
                    aod_state_dfs.append(summary_df)
                #
                print(end=".", flush=True)
                # break
            print("done")
            # break
        aod_df = pd.concat(aod_state_dfs, ignore_index=True)
        nation_df = {}
        for column, value in aod_df.sum().items():
            nation_df[column] = [value]
        nation_df = pd.DataFrame(nation_df)
        nation_df.loc[0, 'geoid'] = ""
        nation_fn = f"{aod_save_path}/nation_summary.csv"
        nation_df.to_csv(nation_fn, index=False)
        # break


if __name__ == "__main__":
    source = "data/processed/bdc/availability/fixed/"
    destination = "data/processed/bdc/availability/fixed/"

    summarize_availability_per_geographic_unit(source=source,
                                               destination=destination)
