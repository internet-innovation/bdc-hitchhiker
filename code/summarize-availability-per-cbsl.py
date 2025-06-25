import json
import os

from pathlib import Path

import pandas as pd

from utils import AVAILABILITY_DTYPES, TECHNOLOGY_CODES, STATUS_CODES


def summarize_availability_per_challenging_bsl(source, destination):
    """Summarizes availability data across challanging BSLs. The summary data
    consists of counters on the number of availability records overall as well
    as for different access technologies for each challenging BSL.

    Args:
        source: Directory where the availability data is stored.

        destination: Directory to save the summary data files.
    """
    # Create destination directory
    os.makedirs(destination, exist_ok=True)
    # Define auxiliary variables
    BSL_ID_COL = "location_id"
    AVAILABILITY_COLS = [
        'location_id',
        'technology',
        'status',
    ]
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
        aod_fn = f"{source}/{as_of_date}/cbsl.csv"
        aod_save_path = f"{destination}/{as_of_date}/"
        summary_fn = f"{aod_save_path}/cbsl_summary.csv"
        # Create destination directory for As of Date
        os.makedirs(aod_save_path, exist_ok=True)
        # Check and skip in case summary files already exist for the as of date
        if Path(summary_fn).is_file():
            print("Summary file already exists. Skipping.")
            continue
        Path(summary_fn).touch()
        # Load availability data for challenging BSLs in the as_of_date
        print(end="    Loading availability data...", flush=True)
        a_df = pd.read_csv(
            aod_fn,
            dtype=AVAILABILITY_DTYPES,
            usecols=AVAILABILITY_COLS,
        )
        print("done")
        # Determine number of unique BSLs
        total_bsls = a_df[BSL_ID_COL].nunique()
        proc_bsls = 0
        # >> Compute summary data for each BSL
        print(end=f"    Computing summary data [{proc_bsls}/{total_bsls}]",
              flush=True)
        summary_dl = []
        for bsl_id, bsl_df in a_df.groupby(BSL_ID_COL):
            bsl_dict = {BSL_ID_COL: bsl_id}
            # >> Total count
            bsl_dict['total_records'] = int(bsl_df.shape[0])
            # >> Per-service status record counts
            for sc, recs in bsl_df.status.value_counts().items():
                bsl_dict[f"s{sc}_records"] = recs
            # >> Per-technology and per-technology+service-level record
            #   counts
            for tc, tc_df in bsl_df.groupby('technology'):
                # >>> Technology counts
                bsl_dict[f"t{tc}_records"] = tc_df.shape[0]
                # >>> Technology+service-level counts
                for sc, recs in tc_df.status.value_counts().items():
                    col = f"t{tc}_s{sc}_records"
                    bsl_dict[col] = recs
            # Add to group1 summary dict list
            summary_dl.append(bsl_dict)
            # Report progress
            proc_bsls = proc_bsls + 1
            if proc_bsls % 1000 == 0:
                print(end=f"\r    Computing summary data [{proc_bsls}/"
                          f"{total_bsls}]",
                      flush=True)
        print(end=f"    Computing summary data [{proc_bsls}/{total_bsls}]",
              flush=True)
        print()
        # Create datafrane out of BSL dicts
        print(end="    Generating dataframe with summary data...", flush=True)
        summary_df = pd.DataFrame().from_dict(summary_dl)
        print("done")
        # Make sure all columns are present
        print(end="    Cleaning and sorting columns...", flush=True)
        summary_cols = [BSL_ID_COL, 'total_records']
        for sc in STATUS_CODES:
            value_lbl = f"s{sc}_records"
            summary_cols.append(value_lbl)
            if value_lbl not in summary_df.columns:
                summary_df[value_lbl] = 0
        for tc in TECHNOLOGY_CODES:
            value_lbl = f"t{tc}_records"
            summary_cols.append(value_lbl)
            if value_lbl not in summary_df.columns:
                summary_df[value_lbl] = 0
        for tc in TECHNOLOGY_CODES:
            for sc in STATUS_CODES:
                value_lbl = f"t{tc}_s{sc}_records"
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
            if col not in [BSL_ID_COL]:
                summary_df[col] = summary_df[col].astype(int)
        print("done")
        # Write (partial) summary data to file
        print(end="    Saving summary data to file...", flush=True)
        summary_df.to_csv(summary_fn, index=False)
        print("done")


if __name__ == "__main__":
    source = "data/processed/bdc/availability/fixed/"
    destination = "data/processed/bdc/availability/fixed/"

    summarize_availability_per_challenging_bsl(source=source,
                                               destination=destination)
