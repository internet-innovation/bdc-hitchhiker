import os

from pathlib import Path

import pandas as pd

from utils import CHALLENGE_DTYPES, OUTCOME_CODES, TECHNOLOGY_CODES, \
    CATEGORY_CODES


def summarize_challenges_per_bsl(source_fn, destination):
    """Summarizes the challenge data across engaged BSLs. The summary data
    consists of counters on the number of challenges for different outcomes,
    access technologies, and category reasons for each BSL.

    Args:
        source_fn: Name of file containing the consolidated challenge data.

        destination: Directory to save the summary data files.
    """
    # Create destination directory
    os.makedirs(destination, exist_ok=True)
    # Define auxiliary variables
    bsl_id_col = "location_id"
    COLUMNS_OF_INTEREST = [
        {
            "label": "outcome_code",
            "values": OUTCOME_CODES,
            "prefix": "o",
        },
        {
            "label": "technology",
            "values": TECHNOLOGY_CODES,
            "prefix": "t",
        },
        {
            "label": "category_code",
            "values": CATEGORY_CODES,
            "prefix": "c",
        },
    ]
    COI_PAIRS = [
        (COLUMNS_OF_INTEREST[0], COLUMNS_OF_INTEREST[1]),
        (COLUMNS_OF_INTEREST[0], COLUMNS_OF_INTEREST[2]),
        (COLUMNS_OF_INTEREST[1], COLUMNS_OF_INTEREST[2]),
    ]
    COI_TRIPLES = [
        (
            COLUMNS_OF_INTEREST[0],
            COLUMNS_OF_INTEREST[1],
            COLUMNS_OF_INTEREST[2]
        ),
    ]
    # Check and abort in case summary file already exists
    destination_fn = f"{destination}/bsl_summary.csv"
    if Path(destination_fn).is_file():
        print("Summary file already exists. Aborting.")
        return
    # Load consolidated challenge file
    print(end="Loading challenge data...", flush=True)
    c_df = pd.read_csv(source_fn, dtype=CHALLENGE_DTYPES, low_memory=False)
    print("done")
    # Summarize challenge data
    summary_dict_list = []
    # Determine number of unique BSLs
    total_bsls = c_df[bsl_id_col].nunique()
    proc_bsls = 0
    # >> Compute summary data for each BSL
    print(end=f"    Computing summary data [{proc_bsls}/{total_bsls}]",
          flush=True)
    for bsl_id, bsl_df in c_df.groupby(bsl_id_col):
        bsl_dict = {bsl_id_col: bsl_id}
        # >>> Total counts
        bsl_dict['total_challenges'] = int(bsl_df.shape[0])
        # >>> Per-column counts
        # >>>> Compute counts
        # >>>>> Individual COIs
        for coi in COLUMNS_OF_INTEREST:
            coi_lbl = coi['label']
            for value, value_count in bsl_df[coi_lbl].value_counts().items():
                col_c = f"{coi['prefix']}{value}_challenges"
                bsl_dict[col_c] = value_count
        # >>>>> COI Pairs
        for ca, cb in COI_PAIRS:
            c_lbls = [ca['label'], cb['label']]
            for (va, vb), value_count in bsl_df[c_lbls].value_counts().items():
                col_c = f"{ca['prefix']}{va}_{cb['prefix']}{vb}_challenges"
                bsl_dict[col_c] = value_count
        # >>>>> COI Triples
        for ca, cb, cc in COI_TRIPLES:
            c_lbls = [ca['label'], cb['label'], cc['label']]
            for (va, vb, vc), value_count in \
                    bsl_df[c_lbls].value_counts().items():
                col_c = f"{ca['prefix']}{va}_{cb['prefix']}{vb}" \
                        f"_{cc['prefix']}{vc}_challenges"
                bsl_dict[col_c] = value_count
        # Add to summary dict
        summary_dict_list.append(bsl_dict)
        # Report progress
        proc_bsls = proc_bsls + 1
        if proc_bsls % 1000 == 0:
            print(end=f"\r    Computing summary data [{proc_bsls}/"
                      f"{total_bsls}]",
                  flush=True)
    print(end=f"    Computing summary data [{proc_bsls}/{total_bsls}]",
          flush=True)
    print()
    # Create summary_df from summary_dict
    summary_df = pd.DataFrame().from_dict(summary_dict_list)
    # Make sure all columns are present and in the correct order
    summary_cols = ['location_id', 'total_challenges']
    new_cols = []
    # Determine column order and missing columns
    for coi in COLUMNS_OF_INTEREST:
        for value in coi['values']:
            value_lbl = f"{coi['prefix']}{value}_challenges"
            summary_cols.append(value_lbl)
            if value_lbl not in summary_df.columns:
                new_cols.append(value_lbl)
    for ca, cb in COI_PAIRS:
        for va in ca['values']:
            for vb in cb['values']:
                value_lbl = f"{ca['prefix']}{va}_{cb['prefix']}{vb}" \
                            f"_challenges"
                summary_cols.append(value_lbl)
                if value_lbl not in summary_df.columns:
                    new_cols.append(value_lbl)
    for ca, cb, cc in COI_TRIPLES:
        for va in ca['values']:
            for vb in cb['values']:
                for vc in cc['values']:
                    value_lbl = f"{ca['prefix']}{va}" \
                                f"_{cb['prefix']}{vb}" \
                                f"_{cc['prefix']}{vc}" \
                                f"_challenges"
                    summary_cols.append(value_lbl)
                    if value_lbl not in summary_df.columns:
                        new_cols.append(value_lbl)
    # > Add missing columns
    tmp = pd.DataFrame(0, index=summary_df.index, columns=new_cols)
    summary_df = pd.concat([summary_df, tmp], axis='columns')
    # > Sort columns by clear order
    summary_df = summary_df.sort_index(
        axis='columns',
        key=lambda x: pd.Index([summary_cols.index(xi) for xi in x])
    )
    # Replace NaN values (i.e., COI value not present for the BSL) with zeros
    summary_df = summary_df.fillna(0)
    # Convert numeric column dtypes to int
    for col in summary_df.columns:
        if col not in [bsl_id_col]:
            summary_df[col] = summary_df[col].astype(int)
    # Write summary data to file
    print(end="    Writing summary data to file...", flush=True)
    summary_df.to_csv(destination_fn, index=False)
    print("done")


if __name__ == "__main__":
    source_fn = "data/processed/bdc/challenge/fixed_resolved/challenge.csv"
    destination = "data/processed/bdc/challenge/fixed_resolved/"

    summarize_challenges_per_bsl(source_fn=source_fn,
                                 destination=destination)
