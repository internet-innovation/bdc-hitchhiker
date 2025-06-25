import os

from pathlib import Path

import pandas as pd

from utils import CHALLENGE_DTYPES, OUTCOME_CODES, TECHNOLOGY_CODES, \
    CATEGORY_CODES


def summarize_challenges_per_geographic_unit(source_fn, destination):
    """Summarizes the challenge data across geography units. The geography
    units under consideration are nation, states/territories/DC,
    counties, and census tracts. The summary data consists of counters on the
    number of challenges and engaged BSLs for different outcomes, access
    technologies, and category reasons for each geography.

    Args:
        source_fn: Name of file containing the consolidated challenge data.

        destination: Directory to save the summary data files.
    """
    # Create destination directory
    os.makedirs(destination, exist_ok=True)
    # Define auxiliary variables
    GEOS = ['nation', 'state', 'county', 'tract', 'block_group', 'block']
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
    # Check and abort in case summary files already exist for geos
    for geo in GEOS:
        destination_fn = f"{destination}/{geo}_summary.csv"
        if Path(destination_fn).is_file():
            print("One or more summary files already exist. Aborting.")
            return
    # Load consolidated challenge file
    print(end="Loading challenge data...", flush=True)
    c_df = pd.read_csv(source_fn, dtype=CHALLENGE_DTYPES, low_memory=False)
    print("done")
    # Add empty geoid for nation level summary (if necessary)
    if "nation" in GEOS:
        c_df['nation_geoid'] = ""
    # Summarize challenge data
    # > Compute summary data on a geography-level-basis
    for geo in GEOS:
        print(f"Geography-Level: {geo}")
        summary_dict_list = []
        # Determine number of unique geoIDs at the current level
        total_geoids = c_df[f"{geo}_geoid"].nunique()
        proc_geoids = 0
        # >> Compute summary data for each geography unit at the current level
        print(end=f"    Computing summary data [{proc_geoids}/{total_geoids}]",
              flush=True)
        for geoid, geoid_df in c_df.groupby(f"{geo}_geoid"):
            geoid_dict = {'geoid': geoid}
            # >>> Total counts
            geoid_dict['total_challenges'] = int(geoid_df.shape[0])
            geoid_dict['total_bsls'] = geoid_df.location_id.nunique()
            # >>> Per-column counts
            # >>>> Compute counts
            # >>>>> Individual COIs
            for coi in COLUMNS_OF_INTEREST:
                for value, value_df in geoid_df.groupby(coi['label']):
                    col_c = f"{coi['prefix']}{value}_challenges"
                    col_bsls = f"{coi['prefix']}{value}_bsls"
                    geoid_dict[col_c] = value_df.shape[0]
                    geoid_dict[col_bsls] = \
                        value_df.location_id.nunique()
            # >>>>> COI Pairs
            for ca, cb in COI_PAIRS:
                c_lbls = [ca['label'], cb['label']]
                for (va, vb), value_df in geoid_df.groupby(c_lbls):
                    col_c = f"{ca['prefix']}{va}_{cb['prefix']}{vb}_challenges"
                    col_bsls = f"{ca['prefix']}{va}_{cb['prefix']}{vb}_bsls"
                    geoid_dict[col_c] = value_df.shape[0]
                    geoid_dict[col_bsls] = \
                        value_df.location_id.nunique()
            # >>>>> COI Triples
            for ca, cb, cc in COI_TRIPLES:
                c_lbls = [ca['label'], cb['label'], cc['label']]
                for (va, vb, vc), value_df in geoid_df.groupby(c_lbls):
                    col_c = f"{ca['prefix']}{va}_{cb['prefix']}{vb}" \
                            f"_{cc['prefix']}{vc}_challenges"
                    col_bsls = f"{ca['prefix']}{va}_{cb['prefix']}{vb}" \
                               f"_{cc['prefix']}{vc}_bsls"
                    geoid_dict[col_c] = value_df.shape[0]
                    geoid_dict[col_bsls] = \
                        value_df.location_id.nunique()
            # Add to summary dict
            summary_dict_list.append(geoid_dict)
            # Report progress
            proc_geoids = proc_geoids + 1
            print(end=f"\r    Computing summary data [{proc_geoids}"
                      f"/{total_geoids}]",
                  flush=True)
        print()
        # Create summary_df from summary_dict
        summary_df = pd.DataFrame().from_dict(summary_dict_list)
        # Make sure all columns are present and in the correct order
        summary_cols = ['geoid', 'total_challenges', 'total_bsls']
        new_cols = []
        # Determine column order and missing columns
        for coi in COLUMNS_OF_INTEREST:
            for value in coi['values']:
                for cnt in ['challenges', 'bsls']:
                    value_lbl = f"{coi['prefix']}{value}_{cnt}"
                    summary_cols.append(value_lbl)
                    if value_lbl not in summary_df.columns:
                        new_cols.append(value_lbl)
        for ca, cb in COI_PAIRS:
            for va in ca['values']:
                for vb in cb['values']:
                    for cnt in ['challenges', 'bsls']:
                        value_lbl = f"{ca['prefix']}{va}_{cb['prefix']}{vb}" \
                                    f"_{cnt}"
                        summary_cols.append(value_lbl)
                        if value_lbl not in summary_df.columns:
                            new_cols.append(value_lbl)
        for ca, cb, cc in COI_TRIPLES:
            for va in ca['values']:
                for vb in cb['values']:
                    for vc in cc['values']:
                        for cnt in ['challenges', 'bsls']:
                            value_lbl = f"{ca['prefix']}{va}" \
                                        f"_{cb['prefix']}{vb}" \
                                        f"_{cc['prefix']}{vc}" \
                                        f"_{cnt}"
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
        # Replace NaN values (i.e., COI value not present for the geographic
        # unit) with zeros
        summary_df = summary_df.fillna(0)
        # Convert numeric column dtypes to int
        for col in summary_df.columns:
            if col not in ['geoid']:
                summary_df[col] = summary_df[col].astype(int)
        # Write summary data to file
        print(end="    Writing summary data to file...", flush=True)
        summary_fn = f"{destination}/{geo}_summary.csv"
        summary_df.to_csv(summary_fn, index=False)
        print("done")


if __name__ == "__main__":
    source_fn = "data/processed/bdc/challenge/fixed_resolved/challenge.csv"
    destination = "data/processed/bdc/challenge/fixed_resolved/"

    summarize_challenges_per_geographic_unit(source_fn=source_fn,
                                             destination=destination)
