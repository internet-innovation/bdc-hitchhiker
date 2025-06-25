import json
import os
import requests

from pathlib import Path
from credentials import BDC_USERNAME, BDC_HASH_VALUE


def download_fixed_availability_data(headers, destination):
    """Downloads the requested availability data into the given directory.

    Directories and files are named according to the following pattern:
        <fixed|mobile>/<as_of_date>/
            <state_fips>_<state_name>_<technology_code>_<technology_desc>.zip

    Args:
        headers: Headers to use for web requests. Should include username and
        hash_value.

        destination: Directory to save the downloaded data into.
    """
    # Set up auxiliary variables
    api_url = "https://broadbandmap.fcc.gov/api/public/map/"
    headers["User-Agent"] = "challenge-explorer 0.0.1"
    session = requests.Session()
    session.headers.update(headers)
    # Get list of as of dates for availability data
    response = session.get(f"{api_url}/listAsOfDates")
    if response.status_code == 200:
        as_of_dates = []
        for item in response.json()['data']:
            if item['data_type'] == 'availability':
                as_of_date = item['as_of_date'][:10]  # Future: Use DateTime
                as_of_dates.append(as_of_date)
        os.makedirs(f"{destination}", exist_ok=True)
        aods_md = {
            'as_of_dates': as_of_dates,
        }
        print(f"Found the following As of Dates: {as_of_dates}")
        with open(f"{destination}/metadata.json", "w") as f:
            json.dump(aods_md, f, indent=4)
    else:
        print("Failed to get 'As of Dates' for Availability Data")
        return
    # Download the files for state-based fixed-broadband availability data for
    # every 'As of Date' present
    for as_of_date in as_of_dates:
        print(f"As of Date: {as_of_date}")
        aod_path = f"{destination}/{as_of_date}/"
        os.makedirs(aod_path, exist_ok=True)
        aod_md = {
            'states': []
        }
        url = f"{api_url}/downloads/listAvailabilityData/{as_of_date}"
        response = session.get(url)
        if response.status_code == 200:
            files = response.json()['data']

            states_md = {}
            for fmd in files:
                if fmd['category'] == 'State' \
                        and fmd['subcategory'] == 'Fixed Broadband':
                    if fmd['state_fips'] not in states_md:
                        states_md[fmd['state_fips']] = {
                            "state_fips": fmd['state_fips'],
                            "state_name": fmd['state_name'],
                            "files": []
                        }
                    fmd['original_file_name'] = fmd['file_name']
                    fn = f"{fmd['technology_code']:0>2s}" \
                         f"_{fmd['technology_code_desc'].replace(' ', '')}"
                    fmd['file_name'] = fn
                    states_md[fmd['state_fips']]['files'].append(fmd)

            for _, smd in states_md.items():
                state_id = f"{smd['state_fips']}_" \
                           f"{smd['state_name'].replace(' ', '')}"
                aod_md['states'].append(state_id)
                print(f"    State: {state_id}")
                state_path = f"{aod_path}/{state_id}/"
                os.makedirs(state_path, exist_ok=True)
                for fmd in smd['files']:
                    print(end=f"        {fmd['file_name']}: ")
                    file_path = f"{state_path}/{fmd['file_name']}.zip"
                    # If file has been downloaded previously, skip
                    if Path(file_path).is_file():
                        print("skipping")
                        continue
                    # Else, download the file
                    file_url = f"{api_url}/downloads/downloadFile/" \
                               f"availability/{fmd['file_id']}"
                    r = session.get(file_url)
                    if r.status_code == 200:
                        with open(file_path, "wb") as f:
                            f.write(r.content)
                        print("done")
                    else:
                        print(f"{r.status_code}: Failed to get file"
                              f" {fmd['file_id']}.")
                        break
                    # break # testing
                with open(f"{state_path}/metadata.json", "w") as f:
                    json.dump(smd, f, indent=4)
            with open(f"{aod_path}/metadata.json", "w") as f:
                json.dump(aod_md, f, indent=4)
        else:
            print(f"Failed to get file list for as of date {as_of_date}")
            return
        # break # testing


if __name__ == "__main__":
    headers = {
        "username": BDC_USERNAME,
        "hash_value": BDC_HASH_VALUE,
    }
    destination = "data/raw/bdc/availability/fixed/"

    download_fixed_availability_data(headers=headers, destination=destination)
