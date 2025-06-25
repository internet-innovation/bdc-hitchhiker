import json
import os
import requests

from pathlib import Path
from credentials import BDC_USERNAME, BDC_HASH_VALUE


def download_resolved_fixed_challenge_data(headers, destination):
    """Downloads the resolved fixed challenge data into the given directory.

    Directories and files are named according to the following pattern:
        fixed_resolved/<as_of_date>/<state_fips>_<state_name>.zip

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
    # Get list of as of dates for challenge data
    response = session.get(f"{api_url}/listAsOfDates")
    if response.status_code == 200:
        as_of_dates = []
        for item in response.json()['data']:
            if item['data_type'] == 'challenge':
                as_of_date = item['as_of_date'][:10]  # Future: Use DateTime
                as_of_dates.append(as_of_date)
        os.makedirs(f"{destination}/", exist_ok=True)
        aods_md = {
            'as_of_dates': as_of_dates,
        }
        print(f"Found the following As of Dates: {as_of_dates}")
        with open(f"{destination}/metadata.json", "w") as f:
            json.dump(aods_md, f, indent=4)
    else:
        print("Failed to get 'As of Dates' for Challenge Data")
        return
    # Download the files for state-based fixed-broadband challenge data for
    # evert 'As of Date' available.
    for as_of_date in as_of_dates:
        print(f"As of Date: {as_of_date}")
        aod_path = f"{destination}/{as_of_date}/"
        os.makedirs(aod_path, exist_ok=True)
        aod_md = {
            'states': []
        }
        url = f"{api_url}/downloads/listChallengeData/{as_of_date}"
        response = session.get(url)
        if response.status_code == 200:
            files = response.json()['data']
            for fmd in files:
                if fmd['category'] == 'Fixed Challenge - Resolved':
                    state_id = f"{fmd['state_fips']}" \
                         f"_{fmd['state_name'].replace(' ', '')}"
                    print(end=f"    State: {state_id} ")
                    fmd['file_name'] = state_id
                    file_path = f"{aod_path}/{state_id}.zip"
                    # If file has been downloaded previously, skip
                    if Path(file_path).is_file():
                        aod_md['states'].append(state_id)
                        print("skipping")
                        continue
                    # Else, download the file
                    file_url = f"{api_url}/downloads/downloadFile/" \
                               f"challenge/{fmd['file_id']}"
                    r = session.get(file_url)
                    if r.status_code == 200:
                        with open(file_path, "wb") as f:
                            f.write(r.content)
                        print("downloaded")
                    else:
                        print(
                            f"\n{r.status_code}: Failed to get file "
                            f"{fmd['file_id']} for {state_id}."
                        )
                        continue
                    aod_md['states'].append(state_id)
                    # break  # testing
            aod_md['states'] = sorted(set(aod_md['states']))
            with open(f"{aod_path}/metadata.json", "w") as f:
                json.dump(aod_md, f, indent=4)
            print('done')
        else:
            print(f"Failed to get file list for as of date {as_of_date}")
            return


if __name__ == "__main__":
    headers = {
        "username": BDC_USERNAME,
        "hash_value": BDC_HASH_VALUE,
    }
    destination = "data/raw/bdc/challenge/fixed_resolved"

    download_resolved_fixed_challenge_data(
        headers=headers,
        destination=destination,
    )
