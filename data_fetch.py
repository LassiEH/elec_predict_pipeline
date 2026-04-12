from datetime import datetime, timedelta, time

import time as t
import requests
import os

api_key = os.environ.get("FINGRID_API_KEY")

# dataset 124 = electricity consumption
req_url = "https://data.fingrid.fi/api/datasets/124/data"

# ISO 8601 date, YYYY-MM-DDTHH:mm:ss
# fetch daily
midnight_t = datetime.combine(datetime.today(), time.min)
midnight_y = midnight_t - timedelta(days=2)
today = midnight_t.isoformat()
yesterday = midnight_y.isoformat()

def fetch_data():

    all_data = []

    page = 1

    headers = {'x-api-key': api_key}

    while True:
        payload = {
            'startTime': yesterday, 
            'endTime': today, 
            'page': page, 
            'pageSize': 100
        }

        r = requests.get(req_url, headers=headers, params=payload)
    
        if r.status_code == 429:
            # rate limit check
            t.sleep(2)
            continue

    
        data = r.json()
        records = data.get('data', [])
        pagination = data.get('pagination', {})

        if not records:
            break

        all_data.extend(records)

        if page >= pagination.get("lastPage", 1):
            break

        page += 1

    return all_data
