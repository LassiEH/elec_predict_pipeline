from datetime import datetime, timedelta, time

import time as t
import requests
import os

api_key = os.environ.get("FINGRID_API_KEY")

# dataset 124 = electricity consumption
req_url = "https://data.fingrid.fi/api/datasets/124/data"

def fetch_data(start_time: datetime, end_time: datetime):

    all_data = []

    page = 1

    headers = {'x-api-key': api_key}

    while True:
        payload = {
            # ISO 8601 date, YYYY-MM-DDTHH:mm:ss

            'startTime': start_time.isoformat(), 
            'endTime': end_time.isoformat(), 
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
