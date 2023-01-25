"""

"""

import json
import requests
import os
import concurrent.futures
from itertools import repeat
from datetime import datetime

api_key = os.environ.get('TMDB_API_KEY')

uri = 'https://api.themoviedb.org/3/tv/{tv_id}'
auth_uri = uri + '?api_key={}'.format(api_key)
full_url = auth_uri + \
    '&append_to_response={}'.format('watch/providers,content_ratings')
headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Connection': 'close'
}

s = requests.Session()
s.headers.update(headers)

# Function that returns results for a page
# In order to speed up the ingestion process, this function will used in
# multi-threaded calls.


def func(session, uri, tv_id,):
    final_url = uri.format(tv_id=tv_id)
    return session.request('GET', final_url).json()


# run SELECT DISTINCT "tv_id" FROM fact_table
tv_ids = [1399]
# Use a with statement to ensure threads are cleaned up promptly
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    # Although TMDb has no rate limits, this is merely a personal project,
    # therefore the number of workers will be capped at 8
    show_results = executor.map(
        func,
        repeat(s),
        repeat(full_url),
        tv_ids
    )

# create a dictionary to store all 25 pages in the "results" list
# the "ingest_date" key to track historical data
now = datetime.now()
ingest_date = now.strftime("%Y-%m-%d")

# finally, dump the formatted contents in JSON file
with open('details.json', 'w') as f:
    for show in show_results:
        show_details['results'].append(show)
        f.write(json.dumps(show_details, indent=2))
