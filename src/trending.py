"""
Trending shows are sorted from most trendy to least trendy.
In other words, page 1 results are more trendy than page 20 results.

Each page stores 20 results.
Max pages = 1000

For simplicity's sake, fetch the top 500 trending shows each week.
Which means 25 pages need to be requested.
"""

import json
import requests
import os
import concurrent.futures
from itertools import repeat
from datetime import datetime

api_key = os.environ.get('TMDB_API_KEY')

uri = 'https://api.themoviedb.org/3/trending/tv/week'
auth_uri = uri + '?api_key={}'.format(api_key)
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
def func(session, auth_uri,  page_num):
    full_url = auth_uri + '&page={}'.format(page_num)
    return session.request('GET', full_url).json()['results']


# first 25 pages == top 500 trending shows
page_numbers = range(1, 26)
# Use a with statement to ensure threads are cleaned up promptly
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    # Although TMDb has no rate limits, this is merely a personal project,
    # therefore the number of workers will be capped at 8
    page_result = executor.map(
        func,
        repeat(s),
        repeat(auth_uri),
        page_numbers
    )

# create a dictionary to store all 25 pages in the "results" list
# the "ingest_date" key to track historical data
now = datetime.now()
ingest_date = now.strftime("%Y-%m-%d")

# finally, dump the formatted contents in JSON file
with open('{}_trending-shows.json'.format(ingest_date), 'w') as f:
    for page in page_result:
        f.write('\n'.join([json.dumps(show) for show in page]))
