import pandas as pd
from bs4 import BeautifulSoup
import urllib.request
import re
import hashlib
import os
import json
import datetime


# Data from https://k12cybersecure.com/2019-year-in-review/
BASE_PATH = 'https://hdanny.org/static/private_data/k12/'

# A dataframe of "School District Name" and "Website"
raw_district_df = pd.read_excel(BASE_PATH + 'SchoolDistrictswIncidents.xlsx')


def main():

    scrape_queue = []

    visited_url = set()

    # Pre-populate the queue
    for (_, row) in raw_district_df.head(10).iterrows():
        scrape_queue += [{
            'base_school_name': row['School District Name'],
            'base_school_website': row['Website'],
            'visit_url': row['Website'],
            'depth': 0
        }]

    # Go through the entire queue
    while scrape_queue:

        info = scrape_queue.pop(0)
        
        # Stop processing beyond Depth = 4
        if info['depth'] == 4:
            continue

        # Skip if we have scraped this URL before
        if info['visit_url'] in visited_url:
            continue

        # Ignore PDF docs
        if info['visit_url'].endswith('.pdf'):
            continue

        # Parse the page
        try:
            html = get_html(info['visit_url'])
        except Exception:
            continue
        page = parse_page(html)
        visited_url.add(info['visit_url'])
        
        # Save the info to disk
        info['title'] = page['title']
        info['timestamp'] = str(datetime.datetime.now())
        with open('output.json', 'a') as fp:
            print(json.dumps(info), file=fp)

        # Explore more links
        for link in page['links']:
            scrape_queue += [{
                'base_school_name': info['base_school_name'],
                'base_school_website': info['base_school_website'],
                'visit_url': link,
                'depth': info['depth'] + 1
            }]


def parse_page(html):
    """Returns a dictionary representation of an HTML page."""

    soup = BeautifulSoup(html, features="html.parser")

    # Extract title
    try:
        title = soup.find('title').get_text()
    except Exception:
        title = ''

    # Extract links
    links = set()
    for a_tag in soup.findAll('a', attrs={'href': re.compile("^http://")}):
        links.add(a_tag.get('href'))
    for a_tag in soup.findAll('a', attrs={'href': re.compile("^https://")}):
        links.add(a_tag.get('href'))   

    return {
        'title': title,
        'links': links
    }


def get_html(url):
    """
    Obtains HTML from URL. If already visited, reads from disk.

    """
    cached_path = os.path.join('raw-data', hashlib.sha256(url.encode('utf-8')).hexdigest())

    # Read from cache
    try:
        with open(cached_path) as fp:
            print('Cached URL:', url)
            return fp.read()
    except IOError:
        pass

    # Visit the site
    with urllib.request.urlopen(url) as response:
        html = response.read()  

    # Cache to disk
    with open(cached_path, 'w') as fp:
        fp.write(str(html))

    print('Fetched URL:', url)

    return html



if __name__ == '__main__':
    main()