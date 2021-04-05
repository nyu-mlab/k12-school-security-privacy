import pandas as pd
from bs4 import BeautifulSoup
import urllib.request
import re
import hashlib
import os
import json
import datetime
import random
import threading
import tldextract
import subprocess


subprocess.call(['mkdir', '-p', 'raw-data'])


# Data from https://k12cybersecure.com/2019-year-in-review/
BASE_PATH = 'https://hdanny.org/static/private_data/k12/'

# A dataframe of "School District Name" and "Website"
raw_district_df = pd.read_excel(BASE_PATH + 'SchoolDistrictswIncidents.xlsx').fillna('')


lock = threading.Lock()


THREAD_COUNT = 40


def main():

    scrape_queue = []

    visited_url = set()

    # Pre-populate the queue
    for (_, row) in raw_district_df.iterrows():
        scrape_queue += [{
            'base_school_name': row['School District Name'],
            'base_school_website': row['Website'],
            'visit_url': row['Website'],
            'depth': 0
        }]

    # Start multiple threads
    thread_list = []
    for _ in range(THREAD_COUNT):
        th = threading.Thread(target=process_queue, args=(scrape_queue, visited_url))
        th.daemon = True
        th.start()
        thread_list.append(th)

    # Wait for all threads to complete
    for th in thread_list:
        th.join()


def process_queue(scrape_queue, visited_url):

    # Go through the entire queue
    while True:

        with lock:
            if len(scrape_queue) == 0:
                return
            random.shuffle(scrape_queue)
            info = scrape_queue.pop(0)
        
        # Stop processing beyond Depth = 10
        if info['depth'] == 10:
            continue

        # Skip if we have scraped this URL before
        with lock:
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

        try:
            page = parse_page(html)
        except Exception:
            continue

        with lock:
            visited_url.add(info['visit_url'])
        
        # Save the info to disk
        info['title'] = page['title']
        info['timestamp'] = str(datetime.datetime.now())
        with lock:
            with open('output.json', 'a') as fp:
                print(json.dumps(info), file=fp)

        # Explore more links only if the current page shares the same domain as the district's
        district_domain = tldextract.extract(info['base_school_website']).registered_domain
        visit_domain = tldextract.extract(info['visit_url']).registered_domain
        if visit_domain != district_domain:
            continue

        for link in page['links']:
            with lock:
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

    # Add headers
    req = urllib.request.Request(url)
    req.add_header('Referer', 'https://www.google.com/')
    req.add_header('User-Agent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36')
    
    # Visit the site
    with urllib.request.urlopen(req, timeout=20) as response:
        html = response.read()  

    # Cache to disk
    with open(cached_path, 'w') as fp:
        fp.write(html.decode('utf-8'))

    print('Fetched URL:', url)

    return html



if __name__ == '__main__':
    main()