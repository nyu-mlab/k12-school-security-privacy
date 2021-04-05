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
import sys


subprocess.call(['mkdir', '-p', 'raw-data'])


MAX_DEPTH = 5


# Data from https://k12cybersecure.com/2019-year-in-review/
BASE_PATH = 'https://hdanny.org/static/private_data/k12/'

# A dataframe of "School District Name" and "Website"
raw_district_df = pd.read_excel(BASE_PATH + 'SchoolDistrictswIncidents.xlsx').fillna('')


lock = threading.Lock()


def main():

    try:
        thread_count = int(sys.argv[1])
    except Exception:
        thread_count = 30

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

    # Resume from previous queue
    try:
        with open('queue.json') as fp:
            for line in fp:
                try:
                    q_element = json.loads(line.strip())                    
                except json.decoder.JSONDecodeError:
                    continue
            if q_element['depth'] < MAX_DEPTH:
                scrape_queue.append(q_element)
    except IOError:
        pass

    print('Queue length:', len(scrape_queue))

    # Start multiple threads
    thread_list = []
    for _ in range(thread_count):
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

            # Randomly select an element from the queue
            if len(scrape_queue) == 0:
                return
            info = scrape_queue.pop(random.randint(0, len(scrape_queue) - 1))
            queue_length = len(scrape_queue)

            # Skip if we have scraped this URL before
            if info['visit_url'] in visited_url:
                continue
        
        # Stop processing beyond Depth
        if info['depth'] >= MAX_DEPTH:
            continue

        # Ignore PDF docs
        if info['visit_url'].endswith('.pdf'):
            continue

        # Parse the page
        try:
            html = get_html(info['visit_url'], queue_length)
        except Exception:
            continue

        try:
            page = parse_page(html)
        except Exception:
            continue

        # Save the info to disk
        info['title'] = page['title']
        info['timestamp'] = str(datetime.datetime.now())
        with lock:
            visited_url.add(info['visit_url'])
            with open('output.json', 'a') as fp:
                print(json.dumps(info), file=fp)

        # Explore more links only if the current page shares the same domain as the district's
        district_domain = tldextract.extract(info['base_school_website']).registered_domain
        visit_domain = tldextract.extract(info['visit_url']).registered_domain
        if visit_domain != district_domain:
            continue

        # Stop exploring if reaching max depth
        if info['depth'] + 1 >= MAX_DEPTH:
            continue
        
        # Add links to queue
        with lock:
            with open('queue.json', 'a') as fp:
                for link in page['links']:          
                    if link in visited_url:
                        continue  
                    q_element = {
                        'base_school_name': info['base_school_name'],
                        'base_school_website': info['base_school_website'],
                        'visit_url': link,
                        'depth': info['depth'] + 1
                    }
                    scrape_queue.append(q_element)
                    print(json.dumps(q_element), file=fp)


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


def get_html(url, queue_length):
    """
    Obtains HTML from URL. If already visited, reads from disk.

    """
    cached_path = os.path.join('raw-data', hashlib.sha256(url.encode('utf-8')).hexdigest())

    # Read from cache
    try:
        with open(cached_path) as fp:
            print(f'{queue_length}: Cached URL:', url)
            return fp.read()
    except IOError:
        pass

    # Add headers
    req = urllib.request.Request(url)
    req.add_header('Referer', 'https://www.google.com/')
    req.add_header('User-Agent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36')
    
    # Visit the site
    with urllib.request.urlopen(req, timeout=10) as response:
        html = response.read()  

    # Cache to disk
    with open(cached_path, 'w') as fp:
        fp.write(html.decode('utf-8'))

    print(f'{queue_length}: Fetched URL:', url)

    return html



if __name__ == '__main__':
    main()
