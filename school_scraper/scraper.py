import pandas as pd
from bs4 import BeautifulSoup
import urllib.request
import requests
from urllib.request import urlopen
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
import waybackpy
from waybackpy import Url


subprocess.call(['mkdir', '-p', 'raw-data'])

year = datetime.datetime.now().year

MAX_DEPTH = 8

# Data from https://k12cybersecure.com/2019-year-in-review/
BASE_PATH = 'https://hdanny.org/static/private_data/k12/'
#BASE_PATH = '/home/kali/Documents/PhD/'

# A dataframe of "School District Name" and "Website"
raw_district_df = pd.read_excel(BASE_PATH + 'SchoolDistrictswIncidents.xlsx').fillna('')
#raw_district_df = pd.read_excel(BASE_PATH + 'SchoolDistrictswIncidents-trim.xlsx').fillna('')

lock = threading.Lock()


def main():

    try:
        thread_count = int(sys.argv[1])
    except Exception:
        thread_count = 50

    scrape_queue = []

    #visited_url = set()
    visited_url = []

    # Pre-populate the queue
    for (_, row) in raw_district_df.iterrows():
        for year in range(2018, 2022):
            scrape_queue += [{
                'base_school_name': row['School District Name'],
                'base_school_website': row['Website'],
                'visit_url': row['Website'],
                'depth': 0,
                'year': year
            }]

    #print(f'scrape queue:', scrape_queue)
	
    
    # Resume from previous queue
    try:
        with open('queue.json') as fp:
            for line in fp:
                try:
                    q_element = json.loads(line.strip())                    
                except json.decoder.JSONDecodeError:
                    continue
                else:
                    if q_element['depth'] < MAX_DEPTH:
                        scrape_queue.append(q_element)
    except IOError:
        pass
    

    #print('Queue length:', len(scrape_queue))

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
            urlinfo = (info['visit_url'],info['year'])
            #print(f"processing url info: {urlinfo}")

        # Skip if we have scraped this URL before
        if (info['visit_url'],info['year']) in visited_url:
            #print('visited already')
            continue
        
        # Stop processing beyond Depth
        if info['depth'] >= MAX_DEPTH:
            continue

        # Ignore PDF docs
        if info['visit_url'].endswith('.pdf'):
            continue

        # Ignore Telephone numbers
        if info['visit_url'].find('Tel:') != -1:
            continue

        # Parse the page
        try:
            #print(f"trying to get_html: {info['visit_url']} year: {info['year']}")
            html = get_html(info['visit_url'], queue_length, info['year'])
        except Exception:
            #print(f"get html failed: {info['visit_url']} year: {info['year']}")
            continue

        try:
            #print(f"parsing: {info['visit_url']} year: {info['year']}")
            page = parse_page(html)
        except Exception:
            #print(f"parsing failed for {info['visit_url']}")
            continue

        # Save the info to disk
        info['title'] = page['title']
        info['timestamp'] = str(datetime.datetime.now())
        with lock:
            #changed visited_url to a list of tuples
            visited_url.append((
                info['visit_url'],
                info['year']
                ))
            #print(f"visited url list: {visited_url}")
            #visited_url.add(info['visit_url'])
            with open('output.json', 'a') as fp:
                print(json.dumps(info), file=fp)


        # Explore more links only if the current page shares the same domain as the district's
        district_domain = tldextract.extract(info['base_school_website']).registered_domain
          
        try:
            s = info['visit_url']
            s2 = s.split('//') 
            start_domain = tldextract.extract(s2[2]).registered_domain
        except Exception:
            start_domain = tldextract.extract(info['visit_url']).registered_domain
        
        if start_domain != district_domain:
            #print(f'start_domain: {start_domain} not equal to district_domain:{district_domain}')
            continue


        # Stop exploring if reaching max depth
        if info['depth'] + 1 >= MAX_DEPTH:
            continue
        
        # Add links to queue
        with lock:
            with open('queue.json', 'a') as fp:
                for link in page['links']:          
                    #changed lookup to a tuple
                    #print(f'link: ', link)
                    if (link,info['year']) in visited_url:
                        continue  
                    q_element = {
                        'base_school_name': info['base_school_name'],
                        'base_school_website': info['base_school_website'],
                        'visit_url': link,
                        'depth': info['depth'] + 1,
                        #adding a year frame to the queue
                        'year': info['year']
                    }
                    scrape_queue.append(q_element)
                    print(json.dumps(q_element), file=fp)


def parse_page(html):
    """Returns a dictionary representation of an HTML page."""
    #debugging the parser
    #print(f'html parsing:', html)

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

    #print(f'links: ', links)
    return {
        'title': title,
        'links': links
    }


def get_html(url, queue_length, year):
    #Obtains HTML from URL. If already visited, reads from disk.

    if url.find('web.archive.org:') != -1:
        user_agent = 'User-Agent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'
        wayback = Url(url, user_agent)
        archive_version = wayback.near(year=year)
        url = archive_version.archive_url
    
    #print(f"wayback url: {url}")
    
    cached_path = os.path.join('raw-data', hashlib.sha256(url.encode('utf-8')).hexdigest())
    
    # Read from cache
    try:
        with open(cached_path) as fp:
            #print(f'{queue_length}: Cached URL:', archive_url)
            return fp.read()
    except IOError:
        pass
    
    #print(f"url: {url}")
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

    #print(f'{queue_length}: Fetched URL: {url}  year:{year}')

    return html


if __name__ == '__main__':
    main()
