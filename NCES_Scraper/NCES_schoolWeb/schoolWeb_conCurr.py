import requests
import pandas as pd
import json
import urllib
import os
import time
import pickle as pikle
from bs4 import BeautifulSoup
import json
import threading




class School:
	def __init__(self, state, page, ID):
		self.state = state
		self.page = page
		self.ID = ID


state_ID_dict = {"Alabama": "01", "Alaska": "02", "Arizona": "04", "Arkansas": "05","California": "06","Colorado": "08",
  "Connecticut": "09","Delaware": "10", "District of Columbia": "11", "Florida": "12","Georgia": "13","Hawaii": "15","Idaho": "16","Illinois": "17",
  "Indiana": "18","Iowa": "19","Kansas": "20","Kentucky": "21","Louisiana": "22","Maine": "23","Maryland": "24",
  "Massachusetts": "25","Michigan": "26","Minnesota": "27","Mississippi": "28","Missouri": "29","Montana": "30",
  "Nebraska": "31","Nevada": "32","New Hampshire": "33","New Jersey": "34","New Mexico": "35","New York": "36",
  "North Carolina": "37","North Dakota": "38","Ohio": "39","Oklahoma": "40","Oregon": "41","Pennsylvania": "42",
  "Rhode Island": "44","South Carolina": "45","South Dakota": "46","Tennessee": "47","Texas": "48","Utah": "49",
  "Vermont": "50","Virginia": "51","Washington": "53","West Virginia": "54","Wisconsin": "55","Wyoming": "56", "American Somaoa": "60", 
  "Bureau of Indian Education": "59", "Guam": "66" , "North Mariana": "69", "Puerto Rico": "72", "Virgin Islands": "78"}

#state_ID_dict = {"Alaska": "02"}



global_lock = threading.Lock()
common_queue = []



def getStatePageNum(state_name):

	state_code = state_ID_dict[state_name]

	cookies_state = {
		    'ASPSESSIONIDCCRCBBCQ': 'JLMEPBIBKKFBNHNBDLBINJDO',
		    '_ga': 'GA1.3.659049883.1624634889',
		    'ASPSESSIONIDCQRBBCRC': 'CABCKJFDPKIANKHPPNIGLLEB',
		    '_gid': 'GA1.3.2099687032.1626379835',
		    'ASPSESSIONIDAQRBBDQD': 'KLFCHJFDLKNJLNKGEFOBBPDH',
		    '_gat_GSA_ENOR0': '1',
		    '_gat_GSA_ENOR1': '1',
		}

	headers_state = {
		    'Connection': 'keep-alive',
		    'Pragma': 'no-cache',
		    'Cache-Control': 'no-cache',
		    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
		    'sec-ch-ua-mobile': '?0',
		    'Upgrade-Insecure-Requests': '1',
		    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
		    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
		    'Sec-Fetch-Site': 'same-origin',
		    'Sec-Fetch-Mode': 'navigate',
		    'Sec-Fetch-User': '?1',
		    'Sec-Fetch-Dest': 'document',
		    'Referer': 'https://nces.ed.gov/ccd/schoolsearch/index.asp',
		    'Accept-Language': 'en,zh-CN;q=0.9,zh;q=0.8,ko;q=0.7',
		    
		}
	params_state = (
		    ('Search', '1'),
		    ('InstName', ''),
		    ('SchoolID', ''),
		    ('Address', ''),
		    ('City', ''),
		    ('State', state_code),     # changes for each state 
		    ('Zip', ''),
		    ('Miles', ''),
		    ('County', ''),
		    ('PhoneAreaCode', ''),
		    ('Phone', ''),
		    ('DistrictName', ''),
		    ('DistrictID', ''),
		    ('SchoolType', ['1', '2', '3', '4']),
		    ('SpecificSchlTypes', 'all'),
		    ('IncGrade', '-1'),
		    ('LoGrade', '-1'),
		    ('HiGrade', '-1'),
		)

	response = requests.get('https://nces.ed.gov/ccd/schoolsearch/school_list.asp', headers=headers_state, params=params_state, cookies=cookies_state)
	soup = BeautifulSoup(response.text, 'html.parser')

	#extract page range
	#page will range from 1 to page_max
	page_max = soup.select('Table[width="100%"][border="0"][cellspacing="0"][cellpadding="0"]')[-1].text.split()[3]

	print("State ", state_name, "has maximum ", page_max, " pages to scrape")

	return(int(page_max))



def requestSchoolUrl_perpage(curr_state, curr_page):

	#currPage_schoolInfo is a list with schoolID as key and the school's page as value
	currPage_schoolInfo = []

	cache_filename = os.path.join('cached', curr_state + '_page' + curr_page) 

	try:
		with open(cache_filename, 'r') as fp:
			# read directly from cache
			soup = BeautifulSoup(fp)

			numUrls_perPage = len(soup.select('a[href*="school_detail.asp"]'))

			for i in range(numUrls_perPage):
				schoolRequestUrl = soup.select('a[href*="school_detail.asp"]')[i]["href"]
				lst = schoolRequestUrl.split('&')
		
				schoolID = lst[-1][3:]
				print("State: ", curr_state, "pageNum: ", curr_page, "schoolID: ", schoolID)

				currPage_schoolInfo.append(schoolID)

			return currPage_schoolInfo


	except IOError:
		pass
		

	state_code = state_ID_dict[curr_state]

	cookies_page = {
	    'ASPSESSIONIDCCRCBBCQ': 'JLMEPBIBKKFBNHNBDLBINJDO',
	    '_ga': 'GA1.3.659049883.1624634889',
	    'ASPSESSIONIDCQRBBCRC': 'CABCKJFDPKIANKHPPNIGLLEB',
	    'ASPSESSIONIDAQRBBDQD': 'KLFCHJFDLKNJLNKGEFOBBPDH',
	    'ASPSESSIONIDCCDQSQSR': 'PEBFFFMAPNBFBKNNEGCPGJJI',
	    'ASPSESSIONIDCCBSTQTQ': 'GCLMHFMAAGFICACLMODMIOIC',
	    'ASPSESSIONIDACDQRQTR': 'KELMOBIDLKGEDOOGEFGCKAEF',
	    '_gid': 'GA1.3.1468487949.1626804363',
	}

	headers_page = {
	    'Connection': 'keep-alive',
	    'Pragma': 'no-cache',
	    'Cache-Control': 'no-cache',
	    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
	    'sec-ch-ua-mobile': '?0',
	    'Upgrade-Insecure-Requests': '1',
	    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
	    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
	    'Sec-Fetch-Site': 'none',
	    'Sec-Fetch-Mode': 'navigate',
	    'Sec-Fetch-User': '?1',
	    'Sec-Fetch-Dest': 'document',
	    'Accept-Language': 'en,zh-CN;q=0.9,zh;q=0.8,ko;q=0.7',
	}

	params_page = (
	    ('Search', '1'),
	    ('InstName', ' '),
	    ('State', state_code),
	    ('SchoolType', ['1', '2', '3', '4']),
	    ('SpecificSchlTypes', 'all'),
	    ('IncGrade', '-1'),
	    ('LoGrade', '-1'),
	    ('HiGrade', '-1'),
	    ('SchoolPageNum', curr_page),
	)

	response = requests.get('https://nces.ed.gov/ccd/schoolsearch/school_list.asp', headers=headers_page, params=params_page, cookies=cookies_page)
	
	soup = BeautifulSoup(response.text, 'html.parser')
	#print(soup)

	with open(cache_filename, 'w') as fp:
		fp.write(soup.prettify())

	# extract school urls

	numUrls_perPage = len(soup.select('a[href*="school_detail.asp"]'))

	for i in range(numUrls_perPage):
		schoolRequestUrl = soup.select('a[href*="school_detail.asp"]')[i]["href"]
		lst = schoolRequestUrl.split('&')
		
		schoolID = lst[-1][3:]
		print("State: ", curr_state, "pageNum: ", curr_page, "schoolID: ", schoolID)

		currPage_schoolInfo.append(schoolID)

	return currPage_schoolInfo


def requestSchoolWeb(curr_state, curr_page, ID):




	state_code = state_ID_dict[curr_state]

	


	cookies = {
	    'ASPSESSIONIDCCRCBBCQ': 'JLMEPBIBKKFBNHNBDLBINJDO',
	    '_ga': 'GA1.3.659049883.1624634889',
	    'ASPSESSIONIDCQRBBCRC': 'CABCKJFDPKIANKHPPNIGLLEB',
	    'ASPSESSIONIDAQRBBDQD': 'KLFCHJFDLKNJLNKGEFOBBPDH',
	    'ASPSESSIONIDCCDQSQSR': 'PEBFFFMAPNBFBKNNEGCPGJJI',
	    'ASPSESSIONIDCCBSTQTQ': 'GCLMHFMAAGFICACLMODMIOIC',
	    'ASPSESSIONIDACDQRQTR': 'KELMOBIDLKGEDOOGEFGCKAEF',
	    '_gid': 'GA1.3.1468487949.1626804363',
	    'ASPSESSIONIDSSSCADCQ': 'DDFJLGPDPLGOMPCKPGIJCNFC',
	    '_gat_GSA_ENOR0': '1',
	    '_gat_GSA_ENOR1': '1',
	}

	headers = {
	    'Connection': 'keep-alive',
	    'Pragma': 'no-cache',
	    'Cache-Control': 'no-cache',
	    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
	    'sec-ch-ua-mobile': '?0',
	    'Upgrade-Insecure-Requests': '1',
	    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
	    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
	    'Sec-Fetch-Site': 'same-origin',
	    'Sec-Fetch-Mode': 'navigate',
	    'Sec-Fetch-User': '?1',
	    'Sec-Fetch-Dest': 'document',
	    'Referer': 'https://nces.ed.gov/ccd/schoolsearch/school_list.asp?Search=1&InstName=+&State=01&SchoolType=1&SchoolType=2&SchoolType=3&SchoolType=4&SpecificSchlTypes=all&IncGrade=-1&LoGrade=-1&HiGrade=-1&SchoolPageNum=2',
	    'Accept-Language': 'en,zh-CN;q=0.9,zh;q=0.8,ko;q=0.7',
	}

	params = (
	    ('Search', '1'),
	    ('InstName', ' '),
	    ('State', state_code),
	    ('SchoolType', ['1', '2', '3', '4']),
	    ('SpecificSchlTypes', 'all'),
	    ('IncGrade', '-1'),
	    ('LoGrade', '-1'),
	    ('HiGrade', '-1'),
	    ('SchoolPageNum', curr_page),
	    ('ID', ID),
	)

	
	response = requests.get('https://nces.ed.gov/ccd/schoolsearch/school_detail.asp', headers=headers, params=params, cookies=cookies)
	
	soup = BeautifulSoup(response.text, 'html.parser')

	schoolWeb = soup.select('a[href*="transfer.asp"]')
	
	# check whether schoolweb exists
	if len(schoolWeb) == 0:
		err = "NA"
		return err

	else:
		web = schoolWeb[0].text
		return web
		



def thread():
	while True:
		
		# print("Entering Lock")
		while common_queue == []:
			time.sleep(1)
		new_school = common_queue.pop(0)

		schoolWeb = requestSchoolWeb(new_school.state, new_school.page, new_school.ID)
		print("State: ", new_school.state, "School ID: ", new_school.ID, "School Web: ", schoolWeb)

		# df = pd.DataFrame(columns=['State', 'SchoolID', 'SchoolWeb'])
		# newRow = {'State': new_school.state, 'SchoolID': new_school.ID, 'SchoolWeb': schoolWeb}
		# df.append(newRow, ignore_index = True)
		schoolWeb_filename = os.path.join('schoolWeb_csv', new_school.state + '_schoolWeb' + '.csv')

		csv_newline = new_school.state + "," + new_school.ID + "," + schoolWeb
		with open(schoolWeb_filename, 'a+') as fp:
			fp.write(csv_newline + "\n")

		
		#df.to_csv(schoolWeb_filename, index = False)





def main():
    for _ in range(10):
    	th = threading.Thread(target = thread)
    	th.daemon = True
    	th.start()

    for state in state_ID_dict:
    	print("Hello")
    	schoolWeb_filename = os.path.join('schoolWeb_csv', state + '_schoolWeb' + '.csv')
    	newline = "State" + "," + "ID" + "," + "Website"
    	with open(schoolWeb_filename, 'w+') as fp:
    		fp.write(newline + "\n")

    	maxPage = getStatePageNum(state)

    	for curr_page in range(1, maxPage+1):
    		# currState_Info is a list with schoolID as key and the school's page as value
    		currPage_Info = requestSchoolUrl_perpage(state, str(curr_page))

    		for ID in currPage_Info:
    			with global_lock:
	    			newSchool = School(state, curr_page, ID)
	    			common_queue.append(newSchool)
	    			print("Appending to queue: ", newSchool.state, newSchool.page, newSchool.ID)

    while True:
    	with global_lock:
    		if common_queue == []:
    			return
    		time.sleep(5)



if __name__ == "__main__":
    main()
















