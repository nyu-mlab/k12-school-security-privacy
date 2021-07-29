import requests
import json
import urllib
import os
import time
from bs4 import BeautifulSoup


state_ID_dict = {"Alabama": "01", "Alaska": "02", "Arizona": "04", "Arkansas": "05","California": "06","Colorado": "08",
  "Connecticut": "09","Delaware": "10", "District of Columbia": "11", "Florida": "12","Georgia": "13","Hawaii": "15","Idaho": "16","Illinois": "17",
  "Indiana": "18","Iowa": "19","Kansas": "20","Kentucky": "21","Louisiana": "22","Maine": "23","Maryland": "24",
  "Massachusetts": "25","Michigan": "26","Minnesota": "27","Mississippi": "28","Missouri": "29","Montana": "30",
  "Nebraska": "31","Nevada": "32","New Hampshire": "33","New Jersey": "34","New Mexico": "35","New York": "36",
  "North Carolina": "37","North Dakota": "38","Ohio": "39","Oklahoma": "40","Oregon": "41","Pennsylvania": "42",
  "Rhode Island": "44","South Carolina": "45","South Dakota": "46","Tennessee": "47","Texas": "48","Utah": "49",
  "Vermont": "50","Virginia": "51","Washington": "53","West Virginia": "54","Wisconsin": "55","Wyoming": "56", "American Somaoa": "60", 
  "Bureau of Indian Education": "59", "Guam": "66" , "North Mariana": "69", "Puerto Rico": "72", "Virgin Islands": "78"}


def download_xls_NCES(state_name):

	state_code = state_ID_dict[state_name]

	# check local cache whether already dowloaded

	cached_excel= "/Users/voronica/Desktop/SchoolScraper/NCES_downloadXls/StateExcel/" + state_name + "_" + state_code + ".xls"

	try:
		with open(cached_excel, 'r') as fp:
			return
	except IOError:
		pass


	
	cookies = {
	    'ASPSESSIONIDCCRCBBCQ': 'JLMEPBIBKKFBNHNBDLBINJDO',
	    '_ga': 'GA1.3.659049883.1624634889',
	    'ASPSESSIONIDCQRBBCRC': 'CABCKJFDPKIANKHPPNIGLLEB',
	    '_gid': 'GA1.3.2099687032.1626379835',
	    'ASPSESSIONIDAQRBBDQD': 'KLFCHJFDLKNJLNKGEFOBBPDH',
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
	    'Referer': 'https://nces.ed.gov/ccd/schoolsearch/index.asp',
	    'Accept-Language': 'en,zh-CN;q=0.9,zh;q=0.8,ko;q=0.7',
	    
	}
	params = (
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

	response = requests.get('https://nces.ed.gov/ccd/schoolsearch/school_list.asp', headers=headers, params=params, cookies=cookies)

	soup = BeautifulSoup(response.text, 'html.parser')

	# extract excel ID
	excel_ID = soup.select('input[name="filename"]')[0]["value"]

	print("getting state: ", state_name)

	# download excel from NCES to StateExcel folder

	dls = "https://nces.ed.gov/tempfiles/excelcreator/ncesdata_" + excel_ID + ".xls"

	

	urllib.request.urlretrieve(dls, cached_excel)
	time.sleep(3)


##############################################################################
# driver code

for state in state_ID_dict:

	try:
		download_xls_NCES(state)
		#time.sleep(3)


	except Exception as e:
		print("Error: ", e)

