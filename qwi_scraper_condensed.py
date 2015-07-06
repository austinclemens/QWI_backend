# qwi_scraper_condensed.py
from __future__ import division
import urllib2
import re
import string
import gzip
import csv
from StringIO import StringIO
import pandas as pd
import os

# Full path to example file: 
# lehd.ces.census.gov/pub/ak/latest_release/DVD-sa_f/qwi_ak_sa_f_gc_ns_op_u.csv.gz
# With variable replacements:
# lehd.ces.census.gov/pub/STATE/latest_release/BASEFOLDER/qwi_STATE_DEMOG_FIRMABREV_INDUSTRY_OWNERSHIP_u.csv.gz

# Change these parameters for different downloads
base_folder='DVD-sa_f'
geog='gc'
firm_abrev='f'
demog='sa'
industry='ns'
ownership='op'

states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
state_codes = {
    'WA': '53', 'DE': '10', 'DC': '11', 'WI': '55', 'WV': '54', 'HI': '15',
    'FL': '12', 'WY': '56', 'PR': '72', 'NJ': '34', 'NM': '35', 'TX': '48',
    'LA': '22', 'NC': '37', 'ND': '38', 'NE': '31', 'TN': '47', 'NY': '36',
    'PA': '42', 'AK': '2', 'NV': '32', 'NH': '33', 'VA': '51', 'CO': '8',
    'CA': '6', 'AL': '1', 'AR': '5', 'VT': '50', 'IL': '17', 'GA': '13',
    'IN': '18', 'IA': '19', 'MA': '25', 'AZ': '4', 'ID': '16', 'CT': '9',
    'ME': '23', 'MD': '24', 'OK': '40', 'OH': '39', 'UT': '49', 'MO': '29',
    'MN': '27', 'MI': '26', 'RI': '44', 'KS': '20', 'MT': '30', 'MS': '28',
    'SC': '45', 'KY': '21', 'OR': '41', 'SD': '46'
}
ind_codes = {
	'11':'Agriculture, Forestry, Fishing and Hunting','21':'Mining, Quarrying, and Oil and Gas Extraction','22':'Utilities','23':'Construction','31-33':'Manufacturing',
	'42':'Wholesale Trade','44-45':'Retail Trade','48-49':'Transportation and Warehousing','51':'Information','52':'Finance and Insurance',
	'53':'Real Estate and Rental and Leasing','54':'Professional, Scientific, and Technical Services','55':'Management of Companies and Enterprises',
	'56':'Administrative and Support and Waste Management and Remediation Services','61':'Educational Services','62':'Health Care and Social Assistance',
	'71':'Arts, Entertainment, and Recreation','72':'Accomodation and Food Services','81':'Other Services','92':'Public Administration'
}
download_folder = '/Users/aclemens/Desktop/QWI_download/'
drop_conditions = {0:'Q',2:'S',4:'S',6:'A05',7:'0',9:'A0',10:'A0',11:'E0'}
time_conditions = {'start':2010,'end':2015}

def scrape():
	master_data=[]

	for state in states:
		try:
			state=state.lower()
			state_url='http://lehd.ces.census.gov/pub/%s/latest_release/%s/qwi_%s_%s_%s_%s_%s_%s_u.csv.gz' % (state,base_folder,state,demog,firm_abrev,geog,industry,ownership)
			print state_url
			dataset=StringIO(urllib2.urlopen(state_url).read())
			dataset=gzip.GzipFile(fileobj=dataset,mode='rb')
			dataset=dataset.read()
			dataset=dataset.split('\n')
			dataset=[row.split(',') for row in dataset]
			dataset=[row for row in dataset if len(row)==80]
			header=dataset[0]
			dataset=pd.DataFrame(dataset[1:],columns=dataset[0])

			if len(master_data)==0:
				master_data=pd.DataFrame(master_data,columns=header)

			for key in drop_conditions.keys():
				dataset=dataset[dataset.iloc[:,key]==drop_conditions[key]]

			dataset['year']=dataset['year'].astype(int)
			dataset=dataset[dataset.iloc[:,14]>=time_conditions['start']]
			dataset=dataset[dataset.iloc[:,14]<=time_conditions['end']]
			master_data.append(dataset)

			# Save to disk.
			save_file=download_folder+'qwi_%s_%s_%s_%s_%s_u.csv' % (state,demog,firm_abrev,industry,ownership)
			print save_file
			dataset.to_csv(save_file)

			for row in dataset:
				master_data.append(row)

		except:
			pass

	master_file=download_folder+'qwi_ALL_%s_%s_%s_%s_u.csv' % (demog,firm_abrev,industry,ownership)
	master_data.to_csv(master_file)


def combine():
	master_data=[]
	paths=os.listdir(download_folder)
	for path in paths:
		if path[-3:]=='csv':
			full_path=download_folder+path
			print full_path
			with open(full_path,'rb') as csvfile:
				reader=csv.reader(csvfile)
				rows=[row for row in reader]
				for row in rows[1:]:
					master_data.append(row)
	master_file=download_folder+'qwi_ALL_%s_%s_%s_%s_u.csv' % (demog,firm_abrev,industry,ownership)
	with open(master_file,'wb') as csvfile:
		writer=csv.writer(csvfile)
		for row in master_data:
			writer.writerow(row)

def create_js_dict(file):
	with open(file, 'rU') as csvfile:
		reader=csv.reader(csvfile)
		rows=[row for row in reader]

	for row in rows[1:]:
		for key in state_codes.keys():
			if state_codes[key]==str(row[1]):
				row[1]=key
		for key in ind_codes.keys():
			if row[2]==key:
				row[2]=ind_codes[key]
		row.append('Q'+str(row[5])+' '+str(row[4]))

	js_dict={}
	# Create basic data structure
	# after this it looks like:
	# {'Mining':{'Q3 2014':[],
	#            'Q4 2014':[],
	#			  etc.. }}
	for row in rows[1:]:
		wages=row[33]
		if row[2] not in js_dict.keys():
			js_dict[row[2]]={}
		if row[-1] not in js_dict[row[2]].keys():
			js_dict[row[2]][row[-1]]=[]

	for key1 in js_dict.keys():
		for key2 in js_dict[key1].keys():
			temp_rows=[]
			for row in rows:
				if row[2]==key1 and row[-1]==key2:
					temp_rows.append(row)

			temp_rows2=[row for row in temp_rows if row[6]!='' and row[33]!='' and row[11]!='' and row[14]!='']
			temp_rows=temp_rows2

			# sum up all earnbeg weighted by employment and sum employment
			w_earnbeg=0
			employment=0
			for row in temp_rows:
				employment=employment+int(row[6])
				w_earnbeg=w_earnbeg+(int(row[6])*int(row[33]))
			average_wages=w_earnbeg/employment
			average_wages=int("{:.0f}".format(average_wages))

			js_dict[key1][key2].append(average_wages)

			# Now get overall turnover - (HirA+Sep)/(2*Emp)
			numerator=0
			denominator=0
			for row in temp_rows:
				numerator=numerator+int(row[11])+int(row[14])
				denominator=denominator+(2*int(row[6]))
			average_turnover=numerator/denominator
			average_turnover=float("{:.3f}".format(average_turnover))

			js_dict[key1][key2].append(average_turnover)
			js_dict[key1][key2].append({})
			for row in temp_rows:
				wages=int(row[33])
				turnover=(int(row[11])+int(row[14]))/(2*int(row[6]))
				turnover=float("{:.3f}".format(turnover))
				js_dict[key1][key2][2][row[1]]=[wages,turnover]

	return js_dict



