# qwi_scraper_condensed.py
import urllib2
import re
import string
import gzip
import csv
import gc
from StringIO import StringIO

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
download_folder='/Users/aclemens/Desktop/QWI_download/'
drop_conditions={0:'Q',2:'S',4:'S',6:'A05',7:'0',9:'A0',10:'A0',11:'E0'}
time_conditions={'start':[1,2005],'end':[2,2015]}

def scrape():
	master_data=[]

	for state in states:
		gc.collect()
		data=[]

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
			dataset=[row for row in dataset]

			data.append(dataset[0])

			if len(master_data)==0:
				master_data.append(dataset[0])

			for key in drop_conditions.keys():
				dataset=[row for row in dataset if row[key]==drop_conditions[key]]

			for row in dataset:
				year=int(row[14])
				quarter=int(row[15])
				if year>=time_conditions['start'][1] and year<=time_conditions['end'][1]:
					if year==time_conditions['start'][1] and quarter>=time_conditions['start'][0]:
						data.append(row)
						master_data.append(row)
					if year==time_conditions['end'][1] and quarter<=time_conditions['end'][0]:
						data.append(row)
						master_data.append(row)
					if year!=time_conditions['end'][1] and year!=time_conditions['start'][1]:
						data.append(row)
						master_data.append(row)

			# Save to disk.
			save_file=download_folder+'qwi_%s_%s_%s_%s_%s_u.csv' % (state,demog,firm_abrev,industry,ownership)
			with open(save_file,'wb') as csvfile:
				writer=csv.writer(csvfile)
				for row in data:
					writer.writerow(row)

		except:
			pass

	master_file=download_folder+'qwi_ALL_%s_%s_%s_%s_u.csv' % (demog,firm_abrev,industry,ownership)
	with open(master_file,'wb') as csvfile:
		writer=csv.writer(csvfile)
		for row in master_data:
			writer.writerow(row)