import urllib2
import re
import string
import MySQLdb
import gzip
import csv
from StringIO import StringIO

# base_url and state list control url formation. Shouldn't be necessary to change these
# unless the server address changes or something crazy happens.
base_url='http://lehd.ces.census.gov/pub/'
states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]


def get_changedates():
	# Iterrate through states and get a last modified date for each one. If no file of such
	# dates exists already, create one. If the file does exist, compare and if the date of
	# last modification on record does not match the newly scraped last modified date, update
	# the entire state.
	url='http://lehd.ces.census.gov/pub/'
	
	for state in states:
		pass


def scrape_base():
	# Iterrate through states. Each state directory is the base URL + lowercase state abrev
	# Always takes the 'latest_release' directory. This step gets the basic data.
	master_csv=[]

	for state in states:
		state_data=scrape_state(state)

		for row in state_data:
			master_csv.append(row)

	# The next step is to drop any existing SQL table and create a new one from scratch. This
	# allows the db to adapt to any changes made to the dataset. The Census provides column
	# names and coding for each variable.
	directory_finder=re.compile('<img src="/icons/folder\.gif" alt="\[DIR\]"></td><td><a href="(.*?)">')
	codebook_finder=re.compile('<a href="(.*?\.csv\.gz)">')
	label_finder=re.compile('<a href="(label_.*?\.csv)">')

	# Find the docs - these are in state files and we only need to find them once. Sometimes states
	# are not present, hence the try block. Break as soon as we find what we need.
	for state in states:
		try:
			url=base_url+state.lower()+'/latest_release/'
			page=urllib2.urlopen(url).read()
			directories=directory_finder.findall(page)

			dir_url=url+directories[0]
			column_defs_url=dir_url+'column_definitions.txt'
			column_defs=urllib2.urlopen(column_defs_url).read()
			column_defs=column_defs.split('\n')
			final_defs=[]

			# If labeling is broken, this is a prime candidate. The values below are based on the
			# fixed widths in a current column_definitions.txt file. If the widths in that file were
			# to change it would affect labelling and possibly SQL table creation. This could maaaybe
			# be made robust to such changes - a method that uses .split, drops spaces, and then 
			# rejoins the label.
			for column_def in column_defs:
				final_def=[column_def[0:14].strip(),column_def[14:29].strip(),column_def[29:37].strip(),column_def[37:]]
			
			# 
			label_files=label_finder.findall(page)


		except:
			pass

	# Finally, populate the SQL database with master_csv.


def scrape_state(state):
	# Passes in a base URL for a state. Returns a list of all data for the state.
	directory_finder=re.compile('<img src="/icons/folder\.gif" alt="\[DIR\]"></td><td><a href="(.*?)">')
	data_finder=re.compile('<a href="(.*?.csv.gz)">')

	try:
		url=base_url+state.lower()+'/latest_release/'
		page=urllib2.urlopen(url).read()
		directories=directory_finder.findall(page)
	except:
		print 'Data does not exist for %s' % state
		directories=[]

	data=[]

	for dir in directories:
		dir_url=url+dir
		dir_page=urllib2.urlopen(dir_url).read()
		data_paths=data_finder.findall(dir_page)

		for dir_page in data_paths:
			data_url=dir_url+dir_page

			dataset=StringIO(urllib2.urlopen(dir_url).read())
			dataset=gzip.GzipFile(fileobj=dataset,mode='rb')
			dataset=dataset.read()
			dataset=dataset.split('\n')

			for row in dataset[1:]:
				data.append(row)

	return data





