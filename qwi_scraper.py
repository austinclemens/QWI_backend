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
	codebook_finder=re.compile('<a href="(.*?.csv.gz)">')

	for state in states:
		try:
			url=base_url+state.lower()+'/latest_release/'
			page=urllib2.urlopen(url).read()
			directories=directory_finder.findall(page)
			
		except:
			pass


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





