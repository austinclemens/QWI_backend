import urllib2
import re
import string
import mysql.connector
import gzip
import csv
import gc
import multiprocessing
from joblib import Parallel, delayed
from StringIO import StringIO

# base_url and state list control url formation. Shouldn't be necessary to change these
# unless the server address changes or something crazy happens.
base_url='http://lehd.ces.census.gov/pub/'
# states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
states= ['AL']
SQL_dict={'user':'test','pass':'test'}


def get_changedates():
	# Iterrate through states and get a last modified date for each one. If no file of such
	# dates exists already, create one. If the file does exist, compare and if the date of
	# last modification on record does not match the newly scraped last modified date, update
	# the entire state.
	url='http://lehd.ces.census.gov/pub/'
	
	for state in states:
		pass


def scrape_base():
	# Find the docs - these are in state files and we only need to find them once. Sometimes states
	# are not present, hence the try block. Break as soon as we find what we need.
	directory_finder=re.compile('<img src="/icons/folder\.gif" alt="\[DIR\]"></td><td><a href="(.*?)">')
	codebook_finder=re.compile('<a href="(.*?\.csv\.gz)">')
	label_finder=re.compile('<a href="(label_.*?\.csv)">')

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
				if column_def[14:29].strip()=='C' or column_def[14:29].strip()=='N':
					final_def=[column_def[0:14].strip(),column_def[14:29].strip(),column_def[29:37].strip(),column_def[37:]]
					final_defs.append(final_def)

			# Now the label files. Set these up as a dict, where keys are variable (column) names
			# and values are lists of [value, label].
			master_label_dict={}
			label_files=label_finder.findall(page)

			for label in label_files:
				url=dir_url+label
				label_file=urllib2.urlopen(url)
				label_file=csv.reader(label_file)
				label_file=[row for row in label_file]
				dict[label_file[0][0]]=label_file[1:]

			# Once one good state has been found, this loop can break - files should be the same
			# for all states.
			break

		except:
			pass

	# Create SQL tables using the column definitions from the text file.
	create_tables(final_defs)

	# Set up the variable string for the insert statement below (just column names separated
	# by commas), and the insert string (same thing but for python insert operators)
	variable_string=''
	for column in final_defs:
		variable_string=variable_string+column[0]+', '
	variable_string=variable_string[0:-2]
	insert_string="%s, "*len(final_defs)
	insert_string=insert_string[0:-2]

	# Iterrate through states. Each state directory is the base URL + lowercase state abrev
	# Always takes the 'latest_release' directory. This step gets the basic data.
	directory_finder=re.compile('<img src="/icons/folder\.gif" alt="\[DIR\]"></td><td><a href="(.*?)">')
	data_finder=re.compile('<a href="(.*?.csv.gz)">')

	for state in states:
		try:
			url=base_url+state.lower()+'/latest_release/'
			page=urllib2.urlopen(url).read()
			directories=directory_finder.findall(page)
		except:
			print 'Data does not exist for %s' % state
			directories=[]

		for dir in directories:
			dir_url=url+dir
			dir_page=urllib2.urlopen(dir_url).read()
			data_paths=data_finder.findall(dir_page)
			data_paths=[dir_url+dir_page for dir_page in data_paths]

			num_cores=1
			# num_cores=multiprocessing.cpu_count()
			Parallel(n_jobs=num_cores)(delayed(get_file)(data_url,variable_string,insert_string) for data_url in data_paths)


def get_file(data_url,variable_string,insert_string):
	data=[]
	
	dataset=StringIO(urllib2.urlopen(data_url).read())
	dataset=gzip.GzipFile(fileobj=dataset,mode='rb')
	dataset=dataset.read()
	dataset=dataset.split('\n')

	for row in dataset[1:]:
		data.append(row.split(','))

	print data_url+' downloaded'

	data=[row for row in data if len(row)>2]
	upload_data(data,variable_string,insert_string)
	print data_url+' uploaded to SQL'


def upload_data(data,variable_string,insert_string):
	# This function sends data from a single file to the appropriate tables according to
	# geographic area.
	cnx=mysql.connector.connect(user=SQL_dict['user'], password=SQL_dict['pass'], host='127.0.0.1', database='QWI')
	cursor=cnx.cursor()

	insert_dict={}

	counties=[row for row in data if row[2]=='C']
	metro_micro=[row for row in data if row[2]=='M']
	national=[row for row in data if row[2]=='N']
	states=[row for row in data if row[2]=='S']
	workforce=[row for row in data if row[2]=='W']

	insert_dict['C']="INSERT INTO counties (%s) VALUES (%s)" % (variable_string,insert_string)
	insert_dict['M']="INSERT INTO metro_micro (%s) VALUES (%s)" % (variable_string,insert_string)
	insert_dict['N']="INSERT INTO national (%s) VALUES (%s)" % (variable_string,insert_string)
	insert_dict['S']="INSERT INTO states (%s) VALUES (%s)" % (variable_string,insert_string)
	insert_dict['W']="INSERT INTO workforce (%s) VALUES (%s)" % (variable_string,insert_string)

	print len(data)+"rows to insert"
	index=0

	for row in data:
		cursor.execute(insert_dict[row[2]],row)
		index=index+1
		print '\r'+str(index),

	gc.collect()
	cursor.close()
	cnx.close()


def create_tables(column_defs):
	# Drop the existing SQL table and create a new one based on the columns file. I set it up
	# this way so that if the data changes - and specifically if new columns are added - that can
	# be automatically accomodated by the tables. Not sure there's any point to just dropping the
	# rows... a new index has to be created either way.

	# There are tradeoffs here between simplicity and speed. The code below creates a separate
	# table for each geography. This will make queries a bit faster (and maybe indexing?) but if
	# more than one geography is desired a join will be necessary.

	# The geo levels are:
	#	C = 'Counties'
	#	M = 'Metropolitan/Micropolitan'
	#	N = 'National (50 States + DC)'
	#	S = 'States'
	#	W = 'Workforce Investment Areas'

	# Set up strings to create tables, specifying the variable name, type, and length.
	table_string=''
	for column in column_defs:
		if column[1]=='C':
			type='varchar(10)'
		if column[1]=='N':
			type='int(11)'

		table_string=table_string+("`"+column[0]+"` "+type+",")
	table_string=table_string[0:-1]

	# Set up the variable string for the insert statement below (just column names separated
	# by commas), and the insert string (same thing but for python insert operators)
	variable_string=''
	for column in column_defs:
		variable_string=variable_string+column[0]+', '
	variable_string=variable_string[0:-2]
	insert_string="%s, "*len(column_defs)
	insert_string=insert_string[0:-2]

	# The actual table creation statements.
	tables={}
	tables['counties'] = ("CREATE TABLE IF NOT EXISTS `counties` ("+table_string+") ENGINE=InnoDB;")
	tables['metro_micro'] = ("CREATE TABLE IF NOT EXISTS `metro_micro` ("+table_string+") ENGINE=InnoDB;")
	tables['national'] = ("CREATE TABLE IF NOT EXISTS `national` ("+table_string+") ENGINE=InnoDB;")
	tables['states'] = ("CREATE TABLE IF NOT EXISTS `states` ("+table_string+") ENGINE=InnoDB;")
	tables['workforce'] = ("CREATE TABLE IF NOT EXISTS `workforce` ("+table_string+") ENGINE=InnoDB;")

	# Open a connection, drop existing tables and create new ones.
	cnx=mysql.connector.connect(user=SQL_dict['user'], password=SQL_dict['pass'], host='127.0.0.1', database='QWI')
	cursor=cnx.cursor()

	geos=['counties','metro_micro','national','states','workforce']

	for geo in geos:
		try:
			cursor.execute("DROP TABLE `%s`" % (geo))
		except:
			pass

	for table in tables.keys():
		cursor.execute(tables[table])

	cursor.close()
	cnx.close()








