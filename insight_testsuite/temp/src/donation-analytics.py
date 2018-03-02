#Code written by Grant Parker Garner for the purpose of the
#Data Engineering Fellowship coding challenge
#Updated March 01, 2018

#!/usr/bin/env python
import pandas as pd
import numpy as np
from heapq import heappush, heappop
import sys

#Read in the top input filenames from the commandline
inFile = sys.argv[1]
inFilePERCENTILE = sys.argv[2]

#Set the output file name from the commandline
outFile = sys.argv[3]

#Read in the desired percentile
PERCENTILE_FILE = np.loadtxt(inFilePERCENTILE)

#Read in the donation data into the dataframe df, only columns 0, 7, 10, 13, 14, 15 as these are
#the recipient id(0)
#the donor name(7)
#the donor zip code(10) -- read in as a string to preserve leading 0s
#the transaction date(13) -- read in as a string to preserve leading 0s
#the donation amount(14), negative values represent returns, I choose not to consider these rows
#declaration of person (NaN) or entity (15)
df = pd.read_csv(inFile, sep='|',header=None, usecols=[0,7,10,13,14,15],dtype={0:'str',7:'str',10:'str',13:'str',14:'int',15:'str'})

#CMTE_ID: identifies the flier, which for our purposes is the recipient of this contribution
#NAME: name of the donor
#ZIP_CODE: zip code of the contributor (we only want the first five digits/characters)
#TRANSACTION_DT: date of the transaction
#TRANSACTION_AMT: amount of the transaction
#OTHER_ID: a field that denotes whether contribution came from a person or an entity

#Calculate the decimal percentile
INPUT_PERCENTILE = PERCENTILE_FILE/100.0

#Name the columns of the loaded data frame, df
df.columns=['CMTE_ID','NAME','ZIP_CODE','TRANSACTION_DT','TRANSACTION_AMT','OTHER_ID']

#Remove any rows that have null values
df = df[df['CMTE_ID'].notnull()]
df = df[df['NAME'].notnull()]
df = df[df['ZIP_CODE'].notnull()]
df = df[df['TRANSACTION_DT'].notnull()]
df = df[df['TRANSACTION_AMT'].notnull()]

#Remove any entity donors
df = df[df['OTHER_ID'].isnull()]

#Remove returns
df = df[df['TRANSACTION_AMT']>=0]


#Standardize the zip code format to 5 digits
df['ZIP_CODE'] = df['ZIP_CODE'].map(lambda x: str(x)[0:5])

#Create a new column with an integer value for the four digit year
df['YEAR'] = pd.to_numeric(df['TRANSACTION_DT'].map(lambda x: str(x)[-4:]))

#Remove rows in which the year cannot make sense
df = df[df['YEAR']>1979]
df = df[df['YEAR']<2019]

#Reformate the transaction date column
df['TRANSACTION_DT'] = pd.to_datetime(df['TRANSACTION_DT'],format='%m%d%Y')

#Sort the dataframe by the transaction date column
df = df.sort_values(by=['TRANSACTION_DT'])

#Only keep rows which have a duplicated combination of name and zip code -- drop the first instance
df = df[df.duplicated(['NAME','ZIP_CODE'])]

#Create lists of all the unique recipient IDs, zip codes of donors, and years present in the data frame

list_recipients = df['CMTE_ID'].unique()
list_ZIPCODES = df['ZIP_CODE'].unique()
list_YEARS = np.sort(df['YEAR'].unique())

#Create a dictionary which points to the heaps necessary for calculating the percentile for each zip code
zip_data = {}

for i in range(len(list_ZIPCODES)):
	id = list_ZIPCODES[i]
	minHeap = []
	maxHeap = []
	percentile = 0
	zip_data[id] = {'minHeap':minHeap,'maxHeap':maxHeap,'percentile':percentile}

#Create array for summing the total number of donors for a recipient in a given zip code
yrly_count_data = np.zeros([len(list_recipients),len(list_ZIPCODES)])

#Create array for summing the total donation amount for a recipient in a given zip code
yrly_gross_data = np.zeros([len(list_recipients),len(list_ZIPCODES)])

delim = '|'
year_index = 0


output_file = open(outFile,'w')
for index, row in df.iterrows():
	curr_recipient = row['CMTE_ID']
	curr_year = row['YEAR']
	curr_zip = row['ZIP_CODE']
	curr_transaction = row['TRANSACTION_AMT']
	
	#When the year changes in the dataframe, reset the dictionary and arrays to empty and zeros
	if curr_year != list_YEARS[year_index]:
		year_index += 1
		
		yrly_count_data.fill(0)
		yrly_gross_data.fill(0)
		
		for i in range(len(list_ZIPCODES)):
			zip_data[list_ZIPCODES[i]]['minHeap'] = []
			zip_data[list_ZIPCODES[i]]['maxHeap'] = []
			zip_data[list_ZIPCODES[i]]['percentile'] = 0
	
	#Calculate the percentile cutoff for the current zipcode in the current year
	if  curr_transaction > zip_data[curr_zip]['percentile']:
		heappush(zip_data[curr_zip]['maxHeap'],curr_transaction)
	else:
		heappush(zip_data[curr_zip]['minHeap'],-curr_transaction)
		
	if len(zip_data[curr_zip]['minHeap']) < np.ceil((len(zip_data[curr_zip]['maxHeap'])+len(zip_data[curr_zip]['minHeap']))*INPUT_PERCENTILE):
		heappush(zip_data[curr_zip]['minHeap'],-heappop(zip_data[curr_zip]['maxHeap']))
		
	if len(zip_data[curr_zip]['minHeap']) > np.ceil((len(zip_data[curr_zip]['maxHeap'])+len(zip_data[curr_zip]['minHeap']))*INPUT_PERCENTILE):
		heappush(zip_data[curr_zip]['maxHeap'],-heappop(zip_data[curr_zip]['minHeap']))
		
	p = -heappop(zip_data[curr_zip]['minHeap'])
	heappush(zip_data[curr_zip]['minHeap'],-p)
	per = p

	zip_data[curr_zip]['percentile'] = per
	
	#Identify the index of the recipient and the zipcode that needs to be updated
	r = np.where(list_recipients == curr_recipient)[0]
	c = np.where(list_ZIPCODES == curr_zip)[0],
	
	yrly_gross_data[r,c] += curr_transaction
	yrly_count_data[r,c] += 1
	
	#Write CMTE_ID|ZIP_CODE|YEAR|PERCENTILE|TOTAL_DONATIONS|TOTAL_DONORS
	output_file.write(curr_recipient+delim+curr_zip+delim+str(curr_year)+delim+str(per)+delim+str(int(yrly_gross_data[r,c]))+delim+str(int(yrly_count_data[r,c]))+'\n')
	
output_file.close()
	


