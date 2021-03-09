##SETTING UP SCRIPT:
import sys
from sys import argv
from termcolor import colored, cprint

# For data handling:
import pandas as pd
import numpy as np
import json

# Intro and set working directory
import os
cprint("\nHi, i'm the geocoder script!", 'red')
print('\nThis is your current working directory:', os.getcwd())

#For geocoding
import censusgeocode as cg

#Bringing in data
prompt = "> "
print("""
\n\t*I'm going to need a csv file from you. Please enter it's
\tname at the prompt below. Make sure it's in the right folder
\tso I can find it, and that it has a unique ID, the street address,
\tcity, state, and zipcode of the adddresses you want to geocode.\n
""")

userfile = input(prompt + "Write your csv file name here: ")

# Set Pandas output options
pd.set_option('display.max_row', 10)
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', 999)

addresses = pd.read_csv(userfile)

##IDENTIFYING VARIABLES FROM USER
print("""
\n\t*Before I start the geocoding i'm going to need you to identify
\tsome variables. First, can you tell me the name of the variable
\t that contains your street address:
""")
add1 = input(prompt + "Write its name here: ")
print("\n\t*Now i need the name of the variable in your data with the city in it.")
add2 = input(prompt + "\nWrite its name here: ")
print("\n\t*Now i need the name of the variable with the state in it.")
add3 = input(prompt + "\nWrite its name here: ")
print("\n\t*Finally i need the name of the variable with the zip code in it.")
add4 = input(prompt + "\nWrite its name here: ")

# print("\nstreet_address %r, city %r, state %r, zipcode %r") %(street_address, city, state, zipcode)

##BEGIN GEOCODING
print("""
\n\t*Now i'm going to start geocoding. This can take a while
\tdepending on how many addresses you want to code up. I'll
\tkeep you updated on my progress though, and feel free to do
\ton other things while I work. Once i'm done, i'll automatically
\tsave a new csv file in your working directory with all your geocoded
\tdata. I'll also save a file called rawdata.txt which is there just in
\tI break at some point.
""")
input("Press Enter to begin\n")


geo_set = []
n = 0
#just test it for 5 addresses
for index, row in addresses.iloc[0:].iterrows():
    try:
        nextline = cg.address(row[add1], city=row[add2], state=row[add3], zipcode=row[add4])
        geo_set.append(nextline)
    except:
        pass
    n = n+1
    sys.stdout.write('\r'+str(n) + " addresses gecoded...") #display progress for user
    with open('rawdata.txt', 'a+') as file:
         file.write(json.dumps(nextline)) #keep backup of json output for backup


###CONVERT JSON OUTPUT INTO PANDAS DATAFRAME
toexport = pd.DataFrame({"fromAddress":[], "streetName":[], "suffixType":[], "state":[], "city":[], "zip":[], "BASENAME": [], "CENTLAT": [], "COUNTY":[], "GEOID":[], "NAME":[], "BLKGRP":[], "BLOCK":[], "matchedAddress":[]})
for p in geo_set:
    for i in p:
        add_comp = i['addressComponents']
        census_block = i['geographies']['2010 Census Blocks'][0]
        next_line = pd.DataFrame({
            "matchedAddress":[i['matchedAddress']],
            "fromAddress":[add_comp['fromAddress']],
            "streetName":[add_comp['streetName']],
            "suffixType":[add_comp['suffixType']],
            "state":[add_comp['state']],
            "city":[add_comp['city']],
            "zip":[add_comp['zip']],
            "BASENAME": [census_block['BASENAME']],
            "CENTLAT": [census_block['CENTLAT']],
            "COUNTY": [census_block['COUNTY']],
            "GEOID": [census_block['GEOID']],
            "NAME": [census_block['NAME']],
            "BLKGRP": [census_block['BLKGRP']],
            "BLOCK": [census_block['BLOCK']]
        })
        toexport = toexport.append(next_line)

###EXPORT AS CSV
print("\n")
print("""\n\t*I'm all done! I've saved a new csv file in your working directory
\tcalled Geocoded_Addresss.csv. You should be able to merge it back to your
\toriginal dataset using address
""")

toexport.to_csv('geocoded_data.csv')
