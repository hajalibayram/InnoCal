#!/usr/bin/env python
# coding: utf-8


import datetime
from pathlib import Path
from threading import local
from dateutil.tz import tzlocal


import pandas as pd
import pyodbc
from pytz import utc 


from Util import  addEventToGoogle, getCalendarService, listCalendarsFromGoogle, setup_log, createCalendars

import json

# Setting up
setup_log()
service = getCalendarService()
cals_dict = listCalendarsFromGoogle(service)
CREATOR_EMAIL = 'calendar@its-ictpiemonte.it'

input_json = open ('config.json', "r")
input_json = json.loads(input_json.read())
server = input_json['server']
port =  input_json['port']
database =  input_json['database']
username =  input_json['username']
password =  input_json['password']

# Establishing connection
try:
    conn = pyodbc.connect('Driver={SQL Server};Server='+server+','+port+';Database='+database+';UID='+username+';PWD='+ password)
except:
    username =  input_json['username_backup']
    password =  input_json['password_backup']
    conn = pyodbc.connect('Driver={SQL Server};Server='+server+','+port+';Database='+database+';UID='+username+';PWD='+ password)

# All the "Aule"
locali_df = pd.read_sql_query('SELECT * FROM v_r_excelsituazione_Pin_Aule', conn)
locali_df.to_csv('locali_df.csv', index=False)
locali = sorted(locali_df['locale'].str.strip().unique().tolist())

# Create calendars
# createCalendars(locali_df, service, locali, cals_dict)

# Aule Occupate
raw_occ_df = pd.read_sql_query('SELECT * FROM v_r_excelAuleCondiviseOCuupate', conn)
occ_df = raw_occ_df.drop(columns=['Mese', 'Giorno', 'Sede'])
occ_df['Locale'] = occ_df['Locale'].str.strip()

# From Innovaplan to Google
addEventToGoogle(occ_df, service, locali, deadline_days=8)

# Write original locali
with open('locali_org.txt', 'w') as f:
    locali_len = len(locali_df['locale'].str.strip().unique().tolist())
    print('Updating the original \"locali\"')
    for i, l in enumerate(sorted(locali_df['locale'].str.strip().unique().tolist())):
        print(i,l)
        f.write(l)
        if i != locali_len-1:
            f.write('\n')   

