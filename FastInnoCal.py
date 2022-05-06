#!/usr/bin/env python
# coding: utf-8


import datetime
from pathlib import Path
from threading import local
from dateutil.tz import tzlocal


import pandas as pd
import pyodbc
from pytz import utc 


from Util import addEventToGoogle, addEventToInnovaplan, createCalendars, getCalendarService, listCalendarsFromGoogle, setup_log

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
try:
    locali_df = pd.read_csv('locali_df.csv')
except FileNotFoundError: 
    locali_df = pd.read_sql_query('SELECT * FROM v_r_excelsituazione_Pin_Aule', conn)
    print('Creating local ')
    locali_df.to_csv('locali_df.csv', index=False)
locali = sorted(locali_df['locale'].str.strip().unique().tolist())

# Aule Occupate
raw_occ_df = pd.read_sql_query('SELECT * FROM v_r_excelAuleCondiviseOCuupate', conn)
occ_df = raw_occ_df.drop(columns=['Mese', 'Giorno', 'Sede'])
occ_df['Locale'] = occ_df['Locale'].str.strip()

addEventToInnovaplan(occ_df, service, cals_dict, locali, conn)

    
    

