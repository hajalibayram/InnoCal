#!/usr/bin/env python
# coding: utf-8


from datetime import datetime
from pathlib import Path

import pandas as pd
import pyodbc
from pytz import utc 


from Util import getCalendarService, addEventToGoogle, setup_log

import json

input_json = open ('config.json', "r")
input_json = json.loads(input_json.read())
server = input_json['server']
port =  input_json['port']
database =  input_json['database']
username =  input_json['username']
password =  input_json['password']

try:
    conn = pyodbc.connect('Driver={SQL Server};Server='+server+','+port+';Database='+database+';UID='+username+';PWD='+ password)
except:
    username =  input_json['username_backup']
    password =  input_json['password_backup']
    conn = pyodbc.connect('Driver={SQL Server};Server='+server+','+port+';Database='+database+';UID='+username+';PWD='+ password)

raw_df = pd.read_sql_query('SELECT * FROM v_r_excelAuleCondiviseOCuupate', conn)

df = raw_df.drop(columns=['Mese', 'Giorno', 'Sede'])
df['Locale'] = df['Locale'].str.strip()

with open('locali.txt', 'r') as f:
    content = f.readlines()

locali = [x.replace('\n', '') for x in content if x[0]!='#']

setup_log()

service = getCalendarService()

addEventToGoogle(df, service, locali)

locali_df = pd.read_sql_query('SELECT * FROM v_r_excelsituazione_Pin_Aule', conn)

# Write original locali
with open('locali_org.txt', 'w') as f:
    locali_len = len(locali_df['locale'].str.strip().unique().tolist())
    print('Updating the original \"locali\"')
    for i, l in enumerate(sorted(locali_df['locale'].str.strip().unique().tolist())):
        print(i,l)
        f.write(l)
        if i != locali_len-1:
            f.write('\n')

