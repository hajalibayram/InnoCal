#!/usr/bin/env python
# coding: utf-8


from datetime import datetime
from pathlib import Path

import pandas as pd
import pyodbc 


from Util import getCalendarService, addEventToGoogle

import json

input_json = open ('input.json', "r")
input_json = json.loads(input_json.read())
server = input_json['server']
port =  input_json['port']
database =  input_json['database']
username =  input_json['username']
password =  input_json['password']

conn = pyodbc.connect('Driver={SQL Server};Server='+server+','+port+';Database='+database+';UID='+username+';PWD='+ password)
raw_df = pd.read_sql_query('SELECT * FROM v_r_excelAuleCondiviseOCuupate', conn)

df = raw_df.drop(columns=['Mese', 'Giorno', 'Sede'])
df['Locale'] = df['Locale'].str.strip()

with open('locali.txt', 'r') as f:
    content = f.readlines()

locali = [x.replace('\n', '') for x in content if x[0]!='#']

service = getCalendarService()

addEventToGoogle(df, service, locali)


# Write original locali
with open('locali_org.txt', 'w') as f:
    locali_len = len(df['Locale'].unique().tolist())
    print('Updating the original \"locali\"')
    for i, l in enumerate(sorted(df['Locale'].unique().tolist())):
        print(i,l)
        f.write(l)
        if i != locali_len-1:
            f.write('\n')

