#!/usr/bin/env python
# coding: utf-8
import subprocess
import sys
import os
import json


import json
input_json = open ('input.json', "r")
input_json = json.loads(input_json.read())

PyFolder = input_json['PyFolder']

# it puts the necessary file inside directory
os.system('copy pyodbc-4.0.32-cp310-cp310-win_amd64.whl ' + PyFolder)
os.chdir(PyFolder)

# install the packages
os.system('pip install -m  pyodbc-4.0.32-cp310-cp310-win_amd64.whl')
os.system('pip install -r ' + os.path.dirname(os.path.realpath(__file__))+'\\requirements.txt')
