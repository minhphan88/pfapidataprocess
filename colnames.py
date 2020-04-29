from flask import Flask, jsonify, request
from flask_restful import Api, Resource

import json
import pandas as pd
import numpy as np
import datetime
import pymongo
from tinydb import TinyDB, Query, where
pd.options.display.float_format = '{:,.2f}'.format


###1.Connect to DB
uri = "mongodb://pfcosmo:BvFxC4HriIHBMTa88KTvsxo8aLjUZto6gfM1i2JdlDywvAdRLczHmwwNJdjsWuPF7ac7sUu9cfVwV1C5wV6LHQ==@pfcosmo.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@pfcosmo@&retrywrites=false"
client = pymongo.MongoClient(uri)
#load a database
db = client.DB_1
ColumMap= db['Mapped Column Details']
##2. Retrieve document as list
col_list=[]
descriptive_name_list=[]
full_list=[]

for e in ColumMap.find({}):
    col_list.append(e['Mapped_Name'])
    currDict1={"Mapped_Name":e['Mapped_Name'],"Descriptive_Name":e['Descriptive_Name']}
    descriptive_name_list.append(currDict1)
    currDict2={"Mapped_Name":e['Mapped_Name'],'Data_type': e['Data_type'],
                "Descriptive_Name":e['Descriptive_Name'],
                'EDM_Table/Field_Name': e['EDM_Table/Field_Name'],
                'Required': e['Required'],
                'Description':e['Description']
                }
    full_list.append(currDict2)

##Jsonify all list
col_list_json = json.dumps(col_list)
descriptive_name_list_json = json.dumps(descriptive_name_list)
full_list_json = json.dumps(full_list)
#3. Create resources

class Item():
    col_list=[]
    descriptive_name_list=[]
    full_list=[]

    for e in ColumMap.find({}):
        col_list.append(e['Mapped_Name'])
        currDict1={"Mapped_Name":e['Mapped_Name'],"Descriptive_Name":e['Descriptive_Name']}
        descriptive_name_list.append(currDict1)
        currDict2={"Mapped_Name":e['Mapped_Name'],'Data_type': e['Data_type'],
                    "Descriptive_Name":e['Descriptive_Name'],
                    'EDM_Table/Field_Name': e['EDM_Table/Field_Name'],
                    'Required': e['Required'],
                    'Description':e['Description']
                    }
        full_list.append(currDict2)

class col_list(Resource):

    def get(self):
        e=Item()
        retMap = {
            'Column Name List': e.col_list,#_json,
            'Descritive Name List': e.descriptive_name_list,#_json,
            'Full List': e.full_list,#_json,
            'Message': 200
        }
        return jsonify(retMap)
