from flask import Flask, jsonify, request
from flask_restful import Api, Resource

import pymongo
import json
import data_cleaners
import mapping_ultility_functions
import mapping_data
import pandas as pd
import numpy as np
import datetime
from tinydb import TinyDB, Query, where
pd.options.display.float_format = '{:,.2f}'.format

##Connect to Mongo DB
#add &retrywrites=false to the String to write MongoDB 3.6 problem
uri = "mongodb://pfcosmo:BvFxC4HriIHBMTa88KTvsxo8aLjUZto6gfM1i2JdlDywvAdRLczHmwwNJdjsWuPF7ac7sUu9cfVwV1C5wV6LHQ==@pfcosmo.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@pfcosmo@&retrywrites=false"
client = pymongo.MongoClient(uri)
#load a database
db = client.DB_1
BuildingClassMap_Test= db['Building Class Test Collection']




class Building_Mapper(Resource):
    def post(self):
        postedData = request.get_json()

        building_map=postedData["Building Map"]
        job_id = postedData["Job ID"]

        #mapping to historical list
        empty_sample= mapping_ultility_functions.empty_col_mapping(building_map)
        resultMap_v1=mapping_ultility_functions.historical_building_map(empty_sample,BuildingClassMap_Test)



        retMap={
            'Message':200,
            'Suggested Building Map': resultMap_v1,
            'last': "oh NO"
        }
        #sucessful rate cal
        # i=0
        # j=0
        # for k,v in resultMap_v1.items():
        #     j+=1
        #     if len(v)==0:
        #         i+=1

        #print(building_map)



        return jsonify(retMap)
