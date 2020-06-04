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




class All_Col_Mapper(Resource):
    def post(self):
        postedData = request.get_json()

        column_name=postedData["Column Name"]
        column_data=postedData["Column Data"]
        job_id = postedData["Job ID"]

        #Search for the column's name from a list?
        a=pd.read_json(column_data)

        ret_column_data=data_cleaners.conform_values(column_name,a[0])
        #Convert ret_column_data to json
        retMap={
            'Message':200,
            'Conformed Column Data': ret_column_data.to_json(orient='records')
        }




        return jsonify(retMap)
