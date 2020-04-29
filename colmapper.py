from flask import Flask, jsonify, request
from flask_restful import Api, Resource

import pymongo
import json
import data_cleaners
import Colum_Detail_Class
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
ColumMap= db['Column Mapping Collection']
ColumMap_DataCollection= db['Column Mapping Collection Data Collection']



#2. Input data quality check
def checkPostedData(postedData, functionName):
    if (functionName == "add"):
        if "x" not in postedData or "y" not in postedData:
            return 301
        else:
            return 200

#API outputs
## mapped_name list
## match probalility
## data types
## Data size
## Des filed name
## EDM table/ Field name
## Req/ # OP
## Description

## 3. Declare all variabes
mapped_label=[]


label= []
job_id = None
region = None
col_name= []
dist_col_vals= []
suggested_label=[]

curr_dict={}

mapping_Result=[]


class Col_Map(Resource):
    def post(self):
        #Step 1: Get posted data from developers:
        postedData = request.get_json()

        #Steb 1b: Verify validity of posted data

        #if (status_code!=200):
        ##Continue

        #If i am here, then status_code == 200
        #Step 2 read all the relevant data
        ###need to accept empty fields as well###
        ## 4 ##
        # read all the input data
        label=postedData["Label"]
        col_name= postedData["Column Name"]
        dist_col_vals=postedData["Distingtive Column Values"]
        job_id = postedData["Job ID"]
        region = postedData["Region"]

        # Create col_data for mapping method from data_cleaners
        col_data=[{"name":col_name,"values":dist_col_vals}]
        # Create dictionary for Data collection MongoDB-via Azure Cosmo
        curr_dict={ "Label" : label,
	               "Column Name" : col_name,
	                "Distingtive Column Values" : dist_col_vals,
	                "Job ID" : job_id,
	                "Region" : region

        }
        #Update MongoDB
        ColumMap_DataCollection.insert_one(curr_dict)

        #Dictionary look up

        mapping_Result = data_cleaners.column_mapper_mongo(col_data,job_id)
        #mapping_Result = data_cleaners.column_mapper(col_data,job_id)

        #######list= mapping_Result[0][col_name]####for debug only
        # Return all the matches with descritive values
        # Each match will return an object of Colum_Detail_Class
        curr_List= mapping_Result[0][col_name]
        details_List=[]
        for e in curr_List:
            detail_Oject= Colum_Detail_Class.Column_details(e)
            details_List.append(detail_Oject.getReturnDict())

        #Create a return map

        retMap={
            'Message':200,
            'Sugeested List': mapping_Result,
            'Details List': details_List
        }

        ####Text processing function#####
        ##### manage cases, use spacy auto-correct, tranlate to english######
        #process the jsonString to list of dictionaries
        #Step3 The algorithm

        #3a: check if label is not Null:
        ##text corrected label
        ## check if label is in the database

        #if yes do:

        ## update the database
        ### customsied method toclean all texts and then update database###

        ## update ret_Map
        ##return jsonify(retMap)

        ################end of the old data_cleaners.col_map##########


        ##continue



        # else
        # if col_name is in any col_name set in data base
        ## update the database
        ## add to the list of mapped_label
        ## update retMap



        ############
        ## Use distict values to List
        ### customised functions to the  cosinse similiarity between all the existing list ####
        ## add to mapped_label
        ##### Not update the data yet #####
        ## update retMap

        ############

        #the_dict = json.loads(mapping)


        #retMap = {
        #    'Message': ret_LOC,
        #    'Status Code': 200
        #}
        return jsonify(retMap)
