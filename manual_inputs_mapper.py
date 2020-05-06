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
ColDataAccumulation= db['ColDataAccumulation Test']


class Manual_Inputs_Map(Resource):
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

        #old_col_mapping=postedData["Old Column Map"]
        manual_col_mapping=postedData["Manual Column Map"]
        job_id = postedData["Job ID"]

        #Create a map that holds only manual inputs
        old_col_mapping=data_cleaners.manual_field_look_up(data_cleaners.empty_col_mapping(manual_col_mapping),ColumMap)
        col_mapping_changes=data_cleaners.subtract_col_mapping(manual_col_mapping, old_col_mapping)




        #Update MongoDB
        data_cleaners.update_data_points(col_mapping_changes, ColDataAccumulation,job_id)
        data_cleaners.update_ColMap(col_mapping_changes, ColumMap,job_id)
        #Dictionary look up

        #mapping_Result = data_cleaners.column_mapper_mongo(col_data,job_id)
        #mapping_Result = data_cleaners.column_mapper(col_data,job_id)

        #######list= mapping_Result[0][col_nßame]####for debug only
        # Return all the matches with descritive values
        # Each match will return an object of Colum_Detail_Class
        #curr_List= mapping_Result[0][col_name]
        #details_List=[]
        #for e in curr_List:
            #detail_Oject= Colum_Detail_Class.Column_details(e)
            #details_List.append(detail_Oject.getReturnDict())

        #Create a return map

        retMap={
            'Message':200,
            'New Column Mapping': manual_col_mapping
            ##'Manual field mapping': manual_field_Map_Result#details_List
            #'Manual field mapping_2': manual_field_Map_Result
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
