from flask import Flask, jsonify, request
from flask_restful import Api, Resource

import json
import data_cleaners
import colnames
import  colmapper
import manual_field_mapper
import manual_inputs_mapper
import building_class_mapper
import pandas as pd
import all_col
import numpy as np
import datetime
from tinydb import TinyDB, Query, where
import test_powerbicall
pd.options.display.float_format = '{:,.2f}'.format

#Create a web app
app = Flask(__name__)
#add the api fucntion to the webapp
api = Api(app)
#########

#########

#add api call to to path /add
api.add_resource(colnames.col_list,"/col_list")
api.add_resource(colmapper.Col_Map, "/col_map")
api.add_resource(manual_field_mapper.Manual_field_Map, "/Manual_field_Map")
api.add_resource(manual_inputs_mapper.Manual_Inputs_Map, "/Manual_Inputs_Map")
api.add_resource(building_class_mapper.Building_Mapper, "/building_map")
api.add_resource(all_col.All_Col_Mapper,"/all_col")
api.add_resource(test_powerbicall.test_PB,"/test_PB")
#api.add_resource(test.col_list_update, "/test_update")Manual_field_Map


##This is the landing page
@app.route('/')
def hello_world():
    return "<h1>Welcome to your API</h1>"


if __name__=="__main__":
    app.run(debug=True)
