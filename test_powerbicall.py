from flask import Flask, jsonify, request
from flask_restful import Api, Resource

import pymongo
import json
import data_cleaners
import Colum_Detail_Class
import pandas as pd
import numpy as np
import datetime


class test_PB(Resource):
    def post(self):
        #Step 1: Get posted data from developers:
        postedData = request.get_json()

        limit=postedData["Limit"]
        excess= postedData["Excess"]
        result= limit*excess



        retMap={
            'Message':200,
            'Sugeested List': result,

        }



        return jsonify(retMap)
