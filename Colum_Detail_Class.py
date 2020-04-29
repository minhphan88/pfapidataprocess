import json
import pymongo
#add &retrywrites=false to the String to write MongoDB 3.6 problem
#uri = "mongodb://pfcosmo:BvFxC4HriIHBMTa88KTvsxo8aLjUZto6gfM1i2JdlDywvAdRLczHmwwNJdjsWuPF7ac7sUu9cfVwV1C5wV6LHQ==@pfcosmo.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@pfcosmo@&retrywrites=false"
#client = pymongo.MongoClient(uri)
#load a database
#db = client.DB_1

#ColumMap= db['Mapped Column Details']
uri = "mongodb://pfcosmo:BvFxC4HriIHBMTa88KTvsxo8aLjUZto6gfM1i2JdlDywvAdRLczHmwwNJdjsWuPF7ac7sUu9cfVwV1C5wV6LHQ==@pfcosmo.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@pfcosmo@&retrywrites=false"
client = pymongo.MongoClient(uri)
#load a database
db = client.DB_1
ColumMap= db['Mapped Column Details']
class Column_details():
    #uri = "mongodb://pfcosmo:BvFxC4HriIHBMTa88KTvsxo8aLjUZto6gfM1i2JdlDywvAdRLczHmwwNJdjsWuPF7ac7sUu9cfVwV1C5wV6LHQ==@pfcosmo.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@pfcosmo@&retrywrites=false"
    #client = pymongo.MongoClient(uri)
#load a database
    #db = client.DB_1
    #ColumMap= db['Mapped Column Details']

    Mapped_Name=None
    Match_Probalility=0
    Data_Type= None
    Descriptive_Field_Name= None
    EDM_table= None
    Required= None
    Description =None
    Mached_Method=None
    def __init__(self, Match_Probalility, Mapped_Name):
        self.Match_Probalility = Match_Probalility
        self.Mapped_Name = Mapped_Name
        currColumn= ColumMap.find_one({"Mapped_Name": Mapped_Name})
        self.Data_Type=currColumn['Data_type']
        self.Descriptive_Field_Name=currColumn['Descriptive_Name']
        self.EDM_table=currColumn['EDM_Table/Field_Name']
        self.Required=currColumn['Required']
        self.Description=currColumn['Description']

    def __init__(self, Mapped_Name):
        self.Match_Probalility = 1.0
        self.Mapped_Name = Mapped_Name

        currColumn= ColumMap.find_one({"Mapped_Name": Mapped_Name})
        self.Data_Type=currColumn['Data_type']
        self.Descriptive_Field_Name=currColumn['Descriptive_Name']
        self.EDM_table=currColumn['EDM_Table/Field_Name']
        self.Required=currColumn['Required']
        self.Description=currColumn['Description']
        self.Mached_Method= "Manual look up"
    def setMatch_Probalility(Match_Probalility):
        self.Match_Probalility=Match_Probalility

    def getReturnDict(self):
        retDict= {"Mapped_Name":self.Mapped_Name,"Match_Probalility":self.Match_Probalility,
                  "Data_type":self.Data_Type,"Descriptive_Name":self.Descriptive_Field_Name,
                  "EDM_Table/Field_Name":self.EDM_table,"Required":self.Required,
                  "Description":self.Description,"Matched_Method": self.Mached_Method
                 }
        json_Dict= json.dumps(retDict)
        return retDict
