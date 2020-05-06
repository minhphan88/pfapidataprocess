
#code for module files - edits here will write to

#############################################
### FUTURE API FUNCTIONS -> TO LEARN MAPPINGS AND USE ML
#############################################
import os
import pandas as pd
import numpy as np
import datetime
import re
import pymongo
import copy
from tinydb import TinyDB, Query, where
pd.options.display.float_format = '{:,.2f}'.format
#load the database
db = TinyDB("./rms_db.json")

#Functions to be converted to APIs
#*********
def column_mapper(col_data,job_id):
    #takes as input a list of dictionaries with structure col_data =[{'name': 'name', 'values': ['value1','value2','value3',...]},..]
    #returns a lsit of mapped values for names we could match on [{'name1' : 'name1_mapped','name2' : 'name2_mapped',..},{},]
    #the list contains first, second, third, ... choice matches
    #####
    #1.region
    #2. labels

    #####
    #load the table and query
    table = db.table('columns')
    Column = Query()

    #the results data
    results=[]

    #To be developed further but for now reads from a stored dict
    for col_entry in col_data:
        matched = table.search( Column.original_name == col_entry['name'] )

        #make sure results is the right size to hold the results
        while len(matched)>len(results):
            #need to append dicts to results
            results.append({})

        #get the matches and append to the results
        for i in range(len(matched)):
            #add the entry to the results
            results[i][col_entry['name']] = matched[i]['mapped_names']

    #now return the mapped data
    #return Confindence metrics
    #
    return results

#takes a RMS column name and returns the values in a format that will not crash the modelling system..
def conform_values(colname,raw_data):
    #colname= RMS conformed column name
    #raw_data is a numpy array of the values
    clean_data=raw_data.copy()

    #post code values
    if(colname=="POSTALCODE"):
        #post codes need to be string with no decimal places
        try:
            raw_data =raw_data.fillna(0).astype(int).astype(str)
        except:
            raw_data =raw_data.fillna('').astype(str)

    if((colname=="BLDGCLASS") or (colname=="OCCTYPE")):
        raw_data=raw_data.astype(str).fillna('0').apply(lambda x: x.split('.')[0])
        raw_data=raw_data.replace('','0')

    if((colname=="BLDGSCHEME") or (colname=="OCCSCHEME")):
        raw_data=raw_data.astype(str).fillna('ATC').apply(lambda x: 'ATC' if x=='' else x)



    second_mods = ['CONSTQUALI', 'ROOFSYS', 'ROOFGEOM','ROOFANCH', 'ROOFAGE', 'ROOFEQUIP', 'CLADSYS', 'CLADRATE',
                   'FOUNDSYS','MECHGROUND', 'RESISTOPEN', 'FLASHING', 'BASEMENT', 'BUILDINGELEVATION',
                   'BUILDINGELEVATIONMATCH', 'FLOODDEFENSEELEVATION','FLOODDEFENSEELEVATIONUNIT', 'NUMSTORIESBG']
    if(colname in second_mods):
        raw_data=raw_data.fillna('').astype(str).fillna('').apply(lambda x: x.split('.')[0]).fillna('')
        raw_data = raw_data.apply(lambda x:x if x.isnumeric() else '')

    #all numeric amounts
    if("CV" in colname and "VAL" in colname):
        #fix all the numerical values
        raw_data = pd.to_numeric(raw_data, errors='coerce')
        raw_data = raw_data.fillna(0.0).round(2)

    if(colname=='FLOORAREA'):
        raw_data = pd.to_numeric(raw_data, errors='coerce').fillna(0).astype(int).astype(str).replace('0','')
    if('YEAR' in colname):
        raw_data = ("31/12/" + pd.to_numeric(raw_data, errors='coerce').fillna(0).astype(int).apply(lambda x: 9999 if x<1800 else x).astype(str)).replace('31/12/0','31/12/9999')

    if(colname=='NUMBLDGS'):
        raw_data = pd.to_numeric(raw_data, errors='coerce').fillna(0).astype(int).astype(str).replace('0','')
        raw_data = raw_data.apply(lambda x:x if x.isnumeric() else '')
    if(colname=='NUMSTORIES'):
        raw_data = pd.to_numeric(raw_data, errors='coerce').fillna(0).astype(int)



    #replace all new row delimiters
    raw_data = raw_data.replace('\n',' ')
    raw_data = raw_data.replace('\t',' ')
    #raw_data = raw_data.apply(lambda x: x.encode("latin-1","ignore").decode("latin-1") if type(x)==str else x )
    raw_data = raw_data.apply(lambda x: re.sub(r"[^-/().&' \w]|_", "", x) if type(x)==str else x )


    #construction types
    #... will lookup in database/use ML



    return raw_data


def map_values(col_name,col_data):
    #print out a list of the mapped values
    dict_mapping = dict(zip(list(col_data.unique()),['']*len(list(col_data.unique()))))

    #auto code to map needed here
    display(dict_mapping)
    return dict_mapping


#this function takes a list of LOC columns and returns the other required columns together with their default values
def extend_loc(existing_columns,job_id):
    additional_columns ={
        'CNTRYSCHEME' : 'ISO2A',
        'CNTRYCODE' :'US', #default to US if not added explicitly
        'BLDGSCHEME' : 'RMS',
        'BLDGCLASS' :'0',
        'OCCSCHEME' : 'ATC',
        'OCCTYPE': '0'}

    #need to add in other custom items

    #put in a default accountnum
    additional_columns['ACCNTNUM']=job_id
    #for every value item add in the corresponding currency, defaulting to USD
    for e_col in existing_columns:

        #check if this is an amount and if it is add a currency column
        if( ("CV" in e_col) and ("VAL" in e_col) and ("CUR" not in e_col)):
            additional_columns[e_col[:-2] + 'CUR'] = 'USD'
        #check if this is a deductible and if it is add a currency column
        if( ("SITEDED" in e_col) and ("CUR" not in e_col)):
            additional_columns[e_col[:-2] + 'CUR'] = 'USD'
        if( 'FLOORAREA' in e_col):
            #assume in square feet
            additional_columns['AREAUNIT']=2

    #now remove any columns from additional columns that have an entry already in exisiting_columns
    for e_col in existing_columns:
        additional_columns.pop(e_col, None)

    return additional_columns

#############################################
### PRODUCE THE ACC FILES
#############################################

def get_acc(policies,job_id):
    policytype = {'EQ' : 1 ,'WS': 2, 'TO': 3}

    policies_final=[]
    #now we create the duplicates needed and add in currencies
    for policy in policies:
        for peril in policy['PERILS']:
            new_policy = policy.copy()
            #remove perils from dict
            new_policy.pop('PERILS',None)
            #add the peril
            new_policy['POLICYTYPE']=policytype[peril]
            #update the policynum
            new_policy['POLICYNUM'] =peril +'-'+new_policy['POLICYNUM']
            #remove the currency
            currency = policy['CUR']
            new_policy.pop('CUR',None)
            #add in the currency fields that are needed
            for key in policy.keys():
                if(('PARTOF' in key) and ('CUR' not in key)):
                    new_policy['PARTOFCUR']=currency
                if(('UNDCOV' in key) and ('CUR' not in key)):
                    new_policy['UNDCOVCUR']=currency
                if(('BLANLIM' in key) and ('CUR' not in key)):
                    new_policy['BLANLIMCUR']=currency
                if(('DED' in key) and ('CUR' not in key)):
                    new_policy[key[:-3]+'CUR']=currency
            policies_final.append(new_policy)

    df_ACC =pd.DataFrame(policies_final)

    return df_ACC

#return a dict for manual mapping
def get_df_dict(df):
    return dict(zip(list(df.columns),len(list(df.columns))*[[]]))

#This function requires manual mapping of fields to the RMS schema
def manual_field_mapping(df,col_mapping,job_id):
    display(get_df_dict(df))
    with pd.option_context('display.max_rows', 10, 'display.max_columns', 99999):
        display(df)
    # CITY, STATE, NUMBLDGS, CNTRYCODE, USERTXT1, CRESTA,USERTXT2,OCCTYPE,EQCV1VAL,WSCV1VAL,TOCV1VAL
    # YEARBUILT,NUMSTORIES,FRSPRINKLERSYS,CNTRYSCHEME,BLDGSCHEME,OCCSCHEME,OCCTYPE
    # WSSITEDED, EQSITEDED,EQCV1VCUR,EQSITEDCUR
    # ACCNTNUM, LOCNAME, LOCNUM


    #Now we save the col mapping in the database
    column_table = db.table('columns')

    #remove the old mapping
    table = db.table('columns')
    table.remove(where('job_id') == job_id)

    for key, value in col_mapping.items():
        if len(value)>0:
            table.insert({'original_name': key, 'mapped_names': value, 'job_id' : job_id})
    return col_mapping

#############################################
### PRODUCE THE RMS FILES
#############################################

def get_loc(df,job_id, mappings={}):

    #try replacing all nan values with ''
    df_clean = df.fillna('')

    #pass the columns to the mapper
    col_data = []
    for col in df_clean.columns:
        #create the dict entry of the values
        this_entry = {'name' : col, 'values':list(df_clean[col].unique())}
        col_data.append(this_entry)

    #now call the mapper and get back the mapped column data
    mapped_cols = column_mapper(col_data,job_id)

    #copy the data frame into a LOC dataframe
    df_LOC = df_clean.copy()

    #assign new columns based on the first match (will worry about multiple matches later)
    #use a set
    all_matched=set()
    for key, values in mapped_cols[0].items():
        for value in values:
            all_matched.add(value)
            if ((value in df_LOC) and (key!=value)):
                #need to merge as already present
                try:
                    #try to see if they are of the same type intially
                    df_LOC[value]=df_LOC[value]+df_LOC[key]
                except:
                    #if mixed types convert to str or numbers for amounts
                    if( ("CV" in value) and ("VAL" in value)):
                       #sum as numbers
                       df_LOC[value]=pd.to_numeric(df_LOC[value], errors='coerce').fillna(0.0)+pd.to_numeric(df_LOC[key], errors='coerce').fillna(0.0)
                    else:
                       #concatenate as strings
                        df_LOC[value]=df_LOC[value].astype(str)+df_LOC[key].astype(str)
            else:
                df_LOC[value]=df_LOC[key]


    #apply any custom mappings
    #nasty hack as we require common columns to be here in str format
    #Bug here as we need to catch all replacement strings
    req_cols=['BLDGSCHEME','OCCSCHEME','BLDGCLASS','OCCTYPE']
    for col in req_cols:
        if col not in df_LOC:
            df_LOC[col]=''
        df_LOC[col] =df_LOC[col].astype(str)

    for key, value in mappings.items():
        df_LOC[key]=df_LOC[key].astype(str).apply(lambda x: value[x] if x in value else x)

    #putting this here for now as needs to operate on scheme type and class type at same time..
    #quick function to strip out scheme types
    def map_schema(class_value,scheme_value):
        if("RMS IND:" in class_value):
            return ("RMS IND",class_value[8:])
        if("RMS:" in class_value):
            return ("RMS",class_value[4:])
        if("ATC:" in class_value):
            return ("ATC",class_value[4:])
        return (scheme_value,class_value)

    #update the schemes with custom values
    df_LOC['BLDGSCHEME']=df_LOC.apply(lambda row: map_schema(row['BLDGCLASS'],row['BLDGSCHEME'])[0],axis=1)
    df_LOC['OCCSCHEME']=df_LOC.apply(lambda row: map_schema(row['OCCTYPE'],row['OCCSCHEME'])[0],axis=1)
    #update the class and type with the correct names
    df_LOC['BLDGCLASS']=df_LOC.apply(lambda row: map_schema(row['BLDGCLASS'],row['BLDGSCHEME'])[1],axis=1)
    df_LOC['OCCTYPE']=df_LOC.apply(lambda row: map_schema(row['OCCTYPE'],row['OCCSCHEME'])[1],axis=1)

    #drop the columns that we haven't matched on
    for col in df_LOC.columns:
        if col not in all_matched:
            df_LOC.drop([col],axis=1,inplace=True)

    #conform the values
    for col in df_LOC.columns:
        df_LOC[col]=conform_values(col,df_LOC[col])


    #now extend the dataframe to include all required fields
    extend_cols = extend_loc(list(df_LOC.columns),job_id)
    for key, value in extend_cols.items():
        df_LOC[key]=value

    df_LOC=df_LOC.reset_index(drop=True)


    #remove any rows that have zero values
    df_LOC['TotalValue'] =0
    for col in df_LOC.columns:
        if( ("CV" in col) and ("VAL" in col) and ("CUR" not in col)):
            df_LOC['TotalValue'] = df_LOC['TotalValue'] + df_LOC[col]
    #now remove zero rows
    df_LOC = df_LOC.loc[df_LOC['TotalValue']>0].reset_index(drop=True)
    df_LOC.drop(['TotalValue'],axis=1,inplace=True)

    #include LOCNUM and ACCNTNUM
    df_LOC['LOCNUM'] = df_LOC.index+1
    #if there is a hyphen in the jobid we need to append this to the LOCNUM to make unique
    if(len(job_id.split('-'))>1):
        #we have a unique identifier
        df_LOC['LOCNUM'] = df_LOC['LOCNUM'].astype(str) + job_id.split('-')[-1]
    #replace any remaingin NaN's
    df_LOC=df_LOC.fillna('')
    return df_LOC

def copy_row_above(df,col):
    #iterate over datframe
    df_copy = df.copy()
    df_copy[col]=df[col].fillna('')
    for i in range(1,len(df_copy)):
        if(df_copy[col].iloc[i]==''):
            df_copy[col].iloc[i] =df_copy[col].iloc[i-1]
    return df_copy


uri = "mongodb://pfcosmo:BvFxC4HriIHBMTa88KTvsxo8aLjUZto6gfM1i2JdlDywvAdRLczHmwwNJdjsWuPF7ac7sUu9cfVwV1C5wV6LHQ==@pfcosmo.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@pfcosmo@&retrywrites=false"
client = pymongo.MongoClient(uri)
#load a database
db_mongo = client.DB_1
ColumMap= db_mongo['Column Mapping Collection']

def column_mapper_mongo(col_data,job_id):
    results_mongo=[]

        #To be developed further but for now reads from a stored dict
    for col_entry in col_data:
        #matched = table.search( Column.original_name == col_entry['name'] )
        list1=[]
        list2=[]
        totalList=[]
        jobID=[]

        for document in ColumMap.find({"original_name" : col_entry['name']}):
            list1.append(document)

        for e in list1:
            totalList+=e["mapped_names"]
            jobID.append(e["job_id"])
        return_Map={'original_name': 'TOTAL','mapped_names':list(set(totalList)),'job_id':jobID }
        #make sure results is the right size to hold the results
        list2.append(return_Map)
            #make sure results is the right size to hold the results
        while len(list2)>len(results_mongo):
                #need to append dicts to results
            results_mongo.append({})

            #get the matches and append to the results
        for i in range(len(list2)):
                #add the entry to the results
            results_mongo[i][col_entry['name']] = list2[i]['mapped_names']
    return results_mongo


def manual_field_mapping_mongo(df,col_mapping,job_id):
    new_col_mapping = copy.deepcopy(col_mapping)

    #create if empty
    if(len(col_mapping)==0):
        print("creating new mapping")
        new_col_mapping=get_df_dict(df)
    #with pd.option_context('display.max_rows', 10, 'display.max_columns', 99999):
        #display(df)

    #Now we save the col mapping in the database
    #column_table is a collection in Mongo
    ##column_table = db.table('columns')
    ##Column = Query()

    #remove the old mapping
    ##table = db.table('columns')
    ##table.remove(where('job_id') == job_id)
    myquery = { "job_id": job_id }
    ColumMap.delete_one(myquery)

    new_col_mapping_copy = copy.deepcopy(new_col_mapping)

    for key, value in new_col_mapping_copy.items():
        if len(value)>0:
            #table.insert({'original_name': key, 'mapped_names': value, 'job_id' : job_id})
            ColumMap.insert_one({'original_name': key, 'mapped_names': value, 'job_id' : job_id})
        else:
            #check if we have a value we can populate
            #matched = column_table.search( Column.original_name == key )
            matched_list=[]
            for document in ColumMap.find({"original_name" : key}):
                matched_list.append(document)
            #add these to the dict
            #print(f"key={key} matched={matched}")

            for i in range(len(matched_list)):
                #add the entry to the results if not already in
                for col_name in matched_list[i]['mapped_names']:
                    #print(f"key={key} col_name={col_name}")
                    if (col_name not in new_col_mapping[key]):
                        new_col_mapping[key].append(col_name)
                    #print(new_col_mapping[key])

    #display(new_col_mapping)
    #return new_col_mapping
    return new_col_mapping
def get_df_dict(df):
    return dict(zip(list(df.columns),len(list(df.columns))*[[]]))
######################################
###New set of methods#####
######################################
def sum_Unique_List(a):
    total_List=[]
    for e in a:
        total_List+=e
    return list(set(total_List))

def manual_field_look_up(col_mapping,ColumMap):
    for k,v in col_mapping.items():
        value_map=[]
        for document in ColumMap.find({"original_name" : k}):
            sum_list=[]
            sum_list.append(document["mapped_names"])
            value_map=sum_Unique_List(sum_list)

        col_mapping[k]=list(set(value_map+v))
    return col_mapping
def new_data_points(df, ColDataAccumulation,job_id):
    col_mapping= get_df_dict(df)
    manual_field_look_map= manual_field_look_up(col_mapping,ColumMap)
    for k,v in manual_field_look_map.items():

        distint_Values = df[k].unique().tolist()

        ColDataAccumulation.delete_one({'Original_Name': k,"Job Id": job_id})
        if len(v)== 0:
            #currCol.setMatch_Probalility(0)
            #currCol.setMatched_Method(None)

            ColDataAccumulation.insert_one({'Original_Name': k, 'Mapped_Name': v, 'Match_Probalility': 0, 'Distinct_Values': distint_Values, 'Matched_Method': None, "Job Id": job_id})
        else:
            ColDataAccumulation.insert_one({'Original_Name': k, 'Mapped_Name': v, 'Match_Probalility': 1, 'Distinct_Values': distint_Values, 'Matched_Method': "History look-up", "Job Id": job_id})

def subtract_col_mapping(manual_col_mapping, old_col_mapping):
    result_dict=[]
    for k1, v1 in manual_col_mapping.items():
        for k2, v2 in old_col_mapping.items():
            cur_dict={}
            if k1==k2:
                cur_dict[k1]=[item for item in v1 if item not in v2]
    return cur_dict

def update_data_points(col_mapping_changes, ColDataAccumulation,job_id):

    for k,v in col_mapping_changes.items():
        myquery = { "Original_Name": k,'Job Id': job_id }
        newvalues = { "$set": { "Mapped_Name": v,"Match_Probalility": 1,"Matched_Method": "Manual Input" } }
        ColDataAccumulation.update_one(myquery, newvalues)
def update_ColMap(col_mapping_changes, ColumMap,job_id):

    for k,v in col_mapping_changes.items():
        ColumMap.insert_one({'original_name' : k,'mapped_names' : v,'job_id' : job_id})

def empty_col_mapping(manual_col_mapping):
    for k,v in manual_col_mapping.items():
        manual_col_mapping[k]=[]
    return manual_col_mapping
