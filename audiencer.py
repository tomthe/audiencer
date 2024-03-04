# 


import sqlite3
import os
import numpy as np
import pandas as pd
import statistics as st
from datetime import datetime
import requests
import json
import time
import constants
import logging
# https://stackoverflow.com/questions/9763116/parse-a-tuple-from-a-string
from ast import literal_eval as make_tuple

# configure logging to file, and log everything:
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(r"g:\theile\facebook\logs\audiencer_info2.log"),
        #logging.StreamHandler()
    ]
)
logging.info("Started logging")


class AudienceCollector:

    def __init__(self, db_file_name, token=None,account_number=None,credentials_fn=None,api_version="17.0"):
        self.db_file_name = db_file_name
        self.collection_id = -1
        if token != None:
            self.token = token
            self.account_number = account_number
        else:
            if credentials_fn == None:
                credentials_fn = "credentials.csv"
            self.token, self.account_number = self.load_credentials_file(credentials_fn)
        self.db = sqlite3.connect(self.db_file_name)
        self.cursor = self.db.cursor()
        self.init_db()
        self.categories = ["scholarities","geo_locations","ages_ranges","genders",
                           "behavior","interests","flexible_spec","publisher_platforms"]
                
        constants.REACHESTIMATE_URL = "https://graph.facebook.com/v" + api_version + "/act_{}/delivery_estimate"

        #catlens = [len(self.input_data_json[cat])+1 for cat in self.categories]

        # Numpy-arrays for fast access to results (needed for predictions)
        self.results_mau = {} #np.ones((catlens))
        self.predictions_median = {} #np.full((catlens),-1)
        self.predictions_stdev = {} #np.full((catlens),-1)
        self.predictions_len = {} #np.full((catlens),-1)

    def read_input_data_json(self,fn_input_data):
        with open(fn_input_data,"r") as inputjson:
            self.input_data_json =json.load(inputjson)
        
        
    def fill_results_mau_from_db(self):
        """
        fills the results_mau dict from the db
        """

        q1 = f"""SELECT collection_id from collections 
                ORDER BY collection_id DESC LIMIT 1"""
        self.cursor.execute(q1)
        self.collection_id = self.cursor.fetchone()[0]
        print("collection_id: ", self.collection_id)
        query = f"""SELECT ias,mau from results where collection_id = ?"""
        self.cursor.execute(query,(self.collection_id,))
        for result in self.cursor.fetchall():
            self.results_mau[make_tuple(result[0])] = int(result[1])
        # return self.results_mau
    
    def fill_results_mau_from_db_test(self):
        """
        fills the results_mau dict from the db
        """
        q1 = f"""SELECT collection_id from collections 
                ORDER BY collection_id DESC LIMIT 1"""
        self.cursor.execute(q1)
        self.collection_id = self.cursor.fetchone()[0]
        print("collection_id: ", self.collection_id)
        query = f"""SELECT ias,mau from results where collection_id = ? LIMIT 5"""
        self.cursor.execute(query,(self.collection_id,))
        print(f"collection_id: {self.collection_id}")
        for result in self.cursor.fetchall():
            print(result)
            print(make_tuple(result[0]))
            print(result[1])
            self.results_mau[make_tuple(result[0])] = int(result[1])
        # return self.results_mau

    def init_db(self):
        """
        creates the tables in the db"""
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS collections (
                            collection_id integer primary key autoincrement,
                            collection_ids_from_same_collection text,
                            collection_name varchar(100),
                            input_data_json json,
                            finished boolean,
                            start_time varchar(50),
                            end_time varchar(50),
                            config json,
                            sub_1000 boolean,
                            comment varchar(100)
                            );''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS queries (
                            pk_queries integer primary key autoincrement,
                            collection_id integer,
                            url varchar(1000),
                            query_string varchar(5000),
                            targeting_spec json,
                            qtime varchar(50),
                            response json,
                            status_code varchar(4),
                            tryNumber integer,
                            ias varchar(25),
                            sub_1000 boolean,
                            comment varchar(100)
                            );''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS results (
                            pk_results integer primary key autoincrement,
                            fk_queries integer,
                            targeting_spec json,
                            collection_id integer,
                            qtime varchar(50),
                            query_string varchar(5000),
                            response json,
                            ias varchar(25),
                            audience_size integer,
                            mau integer,
                            mau_lower integer,
                            mau_upper integer,
                            dau integer,
                            estimate_ready varchar(10),
                            genders varchar(20),
                            geo_locations varchar(100),
                            age_min integer,
                            age_max integer,
                            education_statuses varchar(40),
                            behaviors varchar(60),
                            iquery integer,
                            irun integer,
                            predictions json,
                            prediction_mean float,
                            prediction_std float,
                            prediction_min float,
                            prediction_max float,
                            query_skipped boolean,
                            todo_later integer,
                            desspec varchar(70)
                            );''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS errors (
                            pk_errors integer primary key autoincrement,
                            fk_queries integer,
                            error_message varchar(300),
                            query_string varchar(5000),
                            targeting_spec json,
                            qtime varchar(50),
                            response json,
                            ias varchar(25),
                            comment varchar(100)
                            );''')
        
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS todo_later (
                            pk_todo_later integer primary key autoincrement,
                            fk_results integer,
                            ias varchar(25),
                            comment varchar(100)
                            );''')
        

    def load_credentials_file(self,token_file_path):
        #copied and modified from pySocialWatcher
        with open(token_file_path, "r") as token_file:
            for line in token_file:
                token = line.split(",")[0].strip()
                account_number = line.split(",")[1].strip()
                return token, account_number # return after first line. 
                # PySocialWatcher.add_token_and_account_number(token, account_number)

    def check_last_collection(self):
        """
        Prints information about the last started 
        collection. Did it finish? When was the last 
        succesful query? how many of how many queries 
        are done?
        """
        #get the last collection_id:
        ...
        q1 = f"""SELECT collection_id from collections 
                ORDER BY collection_id DESC LIMIT 1"""
        self.cursor.execute(q1)
        collection_id = self.cursor.fetchone()[0]
        # call check_collection(collection_id)
        self.check_collection(collection_id)

    def check_collection(self,collection_id):
        """
        Prints information about this
        collection. Did it finish? When was the last 
        succesful query? how many of how many queries 
        are done?
        """
        ...
        q1 = f"""SELECT * from collections where collection_id = {collection_id}"""
        self.cursor.execute(q1)
        collection_info = self.cursor.fetchone()
        print(collection_info)
        q2 = f"""SELECT * from queries where collection_id = {collection_id} ORDER BY pk_queries DESC LIMIT 1"""
        self.cursor.execute(q2)
        query_info = self.cursor.fetchone()
        print(query_info)
        q3 = f"""SELECT * from results where fk_queries = {query_info[0]} ORDER BY pk_results DESC LIMIT 1"""
        self.cursor.execute(q3)
        result_info = self.cursor.fetchone()
        print("last result: ", result_info)
        q4 = f"""SELECT * from errors where fk_queries = {query_info[0]} ORDER BY pk_errors DESC LIMIT 1"""
        self.cursor.execute(q4)
        error_info = self.cursor.fetchone()
        print(error_info)
        q5 = f"""SELECT * from todo_later where fk_results = {result_info[0]} ORDER BY pk_todo_later DESC LIMIT 1"""
        self.cursor.execute(q5)
        todo_later_info = self.cursor.fetchone()
        print(todo_later_info)

        
    def restart_collection(self, collection_id=2):
        """
        Restarts a collection.
        1. retrieves necessary collection input from db (what to collect, where to start)
        """
        print("restart collection", collection_id)
        query = f"""SELECT input_data_json,config, collection_name from collections where collection_id = ? """
        self.cursor.execute(query,(collection_id,))
        collection_info = self.cursor.fetchone()
        print("input-data-json: ", collection_info[0])
        print("input-data-json: ", json.loads(collection_info[0]))
        self.input_data_json =json.loads(collection_info[0])
        self.config = json.loads(collection_info[1])

        # get the last query_id
        query = f"""SELECT ias, mau from results where collection_id = ? ORDER BY pk_results DESC"""
        self.cursor.execute(query,(collection_id,))
        # loop through already fetched results and add them to self.results_mau:
        print("restore result")
        for result in self.cursor.fetchall():
            #print("restore result: ", result)
            self.results_mau[make_tuple(result[0])] = int(result[1])
        # print("results_mau", self.results_mau)
        self.collection_id = collection_id
        self.start_collection(collection_config=self.config, collection_id = self.collection_id,)

    def restart_last_collection(self):
        """
        Restarts the last collection.
        """
        q1 = f"""SELECT collection_id from collections 
                ORDER BY collection_id DESC LIMIT 1"""
        self.cursor.execute(q1)
        self.collection_id = self.cursor.fetchone()[0]
        self.restart_collection(self.collection_id)


    def start_new_collection(self,fn_input_data, options_json, collection_name="default_collection", comment=""):
        '''save collection-metadata to collections-column
        then start collection with starting-point=0
        '''
        self.config = options_json
        if fn_input_data != None:
            self.read_input_data_json(fn_input_data)
        else:
            self.input_data_json = None
        print("start_new_collection",(collection_name, self.input_data_json, False, datetime.now(), "", options_json,  comment))
        query = f"""INSERT INTO collections (collection_name, input_data_json, finished, start_time, end_time, config, comment)
                    VALUES (?,?,?,?,?, ?,?)"""
        self.cursor.execute(query, (collection_name, json.dumps(self.input_data_json), str(False), str(datetime.now()), "", json.dumps(options_json),  comment))
        self.collection_id = self.cursor.lastrowid
        self.start_collection(options_json, collection_id = self.collection_id,)

    def finish_collection(self, collection_id):
        '''set finished=True in collections-column
        set end_time=now()
        '''
        query = f"""UPDATE collections SET finished = ?, end_time = ? WHERE collection_id = ? """
        self.cursor.execute(query, ("True", collection_id, datetime.now()))
        self.db.commit()

    # def get_all_predictions(self,ias):
    #     # 1. generate all variations of ias that can form a prediction
    #     # 2. get the prediction for each variation
    #     # 3. profit?!

    #     # what are iasp1 : differs in one category
    #     # then iasp2: is the same as ias in the cat that differs in iasp1
    #     # ... differs in some other category
    #     # then iasp3: is the same as ias in the cat that differs in iasp2 
    #     # and the same in iasp1
    #     ias = list(ias)
    #     iasplist = []
    #     for i in range(len(ias)):
    #         if ias[i]!=0:
    #             for j in range(0,ias[i]): # or only up to ias[i]-1?
    #                 iasp1 = ias[:]
    #                 iasp1[i] = j
    #                 for k in range(len(ias)):
    #                     if k!=i:
    #                         if ias[k]!=0:
    #                             for l in range(0,ias[k]):
    #                                 iasp2 = ias[:]
    #                                 iasp2[k] = l
    #                                 iasp3 = ias[:]
    #                                 iasp3[k] = l
    #                                 iasp3[i] = j
    #                                 iasplist.append((iasp1,iasp2,iasp3))
    #     # todo: We should somehow remove (or avoid) the duplicates, 
    #     # because iasp1 and iasp2 are interchangeable.
    #     # return iasplist
    #     #print("iasplist: ",iasplist)
    #     predictions = []
    #     for iasp1,iasp2,iasp3 in iasplist:
    #         #print(iasp1,iasp2,iasp3)
    #         #print(results[tuple(iasp1)],results[tuple(iasp2)],results[tuple(iasp3)])
    #         #print(results[tuple(iasp1)]*results[tuple(iasp2)]/results[tuple(iasp3)])
    #         predictions.append(self.results_mau[tuple(iasp1)]*self.results_mau[tuple(iasp2)]/self.results_mau[tuple(iasp3)])
    #         #predictions.append(results.item(tuple(iasp1))*results.item(tuple(iasp2))/results.item(tuple(iasp3)))

    #     return predictions

    def collect_one_combination(self,ias,collection_config):
        try:
            if ias in self.results_mau:
                # skip, if result already exists from previous run:
                return
            ias = tuple(ias)
            # 1. make prediction for ias    
            prediction = self.get_all_predictions(ias)
            if collection_config.get("verbose",False)==True:
                print(ias, "prediction: ",prediction)
            if len(prediction)>0:
                self.predictions_median[ias] = st.median(prediction)
                self.predictions_stdev[ias] = st.stdev(prediction)
                self.predictions_len[ias] = len(prediction)
            else:
                self.predictions_median[ias] = -2
                self.predictions_stdev[ias] = -2
                self.predictions_len[ias] = len(prediction)
            if collection_config.get("verbose",False)==True:
                print(ias, "prediction_median: ",self.predictions_median[ias])
            # 2. sub-1000 handling: skip or  make extra-requests?
            if 1 < self.predictions_median[ias] < 300:
                if collection_config.get("skip_sub_1000",True)==True:
                    # save prediction but skip the request
                    self.extract_and_save_result(ias,targeting_spec=self.create_targeting_spec_from_ias(ias),responsecontent="skipped",prediction=prediction,query_skipped=True,collection_id=self.collection_id)
                    # todo: save to table
                    audience = self.predictions_median[ias]
                    #continue
                else: # don't skip, but make extra requests:
                    # ...
                    audience = self.call_request_fb(ias,prediction)
            else:
                # make a normal request:
                audience = self.call_request_fb(ias,prediction)
            #self.results_mau[ias] = audience


            #print("---", ias, audience,"pred:",prediction,"|||")
        except Exception as e:
            try:
                logging.error(f"a collect_one_combination: {str(e)}")
                logging.error(f"b collect_one_combination: {str(ias)}")
                logging.error(f"c collect_one_combination: {self.create_targeting_spec_from_ias(ias)}")
            except Exception as e2:
                logging.error(f"d collect_one_combination e2: {str(e2)}")
   


    def start_collection(self, collection_config={}, collection_id=None, skip_n=0):
        '''
        
        '''
        # 
        if collection_id == None:
            # fail?
            collection_id=-1
        # prepare main loop:
        catlens = [len(self.input_data_json.get(cat,[]))+1 for cat in self.categories]

        

        if collection_config.get("less_combinations",False)==True:
            print("less_combinations==True --> not doing all combinations. Only:", catlens[0]*(catlens[1]-1)*catlens[2]*catlens[3]*(catlens[4]+catlens[5]+catlens[6]+catlens[7]))
            print("less_combinations==True --> not doing all combinations. instead of :", catlens[0]*catlens[1]*(catlens[2]*catlens[3]*catlens[4]*catlens[5]*catlens[6]*catlens[7]))
            # start main loop:
# self.categories = ["scholarities","geo_locations","ages_ranges","genders","behavior",
#   "interests","flexible_spec"]

            for i0 in range(catlens[0]): # edu
                # export collection:
                self.export_results(i0=i0)
                for i1 in range(catlens[1]): # geo
                    for i2 in range(catlens[2]): # age
                        for i3 in range(catlens[3]): # gender
                            i5=i6=i7=0
                            for i4 in range(catlens[4]): # behavior
                                ias = (i0,i1,i2,i3,i4,i5,i6,i7)
                                #print("ias: ", ias, i4, catlens[4])
                                #logging.info(f"ias: {ias}, {i4}, {catlens[4]}")
                                self.collect_one_combination(ias,collection_config)
                            i4=i5=i6=i7=0
                            for i5 in range(catlens[5]): # interests
                                ias = (i0,i1,i2,i3,i4,i5,i6,i7)
                                self.collect_one_combination(ias,collection_config)
                            i4=i5=i6=i7=0
                            for i6 in range(catlens[6]): # flexible_spec
                                ias = (i0,i1,i2,i3,i4,i5,i6,i7)
                                self.collect_one_combination(ias,collection_config)
                            for i7 in range(catlens[7]): # publisher_platforms
                                ias = (i0,i1,i2,i3,i4,i5,i6,i7)
                                self.collect_one_combination(ias,collection_config)
        else:
            print("less_combinations==False --> doing all combinations. not only:", catlens[0]*catlens[1]*(catlens[2]+catlens[3]+catlens[4]+catlens[5]+catlens[6]+catlens[7]))
            print("less_combinations==False --> doing all combinations. but do :", catlens[0]*catlens[1]*(catlens[2]*catlens[3]*catlens[4]*catlens[5]*catlens[6]*catlens[7]))
            # start main loop:
            for i0 in range(catlens[0]):
                self.export_results(i0=i0)
                for i1 in range(catlens[1]):
                    for i2 in range(catlens[2]):
                        for i3 in range(catlens[3]):
                            for i4 in range(catlens[4]):
                                for i5 in range(catlens[5]):
                                    for i6 in range(catlens[6]):
                                        for i7 in range(catlens[7]):
                                            ias = (i0,i1,i2,i3,i4,i5,i6,i7)
                                            self.collect_one_combination(ias, collection_config)
        self.export_results(i0=9999)                                        
        self.finish_collection(collection_id)
        print("collection finished!", len(self.results_mau),datetime.now())
        logging.info(f"collection finished! {len(self.results_mau)} {datetime.now()}")

    # def get_and_save_results(self,ias,prediction,sub1000=True):
    #     """
    #     Gets the results for a given ias-tuple.
    #     Saves the results to the db.
    #     """
    #     audience = self.get_results(ias,sub1000=sub1000)
    #     # save results:
    #     self.extract_and_save_result(ias,audience,prediction)
    #     return audience

    def get_all_predictions(self,ias):
        # 1. generate all variations of ias that can form a prediction
        # 2. get the prediction for each variation
        # 3. profit?!

        # what are iasp1 : differs in one category
        # then iasp2: is the same as ias in the cat that differs in iasp1
        # ... differs in some other category
        # then iasp3: is the same as ias in the cat that differs in iasp2 
        # and the same in iasp1
        ias = list(ias)
        iasplist = []
        for i in range(len(ias)):
            if ias[i]!=0:
                for j in range(0,ias[i]): # or only up to ias[i]-1?
                    iasp1 = ias[:]
                    iasp1[i] = j
                    for k in range(len(ias)):
                        if k!=i:
                            if ias[k]!=0:
                                for l in range(0,ias[k]):
                                    iasp2 = ias[:]
                                    iasp2[k] = l
                                    iasp3 = ias[:]
                                    iasp3[k] = l
                                    iasp3[i] = j
                                    iasplist.append((iasp1,iasp2,iasp3))
        # I have to somehow remove the duplicates, because iasp1 and iasp2 
        # are interchangeable.
        # return iasplist
        #print("iasplist: ", iasplist)
        #print("results_mau: ", self.results_mau)

        predictions = []
        for iasp1,iasp2,iasp3 in iasplist:
            #print(iasp1,iasp2,iasp3)
            #print(results[tuple(iasp1)],results[tuple(iasp2)],results[tuple(iasp3)])
            #print(results[tuple(iasp1)]*results[tuple(iasp2)]/results[tuple(iasp3)])
            #predictions.append(self.results_mau[tuple(iasp1)]*self.results_mau[tuple(iasp2)]/self.results_mau[tuple(iasp3)])
            res_iasp1= self.results_mau.get(tuple(iasp1),-1)
            res_iasp2= self.results_mau.get(tuple(iasp2),-1)
            res_iasp3= self.results_mau.get(tuple(iasp3),-1)
            if res_iasp1>=1000 and res_iasp2>=1000 and res_iasp3>=1000:
                #print("get-all-predictions...",res_iasp1,res_iasp2,res_iasp3)
                predictions.append(res_iasp1*res_iasp2/res_iasp3)
            else:
                pass # predictions.append(-1)
            #predictions.append(self.results_mau.get(tuple(iasp1),*self.results_mau[tuple(iasp2)]/self.results_mau[tuple(iasp3)])
            #predictions.append(results.item(tuple(iasp1))*results.item(tuple(iasp2))/results.item(tuple(iasp3)))

        return predictions



    def handle_send_request_error(self, response, url, params, ias, targeting_spec, prediction, tryNumber):
        """called only by self.send_request"""
        try:
            error_json = json.loads(response.text)
            if error_json["error"]["code"] == constants.API_UNKOWN_ERROR_CODE_1 or error_json["error"][
                "code"] == constants.API_UNKOWN_ERROR_CODE_2:
                logging.error(f"{error_json=}, {params=}, {url=}")
                time.sleep(constants.INITIAL_TRY_SLEEP_TIME * tryNumber)
                return self.send_request(url, params, ias,targeting_spec, prediction, tryNumber)
            # elif error_json["error"]["code"] == constants.INVALID_PARAMETER_ERROR and "error_subcode" in error_json[
            #     "error"] and error_json["error"]["error_subcode"] == constants.FEW_USERS_IN_CUSTOM_LOCATIONS_SUBCODE_ERROR:
            #     return get_fake_response()
            # elif "message" in error_json["error"] and "Invalid zip code" in error_json["error"][
            #     "message"] and constants.INGORE_INVALID_ZIP_CODES:
            #     print_warning("Invalid Zip Code:" + str(params[constants.TARGETING_SPEC_FIELD]))
            #     return get_fake_response()
            elif error_json["error"]["code"] == 80004 or error_json["error"]["code"] == '80004':
                print("80004, baba", datetime.now())
                logging.info(f"80004, baba . sleep for 45 minutes. ")
                time.sleep(60*45)
                return self.send_request(url, params, ias,targeting_spec, prediction, tryNumber)
            else:
                logging.error("Could not handle error.")
                logging.error("Error Code:" + str(error_json["error"]["code"]))
                if "message" in error_json["error"]:
                    logging.error("Error Message:" + str(error_json["error"]["message"]))
                if "error_subcode" in error_json["error"]:
                    logging.error("Error Subcode:" + str(error_json["error"]["error_subcode"]))
                raise Exception(str(error_json["error"]))
        except Exception as e:
            logging.error(f"{e=}")
            logging.error("Could not handle error.")
            logging.error(str(response.text))
            raise Exception(str(response.text))
        
    def send_request(self, url, params,ias,targeting_spec, prediction, tryNumber=0):
        """called only by self.call_request_fb"""
        tryNumber += 1
        time.sleep(3)
        # todo: more intelligent sleep-management
        if tryNumber >= 20: # self.MAX_NUMBER_TRY:
            print("Maximum Number of Tries reached. Failing.")
            logging.error("Maximum Number of Tries reached. Failing.")
            raise Exception("Maximum try reached.")
        try:
            response = requests.get(url, params=params, timeout=constants.REQUESTS_TIMEOUT)
            # save request:
            fk_queries = self.save_request(url, params, response, ias, targeting_spec, tryNumber)
            if response.status_code == 200:
                self.extract_and_save_result(ias, targeting_spec,responsecontent=response.content, prediction=prediction,collection_id=self.collection_id,fk_queries=fk_queries)
            elif response.status_code == 999 :
                print("999")
                time.sleep(30)
        except Exception as error:
            print("Error, ", str(error))
            time.sleep(10)
            #raise Exception(error)
        if response.status_code == 200:
            return response
        else:
            return self.handle_send_request_error(response, url, params, ias, targeting_spec, prediction, tryNumber)


    def call_request_fb(self, ias, prediction):
        targeting_spec = self.create_targeting_spec_from_ias(ias)
        #print("tarspec: ",targeting_spec)
        payload = {
            'optimization_goal': "AD_RECALL_LIFT",
            'targeting_spec': json.dumps(targeting_spec),
            'access_token': self.token,
        }
        payload_str = str(payload)
        #print("\tSending in request: %s" % (payload_str))
        url = constants.REACHESTIMATE_URL.format(self.account_number)
        response = self.send_request(url, payload,ias,targeting_spec,prediction)
        return response.content


    def create_targeting_spec_from_ias(self,ias):
        return self.create_targeting_spec_from_list_of_ias([ias])

    def create_targeting_spec_from_list_of_ias(self,iaslist):
        newspec = {}# todo: add parts that do not change
        print("iaslist: ", iaslist, end=f";!")
        for ias in iaslist:
            print("ias: ", ias, end=f"!?")
            for ia,cat in zip(ias,self.categories):
                print("ia,cat: ", ia,cat, end="|")
                if ia!=0:
                    if cat=="geo_locations":
                        if cat not in newspec:
                            newspec[cat] = {self.input_data_json[cat][ia-1]["name"]:self.input_data_json[cat][ia-1]["values"], "location_types":self.input_data_json[cat][ia-1]["location_types"]}
                        else:
                            newspec[cat][self.input_data_json[cat][ia-1]["name"]].append(self.input_data_json[cat][ia-1]["values"])
                    elif cat=="ages_ranges":
                        if "age_min" not in newspec:
                            if "min" in self.input_data_json[cat][ia-1]:
                                newspec["age_min"] = self.input_data_json[cat][ia-1]["min"]
                        else:
                            newspec["age_min"] = min(newspec["age_min"],self.input_data_json[cat][ia-1]["min"])
                        if "age_max" not in newspec:
                            if "max" in self.input_data_json[cat][ia-1]:
                                newspec["age_max"] = self.input_data_json[cat][ia-1]["max"]
                        else:
                            newspec["age_max"] = max(newspec["age_max"],self.input_data_json[cat][ia-1]["max"])
                    elif cat=="behavior":
                        # error before edits: list indices must be integers or slices, not str
                        if "flexible_spec" not in newspec:
                            newspec["flexible_spec"] = [{"behaviors":[self.input_data_json[cat][ia-1]]}]#{"name":self.input_data_json[cat][ia-1]["name"],"id":self.input_data_json[cat][ia-1]["id"]}]}]#["or"][0]}]}]
                        else:
                            # ia can be 1 or 2 or 3
                            if "behaviors" in newspec["flexible_spec"]:
                                newspec["flexible_spec"]["behaviors"].append(self.input_data_json[cat][ia-1])
                            else:
                                newspec["flexible_spec"].append({"behaviors":[self.input_data_json[cat][ia-1]]})
                    elif cat=="scholarities":
                        if "flexible_spec" not in newspec:
                            # "flexible_spec" is a list of dictionaries
                            newspec["flexible_spec"] = [{"education_statuses":self.input_data_json[cat][ia-1]["or"]}]
                        else:
                            # "flexible_spec" is already there
                            education_statuses_is_in_flexible_spec = False
                            for fl in newspec["flexible_spec"]:
                                if "education_statuses" in fl:
                                    # "education_statuses" is also there. Extend it:
                                    fl["education_statuses"].extend(self.input_data_json[cat][ia-1]["or"])
                                    education_statuses_is_in_flexible_spec = True
                            if not education_statuses_is_in_flexible_spec:
                                # "education_statuses" is not there. Add it:
                                newspec["flexible_spec"].append({"education_statuses":self.input_data_json[cat][ia-1]["or"]})
                    elif cat=="flexible_spec":
                        if cat not in newspec:
                            # "flexible_spec" is a list of dictionaries
                            newspec[cat] = [self.input_data_json[cat][ia-1]]
                        else:
                            newspec[cat].append(self.input_data_json[cat][ia-1])
                            
                    elif cat=="publisher_platforms":
                        if cat not in newspec:
                            newspec[cat] = self.input_data_json[cat][ia-1]
                        else:
                            newspec[cat].extend(self.input_data_json[cat][ia-1])

                    else:
                        if cat not in newspec:
                            newspec[cat]=[self.input_data_json[cat][ia-1]]
                        else:
                            newspec[cat].append(self.input_data_json[cat][ia-1])
                else: # ia==0
                    if cat=="geo_locations":
                        newspec["geo_locations"] = {"country_groups":["worldwide"],"location_types": ["home","recent"]}
                    pass
        print("§", end="|end")
        return newspec

    # def create_targeting_spec(self, ias):
    #     '''
    #         create targeting spec from ias.
    #         problem/todo: 
    #     '''
    #     newspec = {}
    #     # if ias[0] !=0:
    #     #     newspec["geo_locations"] = input["geo_locations"][ias[0]]
    #     # if ias[1] !=0:
    #     #     newspec["genders"] = input["genders"][ias[1]]
    #     for ia,cat in zip(ias,self.categories):
    #         if ia!=0:
    #             newspec[cat]=input[cat][ia-1]
    #         else:
    #             pass
    #     return newspec


    def save_error(error_message, more_information):
        logging.error(str(error_message))
        logging.error(str(more_information))


    # def save_result(self, url, params, response, ias, tryNumber):
    #     try:
    #         # extract audience-numbers from response.json()
    #         resj = response.json()


    #     query = f"""INSERT INTO results (url, qtime, params, response,collection_id,status_code,ias,tryNumber)
    #      VALUES ('{url}', '{str(datetime.now())}', '{params}', '{response.content}', '{self.collection_id}', '{response.status_code}', '{ias}', '{tryNumber}')"""
    #     try:
    #         self.cursor.execute(query)
    #     except Exception as e:
    #         print("\n",tryNumber,ias, "Error while saving the query! ", str(e),str(datetime.now()),)
    #         print()
    #         self.cursor.execute('INSERT INTO errors (error_message, ias,                qtime,        response) VALUES (?,?,?,?)',
    #                                                   (str(e), str(ias),    datetime.now().timestamp()))
    #         #print(cursor.lastrowid)
    #     self.db.commit()
    #     self.results[tuple(ias)] = result

    def save_request(self, url, params, response, ias,targeting_spec, tryNumber):

        #print("save_request",tryNumber,ias,targeting_spec,response.status_code)
        values = (str(url), str(datetime.now()), json.dumps(targeting_spec), str(response.content), str(self.collection_id), str(response.status_code), str(ias), str(tryNumber))
        query = f"""INSERT INTO queries (url, qtime, targeting_spec, response,collection_id,status_code,ias,tryNumber)
         VALUES (?,?,?,?,?,?,?,?)"""
        try:
            self.cursor.execute(query, values)
            return self.cursor.lastrowid
        except Exception as e:
            print("\n",tryNumber,ias, "Error while saving the query 1! ", str(e),str(datetime.now()),)
            print("query:",query)
            print("values...:",values)
            self.cursor.execute('INSERT INTO errors (error_message, ias,                qtime,        response, comment) VALUES (?,?,?,?,?)',
                                                      (str(e), str(ias),    datetime.now().timestamp(),str(response),str(query)))
            #print(cursor.lastrowid)
        self.db.commit()


    def extract_and_save_result(self, ias, targeting_spec,responsecontent="", prediction=[],collection_id=0, query_string="",query_skipped=False,geolocation=None,desspec="|||||",fk_queries=0):
        """
        save query to a local SQLite database.
        Extract some information (mau, ...) if possible
        """
        mau=-5
        returnerror=0
                    
        # try:
        #     jsn = json.loads(responsecontent)
        # except Exception as e:
        #     logging.error(e)
        #     return

        # for cat in self.categories:
        #     try:
        #         cat_value = jsn["data"][cat]
        #     except Exception as e:
        #         logging.error(e)
        #         return

        # print("ts: ", targeting_spec)
        logging.info(f"{ias=}, ts={targeting_spec}, {responsecontent=}")
        try:
            if responsecontent!="skipped":
                try:
                    jsn = json.loads(responsecontent)
                    #print(jsn)
                    mau_lower = str(jsn["data"][0]['estimate_mau_lower_bound'])
                    mau_upper = str(jsn["data"][0]['estimate_mau_upper_bound'])
                    mau =audience_size= int((float(mau_lower)+float(mau_upper))/2)
                    dau = str(jsn["data"][0]['estimate_dau'])
                    estimate_ready = str(jsn["data"][0]['estimate_ready'])
                    # print(ias,mau,dau,estimate_ready,datetime.now())
                    logging.info(f"{ias=}, {mau=}, {dau=}, {estimate_ready=}")
                except Exception as e:
                    logging.info(str(ias) + "savequeryerror1" + str(e) + responsecontent)
                    returnerror=1
                    mau = mau_lower=mau_upper="-2"
                    dau = "-2"
                    estimate_ready = "-2"
                    audience_size = "-2"
                    #time.sleep(3600)
            elif responsecontent=="skipped":
                if True: # self.config.get("verbose",False)==True:
                    # print(ias,"skipped")
                    logging.info(f"{ias=}, skipped")
                mau=dau=mau_lower=mau_upper=audience_size=-3
                estimate_ready = -3
                audience_size = -3
            else:
                mau=mau_lower=mau_upper=audience_size="-4"
                print("this shouldnt happen!")
            try:
                genders = str(json.dumps(targeting_spec['genders']))
            except Exception as e:
                genders = "no"
                #print(ias,"saveerror2 - genders", str(e))
            try:
                geo_locations =  json.dumps(targeting_spec["geo_locations"])
            except Exception as e:
                geo_locations =  "notSpecified"
                print(ias,"saveerror2 - geo_locations", str(e))
                logging.info(f"{ias=}, {str(e)=}")
            try:
                education_statuses = "notSpecified"
                if "flexible_spec" in targeting_spec:
                    for fl in targeting_spec["flexible_spec"] :
                        if "education_statuses" in fl:
                            education_statuses = json.dumps(fl["education_statuses"])
            except Exception as e:
                education_statuses = "notSpecified"
                #print(ias,"saveerror2 - education-statuesegenders", str(e))
            try:
                behaviors = "notSpecified"
                if "flexible_spec" in targeting_spec:
                    for fl in targeting_spec["flexible_spec"] :
                        if "behaviors" in fl:
                            behaviors = json.dumps(fl["behaviors"])
            except Exception as e:
                behaviors = "notSpecified"
                #print(ias,"saveerror2 - behaviors", str(e))
            try:
                age_min = str(json.dumps(targeting_spec['age_min']))
            except Exception as e:
                age_min = "0"
                #print(ias,"saveerror2 - age_min", str(e))
            try:
                age_max = targeting_spec['age_max']
            except Exception as e:
                age_max = "100"
                #print(ias,"saveerror2 - age_max", str(e))
            try:
                prediction_mean = st.mean(prediction)
                prediction_std= st.stdev(prediction)
                prediction_min= min(prediction)
                prediction_max= max(prediction)
            except Exception as e:
                prediction_mean = -2
                prediction_std= -2
                prediction_min= -2
                prediction_max= -2
            if mau > 0:
                self.results_mau[tuple(ias)] = mau

            #print(ias,mau,dau,audience_size,estimate_ready,genders,geo_locations,age_min,age_max,education_statuses,behaviors,ias,irun)
            query_string = '''INSERT INTO results (fk_queries,  targeting_spec,   qtime,      response,mau,mau_lower,mau_upper,
            dau,audience_size,estimate_ready,genders,geo_locations,age_min,age_max,education_statuses,behaviors,ias,collection_id,
            predictions,prediction_mean,prediction_std,prediction_min, prediction_max) VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?)'''
            values = [str(x) for x in [fk_queries, json.dumps(targeting_spec),  str(datetime.now().timestamp()),  responsecontent,mau,mau_lower,mau_upper,
            dau,audience_size,estimate_ready,genders,geo_locations,age_min,age_max,education_statuses,behaviors,str(ias),collection_id,
            prediction[:12],prediction_mean,prediction_std,prediction_min, prediction_max]]
            #print("values:", values)
            self.cursor.execute(query_string,values)
        except Exception as e:
            returnerror=2
            print("\n",ias, "Error while saving the query 2 ! ", str(e),str(datetime.now()), "query:",query_string)
            logging.error(f"{ias=}, {str(e)=}, {query_string=}")
            print()
            self.cursor.execute('INSERT INTO results (query_string, targeting_spec,                qtime,        response) VALUES (?,?,?,?)',
                                                (str(query_string), json.dumps(targeting_spec),    datetime.now().timestamp(),responsecontent))
            #print(cursor.lastrowid)
        self.db.commit()
        return mau,returnerror #cursor.lastrowid

    def export_results(self,fn="",i0=0):
        """
        export results to a csv file
        """
        q = f"""SELECT pk_results,ias,datetime(qtime,"unixepoch") as query_time, mau,
genders, geo_locations,age_min,age_max, education_statuses, behaviors,
mau, dau, mau_lower,mau_upper,targeting_spec,
json_extract(geo_locations, '$.countries[0]') AS country,
json_extract(targeting_spec, '$.flexible_spec') AS flex

FROM results"""
        q2 = f"""SELECT *,
CASE 
  WHEN relationship_statuses = '["3","2","4"]' THEN 'single'
  WHEN relationship_statuses = '["1","12","11","13"]' THEN 'in_partnership'
  ELSE 'all_relationship'
END relationship_alias,
CASE
  WHEN education_statuses = '[2,3,4,5,6,7,8,9,10,11]' THEN 'at_least_highschool'
  ELSE 'all_education'
END education_alias
FROM (
SELECT pk_results,ias,datetime(qtime,"unixepoch") as query_time, mau,
--genders, age_min,age_max, --education_statuses, 
mau, dau, mau_lower,mau_upper,
json_extract(geo_locations, '$.countries[0]') AS country,
json_extract(targeting_spec, '$.interests[0].name') AS interests, 
coalesce(json_extract(targeting_spec, '$.age_min'),"") || "-" || coalesce(json_extract(targeting_spec, '$.age_max'),"") as age_group,
json_extract(targeting_spec, '$.genders') AS gender,
CASE 
  WHEN json_extract(targeting_spec, '$.genders') = '[1]' THEN 'M'
  WHEN json_extract(targeting_spec, '$.genders') = '[2]' THEN 'F'
  ELSE 'all'
END as gender_alias,
coalesce(json_extract(targeting_spec, '$.flexible_spec[0].relationship_statuses'),"") || coalesce(json_extract(targeting_spec, '$.flexible_spec[1].relationship_statuses'),"") || coalesce(json_extract(targeting_spec, '$.flexible_spec[2].relationship_statuses'),"") as relationship_statuses,
coalesce(json_extract(targeting_spec, '$.flexible_spec[0].education_statuses'),"") || coalesce(json_extract(targeting_spec, '$.flexible_spec[1].education_statuses'),"")|| coalesce(json_extract(targeting_spec, '$.flexible_spec[2].education_statuses'),"") as education_statuses,
coalesce(json_extract(targeting_spec, '$.flexible_spec[0].behaviors[0].name'),"") || coalesce(json_extract(targeting_spec, '$.flexible_spec[1].behaviors[0].name'),"")|| coalesce(json_extract(targeting_spec, '$.flexible_spec[2].behaviors[0].name'),"") as behaviors
FROM results
)"""
        df = pd.read_sql_query(q2, self.db)
        if fn=="":
            fn = self.db_file_name + str(datetime.now().minute) + "_" + str(i0) + ".csv"
        df.to_csv(fn, index=False)
        print("exported to ", fn)

    
