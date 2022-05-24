# 


import sqlite3
import os
import numpy as np
import statistics as st
from datetime import datetime
import requests
import json
import time
import constants
import logging




class AudienceCollector:

    def __init__(self, db_file_name, fn_input_data=None, token=None,account_number=None,credentials_fn=None,api_version="13.0"):
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
        self.read_input_data_json(fn_input_data)
        self.categories = ["geo_locations","behavior","genders","ages_ranges","scholarities", "interests"]
                
        constants.REACHESTIMATE_URL = "https://graph.facebook.com/v" + api_version + "/act_{}/delivery_estimate"

        catlens = [len(self.input_data_json[cat])+1 for cat in self.categories]

        # Numpy-arrays for fast access to results (needed for predictions)
        self.results_mau = {} #np.ones((catlens))
        self.predictions_median = {} #np.full((catlens),-1)
        self.predictions_stdev = {} #np.full((catlens),-1)
        self.predictions_len = {} #np.full((catlens),-1)

    def read_input_data_json(self,fn_input_data):
        with open(fn_input_data,"r") as inputjson:
            self.input_data_json =json.load(inputjson)
        
        

    def init_db(self):
        """
        creates the tables in the db"""
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS collections (
                            collection_id integer primary key autoincrement,
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
        print(result_info)
        q4 = f"""SELECT * from errors where fk_queries = {query_info[0]} ORDER BY pk_errors DESC LIMIT 1"""
        self.cursor.execute(q4)
        error_info = self.cursor.fetchone()
        print(error_info)
        q5 = f"""SELECT * from todo_later where fk_results = {result_info[0]} ORDER BY pk_todo_later DESC LIMIT 1"""
        self.cursor.execute(q5)
        todo_later_info = self.cursor.fetchone()
        print(todo_later_info)

        
    def restart_collection(collection_id):
        """
        Restarts a collection.
        1. retrieves necessary collection input from db (what to collect, where to start)
        """
        ...
        query = f"""UPDATE """

    def start_new_collection(self,input_data_json, options_json, collection_name="default_collection", comment=""):
        '''save collection-metadata to collections-column
        then start collection with starting-point=0
        '''
        print("start_new_collection",(collection_name, input_data_json, False, datetime.now(), "", options_json,  comment))
        query = f"""INSERT INTO collections (collection_name, input_data_json, finished, start_time, end_time, config, comment)
                    VALUES (?,?,?,?,?, ?,?)"""
        self.cursor.execute(query, (collection_name, str(input_data_json), str(False), str(datetime.now()), "", str(options_json),  comment))
        self.collection_id = self.cursor.lastrowid
        self.start_collection(input_data_json, options_json, collection_id = self.collection_id,)


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
        print(ias, "prediction_median: ",self.predictions_median[ias])
        print(ias, "prediction_median: ",self.predictions_median)
        # 2. sub-1000 handling: skip or  make extra-requests?
        if 1 < self.predictions_median[ias] < 1050:
            if collection_config.get("skip_sub_1000",True)==True:
                # save prediction but skip the request
                self.extract_and_save_result(ias,prediction,query_skipped=True,collection_config=collection_config)
                # todo: save to table
                audience = self.predictions_median[ias]
                #continue
            else: # don't skip, but make extra requests:
                # ...
                audience = self.call_request_fb(ias,prediction)
        else:
            # make a normal request:
            audience = self.call_request_fb(ias,prediction)
        self.results_mau[ias] = audience


        #print("---", ias, audience,"pred:",prediction,"|||")



    def start_collection(self, input_data_json, collection_config={}, collection_id=None, skip_n=0):
        '''
        
        '''
        # 
        if collection_id == None:
            # fail?
            collection_id=-1
        # prepare main loop:
        catlens = [len(input_data_json[cat])+1 for cat in self.categories]

        

        if collection_config.get("less_combinations",False)==True:
            print("less_combinations==True --> not doing all combinations. Only:", catlens[0]*catlens[1]*(catlens[2]+catlens[3]+catlens[4]+catlens[5]))
            print("less_combinations==True --> not doing all combinations. instead of :", catlens[0]*catlens[1]*(catlens[2]*catlens[3]*catlens[4]*catlens[5]))
            # start main loop:
            for i0 in range(1, catlens[0]):
                for i1 in range(catlens[1]):
                    i2=i3=i4=i5=0
                    for i2 in range(catlens[2]):
                        ias = (i0,i1,i2,i3,i4,i5)
                        self.collect_one_combination(ias,collection_config)
                    i2=i3=i4=i5=0
                    for i3 in range(1,catlens[3]):
                        ias = (i0,i1,i2,i3,i4,i5)
                        self.collect_one_combination(ias,collection_config)
                    i2=i3=i4=i5=0
                    for i4 in range(1,catlens[4]):
                        ias = (i0,i1,i2,i3,i4,i5)
                        self.collect_one_combination(ias,collection_config)
                    i2=i3=i4=i5=0
                    for i5 in range(1,catlens[5]):
                        ias = (i0,i1,i2,i3,i4,i5)
                        self.collect_one_combination(ias,collection_config)
        else:
            print("less_combinations==False --> doing all combinations. not only:", catlens[0]*catlens[1]*(catlens[2]+catlens[3]+catlens[4]+catlens[5]))
            print("less_combinations==False --> doing all combinations. but do :", catlens[0]*catlens[1]*(catlens[2]*catlens[3]*catlens[4]*catlens[5]))
            # start main loop:
            for i0 in range(1,catlens[0]):
                for i1 in range(catlens[1]):
                    for i2 in range(catlens[2]):
                        for i3 in range(catlens[3]):
                            for i4 in range(catlens[4]):
                                for i5 in range(catlens[5]):
                                    ias = (i0,i1,i2,i3,i4,i5)
                                    self.collect_one_combination(ias, collection_config)
    

    def get_and_save_results(self,ias,prediction,sub1000=True):
        """
        Gets the results for a given ias-tuple.
        Saves the results to the db.
        """
        audience = self.get_results(ias,sub1000=sub1000)
        # save results:
        self.extract_and_save_result(ias,audience,prediction)
        return audience

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



        predictions = []
        for iasp1,iasp2,iasp3 in iasplist:
            #print(iasp1,iasp2,iasp3)
            #print(results[tuple(iasp1)],results[tuple(iasp2)],results[tuple(iasp3)])
            #print(results[tuple(iasp1)]*results[tuple(iasp2)]/results[tuple(iasp3)])
            #predictions.append(self.results_mau[tuple(iasp1)]*self.results_mau[tuple(iasp2)]/self.results_mau[tuple(iasp3)])
            res_iasp1= self.results_mau.get(tuple(iasp1),-1)
            res_iasp2= self.results_mau.get(tuple(iasp2),-1)
            res_iasp3= self.results_mau.get(tuple(iasp3),-1)
            if res_iasp1!=-1 and res_iasp2!=-1 and res_iasp3!=-1:
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
                logging.error(f"{error_json}, {params}, {url}")
                time.sleep(constants.INITIAL_TRY_SLEEP_TIME * tryNumber)
                return self.send_request(url, params, ias,targeting_spec, prediction, tryNumber)
            # elif error_json["error"]["code"] == constants.INVALID_PARAMETER_ERROR and "error_subcode" in error_json[
            #     "error"] and error_json["error"]["error_subcode"] == constants.FEW_USERS_IN_CUSTOM_LOCATIONS_SUBCODE_ERROR:
            #     return get_fake_response()
            # elif "message" in error_json["error"] and "Invalid zip code" in error_json["error"][
            #     "message"] and constants.INGORE_INVALID_ZIP_CODES:
            #     print_warning("Invalid Zip Code:" + str(params[constants.TARGETING_SPEC_FIELD]))
            #     return get_fake_response()
            else:
                logging.error("Could not handle error.")
                logging.error("Error Code:" + str(error_json["error"]["code"]))
                if "message" in error_json["error"]:
                    logging.error("Error Message:" + str(error_json["error"]["message"]))
                if "error_subcode" in error_json["error"]:
                    logging.error("Error Subcode:" + str(error_json["error"]["error_subcode"]))
                raise Exception(str(error_json["error"]))
        except Exception as e:
            logging.error(e)
            raise Exception(str(response.text))
        
    def send_request(self, url, params,ias,targeting_spec, prediction, tryNumber=0):
        """called only by self.call_request_fb"""
        tryNumber += 1
        time.sleep(3)
        # todo: more intelligent sleep-management
        if tryNumber >= 20: # self.MAX_NUMBER_TRY:
            print("Maximum Number of Tries reached. Failing.")
            raise Exception("Maximum try reached.")
        try:
            response = requests.get(url, params=params, timeout=constants.REQUESTS_TIMEOUT)
            # save request:
            self.save_request(url, params, response, ias, targeting_spec, tryNumber)
            if response.status_code == 200:
                self.extract_and_save_result(ias, targeting_spec,responsecontent=response.content, prediction=prediction,collection_id=self.collection_id,targetspec=targeting_spec)
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
        print("tarspec: ",targeting_spec)
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
        
        for ias in iaslist:
            for ia,cat in zip(ias,self.categories):
                if ia!=0:
                    if cat!="geo_locations":
                        if cat not in newspec:
                            newspec[cat]=[self.input_data_json[cat][ia-1]]
                        else:
                            newspec[cat].append(self.input_data_json[cat][ia-1])
                    elif cat=="geo_locations":
                        if cat not in newspec:
                            newspec[cat] = {self.input_data_json[cat][ia-1]["name"]:self.input_data_json[cat][ia-1]["values"], "location_types":self.input_data_json[cat][ia-1]["location_types"]}
                        else:
                            newspec[cat][self.input_data_json[cat][ia-1]["name"]].append(self.input_data_json[cat][ia-1]["values"])
                else:
                    pass
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
        logging.error(error_message)
        logging.error(more_information)


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

        print("save_request",tryNumber,ias,targeting_spec,response.status_code)
        values = (str(url), str(datetime.now()), str(targeting_spec), str(response.content), str(self.collection_id), str(response.status_code), str(ias), str(tryNumber))
        query = f"""INSERT INTO queries (url, qtime, targeting_spec, response,collection_id,status_code,ias,tryNumber)
         VALUES (?,?,?,?,?,?,?,?)"""
        try:
            self.cursor.execute(query, values)
        except Exception as e:
            print("\n",tryNumber,ias, "Error while saving the query 1! ", str(e),str(datetime.now()),)
            print("query:",query)
            print("values...:",values)
            self.cursor.execute('INSERT INTO errors (error_message, ias,                qtime,        response, comment) VALUES (?,?,?,?,?)',
                                                      (str(e), str(ias),    datetime.now().timestamp(),str(response),str(query)))
            #print(cursor.lastrowid)
        self.db.commit()


    def extract_and_save_result(self, ias, targeting_spec,responsecontent="", prediction=[],collection_id=0, query_string="",targetspec="",geolocation=None,desspec="|||||"):
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


        try:
            if responsecontent!="skipped":
                try:
                    jsn = json.loads(responsecontent)
                    #print(jsn)
                    mau_lower = str(jsn["data"][0]['estimate_mau_lower_bound'])
                    mau_upper = str(jsn["data"][0]['estimate_mau_upper_bound'])
                    mau = int((float(mau_lower)+float(mau_upper))/2)
                    dau = str(jsn["data"][0]['estimate_dau'])
                    estimate_ready = str(jsn["data"][0]['estimate_ready'])
                except Exception as e:
                    print(ias,"savequeryerror1",str(e),responsecontent,str(datetime.now()))
                    returnerror=1
                    mau = mau_lower=mau_upper="-2"
                    dau = "-2"
                    estimate_ready = "-2"
                    audience_size = "-2"
                    #time.sleep(3600)
            elif responsecontent=="skipped":
                mau=dau=mau_lower=mau_upper=audience_size="-3"
                estimate_ready = "-3"
                audience_size = "-3"
            else:
                mau=mau_lower=mau_upper=audience_size="-4"
                print("this shouldnt happen!")
            try:
                genders = str(json.dumps(targetspec['genders']))
            except Exception as e:
                genders = "no"
                print(ias,"saveerror2 - genders", str(e))
            try:
                geo_locations =  json.dumps(targetspec["geo_locations"])
            except Exception as e:
                geo_locations =  "notSpecified"
                print(ias,"saveerror2 - geo_locations", str(e))
            try:
                education_statuses = json.dumps(targetspec["education_statuses"])
            except Exception as e:
                education_statuses = "notSpecified"
                #print(ias,"saveerror2 - education-statuesegenders", str(e))
            try:
                behaviors = json.dumps(targetspec["flexible_spec"])
            except Exception as e:
                behaviors = "notSpecified"
                print(ias,"saveerror2 - behaviors", str(e))
            try:
                age_min = str(json.dumps(targetspec['age_min']))
            except Exception as e:
                age_min = "0"
                print(ias,"saveerror2 - age_min", str(e))
            try:
                age_max = targetspec['age_max']
            except Exception as e:
                age_max = "100"
                print(ias,"saveerror2 - age_max", str(e))
            try:
                prediction_mean = str(st.mean(prediction))
                prediction_std= str(st.stdev(prediction))
                prediction_min= str(min(prediction))
                prediction_max= str(max(prediction))
            except Exception as e:
                prediction_mean = "-2"
                prediction_std= "-2"
                prediction_min= "-2"
                prediction_max= "-2"
            self.results_mau[tuple(ias)] = mau

            #print(ias,mau,dau,audience_size,estimate_ready,genders,geo_locations,age_min,age_max,education_statuses,behaviors,ias,irun)
            query_string = '''INSERT INTO results (query_string,   targeting_spec,   qtime,      response,mau,mau_lower,mau_upper,
            dau,audience_size,estimate_ready,genders,geo_locations,age_min,age_max,education_statuses,behaviors,ias,collection_id,
            predictions,prediction_mean,prediction_std,prediction_min, prediction_max) VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?)'''
            self.cursor.execute(query_string,
            (str(query_string),  json.dumps(targetspec),  str(datetime.now().timestamp()),  responsecontent,mau,mau_lower,mau_upper,
            dau,audience_size,estimate_ready,genders,geo_locations,age_min,age_max,education_statuses,behaviors,ias,collection_id,
            prediction,prediction_mean,prediction_std,prediction_min, prediction_max))
        except Exception as e:
            returnerror=2
            print("\n",ias, "Error while saving the query 2 ! ", str(e),str(datetime.now()), "query:",query_string)
            print()
            self.cursor.execute('INSERT INTO results (query_string, targeting_spec,                qtime,        response) VALUES (?,?,?,?)',
                                                (str(query_string), json.dumps(targetspec),    datetime.now().timestamp(),responsecontent))
            #print(cursor.lastrowid)
        self.db.commit()
        return mau,returnerror #cursor.lastrowid

