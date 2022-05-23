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
        self.categories = ["geo_locations","behavior","genders","ages_ranges","scholarities","interests"]
                
        constants.REACHESTIMATE_URL = "https://graph.facebook.com/v" + api_version + "/act_{}/delivery_estimate"
        
    def read_input_data_json(self,fn_input_data):
        with open(fn_input_data,"r") as inputjson:
            self.input_data_json =json.load(inputjson)
        
        

    def init_db(self):
        """
        creates the tables in the db"""
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS collections (
                            collection_id integer primary key autoincrement,
                            collection_name text,
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
                            query_string varchar(5000),
                            targeting_spec json,
                            query_time varchar(50),
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
                            query_time varchar(50),
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
                            query_time varchar(50),
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

    def start_new_collection(self,targeting_def_json, options_json):
        '''save collection-metadata to collections-column
        then start collection with starting-point=0
        '''
        ...


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
        # todo: We should somehow remove (or avoid) the duplicates, 
        # because iasp1 and iasp2 are interchangeable.
        # return iasplist
        #print("iasplist: ",iasplist)
        predictions = []
        for iasp1,iasp2,iasp3 in iasplist:
            #print(iasp1,iasp2,iasp3)
            #print(results[tuple(iasp1)],results[tuple(iasp2)],results[tuple(iasp3)])
            #print(results[tuple(iasp1)]*results[tuple(iasp2)]/results[tuple(iasp3)])
            predictions.append(self.results[tuple(iasp1)]*self.results[tuple(iasp2)]/self.results[tuple(iasp3)])
            #predictions.append(results.item(tuple(iasp1))*results.item(tuple(iasp2))/results.item(tuple(iasp3)))

        return predictions

    def collect_one_combination(self,ias,collection_config):
        
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

        # 2. sub-1000 handling: skip or  make extra-requests?
        if self.predictions_median[ias] < 1050:
            if collection_config.get("skip_sub_1000",True)==True:
                # save prediction but skip the request
                self.safe_result(ias,prediction,query_skipped=True,collection_config=collection_config)
                # todo: save to table
                audience = self.predictions_median[ias]
                #continue
            else: # don't skip, but make extra requests:
                # ...
                audience = self.get_and_save_results(ias,prediction,sub1000=True)
        else:
            # make a normal request:
            audience = self.get_and_save_results(ias,prediction)
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

        # Numpy-arrays for fast access to results (needed for predictions)
        results_mau = np.ones((catlens))
        predictions_median = np.full((catlens),-1)
        predictions_stdev = np.full((catlens),-1)
        

        if collection_config.get("less_combinations",False)==True:
            print("less_combinations==True --> not doing all combinations. Only:", catlens[0]*catlens[1]*(catlens[2]+catlens[3]+catlens[4]+catlens[5]))
            print("less_combinations==True --> not doing all combinations. instead of :", catlens[0]*catlens[1]*(catlens[2]*catlens[3]*catlens[4]*catlens[5]))
            # start main loop:
            for i0 in range(catlens[0]):
                for i1 in range(catlens[1]):
                    i2=i3=i4=i5=0
                    for i2 in range(catlens[2]):
                        ias = (i0,i1,i2,i3,i4,i5)
                        self.collect_one_combination(ias,results_mau,predictions_median,predictions_stdev)
                    i2=i3=i4=i5=0
                    for i3 in range(catlens[3]):
                        ias = (i0,i1,i2,i3,i4,i5)
                        self.collect_one_combination(ias,results_mau,predictions_median,predictions_stdev)
                    i2=i3=i4=i5=0
                    for i4 in range(catlens[4]):
                        ias = (i0,i1,i2,i3,i4,i5)
                        self.collect_one_combination(ias,results_mau,predictions_median,predictions_stdev)
                    i2=i3=i4=i5=0
                    for i5 in range(catlens[5]):
                        ias = (i0,i1,i2,i3,i4,i5)
                        self.collect_one_combination(ias,results_mau,predictions_median,predictions_stdev)
        else:
            print("less_combinations==False --> doing all combinations. not only:", catlens[0]*catlens[1]*(catlens[2]+catlens[3]+catlens[4]+catlens[5]))
            print("less_combinations==False --> doing all combinations. but do :", catlens[0]*catlens[1]*(catlens[2]*catlens[3]*catlens[4]*catlens[5]))
            # start main loop:
            for i0 in range(catlens[0]):
                for i1 in range(catlens[1]):
                    for i2 in range(catlens[2]):
                        for i3 in range(catlens[3]):
                            for i4 in range(catlens[4]):
                                for i5 in range(catlens[5]):
                                    ias = (i0,i1,i2,i3,i4,i5)
                                    self.collect_one_combination(ias,results_mau,predictions_median,predictions_stdev)
    

    def get_and_save_results(self,ias,prediction,sub1000=True):
        """
        Gets the results for a given ias-tuple.
        Saves the results to the db.
        """
        # create targeting-spec:
        targeting_spec = self.create_targeting_spec(ias)
        audience = self.get_results(ias,sub1000=sub1000)
        # save results:
        self.save_results(ias,audience,prediction)
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
            predictions.append(self.results[tuple(iasp1)]*self.results[tuple(iasp2)]/self.results[tuple(iasp3)])
            #predictions.append(results.item(tuple(iasp1))*results.item(tuple(iasp2))/results.item(tuple(iasp3)))

        return predictions


    def handle_send_request_error(self, response, url, params, ias, tryNumber):
        try:
            error_json = json.loads(response.text)
            if error_json["error"]["code"] == constants.API_UNKOWN_ERROR_CODE_1 or error_json["error"][
                "code"] == constants.API_UNKOWN_ERROR_CODE_2:
                logging.error(f"{error_json}, {params}, {url}")
                time.sleep(constants.INITIAL_TRY_SLEEP_TIME * tryNumber)
                return self.send_request(url, params, ias, tryNumber)
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
        
    def send_request(self, url, params,ias, tryNumber=0):
        """called only by self.call_request_fb"""
        tryNumber += 1
        if tryNumber >= 20: # self.MAX_NUMBER_TRY:
            print("Maximum Number of Tries reached. Failing.")
            raise Exception("Maximum try reached.")
        try:
            response = requests.get(url, params=params, timeout=constants.REQUESTS_TIMEOUT)
            # save request:
            self.save_request(url, params, response, ias, tryNumber)
        except Exception as error:
            raise Exception(error.message)
        if response.status_code == 200:
            return response
        else:
            return self.handle_send_request_error(response, url, params, ias, tryNumber)


    def call_request_fb(self, ias):
        payload = {
            'optimization_goal': "AD_RECALL_LIFT",
            'targeting_spec': json.dumps(self.create_targeting_spec_from_ias(ias)),
            'access_token': self.token,
        }
        payload_str = str(payload)
        #print("\tSending in request: %s" % (payload_str))
        url = constants.REACHESTIMATE_URL.format(self.account)
        response = self.send_request(url, payload)

        return response.content

    def create_targeting_spec_from_ias(self,ias):
        return self.create_targeting_spec_from_list_of_ias([ias])

    def create_targeting_spec_from_list_of_ias(self,iaslist):
        newspec = {}# todo: add parts that do not change
        
        for ias in iaslist:
            for ia,cat in zip(ias,self.categories):
                if ia!=0:
                    if cat not in newspec:
                        newspec[cat]=self.input_data_json[cat][ia-1]
                    else:
                        newspec[cat]+=self.input_data_json[cat][ia-1]
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
    #         self.cursor.execute('INSERT INTO errors (error_message, ias,                query_time,        response) VALUES (?,?,?,?)',
    #                                                   (str(e), str(ias),    datetime.now().timestamp()))
    #         #print(cursor.lastrowid)
    #     self.connection.commit()
    #     self.results[tuple(ias)] = result

    def save_request(self, url, params, response, ias, tryNumber):
        query = f"""INSERT INTO queries (url, qtime, params, response,collection_id,status_code,ias,tryNumber)
         VALUES ('{url}', '{str(datetime.now())}', '{params}', '{response.content}', '{self.collection_id}', '{response.status_code}', '{ias}', '{tryNumber}')"""
        try:
            self.cursor.execute(query)
        except Exception as e:
            print("\n",tryNumber,ias, "Error while saving the query! ", str(e),str(datetime.now()),)
            print()
            self.cursor.execute('INSERT INTO errors (error_message, ias,                query_time,        response) VALUES (?,?,?,?)',
                                                      (str(e), str(ias),    datetime.now().timestamp()))
            #print(cursor.lastrowid)
        self.connection.commit()


    def save_query(self, ias, responsecontent="", collection_id=0, query_string="",targetspec="",geolocation=None,desspec="|||||"):
        """
        save query to a local SQLite database.
        Extract some information (mau, ...) if possible
        """
        mau=-5
        returnerror=0
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
                    audience_size = mau 
                except Exception as e:
                    print(iquery,"savequeryerror1",str(e),responsecontent,desspec,str(datetime.now()))
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
                mau=mau_lower=mau_upper="-4"
                print("this shouldnt happen!")
            try:
                genders = str(json.dumps(targetspec['genders']))
            except Exception as e:
                genders = "no"
                print(iquery,"saveerror2 - genders", str(e))
            try:
                geo_locations =  json.dumps(targetspec["geo_locations"])
            except Exception as e:
                geo_locations =  "notSpecified"
                print(iquery,"saveerror2 - geo_locations", str(e))
            try:
                education_statuses = json.dumps(targetspec["education_statuses"])
            except Exception as e:
                education_statuses = "notSpecified"
                #print(iquery,"saveerror2 - education-statuesegenders", str(e))
            try:
                behaviors = json.dumps(targetspec["flexible_spec"])
            except Exception as e:
                behaviors = "notSpecified"
                print(iquery,"saveerror2 - behabiors", str(e))
            try:
                age_min = str(json.dumps(targetspec['age_min']))
            except Exception as e:
                age_min = "0"
                print(iquery,"saveerror2 - age_min", str(e))
            try:
                age_max = targetspec['age_max']
            except Exception as e:
                age_max = "100"
                print(iquery,"saveerror2 - age_max", str(e))

            #print(iquery,mau,dau,audience_size,estimate_ready,genders,geo_locations,age_min,age_max,education_statuses,behaviors,iquery,irun)
            self.cursor.execute('INSERT INTO queries (query_string,   targeting_spec,   query_time,      response,mau,mau_lower,mau_upper,dau,audience_size,estimate_ready,genders,geo_locations,age_min,age_max,education_statuses,behaviors,iquery,irun,desspec) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (str(query_string),  json.dumps(targetspec),  str(datetime.now().timestamp()),  responsecontent,mau,mau_lower,mau_upper,dau,audience_size,estimate_ready,genders,geo_locations,age_min,age_max,education_statuses,behaviors,iquery,irun,desspec))
        except Exception as e:
            returnerror=2
            print("\n",iquery, "Error while saving the query! ", str(e),str(datetime.now()))
            print()
            self.cursor.execute('INSERT INTO queries (query_string, targeting_spec,                query_time,        response) VALUES (?,?,?,?)',
                                                (str(query_string), json.dumps(targetspec),    datetime.now().timestamp(),responsecontent))
            #print(cursor.lastrowid)
        self.connection.commit()
        return mau,returnerror #cursor.lastrowid

