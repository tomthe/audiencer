# 


import sqlite3
import os
import numpy as np
import statistics as st
from datetime import datetime


class AudienceCollector:

    def __init__(self, db_file_name, token=None,account_number=None,credentials_fn=None):
        self.db_file_name = db_file_name
        if token != None:
            self.token = token
            self.account_number = account_number
        else:
            if credentials_fn == None:
                credentials_fn = "credentials.csv"
            self.token, self.account_number = self.load_credentials_file(credentials_fn)
        self.db = sqlite3.connect(self.db_file_name)
        

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
        # call check_collection(collection_id)

    def check_collection(collection_id):
        """
        Prints information about this
        collection. Did it finish? When was the last 
        succesful query? how many of how many queries 
        are done?
        """
        ...
        query = f"""SELECT """


    def create_db_tables(self):
        """
        creates the tables in the db
        todo: copy from notebooks
        """
        pass

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

    def start_collection(self, input_data_json, collection_config={}, collection_id=None, skip_n=0):
        '''
        
        '''
        ...
        # 
        if collection_id == None:
            # fail?
            ...
        
        # extract lists from targeting-def-json
        

        # prepare main loop:
        categories = ["geo_locations","genders","ages_ranges","scholarities"]
        catlens = [len(input_data_json[cat])+1 for cat in categories]

        # Numpy-arrays for fast access to results (needed for predictions)
        results_mau = np.ones((catlens))
        predictions_median = np.full((catlens),-1)
        predictions_stdev = np.full((catlens),-1)
        

                
        # start main loop:
        for i0 in range(catlens[0]):
            for i1 in range(catlens[1]):
                for i2 in range(catlens[2]):
                    for i3 in range(catlens[3]):
                        ...
                         # 1. make prediction for ias    
                        ias = (i0,i1,i2,i3)
                        prediction = self.get_all_predictions(ias)
                        if len(prediction)>0:
                            predictions_median[ias] = st.median(prediction)
                            predictions_stdev[ias] = st.stdev(prediction)
                        else:
                            predictions_median[ias] = -2
                            predictions_stdev[ias] = -2

                         # 2. sub-1000 handling: skip or  make extra-requests?
                        if predictions_median[ias] < 1050:
                            if collection_config.get("skip_sub_1000",True)==True:
                                # save prediction but skip the request
                                # todo: save to table
                                audience = predictions_median[ias]
                                #continue
                            else: # don't skip, but make extra requests:
                                # ...
                                audience = self.get_and_save_results(ias,prediction,sub1000=True)
                        else:
                            # make a normal request:
                            audience = self.get_and_save_results(ias,prediction)
                        results_mau[ias] = audience

                        #print("---", ias, audience,"pred:",prediction,"|||")


    def get_all_predictions(ias):
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
            predictions.append(results[tuple(iasp1)]*results[tuple(iasp2)]/results[tuple(iasp3)])
            #predictions.append(results.item(tuple(iasp1))*results.item(tuple(iasp2))/results.item(tuple(iasp3)))

        return predictions