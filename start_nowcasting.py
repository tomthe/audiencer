#%%
import audiencer
import json
import importlib

5+5
#%%


importlib.reload(audiencer)
#%%

# # open json file, load into dict
# with open("dalila_short2.json", "r") as infile:
#     input_data_json = json.load(infile)

options_json={"skip_sub_1000":True,"less_combinations":False, "verbose":True}
# audiencer.
credentials_fn="credentials8-emilio.csv"
credentials_fn="credentials7-maciej1.csv"
audi = audiencer.AudienceCollector("G:\\theile\\facebook\\audiencer_mignow_large_27.sqlite",credentials_fn=credentials_fn)
#%%
audi.export_results(fn=r"N:\Theile\Facebook\facebook_nowcasting\results_nowcast_27_part.csv")
#%%
audi.check_last_collection()

#%%
audi.start_new_collection(fn_input_data="migration_nowcast_middle.json", options_json=options_json, collection_name="mignow_small-24-03-20", comment="-o02test")
#%%

audi.fill_results_mau_from_db_test()
audi.fill_results_mau_from_db()
#%%

importlib.reload(audiencer)
audi.restart_last_collection()

#%%
audi.export_results(fn=r"N:\Theile\Facebook\facebook_nowcasting\results_nowcast_small_1.csv")
#%%
###################################################
# middle collection:

importlib.reload(audiencer)
#%%
options_json={"skip_sub_1000":True,"less_combinations":False, "verbose":True}
audi = audiencer.AudienceCollector("G:\\theile\\facebook\\audiencer_mignow_large_23.sqlite",credentials_fn="credentials2.csv",api_version="20.0")

audi.start_new_collection(fn_input_data="migration_nowcast_middle.json", options_json=options_json, collection_name="mignow_middle-24-02-29_test", comment="-o04middle")

audi.export_results(fn=r"N:\Theile\Facebook\facebook_nowcasting\results_nowcast_middle_2.csv")
#%%
# restart collection
icollection = 25

importlib.reload(audiencer)
options_json={"skip_sub_1000":True,"less_combinations":False, "verbose":False}
audi = audiencer.AudienceCollector(f"G:\\theile\\facebook\\audiencer_mignow_large_{icollection}.sqlite",credentials_fn="credentials7-maciej1.csv")
#%%
audi.check_last_collection()
#%%

audi.restart_last_collection()

#%%
###################################################
# large collection:
for icollection in range(26, 38):
    importlib.reload(audiencer)
    options_json={"skip_sub_1000":True,"less_combinations":False, "verbose":False}
    audi = audiencer.AudienceCollector(f"G:\\theile\\facebook\\audiencer_mignow_large_{icollection}.sqlite",credentials_fn="credentials7-maciej1.csv")

    audi.start_new_collection(fn_input_data="migration_nowcast.json", options_json=options_json, collection_name="mignow_large-24-08-02", comment="-o2large")

    audi.export_results(fn=f"N:\\Theile\\Facebook\\facebook_nowcasting\\results_nowcast_large_{icollection}.csv")
    print("icollection: ", icollection)
    print("finished")
    print("############################################################################################################################################")
    print("########################################################################################################################################\n\n\n")
#%%
###################################################
# large collection:
importlib.reload(audiencer)
options_json={"skip_sub_1000":True,"less_combinations":False, "verbose":False}
audi = audiencer.AudienceCollector("G:\\theile\\facebook\\audiencer_mignow_large_15.sqlite",credentials_fn="credentials3.csv")

audi.start_new_collection(fn_input_data="migration_nowcast.json", options_json=options_json, collection_name="mignow_large-24-06-16", comment="-o2large")

audi.export_results(fn=r"N:\Theile\Facebook\facebook_nowcasting\results_nowcast_large_15.csv")
#%%

importlib.reload(audiencer)

audi = audiencer.AudienceCollector("G:\\theile\\facebook\\audiencer_mignow_large_11.sqlite",credentials_fn="credentials2.csv")
audi.restart_last_collection()
audi.export_results(fn=r"N:\Theile\Facebook\facebook_nowcasting\results_nowcast_large_11.csv")

#%%
audi.db.close()

#%%
i=0
while True:
    i+=1
    print("i: ", i)
    audi.start_new_collection(fn_input_data="input_data_whole_world.json", options_json=options_json, collection_name="wholeworld3", comment="now on hydra02")
    
# %%
audi.che

# %%
N:\Theile\Facebook\facebook_dalila
# start long collection and then 3 short collections
#%%
import audiencer
import json
import importlib
5+5
#%%

# reload audiencer:
importlib.reload(audiencer)
#%%

options_json={"skip_sub_1000":True,"less_combinations":False}
# audiencer.

audi = audiencer.AudienceCollector("G:\\theile\\facebook\\audiencer_dali_short_08.sqlite",credentials_fn="credentials2.csv")
#%%

audi.restart_last_collection()
#%%

audi.start_new_collection(fn_input_data="dalila_long2.json", options_json=options_json, collection_name="dalila_long_2_08", comment="-")
#%%
audi.export_results(fn=r"N:\Theile\Facebook\facebook_dalila\results_dalila_long_02_08_3.csv")

#%%
for i in range(300):
    # i+=1    
    print("i: ", i)
    audi.start_new_collection(fn_input_data="dalila_short3.json", options_json=options_json, collection_name=f"dalila_short_{i}", comment=f"_i{i}_")
    audi.export_results(fn=f"N:\\Theile\\Facebook\\facebook_dalila\\results_dalila_short_04_{i}.csv")
    
# %%
