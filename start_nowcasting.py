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

options_json={"skip_sub_1000":True,"less_combinations":True}
# audiencer.

audi = audiencer.AudienceCollector("G:\\theile\\facebook\\audiencer_dali_short_20.sqlite",credentials_fn="credentials2.csv")
#%%
audi.export_results(fn=r"N:\Theile\Facebook\facebook_dalila\results_dalila_short_19_complete.csv")
#%%
audi.check_last_collection()

#%%
audi.start_new_collection(fn_input_data="dalila_long2.json", options_json=options_json, collection_name="dalila_long-23-11-22_20-test", comment="-o20test")
#%%

audi.fill_results_mau_from_db_test()
audi.fill_results_mau_from_db()
#%%

audi.restart_last_collection()

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
