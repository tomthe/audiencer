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

options_json={"skip_sub_1000":True,"less_combinations":False}
# audiencer.

audi = audiencer.AudienceCollector("G:\\theile\\facebook\\audiencer_dali_short_06.sqlite",credentials_fn="credentials2.csv")
#%%
audi.check_last_collection()
#%%
audi.start_new_collection(fn_input_data="dalila_short3.json", options_json=options_json, collection_name="dalila_short_03_2", comment="-")
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