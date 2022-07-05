#%%
import audiencer
#%%

options_json={"skip_sub_1000":False,"less_combinations":True}

audi = audiencer.AudienceCollector("G:\\tt\\facebook\\audiencer04.sqlite",credentials_fn="credentials2.csv")
#%%
audi.start_new_collection(fn_input_data="input_data_whole_world.json", options_json=options_json, collection_name="wholeworld2", comment="")
#%%

audi.restart_last_collection()

#%%