#%%
import audiencer
5+5
#%%

options_json={"skip_sub_1000":True,"less_combinations":False}
audiencer.

audi = audiencer.AudienceCollector("G:\\theile\\facebook\\audiencer04.sqlite",credentials_fn="credentials2.csv")
#%%
audi.check_last_collection()
#%%
audi.start_new_collection(fn_input_data="input_data_whole_world.json", options_json=options_json, collection_name="wholeworld4", comment="now on hydra02")
#%%

audi.restart_last_collection()

#%%
i=0
while True:
    i+=1
    print("i: ", i)
    audi.start_new_collection(fn_input_data="input_data_whole_world.json", options_json=options_json, collection_name="wholeworld3", comment="now on hydra02")
    
# %%
q = f"""SELECT
datetime(rs.qtime,"unixepoch") as dtime,
fk_queries,
ias,
mau,
mau_lower,mau_upper,dau,
genders,geo_locations,ages_min,age_max,education_statuses,behaviors,interests,life_events
FROM results rs;
"""

dfResults = audi.db.query(q)