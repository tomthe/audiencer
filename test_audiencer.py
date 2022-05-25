
#%%

import audiencer
import importlib
importlib.reload(audiencer)

#def test_audiencer_init():

#audi = audiencer.AudienceCollector("test_audiencer.db",fn_input_data="input_data_whole_world.json", token="test_token",)
#audi.create_targeting_spec_from_list_of_ias([[1,1,1,1,1,0]])
# %%
#create input
#%%

mapping = {
        "CZ": "Czech Republic",
        "DE": "Germany",
        "FR": "France"
    }
countrycodes = list(mapping.keys())
#%%


input_data_json = {
    "name": "test collection1",
    "geo_locations": [],
    "genders": [1,2],
    "ages_ranges": [
        {"min":18, "max":39},
        {"min":40}
    ],
    "behavior": [
    ],
	"scholarities": [
        {
			"name": "Graduated",
			"or": [3, 7, 8, 9, 11]
		},
		# {
		# 	"name": "No Degree",
		# 	"or": [1, 13]
		# }, {
		# 	"name": "High School",
		# 	"or": [2, 4, 5, 6, 10]
		# }
	],
    "interests": []
}

for countrycode in countrycodes:
    input_data_json["geo_locations"].append(
        { "name": "countries", "values": [countrycode],  "location_types": ["home"] }
    )


import pandas as pd
expats = pd.read_csv("./facebook_behavior_expat_origin.csv", header=0)
print(expats.head(3))

for behavior in range(0, min(len(expats["key"]),2)):
    input_data_json["behavior"].append(
        {"name": expats["origin"][behavior], "or": [int(expats["key"][behavior])]}
    )

#print(input_data_json)
print(len(input_data_json["behavior"]),\
    len(input_data_json["geo_locations"]),len(input_data_json["genders"]),\
    len(input_data_json["scholarities"]),len(input_data_json["ages_ranges"]))
#%%
print(len(input_data_json["behavior"])*\
        len(input_data_json["geo_locations"])* (len(input_data_json["genders"])+1)*\
        (len(input_data_json["scholarities"])+1)*(len(input_data_json["ages_ranges"])+1))
#%%
print(len(input_data_json["behavior"])*\
        len(input_data_json["geo_locations"])* ((len(input_data_json["genders"])+1)+\
        (len(input_data_json["scholarities"])+1)+(len(input_data_json["ages_ranges"])+1)))
#%%
# %%
import json
with open("input_data_test.json", "w") as outfile:
    json.dump(input_data_json, outfile)
# %%



importlib.reload(audiencer)
options_json={"skip_sub_1000":True,"less_combinations":False}

audi = audiencer.AudienceCollector("test_audiencer10.sqlite",credentials_fn="credentials2.csv")
#audi.create_targeting_spec_from_list_of_ias([[0,0,0,0,0,0]])
#audi.collect_one_combination([0,0,0,0,0,0],options_json)
audi.start_new_collection(fn_input_data="input_data_test.json", options_json=options_json, collection_name="default_collection", comment="")
# %%
audi.db.close()
# %%
ias = (1, 0, 0, 2, 0, 0)
audi.collect_one_combination([0,0,0,0,0,0],options_json)

#%%
audi.check_last_collection()
# %%
importlib.reload(audiencer)
audi = audiencer.AudienceCollector("test_audiencer7.sqlite",fn_input_data="input_data_test.json",credentials_fn="credentials2.csv")
audi.restart_collection()
# %%

importlib.reload(audiencer)
audi = audiencer.AudienceCollector("audiencer03.sqlite",credentials_fn="credentials2.csv")

options_json={"skip_sub_1000":True,"less_combinations":True}
audi.start_new_collection(fn_input_data="input_data_whole_world.json", options_json=options_json, collection_name="whole_world_limited", comment="")
# %%

## The big collection:
