
#%%

import audiencer
import importlib
importlib.reload(audiencer)

#def test_audiencer_init():

audi = audiencer.AudienceCollector("test_audiencer.db",fn_input_data="input_data_whole_world.json", token="test_token",)
audi.create_targeting_spec_from_list_of_ias([[1,1,1,1,1,0]])
# %%
