#%%
# %pip install duckdb --upgrade --user
# %%
import pandas as pd
import sqlite3
import duckdb
import os
import numpy as np

#show all columns and rows:
pd.set_option('display.max_columns', 300)
pd.set_option('display.max_rows', 200)

print(duckdb.__version__)
# %%
# Read in the data
ncollection = "4"
fn = f"G:\\theile\\facebook\\audiencer_mignow_large_3.sqlite"
# fn = "G:\\theile\\facebook\\audiencer_dali_short_15.sqlite"
# fn = "G:\\theile\\facebook\\audiencer_dali_short_16.sqlite"
#fn = "N:\\Theile\\Facebook\\facebook_dalila\\results_dalila_long_02_08_4.csv"
#fn = "G:\\theile\\facebook\\audiencer_dali_short_10.sqlite"

# con = duckdb.connect(database=fn, read_only=True)
# %%
q2 = f"""SELECT *,
CASE 
  WHEN relationship_statuses = '["3","2","4"]' THEN 'single'
  WHEN relationship_statuses = '["1","12","11","13"]' THEN 'in_partnership'
  ELSE 'all_relationship'
END relationship_alias,
CASE
  WHEN education_statuses = '[2,3,4,5,6,7,8,9,10,11]' THEN 'at_least_highschool'
  ELSE 'all_education'
END education_alias
FROM (
SELECT pk_results,ias,datetime(qtime,"unixepoch") as query_time,qtime, mau,
length(predictions) as pred_len,
--genders, age_min,age_max, --education_statuses, 
mau, dau, mau_lower,mau_upper,round(prediction_mean) as prediction_mean,
coalesce(REPLACE(REPLACE(REPLACE(json_extract(targeting_spec, '$.publisher_platforms'),'"',''),'[',''),']',''),"all_platforms")as publisher_platforms2,
json_extract(geo_locations, '$.countries[0]') AS country,
json_extract(targeting_spec, '$.interests[0].name') AS interests, 
coalesce(json_extract(targeting_spec, '$.age_min'),"") || "-" || coalesce(json_extract(targeting_spec, '$.age_max'),"") as age_group,
json_extract(targeting_spec, '$.genders') AS gender,
CASE 
  WHEN json_extract(targeting_spec, '$.genders') = '[1]' THEN 'M'
  WHEN json_extract(targeting_spec, '$.genders') = '[2]' THEN 'F'
  ELSE 'all'
END as gender_alias,
coalesce(json_extract(targeting_spec, '$.flexible_spec[0].relationship_statuses'),"") || coalesce(json_extract(targeting_spec, '$.flexible_spec[1].relationship_statuses'),"") || coalesce(json_extract(targeting_spec, '$.flexible_spec[2].relationship_statuses'),"") as relationship_statuses,
coalesce(json_extract(targeting_spec, '$.flexible_spec[0].education_statuses'),"") || coalesce(json_extract(targeting_spec, '$.flexible_spec[1].education_statuses'),"")|| coalesce(json_extract(targeting_spec, '$.flexible_spec[2].education_statuses'),"") as education_statuses,
coalesce(json_extract(targeting_spec, '$.flexible_spec[0].behaviors[0].name'),"") || coalesce(json_extract(targeting_spec, '$.flexible_spec[1].behaviors[0].name'),"")|| coalesce(json_extract(targeting_spec, '$.flexible_spec[2].behaviors[0].name'),"") as behaviors
FROM results
)
order by qtime"""
#%%
consqlite = sqlite3.connect(fn)
df = pd.read_sql_query(q2, consqlite)
#%%
df
#%%
print(df.shape)
print(df.relationship_statuses.unique())
df.tail()
#%%
dfsmall = df[["mau","mau_lower","mau_upper","gender","age_group","education_alias","interests", "behaviors","country"]]
#%%
#%%
df.to_csv(f"N:\\Theile\\Facebook\\facebook_nowcasting\\audiencer_mignow_{ncollection}_2.csv")
#%%
