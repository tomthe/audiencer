#%%
%pip install duckdb --upgrade --user
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
ncollection = "19"
fn = f"G:\\theile\\facebook\\audiencer_dali_short_{ncollection}.sqlite"
# fn = "G:\\theile\\facebook\\audiencer_dali_short_15.sqlite"
# fn = "G:\\theile\\facebook\\audiencer_dali_short_16.sqlite"
#fn = "N:\\Theile\\Facebook\\facebook_dalila\\results_dalila_long_02_08_4.csv"
#fn = "G:\\theile\\facebook\\audiencer_dali_short_10.sqlite"

# con = duckdb.connect(database=fn, read_only=True)
# %%

q = f"""SELECT pk_results,ias,datetime(qtime,"unixepoch") as query_time, mau,
genders, geo_locations,age_min,age_max, education_statuses, behaviors,
mau, dau, mau_lower,mau_upper,targeting_spec,
json_extract(geo_locations, '$.countries[0]') AS country,
json_extract(targeting_spec, '$.flexible_spec') AS flex

FROM results"""

q2 = f"""SELECT *, ias, query_time, mau, dau, mau_lower,mau_upper,
country, interests, age_group,
gender,
coalesce(behaviors, "all_behaviors") as behaviors,
CASE 
  WHEN relationship_statuses = '["3","2","4"]' THEN 'single'
  WHEN relationship_statuses = '["1","12","11","13"]' THEN 'in_partnership'
  ELSE 'all_relationship'
END relationship_alias,
CASE
  WHEN education_statuses = '[2,3,4,5,6,7,8,9,10,11]' THEN 'at_least_highschool'
  WHEN education_statuses = '[3,6,7,8,9,10,11]' THEN 'higherTCollege'
  WHEN education_statuses = '[1,2,4,5]' THEN 'lowerTCollege'
  WHEN education_statuses = '[1]' THEN 'noHighschool'
  ELSE 'all_education'
END education_alias
FROM (
SELECT pk_results, collection_id, ias,datetime(qtime,"unixepoch") as query_time,
--genders, age_min,age_max, --education_statuses, 
mau, dau, mau_lower,mau_upper,
coalesce(json_extract(geo_locations, '$.countries[0]'), "all_countries") AS country,
coalesce(json_extract(targeting_spec, '$.interests[0].name'), "all_interests") AS interests, 
coalesce(json_extract(targeting_spec, '$.age_min'),"") || "-" || coalesce(json_extract(targeting_spec, '$.age_max'),"") as age_group,
--json_extract(targeting_spec, '$.genders') AS gender,
CASE 
  WHEN json_extract(targeting_spec, '$.genders') = '[1]' THEN 'M'
  WHEN json_extract(targeting_spec, '$.genders') = '[2]' THEN 'F'
  ELSE 'all'
END as gender,
coalesce(json_extract(targeting_spec, '$.flexible_spec[0].relationship_statuses'),"") || coalesce(json_extract(targeting_spec, '$.flexible_spec[1].relationship_statuses'),"") || coalesce(json_extract(targeting_spec, '$.flexible_spec[2].relationship_statuses'),"") as relationship_statuses,
coalesce(json_extract(targeting_spec, '$.flexible_spec[0].education_statuses'),"") || coalesce(json_extract(targeting_spec, '$.flexible_spec[1].education_statuses'),"")|| coalesce(json_extract(targeting_spec, '$.flexible_spec[2].education_statuses'),"") as education_statuses,
coalesce(json_extract(targeting_spec, '$.flexible_spec[0].behaviors[0].name'),"") || coalesce(json_extract(targeting_spec, '$.flexible_spec[1].behaviors[0].name'),"")|| coalesce(json_extract(targeting_spec, '$.flexible_spec[2].behaviors[0].name'),"") as behaviors
FROM results
--WHERE qtime < 1694642400
--AND collection_id = 1
)"""
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

con = duckdb.connect(database=":memory:", read_only=False)

qp = f"""
PIVOT dfsmall on age_group, gender--, relationship_alias, education_alias, country, interests, behaviors
USING SUM(mau), SUM(mau_lower) as maulower, SUM(mau_upper) as mauupper
"""
dfpivot = con.execute(qp).fetchdf()
dfpivot



# %%
dfsmall = df[["mau","mau_lower","mau_upper","gender","age_group","education_alias","interests", "behaviors","country","relationship_alias"]]
q = f"""
PIVOT dfsmall on interests, behaviors--, relationship_alias, education_alias, country, interests, behaviors
USING string_agg(mau) as mau,string_agg(mau_lower) as maulower,string_agg(mau_upper) as mauupper
GROUP BY age_group, gender,education_alias, country, relationship_alias
"""
dfpivot = con.execute(q).fetchdf()
dfpivot
#%%
# drop rows with mostly NaN behind the 4th column
dfpivot.dropna(axis=0, thresh=15)
#%%
dfpivot.dropna(axis=0, thresh=15).dropna(axis=1, how="all").to_csv(f"N:\\Theile\\Facebook\\facebook_dalila\\audiencer_dali_long2_{ncollection}_wide_format_threshold15.csv")
#%%

# list the number of non-Nan values in each column:
dfpivot.count()
#%%
# drop columns with only NaN
dfpivot.dropna(axis=0, thresh=15).dropna(axis=1, how="all")#.count()

#%%
# count the NaNs in each row
dfpivot["nNaN"] = dfpivot.isnull().sum(axis=1)
dfpivot.to_csv(f"N:\\Theile\\Facebook\\facebook_dalila\\audiencer_dali_short_{ncollection}_wide_format_lower_upper.csv")


# %%
# remove rows with more than 10 NaNs
# dfpivot2 = dfpivot[dfpivot.isnull().sum(axis=1) < 38]
# fill NaNs with -4
dfpivot2 = dfpivot.fillna(999)
dfpivot2
# %%
dfpivot2.to_csv(f"N:\\Theile\\Facebook\\facebook_dalila\\audiencer_dali_short_{ncollection}_wide_format_lower_upper.csv")
# %%
q= f"""SELECT age_group, gender,education_alias, country, relationship_alias,behaviors,interests,
string_agg(ias) as ias,-- COUNT(DISTINCT ias) as countias,
string_agg(mau) as mau,string_agg(mau_lower) as maulower,string_agg(mau_upper) as mauupper,
string_agg(dau) as dau,
string_agg(query_time) as query_time
FROM df
GROUP BY age_group, gender,education_alias, country, relationship_alias,behaviors,interests
order by ias--countias desc
"""
dfiascount = con.execute(q).fetchdf()
print(dfiascount.shape)
dfiascount.head(4)
# %%
dfiascount.to_csv(f"N:\\Theile\\Facebook\\facebook_dalila\\audiencer_dali_short_{ncollection}_long_format.csv")
#%%
dfiascount.sample(7)
#%%
dfiascount.iloc[0,7]
#%%
dfiascount.iloc[0,10]
# %%
