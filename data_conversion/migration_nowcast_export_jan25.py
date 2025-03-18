#%%
import pandas as pd
from datetime import datetime
import sqlite3
import os
#%%
# find all files in G:\\theile\\facebook\\ that contain "audiencer_mignow_large" and are sqlite files
files = [f for f in os.listdir("G:\\theile\\facebook\\") if "audiencer_mignow_large" in f and f.endswith(".sqlite")]
files
#%%


#%%

def export_results_to_csv(fnin, fnout):
    consqlite = sqlite3.connect(fnin)
    """
    export results to a csv file
    """

    q = f"""SELECT *,
    CASE 
    WHEN relationship_statuses = '["3","2","4"]' THEN 'single'
    WHEN relationship_statuses = '["1","12","11","13"]' THEN 'in_partnership'
    ELSE 'all_relationship'
    END relationship_alias,
    CASE
    WHEN education_statuses = '[2, 3, 4, 5, 6, 7, 8, 9, 10, 11]' THEN 'at_least_highschool'
    WHEN education_statuses = '[3, 7, 8, 9, 11]' THEN 'Graduated'
    WHEN education_statuses = '[12]' THEN 'Unspecified'
    WHEN education_statuses = '[1, 13]' THEN 'Primary Education or less'
    WHEN education_statuses = '[2, 4, 5]' THEN 'Secondary education'
    WHEN education_statuses = '[6, 10]' THEN 'Post-secondary education'
    WHEN education_statuses = '[3]' THEN 'Bachelor'
    WHEN education_statuses = '[9, 7]' THEN 'Master'
    WHEN education_statuses = '[11]' THEN 'Doctorate'
    WHEN education_statuses = 'notSpecified' THEN 'notSpecified'
    ELSE 'all_education'
    END education_alias
    FROM (
    SELECT pk_results,ias,datetime(qtime,"unixepoch") as query_time,qtime, mau, dau, education_statuses,
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

    df = pd.read_sql_query(q, consqlite)
    if fnout=="":
        print("no filename?")
        # fn =  + str(datetime.now().minute) + "_" + str(icollection) + ".csv"
    df.to_csv(fnout, index=False)
    print("exported to ", fnout)
    consqlite.close()
    print("closed connection")
    print("############################################################################################################################################")
# %%

#%%
icollection = 82
fnin = f"G:\\theile\\facebook\\audiencer_mignow_large_{icollection}.sqlite"
fnout = f"N:\Theile\\Facebook\\facebook_nowcasting\\results_nowcast_jan25_{icollection}_nx.csv"
export_results_to_csv(fnin, fnout)
#%%

for file in files:
    icollection = file.split("_")[-1].split(".")[0]
    fnin = f"G:\\theile\\facebook\\{file}"
    fnout = f"N:\Theile\\Facebook\\facebook_nowcasting\\results_nowcast_jan25_{icollection}_n.csv"
    print("icollection: ", icollection)
    export_results_to_csv(fnin, fnout)
