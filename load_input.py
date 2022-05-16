#%%
# 
input = {   "name": "Soccer Interest",
     "geo_locations": [
          { "name": "countries", "values": ["QA"] },
          { "name": "countries", "values": ["SA"] },
          { "name": "countries", "values": ["AE"] }
],
"genders": [ 1,2],
"ages_ranges": [
    {"min":18, "max":24},
    {"min":55}
],
"scholarities":[{
    "name" : "Graduated",
    "or" : [3,7,8,9,11]
  }
],
"languages":[{
    "name" : "Arabic",
    "values" : [28]
    },
    None
],
"behavior": [
    {
      "or": [6015559470583],
      "name": "Expats"
    },
    {
      "not": [6015559470583],
      "name": "Not Expats"
    }
],
"interests": [{
        "not": [6003442346642],
        "and": [6004115167424, 6003277229371],
        "name": "Not interested in Football, but interest in physical activity"
    },{
        "or": [6003442346642],
        "name" : "Football"
    }
]
}
# %%
for iage, agegroup in  input["ages_ranges"]:
    print()
    for igender,gendergroup in enumerate(input["genders"]):
        print(igender,iage,agegroup,gendergroup)
# %%

def flatten_spec(spec):
    speclist= []
    speclistlens = []
    speclist.append(None)
    if "geo_locations" in spec:
        speclistlens.append(len(spec["geo_locations"])+1)
        for loc in spec["geo_locations"]:
            speclist.append(loc)
    else:
        speclistlens.append(1)

    speclist.append(None)
    if "genders" in spec:
        speclistlens.append(len(spec["genders"])+1)
        for gender in spec["genders"]:
            speclist.append(gender)
    else:
        speclistlens.append(1)

    speclist.append(None)
    if "ages_ranges" in spec:
        speclistlens.append(len(spec["ages_ranges"])+1)
        for gender in spec["ages_ranges"]:
            speclist.append(gender)
    else:
        speclistlens.append(1)

    return speclist,speclistlens

flatten_spec(input)
# %%

def flatten_spec(spec):
    categories = ["geo_locations","genders","ages_ranges"]
    speclist= []
    speclistlens = []
    for cat in categories:
        speclist.append(None)
        if cat in spec:
            speclistlens.append(len(spec[cat])+1)
            for loc in spec[cat]:
                speclist.append(loc)
        else:
            speclistlens.append(1)

    return speclist,speclistlens

speclist, speclistlens = flatten_spec(input)
speclistlens
# %%


# go through all combinations:
totallen = 1
for i in speclistlens:
    totallen=totallen*i
# %%
for i in range(totallen):
    pass

i=0
for a0 in range(speclistlens[0]):
    for a1 in range(speclistlens[1]):
        for a2 in range(speclistlens[2]):
            
            print(i,a0,a1,a2,speclist[a0],speclist[a1+speclistlens[0]],speclist[a2+sum(speclistlens[:2])])
            i+=1

#%%
categories = ["geo_locations","genders","ages_ranges"]
catlens = [len(input[cat])+1 for cat in categories]
results = np.ones((catlens))


# %%
def get_spec(ias=[0,1,2]):
    newspec = {}
    # if ias[0] !=0:
    #     newspec["geo_locations"] = input["geo_locations"][ias[0]]
    # if ias[1] !=0:
    #     newspec["genders"] = input["genders"][ias[1]]
    for ia,cat in zip(ias,categories):
        if ia!=0:
            newspec[cat]=input[cat][ia-1]
        else:
            pass
    return newspec

get_spec([1,1,2])

# %%

def get_result(ias=(0,1,2)):
    return results.item(tuple(ias))

get_result((1,1,2))
get_result([1,1,2])
#%%


def get_prediction(ias=(0,1,2)):
    pass
    # 1. get the next baseline-result
    # ( 1,1,2) --> (1,1,0) or (1,0,2) or (0,1,2) ###or (0,0,2)
    # 2. get the ratio for the category that is different.
    #  
    # 3. calculate the prediction

    # easy way: always start with the last category.

    prediction=-1
    if ias[-1]!=0:
        ias_baseline = list(ias[:])
        ias_baseline[-1] = 0
        baseline = get_result(ias_baseline)
        if ias[-2]!=0:
            #numerator: oben, more specific
            #denominator: unten, broader category
            ias_ratio_denominator = list(ias[:])
            ias_ratio_denominator[-2] = 0
            ias_ratio_denominator[-1] = 0
            ias_ratio_numerator = list(ias[:])
            ias_ratio_numerator[-2] = 0

            ratio = get_result(ias_ratio_numerator)/get_result(ias_ratio_denominator)
            prediction = baseline*ratio
    return prediction
get_prediction((1,1,2))
#%%

#%%
def generate_iasb(ias):
    # generate all variations of ias that differ in only
    # one category. And that are not zero.
    # e.g. ias=[0,1,2,3] iasb=[(0,1,2,2),(0,1,2,1),(0,1,1,3)]
    iasblist = []
    for i in range(len(ias)):
        if ias[i]!=0:
            for j in range(1,ias[i]):
                iasb = ias[:]
                iasb[i] = j
                iasblist.append(iasb)
    return iasblist

generate_iasb([0,1,2,3])
# %%
def get_all_predictions(ias):
    # 1. generate all variations of ias that can form a prediction
    # 2. get the prediction for each variation
    # 3. profit?!

    # what are iasp1 : differs in one category
    # then iasp2: is the same as ias in the cat that differs in iasp1
    # ... differs in some other category
    # then iasp3: is the same as ias in the cat that differs in iasp2 
    # and the same in iasp1
    ias = list(ias)
    iasplist = []
    for i in range(len(ias)):
        if ias[i]!=0:
            for j in range(0,ias[i]): # or only up to ias[i]-1?
                iasp1 = ias[:]
                iasp1[i] = j
                for k in range(len(ias)):
                    if k!=i:
                        if ias[k]!=0:
                            for l in range(0,ias[k]):
                                iasp2 = ias[:]
                                iasp2[k] = l
                                iasp3 = ias[:]
                                iasp3[k] = l
                                iasp3[i] = j
                                iasplist.append((iasp1,iasp2,iasp3))
    # todo: We should somehow remove (or avoid) the duplicates, 
    # because iasp1 and iasp2 are interchangeable.
    # return iasplist
    print("iasplist: ",iasplist)
    predictions = []
    for iasp1,iasp2,iasp3 in iasplist:
        #print(iasp1,iasp2,iasp3)
        #print(results[tuple(iasp1)],results[tuple(iasp2)],results[tuple(iasp3)])
        #print(results[tuple(iasp1)]*results[tuple(iasp2)]/results[tuple(iasp3)])
        predictions.append(results[tuple(iasp1)]*results[tuple(iasp2)]/results[tuple(iasp3)])
        #predictions.append(results.item(tuple(iasp1))*results.item(tuple(iasp2))/results.item(tuple(iasp3)))

    return predictions

get_all_predictions([0,1,2])
# %%
get_all_predictions([0,1,2,3,4])
# %%
import random
def make_request(tarspec,max_tries=5,try_number=0):
    '''
    This functions generates the request (from url, token, api-version, tarspec/ais, last request-time)
    It also handles errors, wait times and saves the result
    it returns the result

    Wait-time and error handling:
    * several options: 
        a additional delay (sleep constant)
        b calculated delay (sleep calculated, by measuring the time since the last request)
        c no delay till the first error, then look how many requests where
            done in the hour before the failure (or have that hardcoded)
            then: get the time of the first request (in that hour) and add 1 hour -> that is the time for the next request
        --> first implement b, then c
    * if request fails: the function calls itself again, try_number+=1

    save request and result to sqlite
    
    returns: mau, mau_upper_limit, dau, ....
    '''
    audience = random.randint(0,10000)
    if audience < 1000:
        return 1000
    else:
        return audience


# %%

def get_audience(ias):
    ...
    # 1. 
    # 2. make prediction for ias
    # 3. decide how to move on:
    #    - if prediction is high enough, make request, save audience
    #    - if prediction is sub 1000, decide:
    #        - if setting says collect sub1000, call get_audience_sub1000(ias, prediction)
    #        - if setting says not to collect sub1000, save only the prediction and move on

# %%
# sanitize input: remove None...

# fake numbers:
r0=[]
for i0 in range(catlens[0]):
    r0.append(random.randint(1,10))
r1=[]
for i1 in range(catlens[1]):
    r1.append(random.randint(1,5))
r2=[]
for i2 in range(catlens[2]):
    r2.append(random.randint(1,7))
r3=[]
for i3 in range(catlens[3]):
    r3.append(random.randint(1,5))


#%%
import numpy as np
import statistics as st

categories = ["geo_locations","genders","ages_ranges","scholarities"]
catlens = [len(input[cat])+1 for cat in categories]
results = np.ones((catlens))

predictions = {}

for i0 in range(catlens[0]):
    for i1 in range(catlens[1]):
        for i2 in range(catlens[2]):
            for i3 in range(catlens[3]):
                ias = (i0,i1,i2,i3)
                prediction = get_all_predictions(ias)
                predictions[ias] = prediction
                audience = r0[i0]*r1[i1]*r2[i2]*r3[i3] # make_request(ias)
                print("---", ias,(r1[i1],r2[i2],r3[i3]),audience,"pred:",prediction,"|||")
                if len(prediction)>1:
                    print(st.fmean(prediction),st.stdev(prediction))
                results[ias] = audience
                #print(ias,prediction,audience)

# %%
