#%%
locations =  ["allloc","DEU","FRA","GBR","ITA","JPN","KOR","MEX","USA"]
location_ratios = [1,0.05,0.04,0.04,0.035,0.07,0.03,0.07,0.2]
genders = ["allgender","male","female"]
gender_ratios = [1,0.47,0.4]
agegroups = ["allage","18-34","35-65"]
agegroup_ratios = [1,0.44,0.38]

# create df:


#%%


results = []
rrates = []
#how should rrates look like?

# it should be easily possible to do: get_rate(category, (location,gender,age))
def get_rate(category="gender",):
    pass


def set_rate(age,gender,loc,result):
    pass



def get_result():
    pass

#%%
rrates=[]
results=[]
country_ratios = [None for _ in locations] #c:r
inferred_gender_ratios = [None for _ in genders] #c:r
country_ratios
#%%

a = 0
sex=1
c = 2


for iage, (agegroup,ageratio) in enumerate(zip(agegroups,agegroup_ratios)):
    print()
    for igender,(gender,genderratio) in enumerate(zip(genders, gender_ratios)):
        for ilocation, (loc, locratio) in enumerate(zip(locations,location_ratios)):
            audience = round(1000*ageratio*genderratio*locratio)
            results.append((iage,igender,ilocation,audience))
            print(agegroup,gender,loc,audience)
            print(iage,igender,ilocation,audience)

            if (iage==0):
                pass

            #country-ratios:
            if (ilocation==0):
                if (iage==0 and igender==0):
                    country_ratios[0]=audience
            elif (iage==0 and igender==0):
                country_ratios[ilocation]=audience/country_ratios[0]
                print("country_ratio for",loc,"is",country_ratios[ilocation])

            # gender-ratios:
            if (igender==0):
                if(iage==0) and ilocation==0:
                    inferred_gender_ratios[0] = audience
            elif ( iage==0 and ilocation==0):
                inferred_gender_ratios[igender] = audience/inferred_gender_ratios[0]

            # create rate from result
             # find a result, where everything is either the same or "all"
            for resage,resgender,reslocation,resaudience in results:
                if ((resage==iage or resage==0) and 
                (resgender==igender or resgender==0) and
                (reslocation==ilocation or reslocation==0)):
                    if ((resage==iage) and 
                (resgender==igender) and
                (reslocation==ilocation)):
                        pass
                    else:
                        #print(resage,resgender,reslocation,resaudience)
                        ratio = audience/resaudience
                        rrates.append(((iage,resage),(igender,resgender),(ilocation,reslocation),resaudience,ratio))

            # save rate to rate-list


            # predict audience from collected rates:
# %%
import math
for ((iage,resage),(igender,resgender),(ilocation,reslocation),resaudience,ratio) in rrates:
    print(((iage,resage),(igender,resgender),(ilocation,reslocation),resaudience,ratio))
    n_same = sum([(iage==resage),(igender==resgender),(ilocation==reslocation)])
    print(n_same,([(iage==resage),(igender==resgender),(ilocation==reslocation)]))
0# %%
