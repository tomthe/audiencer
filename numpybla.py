#%%
%pip install numpy

#%%

import numpy as np


#%%

bla = np.zeros((3,3,3))
bla
# %%
bla[0,0]

#%%
aa=np.array([0.3,0.2,0.5])
aa
# %%
for a in range(3):
    for b in range(3):
        for c in range(3):
            bla[a,b,c] = aa[a]*aa[b]*aa[c]


#%%
blaa = np.zeros(a*b*c)
i=0
for d in range(a*b*c):
    aa=i/(bla[0])%(bla[1]*bla[2])
    bb=i/(bla[0])%(bla[1]*bla[2])
    cc=i/(bla[0])%(bla[1]*bla[2])
    print(aa,bb,cc)
    print(i, blaa[d], i/a,i/b,i/c)
    i+=1
    blaa[d]=i
# %%
