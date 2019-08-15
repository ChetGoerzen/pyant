#!/usr/bin/env python
# coding: utf-8
'''
Writes a text file to 
'''


import pandas as pd
input_file = 'snap_201704-201707.txt'
output_file = 'db.arrival'

#%%

arr = pd.read_csv(input_file, delim_whitespace=True, header=None)
cat = arr.drop_duplicates(subset=[0,1,3,6,7])
cat.columns = ['sta','time','arid','jdate','stassid','chanid','chan','iphase','stype','deltim','azimuth','delaz','slow','delslo', 'ema', 'rect', 'amp', 'per', 'logat', 'clip', 'fm', 'snr', 'qual', 'auth', 'commid', 'Iddate']
cat = cat[['sta','chan','iphase','arid','time']]
cat = cat.reset_index(drop=True)
arid = list(cat.index+1)

cat.sta = cat.sta.astype(str)
cat.chan = cat.chan.astype(str)
cat.iphase = cat.iphase.astype(str)
cat.arid = cat.arid.astype(int)
cat.time = cat.time.astype(float)
#%%


# In[20]:


new_df = pd.DataFrame()
for i in range(len(cat)):
    temp = cat.iloc[i]
    if temp['sta'] == 'NBC8' and temp['chan'] in ['HHE', 'HHN']:
        temp['new_chan'] = 'HH1'
    else:
        temp['new_chan'] = temp['chan']
    new_df = new_df.append(temp)


# In[23]:


for i in range(len(new_df)): 
    print(new_df.iloc[i])


# In[27]:


import datetime;
ts = datetime.datetime.now().timestamp()
t = round(ts, 5)
print(t)
default_time = -999999999.999

f = open(output_file, 'w')

for n in range(len(new_df.arid)):
    f.write('%-6s %17.5f %8d %8d %8d %8d %-8s %-8s %s %6.3f %7.2f %7.2f %7.2f %7.2f %7.2f %7.3f %10.1f %7.2f %7.2f %s %-2s %10d %s %-16s %7d %17.5f\n' % (new_df.sta[n], new_df.time[n], new_df.arid[n], -1, -1, -1, new_df.new_chan[n], new_df.iphase[n], '-', -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -999.00, '-', '-', -1, '-', 'dbp:pgcseismola', -1, t))

f.close()

