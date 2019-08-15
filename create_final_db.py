from datetime import datetime
from antelope_load import Load
from merge_dbs import merge_dbs
import pandas as pd
import os

#%%

home = os.path.expanduser('~/')

def func(origin, arrival, db):
    if db == 'review_outliers':
        outliers = pd.read_csv(home + 'Desktop/Work/pyant/data/outliers.csv')
        orids_to_check = outliers.orid
        origin = origin[~origin.orid.isin(orids_to_check)]
        arrival = arrival[arrival.orid.isin(origin.orid)]
        
    origin = origin[(origin.lat <= 56.3) & (origin.lat >= 55.5) & 
                    (origin.lon <= -119.8) & (origin.lon >= -121.2) &
                    (origin.smajax <= 10) & (origin.sdobs <=1.)]
        
    return origin, arrival

#%%
    
in_path = home + 'Documents/chet/dbs/'
db_names = ['outliers_to_check/', 'review_outliers/']
out_path = in_path + 'final_db/'

merge_dbs(in_path=in_path, db_names=db_names, out_path=out_path, processing=func)

#%%

#os.system(" echo "+home+" > out.txt")
#
##%%
#
#os.system("cat out.txt")
#
##%%
#
##rundbevproc for all the orids
#os.system("for i in {1..7672} do rundbevproc db $i done")