from merge_dbs import merge_dbs
import os
from antelope_load import Load
import pandas as pd
from write_antelope_db import write_antelope_db

#%%

home = os.path.expanduser('~/')
in_path = home + 'Documents/chet/dbs/'
db_names = ['test/']
out_path = in_path + 'review_outliers/'

def my_func(origin, arrival):
    origin = origin[origin.mlid > 0]
    orid_list = origin.orid
    arrival = arrival[arrival.orid.isin(orid_list)]
    
    return origin, arrival

merge_dbs(in_path=in_path, db_names=db_names, out_path=out_path, processing=my_func)

#%%

data = Load(in_path + 'test/')
arr = data.arrival()
arr_assoc = data.arrival_assoc()

clean = data.clean()

oo = clean["clean_origin_origerr"]

oaa = clean["clean_origin_arrival_assoc"]

#%%

complete_oo = oo[oo.mlid > 0]

complete_oo.to_csv(home + 'Desktop/Work/pyant/data/to_check.csv')

#%%

my_origin, my_arrival = my_func(oo, oaa)

#%%

outliers = pd.read_csv(home + 'Desktop/Work/pyant/data/outliers.csv')
orids_to_check = outliers.orid

def get_outliers(origin, arrival, orids_to_check=orids_to_check):
    origin = origin[origin.orid.isin(orids_to_check)]
    arrival = arrival[arrival.orid.isin(origin.orid)]
    
    return origin, arrival

out_path = in_path + 'outliers_to_check/'
merge_dbs(in_path=in_path, db_names=db_names, out_path=out_path, 
          processing=get_outliers)

#%%






