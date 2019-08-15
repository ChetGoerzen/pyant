#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 15:14:23 2019

@author: pgcseismolab
"""
import os
import pandas as pd

from merge_dbs import merge_dbs
from antelope_load import Load

#%%

home = os.path.expanduser('~/')
in_path = home + 'Documents/chet/dbs/'
db_names = ['final_db/']
out_path = in_path + 'remove_within_0.5/'

#def remove_duplicates(origin, arrival, db=''):
#    '''
#    Removes events that are within one second of each other. An extremely
#    large number of events were removed when I used this function and I need
#    to find out why.
#    '''
#    #Rounds to the nearest second
#    origin["round"] = origin.time.round(1)
#    origin.sort_values(by="ndef", inplace=True)
#    round_origin = origin.drop_duplicates(subset="round")
#    
#    def trunc(time):
##        int_str = int(time)
##        
##        return int_str
#        time_str = str(time)
#        split_time = time_str.split('.')
#        dec = split_time[-1][0]
#        final_time = float(split_time[0] +'.' + dec)
#        
#        return final_time
#    #Truncates to the nearest second
#    round_origin["trunc"] = round_origin.time.apply(trunc)
#    print("Length of rounded origin: "+str(len(round_origin)))
#    clean_origin = round_origin.drop_duplicates(subset="trunc")
#    print("length of clean origin: "+str(len(clean_origin)))
#    
#    clean_arrival = arrival[arrival.orid.isin(clean_origin.orid)]
#    
#    return clean_origin, clean_arrival

def remove_duplicates(origin, arrival, db=''):
    
    origin1 = origin.copy(deep=True)
    
    duplicate_id = 0
    clean_origin = pd.DataFrame()
    for i in origin1.index:
        
        ev_time = origin1.time[i]
        start = ev_time - 1
        end = ev_time + 1
        
        duplicates = origin1.loc[(origin1.time >= start) & (origin1.time <= end)]
        
    
        
        if len(duplicates) > 1:
#            origin1.loc[duplicates.index, "duplicated"] = "Y"
            origin1.loc[duplicates.index, "duplicate_id"] = duplicate_id
            duplicate_id += 1
            
        if len(duplicates) == 0:
            raise ValueError('The script did not consider two identical '
                             'earthquakes to be duplicates. Something is '
                             'wrong')
            
        if len(duplicates) == 1:
#            origin1.loc[duplicates.index, "duplicated"] = "N"
            origin1.loc[duplicates.index, "duplicate_id"] = duplicate_id
            duplicate_id += 1
        
        
    origin1.sort_values(by='ndef', inplace=True)
    clean_origin = origin1.drop_duplicates(subset='duplicate_id', keep='first')
            
    clean_arrival = arrival[arrival.orid.isin(clean_origin.orid)]
    
    return clean_origin, clean_arrival
    
#            duplicates = tmp_df[tmp_df.diff <= 0.5]
#            orids_to_remove.append(duplicates.ndef.idxmin())
                
           
        

#%%
#time = tmp
#time_str = str(time)
#split_time = time_str.split('.')
#dec = split_time[-1][0]
#final_time = float(split_time[0] +'.' + dec)

#%%

merge_dbs(in_path, db_names, out_path, processing=remove_duplicates)

#%%


#data = Load(in_path + 'final_db/')
#clean = data.clean()
#origin = clean["clean_origin_origerr"]
#arrival = clean["clean_origin_arrival_assoc"]

