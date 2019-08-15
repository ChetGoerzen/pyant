#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 12:01:36 2019

@author: pgcseismolab
"""

'''
This is the script I used to create the "test" database. It creates a new 
database with fixed ID's. All the Antelope data is within the study area, has
smajax <= 10, and sdobs <= 1. The pick-view data is all the s-snap detections that
are classified correct. They are within the study area and the quality is >= 3.
'''


from write_antelope_db import *
from antelope_load import * 
from pick_view_data import *

import os

#%%

home = os.path.expanduser('~/')
begin_path = home + 'Documents/pick-view-data/'
 
jan_may= PickViewData(begin_path + '201901-201905')
jan_may_arrival = jan_may.correct_arrival()
jan_may_origin = jan_may.correct_origin()

june = PickViewData(begin_path + '201906')
june_arrival = june.correct_arrival()
june_origin = june.correct_origin()

july = PickViewData(begin_path + '201907')
july_arrival = july.correct_arrival()
july_origin = july.correct_origin()


origin = jan_may_origin.append(june_origin.append(july_origin))
origin = origin[(origin.lat <= 56.3) & (origin.lat >= 55.5) 
                & (origin.lon >= -121.2) & (origin.lon <= -119.8)
                & (origin.quality >= 3)]

arrival = jan_may_arrival.append(june_arrival.append(july_arrival))
#arrival.drop(1339, inplace=True)
arrival.dropna(subset=["datetime"], inplace=True)

#%%

ant_path = home + 'Documents/chet/dbs/'

ant_jan_may = Load(ant_path + '201901-201905/')
ant_jan_may_arrival = ant_jan_may.arrival()
ant_june = Load(ant_path + '201906/')
ant_july = Load(ant_path + '201907/')
ant_pick_view = Load(ant_path + 'pick_view_test/')

site = ant_july.site()

write_antelope_db(home + 'Documents/chet/dbs/pick_view_test/', 
                  origin, arrival, site=site, algorithm='S-SNAP',
                  auth_name='PGC')




#%%
clean_jan_may = ant_jan_may.clean()
jan_may_oaa = clean_jan_may["clean_origin_arrival_assoc"]
jan_may_oo = clean_jan_may["clean_origin_origerr"]

clean_june = ant_june.clean()
june_oaa = clean_june["clean_origin_arrival_assoc"]
june_oo = clean_june["clean_origin_origerr"]

clean_july = ant_july.clean()
july_oaa = clean_july["clean_origin_arrival_assoc"]
july_oo = clean_july["clean_origin_origerr"]

clean_pick_view = ant_pick_view.clean()
pick_view_oaa = clean_pick_view["clean_origin_arrival_assoc"]
pick_view_oo = clean_pick_view["clean_origin_origerr"]

#%%

from datetime import datetime

db_path = home + 'Documents/chet/dbs/test/'

origin_file = open(db_path + 'db.origin', 'w')
arrival_file = open(db_path + 'db.arrival', 'w')
assoc_file = open(db_path + 'db.assoc', 'w')
event_file = open(db_path + 'db.event', 'w')
origerr_file = open(db_path + 'db.origerr', 'w')

algorithm = 'dbgenloc'
auth_name = 'PGC'

orid = 1
arid = 1
ts = datetime.now().timestamp()
t = round(ts, 5)

origin_dict = {'jan-may':jan_may_oo, 'june':june_oo, 'july':july_oo, 'pick-view':pick_view_oo}
arrival_dict = {'jan-may':jan_may_oaa, 'june':june_oaa, 'july':july_oaa, 'pick-view':pick_view_oaa}

for dates in ['jan-may', 'june', 'july', 'pick-view']:
    #for i in range(len(db.index)):
    origin = origin_dict[dates]
    arrival = arrival_dict[dates]
    for i in origin.index:
        
        tmp_orid = origin.orid[i]
        tmp_arrival = arrival[arrival.orid==tmp_orid]
        arrival_len = len(tmp_arrival)
        
        if origin.nass[i] == arrival_len:
            ndef = origin.ndef[i]
            nass = origin.nass[i]
            
        elif origin.nass[i] != arrival_len:
            ndef = arrival_len
            nass = arrival_len
        
        #Prints a line to the origin file with the correct formatting
        origin_file.write('%9.4f %9.4f %9.4f %17.5f %8d %8d %8d %4d %4d %4d %8d '
                          '%8d %-2s %-4s %9.4f %-s %7.2f %8d %7.2f %8d %7.2f %8d '
                          '%-15s %-15s %8d %17.5f\n' % (origin.lat[i], origin.lon[i],
                           origin.depth[i], origin.time[i], orid, orid, -1, 
                           nass, ndef, -1, -1, -1, '-', '-', 
                           -999, '-', -999, -1, -999, -1, -999, -1, algorithm, 
                           algorithm+':'+auth_name, -1, t))
        
        #Prints a line to the event file with the correct formatting
        event_file.write('%8d %-15s %8d %-15s %8d %17.5f\n' % (orid, '-', -1, 'S-SNAP:PGC', -1, t))
        
        origerr_file.write('%8d %15.4f %15.4f %15.4f %15.4f %15.4f %15.4f '
                           '%15.4f %15.4f %15.4f %15.4f %9.4f %9.4f %9.4f %6.2f %9.4f '
                           '%8.2f %5.3f %8d %17.5f\n'
                           % (orid, -999999999.9999, -999999999.9999, 
                           -999999999.9999, -999999999.9999, -999999999.9999,
                           -999999999.9999, -999999999.9999, -999999999.9999, 
                           -999999999.9999, -999999999.9999, origin.sdobs[i], 
                           origin.smajax[i],origin.sminax[i], origin.strike[i],
                           origin.sdepth[i], -1.00, 0.000, -1, t))
        
        
        for j in tmp_arrival.index: 
            
            #Prints a line to the arrival file with correct formatting
            arrival_file.write('%-6s %17.5f %8d %8d %8d %8d %-8s %-8s %-s %6.3f %7.2f %7.2f %7.2f '
                               '%7.2f %7.2f %7.3f %10.1f %7.2f %7.2f %-s %-2s %10.5g %-s %-15s %8d %17.5f\n' 
                               % (arrival.sta[j], arrival.arrival_time[j], arid, -1, -1, -1, arrival.chan[j], arrival.phase[j], 
                               '-', -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -999.00, 
                               '-', '-', -1.00, '-', 'S-SNAP:PGC', -1, t))
            
            #Prints a line to the assoc file with correct formatting.
            assoc_file.write('%8d %8d %-6s %-8s %4.2f %8.3f %7.2f %7.2f %8.3f '
                             '%-s %7.1f %-s %-7.2f %-s %7.1f %6.3f %-15s %8d %17.5f\n' % 
                             (arid, orid, arrival.sta[j], arrival.phase[j], 
                              9.99, arrival.delta[j],
                              -999.00, -999.00, arrival.timeres[j], 
                              arrival.timedef[j], -999.0, '-', -999.00, '-', -999.0, 
                              arrival.wgt[j], '-', -1, t))
            arid += 1
            
        orid += 1
        
origin_file.close()
arrival_file.close()
assoc_file.close()
event_file.close()

#%%

