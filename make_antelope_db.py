#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 14:47:40 2019

@author: pgcseismolab
"""

import pandas as pd
from datetime import datetime
import os

from pick_view_data import PickViewData

#%%

def str_to_epoch(str_datetime):
    
    my_time = str_datetime[0:-3]
    utc_time = datetime.strptime(my_time, "%Y-%m-%d %H:%M:%S.%f") 
    epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
    
    return epoch_time

home = os.path.expanduser('~/')
path = home+'Documents/pick-view-data/201906/'
data = PickViewData(path)
db_path = 'data/'

#df = data.correct_arrival()
#
#db_path = 'data/'
#ts = datetime.now().timestamp()
#t = round(ts, 5)
#df["epoch_time"] = df.datetime.apply(str_to_epoch)
#
#f = open(db_path + 'db.arrival', 'w')
#
#for n in range(len(df.index)):
#    
#    if df.phase[n] == 'S' and df.sta[n] in ['NBC8', 'NBC7', 'TD009', 'TD002']:
#        chan = 'HH1'
#        
#    elif df.phase[n] == 'S' and df.sta[n] not in ['NBC8', 'NBC7', 'TD009', 'TD002']:
#        chan = 'HHE'
#        
#    elif df.phase[n] == 'P':
#        chan = 'HHZ'
#        
#    f.write('%-6s %17.5f %8d %8d %8d %8d %-8s %-8s %s %6.3f %7.2f %7.2f %7.2f '
#            '%7.2f %7.2f %7.3f %10.1f %7.2f %7.2f %s %-2s %10d %s %-16s %7d %17.5f\n' 
#            % (df.sta[n], df.epoch_time[n], n, -1, -1, -1, chan, df.phase[n], 
#            '-', -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -999.00, 
#            '-', '-', -1, '-', 'dbp:pgcseismola', -1, t))
#
#f.close()
#
##%%
#
#origin = data.correct_origin()
#db_path = 'data/'
#
#
#origin["epoch_time"] = origin.datetime.apply(str_to_epoch)
#
#origin_file = open(db_path + 'db.origin', 'w')
#
#for n in range(len(origin.index)):
#    
#    ts = datetime.now().timestamp()
#    t = round(ts, 5)
#    origin_file.write('%9.4f %9.4f %9.4f %17.5f %8d %8d %8d %4d %4d %4d %8d '
#                      '%8d %-2s %-4s %9.4f %-s %7.2f %8d %7.2f %8d %7.2f %8d '
#                      '%-15s %-15s %8d %17.5f\n' % (origin.lat[n], origin.lon[n],
#                       origin.depth[n], origin.epoch_time[n], n+1, n+1, -1, 
#                       origin.ndef[n], origin.ndef[n], -1, -1, -1, '-', '-', 
#                       -999, '-', -999, -1, -999, -1, -999, -1, 'S-SNAP', 
#                       'S-SNAP:PGC', -1, t))
#    
#origin_file.close()

#%%


arrival = data.correct_arrival()
origin = data.correct_origin()

origin["epoch_time"] = origin.datetime.apply(str_to_epoch)
arrival["epoch_time"] = arrival.datetime.apply(str_to_epoch)

origin_file = open(db_path + 'db.origin', 'w')
arrival_file = open(db_path + 'db.arrival', 'w')
assoc_file = open(db_path + 'db.assoc', 'w')
event_file = open(db_path + 'db.event', 'w')

ts = datetime.now().timestamp()
t = round(ts, 5)

arid = 1

for i in range(len(origin.index)):
    
    origin_file.write('%9.4f %9.4f %9.4f %17.5f %8d %8d %8d %4d %4d %4d %8d '
                      '%8d %-2s %-4s %9.4f %-s %7.2f %8d %7.2f %8d %7.2f %8d '
                      '%-15s %-15s %8d %17.5f\n' % (origin.lat[i], origin.lon[i],
                       origin.depth[i], origin.epoch_time[i], i+1, i+1, -1, 
                       origin.quality[i], origin.quality[i], -1, -1, -1, '-', '-', 
                       -999, '-', -999, -1, -999, -1, -999, -1, 'S-SNAP', 
                       'S-SNAP:PGC', -1, t))
    
    event_file.write('%8d %-15s %8d %-15s %8d %17.5f\n' % (i+1, '-', -1, 'S-SNAP:PGC', -1, t))
    
    tmp_orid = origin.orid[i]
    tmp_arrival = arrival[arrival.orid==tmp_orid]
    for j in tmp_arrival.index: #range(len(tmp_arrival.index)):
            if arrival.phase[j] == 'S' and arrival.sta[j] in ['NBC8', 'NBC7', 'TD009', 'TD002']:
                chan = 'HH1'
                
            elif arrival.phase[j] == 'S' and arrival.sta[j] not in ['NBC8', 'NBC7', 'TD009', 'TD002']:
                chan = 'HHE'
                
            elif arrival.phase[j] == 'P':
                chan = 'HHZ'
                
            arrival_file.write('%-6s %17.5f %8d %8d %8d %8d %-8s %-8s %s %6.3f %7.2f %7.2f %7.2f '
                               '%7.2f %7.2f %7.3f %10.1f %7.2f %7.2f %s %-2s %10d %s %-16s %7d %17.5f\n' 
                               % (arrival.sta[j], arrival.epoch_time[j], arid, -1, -1, -1, chan, arrival.phase[j], 
                               '-', -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -999.00, 
                               '-', '-', -1, '-', 'S-SNAP:PGC', -1, t))
            
            assoc_file.write('%8d %8d %-6s %-8s %4.2f %8.3f %7.2f %7.2f %8.3f '
                             '%-s %7.2f %-s %-7.2f %-s %7.1f %6.3f %-15s %8d %17.5f\n' % 
                             (arid, i+1, arrival.sta[j], arrival.phase[j], 9.99, 
                              -1, -1, -1, -1, '-', -999, '-', -999, '-', -999, 
                              1, '-', -1, t))
            arid += 1
            
origin_file.close()
arrival_file.close()
assoc_file.close()
event_file.close()

