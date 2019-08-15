#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 14:44:28 2019

@author: pgcseismolab
"""

import datetime as dt
import pandas as pd

#%%

arrival_sta = final_arrivals.copy(deep=True)

cat = arrival_sta[['sta','chan','phase','date']].sort_values('date').reset_index(drop=True)
arid = list(cat.index+1)
cat['arid'] = arid
cat.arid = cat.arid.astype(int)

cat.columns = ['sta','chan','iphase','time','arid'] # rename columns

arrival_sta['arid'] = arrival_sta.index+1

arrival_sta = arrival_sta[['sta','chan','phase','date','arid']]
arrival_sta['jdate'] = arrival_sta.date.dt.strftime('%Y%j')
arrival_sta.columns=['sta','chan','iphase','time','arid','jdate']
arrival_sta['time'] = (arrival_sta['time'] - dt.datetime(1970,1,1)).dt.total_seconds()

arrival_sta['time'] = np.round(arrival_sta['time'],5).astype(str).str.pad(16,side='right',fillchar='0')

arrival_deets = pd.read_csv('~/Documents/SNAP/arrival_deets.csv', index_col=['headers'])

arrival_deets.snr.nulls = str(int(arrival_deets.snr.nulls[0:2]))

arrival_deets.snr.padded_nulls = str(int(arrival_deets.snr.padded_nulls[5:7]))

arrival_deets.snr.padded_nulls = '        '+arrival_deets.snr.padded_nulls

def padder(instruction_df, input_df, columns):
    '''
    Pass in instruction table and table with arrivals. 
    
    Inputs null values where necessary.
    
    Time must be given in epoch time (seconds since x, 1970)
    '''
    
    for line in columns:
        
        print(line)
        
        if line != 'sta' and line != 'chan' and line != 'iphase' and line != 'time' and line != 'arid' and line != 'jdate': # columns that don't need to be padded, change if you have more columns with data.
        
            nulls = instruction_df[line].padded_nulls
            
            input_df[line] = nulls

        else: # columns to be padded, change if you have more columns with data.
            
            input_df[line] = input_df[line].astype(str)
            
            max_length = instruction_df[line].character_length
            print(max_length)
            alignments = instruction_df[line].alignment
          
            if alignments == 'right':

                input_df[line] = input_df[line].str.pad(int(max_length),side='left')

            else:

                input_df[line] = input_df[line].str.pad(int(max_length),side='right')
           
    return input_df
        
yummer = padder(arrival_deets,arrival_sta, arrival_deets.columns)

yummer = yummer[arrival_deets.columns]

filename =  '/Volumes/blackbox/dbs/snap_test/arrivaler.txt'
yummer.to_csv(filename, header=False, index=False, sep='\\')

f = open(filename, 'r')
text = f.read()
f.close()
text = text.replace('\\',' ')
f = open(filename, 'w')
f.write(text)
f.close()

#%%


# =============================================================================
# PUT The arrival table into antelope and calculate magnitudes!
# =============================================================================

#%%

origins = dfer.copy(deep=True)

cat = origins[['lat','lon','depth','date','index']].sort_values('date').reset_index(drop=True)
#arid = list(cat.index+1)
#cat['arid'] = arid
#cat.arid = cat.arid.astype(str)

cat.columns = ['lat','lon','depth','date','orid'] # rename columns

#origins['arid'] = origins.index

origins = origins[['lat','lon','depth','date','index']]
origins['jdate'] = origins.date.dt.strftime('%Y%j')
origins.columns=['lat','lon','depth','time','orid','jdate']
origins['evid']= origins.orid

origins['time'] = (origins['time'] - dt.datetime(1970,1,1)).dt.total_seconds()
origins['auth'] = ':visser'
origins['algorithm'] = 'dbgenloc:cn01'
#origins.orid = origins.orid.astype(int)
#origins.lat = origins.lat.str('%9.4f')
#origins['lat'] = origins['lat'].map('  {:.4f}'.format).str.pad(width=9,side='left',fillchar=' ')
#origins['lon'] = origins['lon'].map('{:.4f}'.format).str.pad(width=9,side='left',fillchar=' ')
#origins['depth'] = origins['depth'].map('{:.4f}'.format).str.pad(width=9,side='left',fillchar=' ')
#origins['time'] = origins['time'].map('{:.5f}'.format).str.pad(width=17,side='left',fillchar=' ')
#origins['orid'] = origins['orid'].astype(str).str.pad(width=8,side='left',fillchar=' ')

#origins['time'] = np.round(origins['time'],5).astype(str).str.pad(16,side='right',fillchar='0')
#
origin_deets = pd.read_csv('~/Documents/ForDawei/build_antelope_db/origin_deets.csv', index_col=['headers'])

#origin_deets.snr.nulls = str(int(arrival_deets.snr.nulls[0:2]))
#
#origin_deets.snr.padded_nulls = str(int(arrival_deets.snr.padded_nulls[5:7]))
#
#origin_deets.snr.padded_nulls = '        '+arrival_deets.snr.padded_nulls

#%%

import datetime as dt

def padder(instruction_df, input_df, columns):
    '''
    Pass in instruction table and table with arrivals. 
    
    Inputs null values where necessary.
    
    Time must be given in epoch time (seconds since x, 1970)
    '''
    
    for column in instruction_df.columns:
        
#        print(column)
    
        if input_df.columns.isin(np.array([column])).any():
            
            decimals = int(instruction_df[column]['decimals'])
            width = int(instruction_df[column]['character_length'])
            alignment = instruction_df[column]['alignment']

            if alignment == 'left':
                alignment='right'
                
            else:
                alignment='left'

            typer = instruction_df[column]['type']
            
            command = '{:.'+str(decimals)+'f}'
            
            if decimals != 0:
                
#                print(column)
                
                input_df[column] = input_df[column].astype(typer).astype(str)
                
                input_df[column] = input_df[column].astype(float).map(command.format).astype(str)
                input_df[column] = input_df[column].str.pad(width=width,side=alignment,fillchar=' ')
                
            else:
                
#                print(column)
                input_df[column] = input_df[column].astype(typer)  
                
                if column=='orid':
                    
                    print(input_df[column])
                
                input_df[column] = input_df[column].astype(str)                
                input_df[column] = input_df[column].str.pad(width=width,side=alignment,fillchar=' ')
                
                if column=='orid':
                    
                    print(input_df[column])
#                input_df[column] = input_df[column].astype(typer)

                
                
        else:
            
            null = instruction_df[column]['nulls']
            
#            print(null)
            
            decimals = int(instruction_df[column]['decimals'])
            width = int(instruction_df[column]['character_length'])
            alignment = instruction_df[column]['alignment']
            if alignment == 'left':
                alignment='right'
                
            else:
                alignment='left'
            typer = instruction_df[column]['type']

            command = '{:.'+str(decimals)+'f}'
            
            if column == 'Iddate':
            
                input_df[column] = float((dt.datetime.now() - dt.datetime(1970,1,1)).total_seconds())


                    
            else:
                input_df[column] = null
                input_df[column] = input_df[column].astype(typer)
    
            if decimals != 0:
                
#                print(column)
                
                input_df[column] = input_df[column].astype(typer).astype(str)
                
                input_df[column] = input_df[column].astype(float).map(command.format).astype(str)
                input_df[column] = input_df[column].str.pad(width=width,side=alignment,fillchar=' ')
                
            else:
                input_df[column] = input_df[column].astype(str)                
                input_df[column] = input_df[column].str.pad(width=width,side=alignment,fillchar=' ')

                
#                print(column, width)
                
#                input_df[column] = input_df[column].astype(typer)
    
#    for line in columns:
#        
#        print(line)
#        
#        if line != 'lat' and line != 'lon' and line != 'depth' and line != 'time' and line != 'orid' and line != 'jdate': # columns that don't need to be padded, change if you have more columns with data.
#        
#            nulls = instruction_df[line].padded_nulls
#            
#            input_df[line] = nulls
#
#        else: # columns to be padded, change if you have more columns with data.
#            
#            input_df[line] = input_df[line].astype(str)
#            
#            max_length = instruction_df[line].character_length
#            print(max_length)
#            alignments = instruction_df[line].alignment
#          
#            if alignments == 'right':
#
#                input_df[line] = input_df[line].str.pad(int(max_length),side='left')
#
#            else:
#
#                input_df[line] = input_df[line].str.pad(int(max_length),side='right')
#           
    return input_df
        
yummer = padder(origin_deets,origins, origins.columns)

yummer = yummer[origin_deets.columns]

filename =  '/Volumes/blackbox/dbs/snap_test/originer.txt'
yummer.to_csv(filename, header=False, index=False, sep='\\')

f = open(filename, 'r')
text = f.read()
f.close()
text = text.replace('\\',' ')
f = open(filename, 'w')
f.write(text)
f.close()

#%%
from geopy.distance import great_circle

assoc = pd.DataFrame(columns=['arid','orid','sta','phase','wgt','vmodel','timedef','delta'])
assoc.arid = final_arrivals.index+1
assoc.orid = final_arrivals['index'].astype(int)
assoc.sta = final_arrivals.sta
assoc.phase = final_arrivals.phase
assoc.timedef = 'd'
assoc.wgt = 1
assoc.vmodel = '1dcvl/cn01'

#%%

stations = pd.read_csv('~/Documents/all-in-one/4_s-snap/station_master_antelope3.csv')
stations = stations.set_index('sta')

#%%

for id1, row1 in dfer.iterrows():
    
    assoc_iter = assoc[assoc.orid==id1]
    
    for id2, row2 in assoc_iter.iterrows():
        
        print(id1, id2)
        
        station = stations.loc[row2.sta]
        lat1 = float(station.lat)
        lon1 = float(station.lon)
        lat2 = float(row1.lat)
        lon2 = float(row1.lon)
        dist = (great_circle((lat1,lon1),(lat2,lon2)).km)/111.1
        
        assoc.loc[id2,'delta'] = dist

assoc_deets = pd.read_csv('~/Documents/ForDawei/build_antelope_db/assoc_deets.csv', index_col=['headers'])

#assoc_deets = assoc_deets[['azdef']]

yummer = padder(assoc_deets,assoc, assoc.columns)

yummer = yummer[assoc_deets.columns]

filename =  '/Volumes/blackbox/dbs/snap_test/assocer.txt'
yummer.to_csv(filename, header=False, index=False, sep='\\')

f = open(filename, 'r')
text = f.read()
f.close()
text = text.replace('\\',' ')
f = open(filename, 'w')
f.write(text)
f.close()

#%%

emodel = pd.DataFrame(columns=['orid','emodelx','emodely','emodelz','emodelt'])
emodel.orid = dfer['index'].astype(int)
emodel.emodelx = 0.5
emodel.emodely = 0.5
emodel.emodelz = 0.5
emodel.emodelt = 0.5

emodel_deets = pd.read_csv('~/Documents/ForDawei/build_antelope_db/emodel_deets.csv', index_col=['headers'])

#assoc_deets = assoc_deets[['azdef']]

yummer = padder(emodel_deets,emodel, emodel.columns)

yummer = yummer[emodel_deets.columns]

filename =  '/Volumes/blackbox/dbs/snap_test/emodel.txt'
yummer.to_csv(filename, header=False, index=False, sep='\\')

f = open(filename, 'r')
text = f.read()
f.close()
text = text.replace('\\',' ')
f = open(filename, 'w')
f.write(text)
f.close()

#%%

event = pd.DataFrame(columns=['evid','prefor','auth'])
event.evid = dfer['index'].astype(int)
event.prefor = dfer['index'].astype(int)
event.auth = ':visser'


event_deets = pd.read_csv('~/Documents/ForDawei/build_antelope_db/event_deets.csv', index_col=['headers'])

#assoc_deets = assoc_deets[['azdef']]

yummer = padder(event_deets,event, event.columns)

yummer = yummer[event_deets.columns]

filename =  '/Volumes/blackbox/dbs/snap_test/event.txt'
yummer.to_csv(filename, header=False, index=False, sep='\\')

f = open(filename, 'r')
text = f.read()
f.close()
text = text.replace('\\',' ')
f = open(filename, 'w')
f.write(text)
f.close()