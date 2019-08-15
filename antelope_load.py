#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 26 10:14:50 2019

@author: visser
"""

'''
Future developements:
=====================
- Write to different formats
- Merge with other databases
- Python script to run dbevproc
- Automatic database backup
- Script to create Antelope database from other types of pick files
- Automatic running of s-snap
'''

import pandas as pd
import numpy as np
from geopy.distance import geodesic


#%%
class Load:
    '''
    A class that loads an Antelope database into Pandas DataFrames. 
    Magnitudes must have been calculated using the Antelope module rundbevproc.
    '''
    
    def __init__(self, path):
        '''
        Gets the paths from the descriptor file that Antelope uses to find the data.
        '''
        
        d = np.loadtxt(path+'db', dtype=str)
        
        ##Get the paths to the data##
        for i in d:
            if i[0] == 'dbpath':
                db_path = i[1]
                
        paths = db_path.split(':')
        s = paths[0]
        name = s[s.find("{")+1:s.find("}")]
        dbmaster = paths[1]
        dbmaster_name = dbmaster[dbmaster.find("{")+1:dbmaster.find("}")]
        dbmaster_path = dbmaster[0:dbmaster.find("{")] + dbmaster_name
        
        self.name = name
        self.path = path
        self.dbmaster_path = dbmaster_path
        self.dbmaster_name = dbmaster_name
        #self.b_value = ?
        
        
    def origin(self):
        '''
        Loads the Antelope origin table into a Pandas DataFrame.
        
        Parameters
        ----------
        self : Load class
            Uses self.path and self.name to locate the Antelope
            origin file.
            
        Returns
        -------
        df : pd.DataFrame()
            A DataFrame containing origin information.
            
        '''
        
        df = pd.read_csv(str(self.path)+str(self.name)+'.origin', 
                         header=None, 
                         delim_whitespace=True, 
                         usecols=[0,1,2,3,4,5,6,7,8,9,
                                  10,11,12,13,14,15,16,
                                  17,18,19,20,21,22,23,
                                  24,25],
                         names=['lat','lon','depth','time','orid','evid',
                                'julian_date','nass','ndef','ndp','grn','srn',
                                'etype','review','depdp','dtype','mb','mbid',
                                'ms','msid','ml','mlid','algorithm','origin_auth',
                                'origin_commid','origin_lddate'])
        
        df = df[df.time>0].reset_index(drop=True)
        df['date'] = pd.to_datetime(df['time'], unit='s')
        
        return df

    def origerr(self):
        
        '''
        Loads the Antelope origerr table into a Pandas DataFrame.
        
        Parameters
        ----------
        self : Load class
            Uses self.path and self.name to locate the Antelope
            origerr file.
            
        Returns
        -------
        df : pd.DataFrame()
            A DataFrame containing origerr information.
            
        '''
        
        df = pd.read_csv(str(self.path)+str(self.name)+'.origerr', header=None,
                         delim_whitespace=True, 
                         usecols=[0,1,2,3,4,5,6,7,8,9,
                                  10,11,12,13,14,15,16,
                                  17,18,19], 
                         names=['orid','sxx','syy','szz','stt','sxy','sxz',
                                'syz','stx','sty','stz','sdobs','smajax',
                                'sminax','strike','sdepth','stime','conf',
                                'origerr_commid','origerr_lddate'])
        
        return df
    
    def arrival(self):
        
        '''
        Loads the Antelope arrival table into a Pandas DataFrame.
        
        Parameters
        ----------
        self : Load class
            Uses self.path and self.name to locate the Antelope
            arrival file.
            
        Returns
        -------
        df : pd.DataFrame()
            A DataFrame containing arrival information.
            
        '''
    
        df = pd.read_csv(str(self.path)+str(self.name)+'.arrival', header=None, 
                         delim_whitespace=True, 
                         usecols=[0,1,2,3,4,5,6,7,8,9,
                                  10,11,12,13,14,15,16,
                                  17,18,19,20,21,22,23,
                                  24,25], 
                         names=['sta','time','arid','jdate','stassid','chanid',
                                'chan','phase','stype','deltim','azimuth',
                                'delaz','slow','delslo','ema','rect','amp',
                                'per','logat','clip','fm','snr','qual','arrival_auth',
                                'arrival_commid','arrival_lddate'])
        
        df['date'] = pd.to_datetime(df['time'], unit='s')
        
        return df

    def assoc(self):
        
        '''
        Loads the Antelope assoc table into a Pandas DataFrame.
        
        Parameters
        ----------
        self : Load class
            Uses self.path and self.name to locate the Antelope
            assoc file.
            
        Returns
        -------
        df : pd.DataFrame()
            A DataFrame containing assoc information.
            
        '''
    
        df = pd.read_csv(str(self.path)+str(self.name)+'.assoc', header=None, 
                         delim_whitespace=True, 
                         usecols=[0,1,2,3,4,5,6,7,8,9,
                                  10,11,12,13,14,15,16,
                                  17,18], 
                         names=['arid','orid','sta','phase','belief','delta',
                                'seaz','esaz','timeres','timedef','azres',
                                'azdef','slores','slodef','emares','wgt',
                                'vmodel','commid','lddate'])
        
        return df
    
    def stamag(self):
        
        '''
        Loads the Antelope stamag table into a Pandas DataFrame.
        
        Parameters
        ----------
        self : Load class
            Uses self.path and self.name to locate the Antelope
            stamag file.
            
        Returns
        -------
        df : pd.DataFrame()
            A DataFrame containing stamag information.
            
        '''
        
        df = pd.read_csv(str(self.path)+str(self.name)+'.stamag', header=None, 
                         delim_whitespace=True, usecols=[1,2,3,7,9], 
                         names=['sta','arid','orid','mag','auth'])
        
        return df
    
    def wfmeas(self):
        
        '''
        Loads the Antelope wfmeas table into a Pandas DataFrame.
        
        Parameters
        ----------
        self : Load class
            Uses self.path and self.name to locate the Antelope
            wfmeas file.
            
        Returns
        -------
        df : pd.DataFrame()
            A DataFrame containing wfmeas information.
            
        '''
        
        df = pd.read_csv(str(self.path)+str(self.name)+'.wfmeas', header=None, 
                         delim_whitespace=True, usecols=[0,1,3,6,8,12,13], 
                         names=['sta','chan','filter','time','amp','arid','auth'])
        
        df['date'] = pd.to_datetime(df['time'], unit='s')
        
        return df
    
    def site(self):
        
        '''
        Loads the Antelope site table into a Pandas DataFrame.
        
        Parameters
        ----------
        self : Load class
            Uses self.path and self.dbmaster_path to locate the Antelope
            site file.
            
        Returns
        -------
        df : pd.DataFrame()
            A DataFrame containing site information.
            
        '''
        
        df = pd.read_csv(str(self.path)+str(self.dbmaster_path)+'.site', 
                         header=None, delim_whitespace=True, usecols=[0,1,2,3,4], 
                         names=['sta','ondate','offdate','lat','lon'])
        
        return df
    
    def arrival_assoc(self):
        
        '''
        Merges the Antelope arrival and assoc tables on arid
        
        Parameters
        ----------
        self : Load class
            Uses self.arrival and self.assoc to merge.
            
        Returns
        -------
        arrassoc : pd.DataFrame()
            A DataFrame containing arrival information merged with assoc 
            information.
            
        '''
        
        arrival = self.arrival()
        #arrival = arrival.drop(['phase','sta','snr'],axis=1)
        assoc = self.assoc()
        
        arrassoc = pd.merge(arrival,assoc,how='inner',on=['arid','sta','phase'])
        
        return arrassoc

    def origin_arrival_assoc(self):
        
        '''
        Takes the merged Antelope arrival and assoc tables and then merges them
        with the Antelope origin table on orid.
        
        Parameters
        ----------
        self : Load class
            Uses self.arrival_assoc and self.origin to merge.
            
        Returns
        -------
        originassoc : pd.DataFrame()
            A DataFrame containing arrival information merged with assoc 
            information. Every arrival entry has the corresponding event information
            contained on the same row.
            
        '''
        
        arrival_assoc = self.arrival_assoc()
        arrival_assoc = arrival_assoc.rename({'date':'arrival_date','time':'arrival_time'}, axis=1)
        origin = self.origin()
#        origerr = self.origerr()
        origin = origin.rename({'date':'origin_date','time':'origin_time'}, axis=1)
        
        originassoc = pd.merge(arrival_assoc,origin,how='inner',on='orid')

        return originassoc
        
    def origin_origerr(self):
        
        '''
        Merges the Antelope origin and origerr tables.
        
        Parameters
        ----------
        self : Load class
            Uses self.origin and self.origerr
            
        Returns
        -------
        origin_origerr : pd.DataFrame()
            A DataFrame containing origin information merged with origerrr
            information.
        '''
        
        origin = self.origin()
        origerr = self.origerr()
        origin_origerr = pd.merge(origin,origerr,how='inner',on='orid')
        
        return origin_origerr
    
    def clean(self):
        
        '''
        Cleans the data, making sure that the data is within quality control standards.
        
        Params
        -----
        self: The Load class. Uses origin_origerr and origin_arrival_assoc
        
        Returns
        -------
        clean: A dict containing the cleaned origin_origerr table and the 
               cleaned origin_arrival_assoc table.
        '''
        
        origin_origerr = self.origin_origerr()
        origin_arrival_assoc = self.origin_arrival_assoc()
        
        qc = [(origin_origerr.lat <= 56.3) & (origin_origerr.lat >= 55.5)
            & (origin_origerr.lon <= -119.8) & (origin_origerr.lon >= -121.2)
            & (origin_origerr.sdobs <= 1.) & (origin_origerr.smajax <= 10)]

        clean_origin_origerr = origin_origerr[qc[0]]
        clean_origin_origerr.reset_index(inplace=True, drop=True)
        arrival_orids = origin_arrival_assoc.orid
        origin_orids  = clean_origin_origerr.orid
        clean_origin_arrival_assoc = origin_arrival_assoc[arrival_orids.isin(origin_orids)]
        clean_origin_arrival_assoc.reset_index(inplace=True, drop=True)
        
        clean = {'clean_origin_origerr':clean_origin_origerr, 'clean_origin_arrival_assoc':clean_origin_arrival_assoc}
        
        return clean
    
    def stamag_wfmeas(self):
        
        '''
        Merges the stamag and wfmeas information.
        
        Parameters
        ----------
        self : Load class
            Uses self.stamag and self.wfmeas.
            
        Returns
        -------
        stamag_wfmeas : pd.DataFrame()
            The merged stamag and wfmeas DataFrames.
        '''
        
        stamag = self.stamag()
        wfmeas = self.wfmeas()
        
        stamag = stamag[['arid','orid','mag']]
        stamag.arid = stamag.arid.astype(int)
        wfmeas = wfmeas[['arid','sta','chan','filter','amp','date','auth']]
        wfmeas.arid = wfmeas.arid.astype(int)
        
        stamag_wfmeas = pd.merge(stamag,wfmeas,how='outer',on='arid')
           
        return stamag_wfmeas
            
    def event(self):
        
        '''
        Merges the stamag_wfmeas and origin information and then adds a 
        column containing the corrected magnitude.
        
        Parameters
        ----------
        self : Load class
            Uses self.stamag_wfmeas, self.origin and self.site.
            
        Returns
        -------
        
        event : pd.DataFrame()
            All the statmagwfmeas data for each event as well as corrected magnitude.
        '''
        
        stamag_wfmeas = self.stamag_wfmeas()
        site = self.site()
        origin = self.origin()
        event = pd.merge(stamag_wfmeas, origin, how='inner', on='orid')
        event['corrected_mag'] = event.apply(lambda event: mag_correction(event=event, site=site), axis=1)
        
        return event

def mag_correction(event, site):
    '''
    Implements the magnitude correction as described by Mahani et al., 2018
    
    Parameters
    ----------
    event: One row of the event DataFrame
    site: A DataFrame containing staion location information
    
    Returns
    -------
    output_ml: The corrected station magnitude
    '''

    ev_lat = event.lat
    ev_lon = event.lon
    depth = event.depth
    amp = event.amp
    sta = event.sta
    if len(site.lat[site.sta==sta]) > 0: #Ensure that there is data to use, otherwise insert a null value
        sta_lat = site.lat[site.sta==sta].iloc[0]
        sta_lon = site.lon[site.sta==sta].iloc[0]
        sta_coords = (sta_lat, sta_lon)
        ev_coords = (ev_lat, ev_lon)
        epi_dist = geodesic(sta_coords, ev_coords).km
        d_hypo = np.sqrt((epi_dist**2) + depth**2)
        if (d_hypo<=85):
            #output_ml = np.log10(amp) + ((0.7974*np.log10(d_hypo/100)) + (0.0016*(d_hypo - 100))  + 3.)
            a = np.log10(amp)
            anot = 0.7974*np.log10(d_hypo/100)+0.0016*(d_hypo-100)+3
        else:
            #output_ml = np.log10(amp) + ((-0.1385*np.log10(d_hypo/100)) + (0.0016*(d_hypo - 100)) + 3.)
            a = np.log10(amp)
            anot = -0.1385*np.log10(d_hypo/100)+0.0016*(d_hypo-100)+3
            
        output_ml = a + anot 
    else:
        
        output_ml = -999.99
    
    return output_ml

def make_plots():
    '''
    Makes some useful plots of the dataset
    '''
    
    return None
    
    
    





