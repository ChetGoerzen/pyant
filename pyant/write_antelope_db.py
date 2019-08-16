import os

from datetime import datetime
from geopy.distance import geodesic
from obspy.geodetics.base import kilometers2degrees

def str_to_epoch(my_datetime):
    
    '''
    Converts a str of the format %Y-%m-%d %H:%M:%S.%f into epoch time.
    
    Parameters
    ----------
    
    my_datetime : str
        A str of the format %Y-%m-%d %H:%M:%S.%f that describes a valid date after
        January 1, 1970
        
    Returns
    -------
    
    epoch_time : float
        A float describing the given date in epoch time.
    '''

    str_datetime = str(my_datetime)
    my_time = str_datetime[0:-3]
    utc_time = datetime.strptime(my_time, "%Y-%m-%d %H:%M:%S.%f") 
    epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
    
    return epoch_time

def write_antelope_db(db_path, origin, arrival, site, algorithm, auth_name):
    
    '''
    Creates an Antelope database from csv data so that Antelope utilities
    can be applied to the data. The assoc file contains the required
    information to relate an event to it's picks.

    Parameters
    ----------
    db_path : str
        The path to the directory where you would like to create the database
        
    origin : pd.DataFrame()
        A DataFrame with the following columns:
            
        - lat: A column of floats describing latitude information.
        
        - lon: A column of floats describing longitude information.
        
        - depth: A column of floats describing depth information.
        
        - datetime: A str of the form %Y-%m-%d %H:%M:%S.%f
        
        - quality: A float describing the number of picks used in the 
                   location calculation
                   
        - orid: A column of unique identifiers for origin. Has corresponding 
                entries in the arrival table
        
    arrival : pd.DAtaFrame()
        A dataframe with the following columns:
            
        - sta: A str describing the station name
        
        - datetime: A str of the form %Y-%m-%d %H:%M:%S.%f
        
        - phase: A str describing the phase type of a given arrival, must be P
                 or S
        
        - orid: The origin that each arrival corresponds to. Has corresponding
                entries in the origin table
                
    site : pd.DataFrame()
        A DataFrame with the following columns:
            
        - sta: A str describing the station name
        
        - lat : A float describing the latitude of the station
        
        - lon : A float describing the longitude of the station
            
    algorithm : str
        The name of the algorithm used to calculate earthquake location.
        
    auth_name : str
        The name of the author of the database. I would suggest using the agency you
        are affiliated with.
        
    Returns
    -------
    
    None : 
        Returns nothing as the purpose of this function is to create database files
        that can be used by Antelope.
    '''
    
    if os.path.exists(db_path):
        pass
    
    else:
        os.mkdir(db_path)

    #Resets the index's of the DataFrames so that the index can be used to iterate
    origin.reset_index(inplace=True, drop=True)
    arrival.reset_index(inplace=True, drop=True)
    
    origin["epoch_time"] = origin.datetime.apply(str_to_epoch)
    arrival["epoch_time"] = arrival.datetime.apply(str_to_epoch)
    
    origin_file = open(db_path + 'db.origin', 'w')
    arrival_file = open(db_path + 'db.arrival', 'w')
    assoc_file = open(db_path + 'db.assoc', 'w')
    event_file = open(db_path + 'db.event', 'w')
    origerr_file = open(db_path + 'db.origerr', 'w')
    
    ts = datetime.now().timestamp()
    t = round(ts, 5)
    
    arid = 1
    
    #Loops over all the origins
    for i in range(len(origin.index)):
        
        #Prints a line to the origin file with the correct formatting
        origin_file.write('%9.4f %9.4f %9.4f %17.5f %8d %8d %8d %4d %4d %4d %8d '
                          '%8d %-2s %-4s %9.4f %-s %7.2f %8d %7.2f %8d %7.2f %8d '
                          '%-15s %-15s %8d %17.5f\n' % (origin.lat[i], origin.lon[i],
                           origin.depth[i], origin.epoch_time[i], i+1, i+1, -1, 
                           origin.nass[i], origin.ndef[i], -1, -1, -1, '-', '-', 
                           -999, '-', -999, -1, -999, -1, -999, -1, algorithm, 
                           algorithm+':'+auth_name, -1, t))
        
        #Prints a line to the event file with the correct formatting
        event_file.write('%8d %-15s %8d %-15s %8d %17.5f\n' % (i+1, '-', -1, 'S-SNAP:PGC', -1, t))
        
        #Prints a line to the origerr file with the correct formatting
        
        #=============================================================
        #orid, sxx, syy, szz, stt, sxy, sxz, syz, stx, sty, stz, sdobs, 
        #smajax, sminax, strike, sdepth, stime, conf, lddate
        #=============================================================
        origerr_file.write('%8d %15.4f %15.4f %15.4f %15.4f %15.4f %15.4f '
                           '%15.4f %15.4f %15.4f %15.4f %9.4f %9.4f %9.4f %6.2f %9.4f '
                           '%8.2f %5.3f %8d %17.5f\n'
                           % (i+1, -999999999.9999, -999999999.9999, 
                           -999999999.9999, -999999999.9999, -999999999.9999,
                           -999999999.9999, -999999999.9999, -999999999.9999, 
                           -999999999.9999, -999999999.9999, -1.0000, -1.0000,
                           -1.0000, -1.00, -1.0000, -1.00, 0.000, -1, t))
        
        tmp_orid = origin.orid[i]
        tmp_arrival = arrival[arrival.orid==tmp_orid]
        
        ev_lat = origin.lat[i]
        ev_lon = origin.lon[i]
        
        for j in tmp_arrival.index: #range(len(tmp_arrival.index)):
            
                sta = arrival.sta[j]
                sta_lat = site.lat[site.sta==sta].iloc[0]
                sta_lon = site.lon[site.sta==sta].iloc[0]
                sta_coords = (sta_lat, sta_lon)
                ev_coords = (ev_lat, ev_lon)
                dist = geodesic(sta_coords, ev_coords).km
                delta = kilometers2degrees(dist)
            
                if arrival.phase[j] == 'S' and sta in ['NBC8', 'NBC7', 'TD009', 'TD002']:
                    chan = 'HH1'
                    
                elif arrival.phase[j] == 'S' and sta not in ['NBC8', 'NBC7', 'TD009', 'TD002']:
                    chan = 'HHE'
                    
                elif arrival.phase[j] == 'P':
                    chan = 'HHZ'
                
                #Prints a line to the arrival file with correct formatting
                arrival_file.write('%-6s %17.5f %8d %8d %8d %8d %-8s %-8s %s %6.3f %7.2f %7.2f %7.2f '
                                   '%7.2f %7.2f %7.3f %10.1f %7.2f %7.2f %s %-2s %10d %s %-16s %7d %17.5f\n' 
                                   % (arrival.sta[j], arrival.epoch_time[j], arid, -1, -1, -1, chan, arrival.phase[j], 
                                   '-', -1.000, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -999.00, 
                                   '-', '-', -1, '-', 'S-SNAP:PGC', -1, t))
                
                #Prints a line to the assoc file with correct formatting.
                assoc_file.write('%8d %8d %-6s %-8s %4.2f %8.3f %7.2f %7.2f %8.3f '
                                 '%-s %7.1f %-s %-7.2f %-s %7.1f %6.3f %-15s %8d %17.5f\n' % 
                                 (arid, i+1, arrival.sta[j], arrival.phase[j], 9.99, 
                                  delta, -1, -1, -999.000, 'd', -999.0, '-', -999, '-', -999.0, 
                                  1, '-', -1, t))
                arid += 1
                
    origin_file.close()
    arrival_file.close()
    assoc_file.close()
    event_file.close()
    
    return None