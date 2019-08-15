from datetime import datetime
from .antelope_load import Load
import os

#%%

def none_func(origin, arrival, db=''):
    return origin, arrival

def merge_dbs(in_path, db_names, out_path, processing=none_func):
    
    '''
    Merges all the databases listed in db_names into one db specified in out_path.
    The index will be reset so that it starts from 1. 
    
    Parameters
    ----------
    
    in_path: str
            The path to a directory containing the desired databases
            
    db_names: list
            A list of the names of the dbs you would like to merge.
            
    out_path: str
            The directory and db name that you would like to create
            
    processing: function, optional
            A function that you would like to apply to the data. The input to 
            this function must be an origin_origerr file and an 
            origin__arrival_assoc file. This function should return these 
            tables that have been modified.
    '''
    
    if os.path.exists(out_path):
        pass
    
    else:
        os.mkdir(out_path)

    origin_file = open(out_path + 'db.origin', 'w')
    arrival_file = open(out_path + 'db.arrival', 'w')
    assoc_file = open(out_path + 'db.assoc', 'w')
    event_file = open(out_path + 'db.event', 'w')
    origerr_file = open(out_path + 'db.origerr', 'w')
    
    #Set orid and arid to 1
    orid = 1
    arid = 1
    ts = datetime.now().timestamp()
    t = round(ts, 5)
    

    
    for db in db_names:
        
        tmp_load = Load(in_path + db)
        clean_dfs = tmp_load.clean()
        origin = clean_dfs['clean_origin_origerr']
        arrival = clean_dfs['clean_origin_arrival_assoc']
        
        origin, arrival = processing(origin, arrival, db)
        
        for i in origin.index:
            
            tmp_orid = origin.orid[i]
            tmp_arrival = arrival[arrival.orid==tmp_orid]
            arrival_len = len(tmp_arrival)
            
            #Makes sure that the nass and ndef are sane, if not replaces them 
            #with a possible value
            if origin.nass[i] == arrival_len:
                ndef = origin.ndef[i]
                nass = origin.nass[i]
                
            elif origin.nass[i] != arrival_len:
                ndef = arrival_len
                nass = arrival_len
            
            #Prints a line to the origin file with the correct formatting
            origin_file.write('%9.4f %9.4f %9.4f %17.5f %8d %8d %8d %4d %4d %4d %8d '
                              '%8d %-2s %-4s %9.4f %-s %7.2f %8d %7.2f %8d %7.2f %8d '
                              '%-15s %-15s %8d %17.5f\n' % 
                              (origin.lat[i], origin.lon[i],
                               origin.depth[i], origin.time[i], orid, orid, 
                               origin.julian_date[i], nass, ndef, 
                               origin.ndp[i], origin.grn[i],  origin.srn[i], 
                               origin.etype[i], origin.review[i], origin.depdp[i],
                               origin.dtype[i], origin.mb[i], origin.mbid[i], 
                               origin.ms[i],origin.msid[i], origin.ml[i],
                               origin.mlid[i], origin.algorithm[i], 
                               origin.origin_auth[i], origin.origin_commid[i], t))
            
            #Prints a line to the event file with the correct formatting
            event_file.write('%8d %-15s %8d %-15s %8d %17.5f\n' % 
                             (orid, '-', -1, origin.origin_auth[i], 
                              origin.origin_commid[i], t))
            
            #Prints a line to the origerr file with correct formatting
            origerr_file.write('%8d %15.4f %15.4f %15.4f %15.4f %15.4f %15.4f '
                               '%15.4f %15.4f %15.4f %15.4f %9.4f %9.4f %9.4f %6.2f %9.4f '
                               '%8.2f %5.3f %8d %17.5f\n'
                               % (orid, origin.sxx[i], origin.syy[i], 
                               origin.szz[i], origin.stt[i], origin.sxy[i],
                               origin.sxz[i], origin.syz[i], origin.stx[i], 
                               origin.sty[i], origin.stz[i], origin.sdobs[i], 
                               origin.smajax[i],origin.sminax[i], origin.strike[i],
                               origin.sdepth[i], origin.stime[i], 
                               origin.conf[i], origin.origin_commid[i], t))
            
            
            for j in tmp_arrival.index: 
                
                #Prints a line to the arrival file with correct formatting
                arrival_file.write('%-6s %17.5f %8d %8d %8d %8d %-8s %-8s %-s '
                                   '%6.3f %7.2f %7.2f %7.2f %7.2f %7.2f %7.3f '
                                   '%10.1f %7.2f %7.2f %-s %-2s %10.5g %-s '
                                   '%-15s %8d %17.5f\n' % 
                                   (arrival.sta[j], arrival.arrival_time[j],
                                    arid, arrival.jdate[j], arrival.stassid[j],
                                    arrival.chanid[j], arrival.chan[j], 
                                    arrival.phase[j], arrival.stype[j], 
                                    arrival.deltim[j], arrival.azimuth[j], 
                                    arrival.delaz[j], arrival.slow[j], 
                                    arrival.delslo[j], arrival.ema[j], 
                                    arrival.rect[j], arrival.amp[j], 
                                    arrival.per[j], arrival.logat[j], 
                                    '-', '-', 
                                    arrival.snr[j], arrival.qual[j], 
                                    arrival.arrival_auth[j], 
                                    arrival.arrival_commid[j], t))
                
                #Prints a line to the assoc file with correct formatting.
                assoc_file.write('%8d %8d %-6s %-8s %4.2f %8.3f %7.2f %7.2f %8.3f '
                                 '%-s %7.1f %-s %-7.2f %-s %7.1f %6.3f %-15s %8d %17.5f\n' % 
                                 (arid, orid, arrival.sta[j], arrival.phase[j], 
                                  9.99, arrival.delta[j], -999.00, -999.00, arrival.timeres[j], 
                                  arrival.timedef[j], -999.0, '-', -999.00, '-', -999.0, 
                                  arrival.wgt[j], '-', -1, t))
                arid += 1
                
            orid += 1
            
    origin_file.close()
    arrival_file.close()
    assoc_file.close()
    event_file.close()