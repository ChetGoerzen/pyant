import os 
import pandas as pd
from datetime import datetime

class PickViewData:
    
    '''
    A class to load in data from the pick-view data into pandas DataFrames.
    Requires the event filenames to end with '-out.csv'. The origin files must
    contain 'origin' after the first underscore. The arrival files must contain
    'arrival' after the first underscore. Some examples are
    '2019-06-01-1564418738.1480892-out.csv', 
    'snap_origin_2019-06-01--2019-06-14.csv', and
    'snap_arrival_2019-06-01--2019-06-14.csv'
    '''
    
    def __init__(self, path):
        
        self.path = path
        self.data_dir = os.listdir(path)
        
    def correct_events(self):
        
        '''
        A pd.DataFrame() containing the orids of events marked 'correct'.
        Duplicate orids are removed.
        '''
        
        data_dir = self.data_dir
        path = self.path
        
        out_df = pd.DataFrame()
        for file in data_dir:
            if file.split('-')[-1] == 'out.csv':
                tmp_df = pd.read_csv(path + '/' + file, index_col=0)
                out_df = out_df.append(tmp_df)
                
        correct_events = out_df.loc[out_df.status == 'correct']
        correct_events = correct_events.drop_duplicates(subset="orid")
        
        return correct_events
        
    def review_events(self):
        
        '''
        A pd.DataFrame() containing the orids of events marked 'review'.
        Duplicate orids are removed.
        '''
        
        data_dir = self.data_dir
        path = self.path
        
        out_df = pd.DataFrame()
        for file in data_dir:
            if file.split('-')[-1] == 'out.csv':
                tmp_df = pd.read_csv(path + '/' + file, index_col=0)
                out_df = out_df.append(tmp_df)
                
        review_events = out_df.loc[out_df.status == 'review']
        review_events = review_events.drop_duplicates(subset="orid")
        
        return review_events
    
    def false_events(self):
        
        '''
        A pd.DataFrame() containing the orids of events marked 'false'.
        Duplicate orids are removed.
        '''
        
        data_dir = self.data_dir
        path = self.path
        
        out_df = pd.DataFrame()
        for file in data_dir:
            if file.split('-')[-1] == 'out.csv':
                tmp_df = pd.read_csv(path + '/' + file, index_col=0)
                out_df = out_df.append(tmp_df)
                
        false_events = out_df.loc[out_df.status == 'false']
        false_events = false_events.drop_duplicates(subset="orid")
        
        return false_events
    
    def origin(self):
        
        '''
        A pd.DataFrame() containing all origins in the pick-view data.
        '''
        
        data_dir = self.data_dir
        path = self.path
        
        origin_df = pd.DataFrame()
        for file in data_dir:
            if len(file.split('_')) >1:
                if file.split('_')[1] == 'origin':
                    tmp_df = pd.read_csv(path + '/' + file, index_col=0)
                    origin_df = origin_df.append(tmp_df)
                    
        origin_df["nass"] = origin_df.ndef
        
        return origin_df
    
    def correct_origin(self):
        
        '''
        A pd.DataFrame() containing all origins in the pick-view data marked 'correct'.
        Duplicate orids are removed and the index is reset.
        '''
        
        origin = self.origin()
        correct_events = self.correct_events()
        
        correct_origin = origin.loc[origin["orid"].isin(correct_events["orid"])]
        correct_origin = correct_origin.drop_duplicates(subset="orid")
        correct_origin.reset_index(inplace=True, drop=True)
        
        return correct_origin
    
    def review_origin(self):
        
        '''
        A pd.DataFrame() containing all origins in the pick-view data marked 'review'.
        Duplicate orids are removed and the index is reset.
        '''
        
        origin = self.origin()
        review_events = self.review_events()
        
        review_origin = origin.loc[origin["orid"].isin(review_events["orid"])]
        review_origin = review_origin.drop_duplicates(subset="orid")
        review_origin.reset_index(inplace=True, drop=True)
        
        return review_origin
    
    def false_origin(self):
        
        '''
        A pd.DataFrame() containing all origins in the pick-view data marked 'false'.
        Duplicate orids are removed and the index is reset.
        '''
        
        origin = self.origin()
        false_events = self.false_events()
        
        false_origin = origin.loc[origin["orid"].isin(false_events["orid"])]
        false_origin = false_origin.drop_duplicates(subset="orid")
        false_origin.reset_index(inplace=True, drop=True)
        
        return false_origin
    
    def arrival(self):
        
        '''
        A pd.DataFrame() containing all arrivals in the pick-view data.
        Duplicate orids are removed and the index is reset.
        '''
        
        data_dir = self.data_dir 
        path = self.path
        
        arr_df = pd.DataFrame()
        for file in data_dir:
            if len(file.split('_')) > 1:
                if file.split('_')[1] == 'arrival':
                    tmp_df = pd.read_csv(path + '/' + file, index_col=0)
                    arr_df = arr_df.append(tmp_df)
                
        return arr_df
    
    def correct_arrival(self):
        
        '''
        A pd.DataFrame() containing all arrivals in the pick-view data marked 'correct'.
        Duplicate datetimes are removed and the index is reset.
        '''
        
        arrival = self.arrival()
        correct_events = self.correct_events()
        
        correct_arrival = arrival[arrival["orid"].isin(correct_events["orid"])]
       # correct_arrival.drop_duplicates(subset=["datetime", "sta"], inplace=True)
        correct_arrival.reset_index(inplace=True, drop=True)
        
        return correct_arrival
    
    def review_arrival(self):
        
        '''
        A pd.DataFrame() containing all arrivals in the pick-view data marked 'review'.
        Duplicate datetimes are removed and the index is reset.
        '''
        
        arrival = self.arrival()
        review_events = self.review_events()
        
        review_arrival = arrival[arrival["orid"].isin(review_events["orid"])]
        trim_arrival = review_arrival.drop_duplicates(subset="datetime")
        clean_arrival = trim_arrival.reset_index(drop=True)
        
        return clean_arrival
    
    def false_arrival(self):
        
        '''
        A pd.DataFrame() containing all arrivals in the pick-view data marked 'false'.
        Duplicate datetimes are removed and the index is reset.
        '''
        
        arrival = self.arrival()
        false_events = self.false_events()
        
        false_arrival = arrival[arrival["orid"].isin(false_events["orid"])]
        false_arrival.drop_duplicates(subset="datetime", inplace=True)
        false_arrival.reset_index(inplace=True, drop=True)
        
        return false_arrival
    
def str_to_epoch(str_datetime):
    
    '''
    Converts a str of the format %Y-%m-%d %H:%M:%S.%f into epoch time.
    
    Parameters
    ----------
    
    str_datetime : str
        A str of the format %Y-%m-%d %H:%M:%S.%f that describes a valid date after
        January 1, 1970
        
    Returns
    -------
    
    epoch_time : float
        A float describing the given date in epoch time.
    '''
    
    my_time = str_datetime[0:-3]
    utc_time = datetime.strptime(my_time, "%Y-%m-%d %H:%M:%S.%f") 
    epoch_time = (utc_time - datetime(1970, 1, 1)).total_seconds()
    
    return epoch_time
    
def write_antelope_arrival(db_path, data_path, category_of_picks='review'):
    
    '''
    Write an antelope arrival table from a pick-view arrival file.
    
    Parameters
    ----------
    db_path : str
        The path to the directory where you would like to write the arrival files to.
        
    data_path : str
        The path to the directory containing the pick-view data.
        
    category_of_picks : str, optional
        The category of picks that you would like to include in the arrival file.
        Either 'review', 'false', or 'corrrect'. Defaults to 'review'
        
    Returns
    -------
    
    None : 
        Returns nothing as the purpose of this function is to create an arrival file
    '''
    
    data = PickViewData(data_path)
   
    if category_of_picks == 'corrrect':
        df = data.correct_arrival()
        
    elif category_of_picks == 'review':
        df = data.review_arrival()
        
    elif category_of_picks == 'false':
        df = data.false_arrival()
        
    else:
        raise Exception("Invalid category!")
    
    df["epoch_time"] = df.datetime.apply(str_to_epoch)
    ts = datetime.now().timestamp()
    t = round(ts, 5)
    
    f = open(db_path + 'db.arrival', 'w')
    
    for n in range(len(df.index)):
        
        if df.phase[n] == 'S' and df.sta[n] in ['NBC8', 'NBC7', 'TD009', 'TD002']:
            chan = 'HH1'
            
        elif df.phase[n] == 'S' and df.sta[n] not in ['NBC8', 'NBC7', 'TD009', 'TD002']:
            chan = 'HHE'
            
        elif df.phase[n] == 'P':
            chan = 'HHZ'
            
        f.write('%-6s %17.5f %8d %8d %8d %8d %-8s %-8s %s %6.3f %7.2f %7.2f '
                '%7.2f %7.2f %7.2f %7.3f %10.1f %7.2f %7.2f %s %-2s %10d %s %-16s %7d %17.5f\n' 
                % (df.sta[n], df.epoch_time[n], n, -1, -1, -1, chan, df.phase[n],
                '-', -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0, 
                -999.00, '-', '-', -1, '-', 'dbp:pgcseismola', -1, t))
    
    f.close()
    
def write_antelope_origin(db_path, data_path, category_of_picks='review'):
    
    '''
    Write an antelope origin table from a pick-view origin file.
    
    Parameters
    ----------
    db_path : str
        The path to the directory where you would like to write the origin files to.
        
    data_path : str
        The path to the directory containing the pick-view data.
        
    category_of_picks : str, optional
        The category of events that you would like to include in the origin file.
        Either 'review', 'false', or 'corrrect'. Defaults to 'review'
        
    Returns
    -------
    
    None : 
        Returns nothing as the purpose of this function is to create an origin file
    '''
    
    data = PickViewData(data_path)
    if category_of_picks == 'corrrect':
        origin = data.correct_origin()
        
    elif category_of_picks == 'review':
        origin = data.review_origin()
        
    elif category_of_picks == 'false':
        origin = data.false_origin()
    
    origin["epoch_time"] = origin.datetime.apply(str_to_epoch)
    ts = datetime.now().timestamp()
    t = round(ts, 5)

    origin_file = open(db_path + 'db.origin', 'w')
    
    for n in range(len(origin.index)):
    
        origin_file.write('%9.4f %9.4f %9.4f %17.5f %8d %8d %8d %4d %4d %4d %8d '
                          '%8d %-2s %-4s %9.4f %-s %7.2f %8d %7.2f %8d %7.2f %8d '
                          '%-15s %-15s %8d %17.5f\n' % (origin.lat[n], origin.lon[n],
                           origin.depth[n], origin.epoch_time[n], n+1, n+1, -1, 
                           origin.ndef[n], origin.ndef[n], -1, -1, -1, '-', '-', 
                           -999, '-', -999, -1, -999, -1, -999, -1, 'S-SNAP', 
                           'S-SNAP:PGC', -1, t))
    
    origin_file.close()
    
def write_antelope_db_from_pick_view(db_path, data_path, category_of_picks='review'):
    
    '''
    Creates an Antelope database from pick-view data so that Antelope utilities
    can be applied to the data. The orid and arid values start from one and increase
    by one fro each entry
    
    Parameters
    ----------
    db_path : str
        The path to the directory where you would like to create the database
        
    data_path : str
        The path to the directory containing the pick-view data.
        
    category_of_picks : str, optional
        The category of events you would like to include in the database.
        Either 'review', 'false', or 'corrrect'. Defaults to 'review'
        
    Returns
    -------
    
    None : 
        Returns nothing as the purpose of this function is to create an Antelope database
    '''
    
    data = PickViewData(data_path)
    
    if category_of_picks == 'corrrect':
        origin = data.correct_origin()
        arrival = data.correct_arrival()
        
    elif category_of_picks == 'review':
        origin = data.review_origin()
        arrival = data.review_arrival()
        
    elif category_of_picks == 'false':
        origin = data.false_origin()
        arrival = data.false_arrival()
    
    origin["epoch_time"] = origin.datetime.apply(str_to_epoch)
    arrival["epoch_time"] = arrival.datetime.apply(str_to_epoch)
    
    if os.path.exists(db_path):
        pass
    
    else:
        os.mkdir(db_path)

    
    origin_file = open(db_path + 'db.origin', 'w')
    origerr_file = open(db_path + 'db.origerr', 'w')
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
        
        #Prints a line to the origerr file with correct formatting
        origerr_file.write('%8d %15.4f %15.4f %15.4f %15.4f %15.4f %15.4f '
                           '%15.4f %15.4f %15.4f %15.4f %9.4f %9.4f %9.4f %6.2f %9.4f '
                           '%8.2f %5.3f %8d %17.5f\n' % 
                           (i+1, -999999999.9999, -999999999.9999, 
                           -999999999.9999, -999999999.9999, -999999999.9999,
                           -999999999.9999, -999999999.9999, -999999999.9999, 
                           -999999999.9999, -999999999.9999, -1.0000, -1.0000,
                           -1.0000, -1.00, -1.0000, -1.00, 0.000, -1, t))
        
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
