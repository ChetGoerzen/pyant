import pandas as pd

from write_antelope_db import write_antelope_db
from antelope_load import Load

origin = pd.read_csv('../data/pick-view-data/snap_origin_2019-06-01--2019-06-'
                     '14.csv')

arrival = pd.read_csv('../data/pick-view-data/snap_arrival_2019-06-01--2019-06-'
                      '14.csv')

data = Load('../data/201907/')
site = data.site()

write_antelope_db('../data/db_from_pick_view/', origin=origin, arrival=arrival,
                  site=site, algorithm='S-SNAP', auth_name='PGC')
