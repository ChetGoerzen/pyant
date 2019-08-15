#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 17 17:11:31 2019

@author: pgcseismolab
"""

##  python -m pytest test/

from antelope_load import Load
from antelope_load import mag_correction
import pandas as pd
from pandas import Timestamp
from geopy.distance import geodesic
import numpy as np
import numpy.testing as npt

#%%

path = '/Users/pgcseismolab/Documents/chet/dbs/201901-201905/' 
test = Load(path)

clean = test.clean()
event = test.event()

#%%

## Test mag_correction on some fake data
row = [[190306,
       16819,
       3.43,
       'BDMTA',
       'HHZ',
       'WAV',
       0.001,
       Timestamp('2019-07-13 09:00:41.150000095'),
       'dbevproc',
       56.7926,
       -122.1463,
       9.6649,
       1563008351.86508,
       6499,
       2019194,
       59,
       3.79,
       240,
       Timestamp('2019-07-13 08:59:11.865080118')]]

columns = ['arid', 'orid', 'mag', 'sta', 'chan', 'filter', 'amp', 'date_x', 'auth',
       'lat', 'lon', 'depth', 'time', 'evid', 'julian_date', 'ndef', 'ml',
       'mlid', 'date_y']

df = pd.DataFrame(row, columns=columns)
my_event = df.iloc[0]
site = pd.DataFrame([[56.7926, -120.509863106732, 'BDMTA']], columns=['lat', 'lon', 'sta'])

corrected_mag = mag_correction(my_event, site)

npt.assert_almost_equal(0., corrected_mag, decimal=3)
