#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 14:00:27 2019

@author: pgcseismolab
"""

from pick_view_data import write_antelope_arrival
import os

#%%

home = os.path.expanduser('~/')

##Change this!##
data_path = home + 'Documents/pick-view-data/201906'
db_path = home + 'Documents/chet/dbs/201906/'

write_antelope_arrival(db_path, data_path)

