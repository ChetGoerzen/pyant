#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 13:33:39 2019

@author: pgcseismolab
"""


import pandas as pd
import os

from antelope_load import Load
from pick_view_data import PickViewData
#%%

home = os.path.expanduser('~/')
begin_path = home + 'Documents/chet/dbs/'

antelope_path = ['201901-201905/', '201906/']
antelope_dict = dict()
for path in antelope_path:
    antelope_dict[path] = Load(begin_path + path)
    
begin_pick_view = home+'Documents/pick-view-data/'
pick_view_path = ['201901-201905/', '201906/']
pick_view_dict = dict()

pick_view_arrival = pd.DataFrame()
pick_view_origin = pd.DataFrame()

for path in pick_view_path:
    pick_view_dict[path] = PickViewData(begin_pick_view + path)
    tmp_arrival = pick_view_dict[path].correct_arrival()
    pick_view_arrival = pick_view_arrival.append(tmp_arrival)
    tmp_origin = pick_view_dict[path].correct_origin()
    pick_view_origin = pick_view_origin.append(tmp_origin)
    
#%%
    
tmp = pick_view_dict['201906/']

tmp.correct_arrival()
