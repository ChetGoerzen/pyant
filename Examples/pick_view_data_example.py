'''
An example demonstrating the use of the pick_view_data module
'''
import matplotlib.pyplot as plt
import os

from pyant import PickViewData
from pyant import write_antelope_db_from_pick_view

data = PickViewData('../data/pick-view-data/')

correct_origin = data.correct_origin()
review_origin = data.review_origin()
false_origin = data.false_origin()

# Make some nice plots
plt.scatter(x=correct_origin.lon, y=correct_origin.lat, marker='o',
            color='green')

plt.scatter(x=review_origin.lon, y=review_origin.lat, marker='s',
            color='yellow')

plt.scatter(x=false_origin.lon, y=false_origin.lat, marker='^',
            color='red')

# Create the Antelope database from pick-view data
write_antelope_db_from_pick_view(db_path='../data/db_from_pick_view/',
                                 data_path='../data/pick-view-data/')

# Create a descriptor file 
os.system("echo '# Descriptor file\n schema css3.0\n dbpath	.{db}:../dbmaster/"
          "{nbc}:\n dblocs local' > ../data/db_from_pick_view"
          "/db")