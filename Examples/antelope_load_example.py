'''
Loads an Antelope database into pandas DataFrames
'''
import matplotlib.pyplot as plt

from pyant import Load



data = Load('../data/201907/')

clean = data.clean()
origin_origerr = clean["clean_origin_origerr"]
x = origin_origerr.lon
y = origin_origerr.lat

plt.scatter(x, y, alpha=0.6)
plt.title('Earthquakes in Northern BC, July 2019')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
