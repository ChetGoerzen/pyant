from pyant import merge_dbs

db_names = ['201907/', 'db_from_pick_view/']
merge_dbs(in_path='../data/', db_names=db_names, out_path='../data/merged_dbs/')