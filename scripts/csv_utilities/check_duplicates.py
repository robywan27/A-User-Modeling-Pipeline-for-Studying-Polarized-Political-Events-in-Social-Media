import pandas as pd


# User operations
intersect_userids = pd.read_csv('/data/project/lombardia/userids_intersect.csv', header=None, names=['userid'])


# convert ids to string so that it's not converted to decimal
intersect_userids['userid'] = intersect_userids['userid'].astype(str)

duplicate_userids = intersect_userids[intersect_userids.duplicated() == False]['userid']
duplicate_userids.to_csv('/data/project/lombardia/duplicate_userids.csv', index=False)