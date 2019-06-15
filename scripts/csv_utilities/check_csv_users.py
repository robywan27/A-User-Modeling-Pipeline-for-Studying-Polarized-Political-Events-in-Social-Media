import pandas as pd

# User operations
collected_userids = pd.read_csv('/data/project/catalonia/userids_temp2.csv', header=None, names=['userid'])
all_userids = pd.read_csv('/data/project/catalonia/userids_temp.csv', header=None, names=['userid'])

# convert ids to string so that it's not converted to decimal
collected_userids['userid'] = collected_userids['userid'].astype(str)
all_userids['userid'] = all_userids['userid'].astype(str)

# with open("/data/project/referendum_catalonia/duplicate_userids.csv", "w") as file:
# 	file.write(str(collected_userids[collected_userids.duplicated(keep=False)]))

merged_userids = all_userids.merge(collected_userids, indicator=True, how='outer')
difference_userids = merged_userids[merged_userids['_merge'] == 'left_only']['userid']

difference_userids.to_csv('/data/project/catalonia/userids_left.csv', index=False)