import pandas as pd

# Tweet operations
# read from csv; header=None is necessary for csv files without any header, otherwise the first line is considered the header; create a column name called 'tweetid'
collected_tweetids = pd.read_csv('/data/project/catalonia/tweetids_new.csv', header=None, names=['tweetid'])
all_tweetids = pd.read_csv('/data/project/catalonia/tweetids_all.csv', header=None, names=['tweetid'])

# convert ids to string so that it's not converted to decimal
collected_tweetids['tweetid'] = collected_tweetids['tweetid'].astype(str)
all_tweetids['tweetid'] = all_tweetids['tweetid'].astype(str)

# check for duplicates
# with open("/data/project/referendum_catalonia/duplicate_tweetids.csv", "w") as file:
# 	file.write(str(collected_tweetids[collected_tweetids.duplicated(keep=False)]))

# outer join between the two set of tweetids
merged_tweetids = all_tweetids.merge(collected_tweetids, indicator=True, how='outer')
# select only the tweetids that are only present in all_tweetids
difference_tweetids = merged_tweetids[merged_tweetids['_merge'] == 'left_only']['tweetid']

# write to csv file; index=False avoids including index (row) number together with the tweetids
difference_tweetids.to_csv('/data/project/catalonia/tweetids_discard.csv', index=False)