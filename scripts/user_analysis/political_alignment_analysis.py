import psycopg2
import io




conn_string = "host='localhost' dbname='referendum_catalonia_db' user='postgres' password='########'"

tweets_table = "tweets_metadata"
users_table = "processed_users"



yes_keywords = []
no_keywords = []
neutral_keywords = []



def prepare_keywords():
	with io.open('./catalonia/yes_keywords.txt', encoding='utf-8') as fp:
		for line in fp:
			a = line.strip()
			yes_keywords.append(a)
	with io.open('./catalonia/no_keywords.txt', encoding='utf-8') as fp2:
		for line in fp2:
			a = line.strip()
			no_keywords.append(a)
	with io.open('./catalonia/neutral_keywords.txt', encoding='utf-8') as fp3:
		for line in fp3:
			a = line.strip()
			neutral_keywords.append(a)
	





def assess_alignment():
	select_string = "SELECT tweetid, userid, hashtags FROM " + tweets_table
	select_users = "SELECT userid FROM " + users_table
	
	prepare_keywords()
	
	yes_dict = {}
	no_dict = {}
	neutral_dict = {}
	
	yes_count = 0
	no_count = 0
	neutral_count = 0
	yes_users = 0
	yes_tweets = 0
	no_users = 0
	no_tweets = 0
	neutral_users = 0
	neutral_tweets = 0

	users = []
	
	try:
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()
		cur.execute(select_users)
		rows = cur.fetchall()

		for row in rows:
			userid = row[0]
			users.append(userid)
	except psycopg2.Error as e:
		print(e.pgerror)
		print("DB Error: " + str(e))
		
	try:
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()
		cur.execute(select_string)
		rows = cur.fetchall()

		for row in rows:			
			tweetid = row[0]
			userid = row[1]
			hashtags = row[2]

			# check whether the user is one of the processed users (don't considered the discarded users)
			if(userid in users):
				if(hashtags != None):
					yes_flag = 0
					no_flag = 0
					neutral_flag = 0
					for hashtag in hashtags:
						if(hashtag != None):						
							if (hashtag["text"].lower() in yes_keywords):
								yes_count += 1
								yes_flag = 1					
							if (hashtag["text"].lower() in no_keywords):
								no_count += 1
								no_flag = 1
							if (hashtag["text"].lower() in neutral_keywords):
								neutral_count += 1
								neutral_flag = 1
					if yes_flag == 1:		
						yes_tweets += 1
						if(userid not in yes_dict):
							yes_dict[userid] = userid
					if no_flag == 1:		
						no_tweets += 1
						if(userid not in no_dict):
							no_dict[userid] = userid
					if neutral_flag == 1:		
						neutral_tweets += 1
						if(userid not in neutral_dict):
							neutral_dict[userid] = userid			
		conn.close()
	except psycopg2.Error as e:
		print(e.pgerror)
		print("DB Error: " + str(e))
	
	for userid in yes_dict:
		yes_users += 1		
	print("Yes count: " + str(yes_count))
	print("Yes users: " + str(yes_users))
	print("Yes tweets: " + str(yes_tweets))	

	for userid in no_dict:
		no_users += 1		
	print("No count: " + str(no_count))
	print("No users: " + str(no_users))
	print("No tweets: " + str(no_tweets))	

	for userid in neutral_dict:
		neutral_users += 1		
	print("Neutral count: " + str(neutral_count))
	print("Neutral users: " + str(neutral_users))
	print("Neutral tweets: " + str(neutral_tweets))	
	






def find_political_alignment_by_user():	
	select_tweets = "SELECT tweetid, userid, hashtags FROM " + tweets_table
	select_users = "SELECT userid FROM " + users_table
	
	political_alignments = {}

	prepare_keywords()

	users = []
	
	try:
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()
		cur.execute(select_users)
		rows = cur.fetchall()

		for row in rows:
			userid = row[0]
			users.append(userid)
	except psycopg2.Error as e:
		print(e.pgerror)
		print("Conenction2 failed!")	
	
	try:
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()		
		cur.execute(select_tweets)
		rows = cur.fetchall()

		for row in rows:			
			tweetid = row[0]
			userid = row[1]
			hashtags = row[2]

			# check whether the user is one of the processed users (don't considered the discarded users)
			if(userid in users):
				number_of_yes_words = 0
				number_of_no_words = 0
				number_of_none_words = 0

				if(hashtags != None):
					for hashtag in hashtags:
						if(hashtag != None):							
							if(hashtag["text"].lower() in no_keywords):
								number_of_no_words += 1
							elif(hashtag["text"].lower() in yes_keywords):
								number_of_yes_words += 1
							elif(hashtag["text"].lower() in neutral_keywords):
								number_of_none_words += 1
				if(userid in political_alignments):
					political_alignments[userid]['yes'] += number_of_yes_words
					political_alignments[userid]['no'] += number_of_no_words
					political_alignments[userid]['none'] += number_of_none_words
				else:
					user = {}
					user['yes'] = number_of_yes_words
					user['no'] = number_of_no_words
					user['none'] = number_of_none_words
					political_alignments[userid] = user									
		conn.close()
	except psycopg2.Error as e:
		print(e.pgerror)
		print("Conenction1 failed!")
	
	no_users = 0
	yes_users = 0
	neutral_users = 0
	yes_no_greater_than_neutral_users = 0
	neutral_greater_than_yes_no_users = 0
	yes_no_neutral_equal_users = 0	
	all_zeros = 0
	yes_no_greater_than_neutral_0 = 0
	yes_no_greater_than_neutral_not_0 = 0
	neutral_greater_than_yes_no_0 = 0
	neutral_greater_than_yes_no_not_0 = 0
	
	for userid in political_alignments:
		if(political_alignments[userid]['yes'] > political_alignments[userid]['no']):
			yes_users += 1
		elif(political_alignments[userid]['no'] > political_alignments[userid]['yes']):
			no_users += 1
		elif(political_alignments[userid]['yes'] == political_alignments[userid]['no']):
			neutral_users += 1
			if(political_alignments[userid]['yes'] > political_alignments[userid]['none']):
				yes_no_greater_than_neutral_users += 1
				if(political_alignments[userid]['none'] == 0):
					yes_no_greater_than_neutral_0 += 1
				else:
					yes_no_greater_than_neutral_not_0 += 1
			elif(political_alignments[userid]['yes'] < political_alignments[userid]['none']):
				neutral_greater_than_yes_no_users += 1
				if(political_alignments[userid]['yes'] == 0):
					neutral_greater_than_yes_no_0 += 1
				else:
					neutral_greater_than_yes_no_not_0 += 1
			elif(political_alignments[userid]['yes'] == political_alignments[userid]['none']):
				yes_no_neutral_equal_users += 1
				if(political_alignments[userid]['yes'] == 0):
					all_zeros += 1
	
	print("Users voting no: " + str(no_users))
	print("Users voting yes: " + str(yes_users))
	print("Neutral Users: " + str(neutral_users))
	print("Yes_No_Greater_Than_Neutral Users: " + str(yes_no_greater_than_neutral_users))
	print("Yes_No_Greater_Than_Neutral_0 Users: " + str(yes_no_greater_than_neutral_0))
	print("Yes_No_Greater_Than_Neutral_Not_0 Users: " + str(yes_no_greater_than_neutral_not_0))
	print("Neutral_Greater_Than_Yes_No Users: " + str(neutral_greater_than_yes_no_users))
	print("Neutral_Greater_Than_Yes_No_0 Users: " + str(neutral_greater_than_yes_no_0))
	print("Neutral_Greater_Than_Yes_No_Not_0 Users: " + str(neutral_greater_than_yes_no_not_0))
	print("Yes_No_Neutral_Equal Users: " + str(yes_no_neutral_equal_users))	
	print("All Zeros: " + str(all_zeros))
	






def find_political_alignment_of_users_by_tweet():	
	select_tweets = "SELECT tweetid, userid, hashtags FROM " + tweets_table
	select_users = "SELECT userid FROM " + users_table
	
	political_alignments = {}

	prepare_keywords()

	users = []
	counter = 0
	
	try:
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()
		cur.execute(select_users)
		rows = cur.fetchall()
		cur.close()
		conn.close()
		
	except psycopg2.Error as e:
		print(e.pgerror)
		print("Conenction2 failed!")

	for row in rows:
		userid = row[0]
		users.append(userid)
	
	try:
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()		
		cur.execute(select_tweets)
		rows = cur.fetchall()
		cur.close()

		for row in rows:			
			tweetid = row[0]
			userid = row[1]
			hashtags = row[2]

			# check whether the user is one of the processed users (don't considered the discarded users)
			if(userid in users):
				number_of_yes_words = 0
				number_of_no_words = 0
				number_of_none_words = 0

				if(hashtags != None):
					for hashtag in hashtags:
						if(hashtag != None):							
							if(hashtag["text"].lower() in no_keywords):
								number_of_no_words += 1
							elif(hashtag["text"].lower() in yes_keywords):
								number_of_yes_words += 1
							elif(hashtag["text"].lower() in neutral_keywords):
								number_of_none_words += 1
				
				if(userid in political_alignments):
					if(number_of_yes_words > number_of_no_words):
						political_alignments[userid]['yes'] += 1
					elif(number_of_no_words > number_of_yes_words):
						political_alignments[userid]['no'] += 1
					elif(number_of_yes_words == number_of_no_words):
						political_alignments[userid]['neutral'] += 1
						if(number_of_yes_words == 0 and number_of_none_words == 0):
							political_alignments[userid]['empty'] += 1
				else:
					user = {}
					user['yes'] = 0
					user['no'] = 0
					user['neutral'] = 0
					user['empty'] = 0
					
					if(number_of_yes_words > number_of_no_words):
						user['yes'] = 1
					elif(number_of_no_words > number_of_yes_words):
						user['no'] = 1
					elif(number_of_yes_words == number_of_no_words):
						user['neutral'] = 1
						if(number_of_yes_words == 0 and number_of_none_words == 0):
							user['empty'] = 1
					
					political_alignments[userid] = user
			
		conn.close()
	except psycopg2.Error as e:
		print(e.pgerror)
		print("Conenction1 failed!")
	
	no_users = 0
	yes_users = 0
	neutral_users = 0
	yes_no_greater_than_neutral_users = 0
	neutral_greater_than_yes_no_users = 0
	yes_no_neutral_equal_users = 0
	users_having_empty_tweets = 0
	empty_tweets = 0
	all_zeros = 0
	
	yes_no_greater_than_neutral_0 = 0
	yes_no_greater_than_neutral_not_0 = 0
	neutral_greater_than_yes_no_0 = 0
	neutral_greater_than_yes_no_not_0 = 0
	
	# vote_thr = 0.75
	for userid in political_alignments:
		# yess = political_alignments[userid]['yes']
		# nos = political_alignments[userid]['no']
		# neus = political_alignments[userid]['neutral']
		# totals = yess + nos
		# user_ratio = 0.0
		# if(totals != 0):
		# 	user_ratio = float(max(yess, nos)) / totals
		
		if(political_alignments[userid]['yes'] > political_alignments[userid]['no']):
			# if(user_ratio >= vote_thr):
			yes_users += 1
			politicalview = 'Positive'
		elif(political_alignments[userid]['no'] > political_alignments[userid]['yes']):
			# if(user_ratio >= vote_thr):
			no_users += 1
			politicalview = 'Negative'
		elif(political_alignments[userid]['yes'] == political_alignments[userid]['no']):
			neutral_users += 1
			politicalview = 'Neutral'	
			if(political_alignments[userid]['yes'] > political_alignments[userid]['neutral']):
				yes_no_greater_than_neutral_users += 1
				if(political_alignments[userid]['neutral'] == 0):
					yes_no_greater_than_neutral_0 += 1
				else:
					yes_no_greater_than_neutral_not_0 += 1
			elif(political_alignments[userid]['yes'] < political_alignments[userid]['neutral']):
				neutral_greater_than_yes_no_users += 1
				if(political_alignments[userid]['yes'] == 0):
					neutral_greater_than_yes_no_0 += 1
				else:
					neutral_greater_than_yes_no_not_0 += 1
			elif(political_alignments[userid]['yes'] == political_alignments[userid]['neutral']):
				yes_no_neutral_equal_users += 1
				if(political_alignments[userid]['yes'] == 0):
					all_zeros += 1
							
		if(political_alignments[userid]['empty'] > 0):
			empty_tweets += political_alignments[userid]['empty']
			users_having_empty_tweets += 1

					
		if politicalview == 'Positive' or politicalview == 'Negative':
			counter += 1
			print(str(counter) + ': ' + politicalview)
			try:
				conn = psycopg2.connect(conn_string)
				cur = conn.cursor()
				cur.execute("UPDATE " + users_table + " SET politicalview = %s WHERE userid = %s", (politicalview, userid, ))
				conn.commit()
				cur.close()
				conn.close()			
			except Exception as e:
				print("Update Userrole Error: " + str(e))
				pass
	
	print("Users voting no: " + str(no_users))
	print("Users voting yes: " + str(yes_users))
	print("Neutral Users: " + str(neutral_users))
	print("Yes_No_Greater_Than_Neutral Users: " + str(yes_no_greater_than_neutral_users))
	print("Yes_No_Greater_Than_Neutral_0 Users: " + str(yes_no_greater_than_neutral_0))
	print("Yes_No_Greater_Than_Neutral_Not_0 Users: " + str(yes_no_greater_than_neutral_not_0))
	print("Neutral_Greater_Than_Yes_No Users: " + str(neutral_greater_than_yes_no_users))
	print("Neutral_Greater_Than_Yes_No_0 Users: " + str(neutral_greater_than_yes_no_0))
	print("Neutral_Greater_Than_Yes_No_Not_0 Users: " + str(neutral_greater_than_yes_no_not_0))
	print("Yes_No_Neutral_Equal Users: " + str(yes_no_neutral_equal_users))
	print("Users having empty tweets: " + str(users_having_empty_tweets))
	print("Empty Tweets: " + str(empty_tweets))
	print("All Zeros: " + str(all_zeros))
	
	











# assess_alignment()
# find_political_alignment_by_user()
find_political_alignment_of_users_by_tweet()