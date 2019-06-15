import tweepy
import json
import psycopg2
import botometer
import time
import datetime



# Notes:
# -3 -> first elimination
# -2 -> could not evaluate
# -1 -> no return


conn_string = "host='localhost' dbname='referendum_lombardia_db' user='postgres' password='########'"

table = "users_new"


# with v2 of the API a Mashape key is needed too

mashape_key = "VCkbrT1IBXmshreqfpZcFa9dEzY0p1vJ5FCjsnMDvLOsN5fPBD"
mashape_key2 = "DKUYjwNDdwmshM0cH6XOtK5rA5NXp1dVWZujsncgAxs7doiM2K"
mashape_key3 = "He8vlLdmMfmshAc282nS5AnF6yXMp1y9upojsn4BzakXAOFTcy"



twitter_app_auth = {
    'consumer_key': 'kHvNMDJhGKPkhi66P3XtqhkHI',
    'consumer_secret': 'SZdo7R8JQ2KXQP5exwOdspNqN5GAgvraxj3ZQUutDQ6sbuaS1X',
    'access_token': '785229010531979264-OZjOEpdk3ecsEkyXekVGZJ24nVwOkj4',
    'access_token_secret': 'pIRRqtGsDlTGj2T8r7DOXQhd0cxpLroY4DhZ8vuhaxPIr',
}

twitter_app_auth2 = {
    'consumer_key': 'Wh3kQCeravcDqJ0i3IP4Q81dX',
    'consumer_secret': 'zfWPaIWwPrhCNHvCBayXzbQVOQ4hIiEv9LGzZU2RxgHj6U0NQp',
    'access_token': '785229010531979264-7NAa3Jq90Zlzgqvxcmth69TBuW3MT49',
    'access_token_secret': 'x8JVhBnURwdj5qvkI64AlzlNsgSn92jBGItJ9s8q0F0k7',
}

twitter_app_auth3 = {
    'consumer_key': 'yMoy4FQU4m4Cmzte66imeI5f3',
    'consumer_secret': 'ovesXl7vadgYEnN31iZMAvtkRRuUiFsbC9cHN8xlvkFQk1Si36',
    'access_token': '785229010531979264-dsRAp13EO2Vm5EJTeDdmhCF3aEXvYAM',
    'access_token_secret': 'vhk8UcNruuxlr3jUdFIlWt16pZ2NhynTlJaGkUVJtg58T',    
}


bon = []
bon_length = 3

bon.append(botometer.Botometer(wait_on_ratelimit=True, mashape_key=mashape_key, **twitter_app_auth))
bon.append(botometer.Botometer(wait_on_ratelimit=True, mashape_key=mashape_key2, **twitter_app_auth2))
bon.append(botometer.Botometer(wait_on_ratelimit=True, mashape_key=mashape_key3, **twitter_app_auth3))




# error_sleep_time = 10
bot_threshold = 0.4
# limit = 10000
botometer_window_rate_limit = 180
botometer_api_rate_limit = 17280
day_in_seconds = 86400
iter = 0

# initialized to 1 to avoid modulo 0 later
processed_users = [[1 for i in range(1)] for j in range(bon_length)]
rate_limits = [[1 for i in range(1)] for j in range(bon_length)]
wait_times = [[1 for i in range(1)] for j in range(bon_length)]


# select the users whose bot avg score is null and calculate:
#	the overall score of all 6 categories (Network, User, Friend, Temporal, Content, Sentiment)
#	the average score of the 4 language-independent categories (Network, User, Friend, Temporal)
# finally, the overall and avg scores are updated in the db
def detect_bot():
	try:		
		select_string = "SELECT userid, screen_name, friends_count, followers_count, statuses_count FROM " + table + " WHERE botaverage IS NULL AND userrole <> 'Outlier'"
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()
		# cur.execute(select_string, (limit, ))
		cur.execute(select_string)
		rows = cur.fetchall()

		counter = 0	
		global iter	

		# check, for all API keys, if any one of those which reached the api limit of 17280 has to be unblocked again: 
			# calculate if it elapsed at least 1 day from the moment it was unlocked:
				# reset the waiting time and reset the rate limit for that key
		for row in rows:			
			if counter % 1000 == 0:
				for i in range(bon_length):
					if wait_times[i][0] != 0:
						if time.time() - wait_times[i][0] >= day_in_seconds:
							wait_times[i][0] = 0
							rate_limits[i][0] = 0
			if counter % 10 == 0:
				print("Counter at: " + str(counter))
				print("Iter at: " + str(iter))
				print("Number of calls made: " + str(processed_users[iter][0]))
				print("Rate limit reached?: " + str(rate_limits[iter][0]))
				print("Waiting time: " + str(wait_times[iter][0]))	
			# check if the current API key has reached the api limit of 17280:
				# activate the flag rate limit (value = 1), set the waiting time to the current time, reset the number of API calls done for the current key
			if processed_users[iter][0] % botometer_api_rate_limit == 0:							
				rate_limits[iter][0] = 1
				wait_times[iter][0] = time.time()
				processed_users[iter][0] = 0
			# check if the current API key has reached the window limit of 180 calls per 15 minute window:
				# scan the keys and set the current API key to the one which did not reache the api limit yet
				# if no key is available, terminate the execution of the program
			if processed_users[iter][0] % botometer_window_rate_limit == 0:
				no_key_available = 1
				for i in range(bon_length):
					iter = (iter + 1) % bon_length
					if rate_limits[iter][0] == 0:
						no_key_available = 0
						break
				if no_key_available == 1:					
					print("Exiting program")
					exit()

			counter += 1

			# if counter % 5 == 0:
			# 	iter = (iter + 1) % bon_length

			userid = row[0]
			screen_name = row[1]
			friends_count = row[2]
			followers_count = row[3]
			statuses_count = row[4]
			bot_score_avg = -1
			bot_score_actual = -1

			# Some characteristics of bots		
			if(followers_count != 0 and statuses_count >= 100 and statuses_count <= 75000):
				# Bots are most likely those users with a greater number of followers than followees.
				# if the proportion of followers is at least 40% of the total of followers + followees, ie, there is a high number of followers
				if(followers_count != 0 and (float(followers_count) / (followers_count + friends_count)) >= bot_threshold):
					result = get_bot_score(userid, screen_name)
					bot_score_avg = result["average"]
					bot_score_actual = result["actual"]						
				else:
					bot_score_avg = -3
					bot_score_actual = -3				
			else:
				bot_score_avg = -3
				bot_score_actual = -3

			print(bot_score_avg)
			
			try:
				conn2 = psycopg2.connect(conn_string)
				cur2 = conn2.cursor()
				cur2.execute("UPDATE " + table + " SET botaverage = %s, botactual = %s WHERE userid = %s", (int(bot_score_avg), int(bot_score_actual), userid, ))
				conn2.commit()
				cur2.close()
			except Exception as e:
				print("Error in Update bot score: " + str(e))
				pass
			except psycopg2.Error as e:
				print(e.pgerror)		
				pass
			conn2.close()
		conn.close()	
	except Exception as e:
		print("Error occurred in Botometer: " + str(e))
		# time.sleep(error_sleep_time)
		pass
	# except KeyboardInterrupt:
				



# given a user, the overall and average bot scores are returned
def get_bot_score(userid, screen_name):
	result = {}
	result["actual"] = -1
	result["average"] = -1

	try:
		# api_result = bon.check_account(screen_name)
		api_result = bon[iter].check_account(screen_name)
		print(api_result)
		
		if("user" in api_result):
			if(api_result["user"] != None):
				if ("id_str" in api_result["user"]):
					if (api_result["user"]["id_str"] != None):
						if(str(userid) == api_result["user"]["id_str"]):
							# get the scores
							if("scores" in api_result):
								if (api_result["scores"] != None):
									# get the overall score (omitting sentiment and content features, both of which are English-specific) and make it a number in the interval (0, 100)
									if(api_result["scores"]["universal"] != None):
										result["average"] = api_result["scores"]["universal"] * 100
									# get the overall score (considering all 6 categories) and make it a number in the interval (0, 100)
									if(api_result["scores"]["english"] != None):
										result["actual"] = api_result["scores"]["english"] * 100
									processed_users[iter][0] += 1											
		elif("reset" in api_result):
			if(api_result["reset"] != None):
				print(api_result["reset"])
		else:
			result["average"] = -2
			result["actual"] = -2		
	except:
		print("Bot Error for screen_name: " + screen_name + " with userid: " + str(userid))
		pass
	
	return result









detect_bot()