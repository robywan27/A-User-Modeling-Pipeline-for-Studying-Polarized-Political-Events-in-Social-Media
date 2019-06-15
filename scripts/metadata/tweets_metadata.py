# coding=utf-8
import tweepy
import json
import psycopg2
import io
import time
from http.client import IncompleteRead
import sys
import csv
from math import ceil
import time

from tweepy import TweepError
from tweepy import RateLimitError

conn_string = "host='localhost' dbname='referendum_catalonia_db' user='postgres' password='########'"


CONSUMER_KEY = 'kHvNMDJhGKPkhi66P3XtqhkHI'
CONSUMER_SECRET = 'SZdo7R8JQ2KXQP5exwOdspNqN5GAgvraxj3ZQUutDQ6sbuaS1X'
ACCESS_TOKEN = '785229010531979264-OZjOEpdk3ecsEkyXekVGZJ24nVwOkj4'
ACCESS_TOKEN_SECRET = 'pIRRqtGsDlTGj2T8r7DOXQhd0cxpLroY4DhZ8vuhaxPIr'

CONSUMER_KEY2 = 'Wh3kQCeravcDqJ0i3IP4Q81dX'
CONSUMER_SECRET2 = 'zfWPaIWwPrhCNHvCBayXzbQVOQ4hIiEv9LGzZU2RxgHj6U0NQp'
ACCESS_TOKEN2 = '785229010531979264-7NAa3Jq90Zlzgqvxcmth69TBuW3MT49'
ACCESS_TOKEN_SECRET2 = 'x8JVhBnURwdj5qvkI64AlzlNsgSn92jBGItJ9s8q0F0k7'

CONSUMER_KEY3 = 'yMoy4FQU4m4Cmzte66imeI5f3'
CONSUMER_SECRET3 = 'ovesXl7vadgYEnN31iZMAvtkRRuUiFsbC9cHN8xlvkFQk1Si36'
ACCESS_TOKEN3 = '785229010531979264-dsRAp13EO2Vm5EJTeDdmhCF3aEXvYAM'
ACCESS_TOKEN_SECRET3 = 'vhk8UcNruuxlr3jUdFIlWt16pZ2NhynTlJaGkUVJtg58T'


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

auth2 = tweepy.OAuthHandler(CONSUMER_KEY2, CONSUMER_SECRET2)
auth2.set_access_token(ACCESS_TOKEN2, ACCESS_TOKEN_SECRET2)

auth3 = tweepy.OAuthHandler(CONSUMER_KEY3, CONSUMER_SECRET3)
auth3.set_access_token(ACCESS_TOKEN3, ACCESS_TOKEN_SECRET3)



apis = []
apis_len = 3

apis.append(tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True))
apis.append(tweepy.API(auth2, wait_on_rate_limit=True, wait_on_rate_limit_notify=True))
apis.append(tweepy.API(auth3, wait_on_rate_limit=True, wait_on_rate_limit_notify=True))



with open('/data/project/catalonia/tweetids_new.csv', encoding='utf8', mode='r') as tweetids_csv:
	reader = csv.reader(tweetids_csv)
	tweetids_list = list(reader)

tweetids = [int(tweetid[0]) for tweetid in tweetids_list]

iterations = ceil(len(tweetids) / 100)
json_string = []
c = 0
apikey_iter = 0
saved_i = 0

for i in range(c, iterations):
	try:
		statuses = apis[apikey_iter].statuses_lookup(tweetids[saved_i:saved_i+100], include_entities=True)
		saved_i += 100			
		c += 1		
		print(str(c))
		
		if c % 300 == 0:
			apikey_iter = (apikey_iter + 1) % apis_len

		for status in statuses:
			tweetid = -1
			tweet_created_at = None
			text = None
			in_reply_to_status_id = -1
			in_reply_to_user_id = -1
			in_reply_to_screen_name = None
			is_quote_status = None
			retweet_count = -1
			favorite_count = -1     
			lang = None
			quoted_status_id = -1
			source = None
			possibly_sensitive = None

			# the following attributes are the ones extracted from complex attributes
			latitude = None
			longitude = None
			hashtags = None
			urls = None
			user_mentions = None
			media = None
			place = None
			coordinates = None
			entities = None     
			user = None
			quoted_status = None

			userid = -1
			name = None
			screen_name = None
			location = None     
			description = None
			verified = None     
			followers_count = -1
			friends_count = -1
			listed_count = -1
			favourites_count = -1
			statuses_count = -1
			user_created_at = None      
			time_zone = None
			geo_enabled = None  
			user_lang = None

			utc_offset = -1
			profile_image_url = None
			url = None
			protected = None



			json_string = json.dumps(status._json)
			json_load = json.loads(json_string)

			if("id" in json_load):
				if(json_load["id"] != None):
					tweetid = json_load["id"]
			if("created_at" in json_load):
				if(json_load["created_at"] != None):
					tweet_created_at = json_load["created_at"]
			if("text" in json_load):
				if(json_load["text"] != None):
					text = json_load["text"]
			if("in_reply_to_status_id" in json_load):
				if(json_load["in_reply_to_status_id"] != None):
					in_reply_to_status_id = json_load["in_reply_to_status_id"]
			if("in_reply_to_user_id" in json_load):
				if(json_load["in_reply_to_user_id"] != None):
					in_reply_to_user_id = json_load["in_reply_to_user_id"]
			if("in_reply_to_screen_name" in json_load):
				if(json_load["in_reply_to_screen_name"] != None):
					in_reply_to_screen_name = json_load["in_reply_to_screen_name"]		
			if("is_quote_status" in json_load):
				if(json_load["is_quote_status"] != None):
					is_quote_status = json_load["is_quote_status"]					
			if("retweet_count" in json_load):
				if(json_load["retweet_count"] != None):
					retweet_count = json_load["retweet_count"]
			if("favorite_count" in json_load):
				if(json_load["favorite_count"] != None):
					favorite_count = json_load["favorite_count"]
			if("lang" in json_load):
				if(json_load["lang"] != None):
					lang = json_load["lang"]
			if("quoted_status_id" in json_load):
				if(json_load["quoted_status_id"] != None):
					quoted_status_id = json_load["quoted_status_id"]
			if("quoted_status" in json_load):
				if(json_load["quoted_status"] != None):
					quoted_status = json_load["quoted_status"]			
			if("source" in json_load):
				if(json_load["source"] != None):
					source = json_load["source"]
			if("possibly_sensitive" in json_load):
				if(json_load["possibly_sensitive"] != None):
					possibly_sensitive = json_load["possibly_sensitive"]
			if("coordinates" in json_load):
				if(json_load["coordinates"] != None):
					if("coordinates" in json_load["coordinates"]):
						if(json_load["coordinates"]["coordinates"] != None):
							coordinates = json_load["coordinates"]["coordinates"]
							longitude = coordinates[0]
							latitude = coordinates[1]
			if("place" in json_load):
				if(json_load["place"] != None):
					place = json_load["place"]					
			if("entities" in json_load):
				if(json_load["entities"] != None):
					if("hashtags" in json_load["entities"]):
						if(json_load["entities"]["hashtags"] != None):
							hashtags = json_load["entities"]["hashtags"]
					if("urls" in json_load["entities"]):
						if(json_load["entities"]["urls"] != None):
							urls = json_load["entities"]["urls"]
					if("user_mentions" in json_load["entities"]):
						if(json_load["entities"]["user_mentions"] != None):
							user_mentions = json_load["entities"]["user_mentions"]
					if("media" in json_load["entities"]):
						if(json_load["entities"]["media"] != None):
							media = json_load["entities"]["media"]			
			if("user" in json_load):
				if(json_load["user"] != None):
					user = json_load["user"]
					if("id" in user):
						if(user["id"] != None):
							userid = user["id"]
					if("name" in user):
						if(user["name"] != None):
							name = user["name"]
					if("screen_name" in user):
						if(user["screen_name"] != None):
							screen_name = user["screen_name"]				
					if("location" in user):
						if(user["location"] != None):
							location = user["location"]
					if("description" in user):
						if(user["description"] != None):
							description = user["description"]
					if("verified" in user):
						if(user["verified"] != None):
							verified = user["verified"]				
					if("followers_count" in user):
						if(user["followers_count"] != None):
							followers_count = user["followers_count"]
					if("friends_count" in user):
						if(user["friends_count"] != None):
							friends_count = user["friends_count"]
					if("listed_count" in user):
						if(user["listed_count"] != None):
							listed_count = user["listed_count"]
					if("favourites_count" in user):
						if(user["favourites_count"] != None):
							favourites_count = user["favourites_count"]
					if("statuses_count" in user):
						if(user["statuses_count"] != None):
							statuses_count = user["statuses_count"]				
					if("created_at" in user):
						if(user["created_at"] != None):
							user_created_at = user["created_at"]
					if("time_zone" in user):
						if(user["time_zone"] != None):
							time_zone = user["time_zone"]
					if("geo_enabled" in user):
						if(user["geo_enabled"] != None):
							geo_enabled = user["geo_enabled"]
					if("lang" in user):
						if(user["lang"] != None):
							user_lang = user["lang"]						
					if("utc_offset" in user):
						if(user["utc_offset"] != None):
							utc_offset = user["utc_offset"]
					if("profile_image_url" in user):
						if(user["profile_image_url"] != None):
							profile_image_url = user["profile_image_url"]
					if("url" in user):
						if(user["url"] != None):
							url = user["url"]
					if("protected" in user):
						if(user["protected"] != None):
							protected = user["protected"]

			
			try:                
				conn = psycopg2.connect(conn_string)
				conn.autocommit = True
				cur = conn.cursor()    			
				cur.execute("INSERT INTO users_new(userid, name, screen_name, location, description, verified, followers_count, friends_count, listed_count, favourites_count, statuses_count, created_at, time_zone, geo_enabled, utc_offset, lang, profile_image_url, url, protected) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (userid, name, screen_name, location, description, verified, followers_count, friends_count, listed_count, favourites_count, statuses_count, user_created_at, time_zone, geo_enabled, utc_offset, user_lang, profile_image_url, url, protected, ))
				cur.close()
			except psycopg2.IntegrityError:
				# print("Twitter User insertion failed: Integrity error!")
				# error_type = sys.exc_info()[0]
				# error_value = sys.exc_info()[1]
				# print('ERROR:', error_type, error_value)
				pass
			except ValueError:
				pass
			try:
				conn = psycopg2.connect(conn_string)
				conn.autocommit = True
				cur2 = conn.cursor()				
				cur2.execute("INSERT INTO tweets_temp(tweetid, userid, created_at, text, lang, in_reply_to_status_id, in_reply_to_user_id, in_reply_to_screen_name, retweet_count, favorite_count, source, possibly_sensitive, latitude, longitude, place, hashtags, urls, user_mentions, media, is_quote_status, quoted_status_id, quoted_status) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (tweetid, userid, tweet_created_at, text, lang, in_reply_to_status_id, in_reply_to_user_id, in_reply_to_screen_name, retweet_count, favorite_count, source, possibly_sensitive, latitude, longitude, json.dumps(place), json.dumps(hashtags), json.dumps(urls), json.dumps(user_mentions), json.dumps(media), is_quote_status, quoted_status_id, json.dumps(quoted_status), ))
				cur2.close()
			except Exception as e:
				# print("Twitter Data insertion failed! " + str(e))
				# error_type = sys.exc_info()[0]
				# error_value = sys.exc_info()[1]
				# print('ERROR:', error_type, error_value)
				pass
			except Exception as e:
				# print("Twitter User insertion failed! " + str(e))
				# error_type = sys.exc_info()[0]
				# error_value = sys.exc_info()[1]
				# print('ERROR:', error_type, error_value)
				pass
			else:
				try:
					cur2 = conn.cursor()				
					cur2.execute("INSERT INTO tweets_temp(tweetid, userid, created_at, text, lang, in_reply_to_status_id, in_reply_to_user_id, in_reply_to_screen_name, retweet_count, favorite_count, source, possibly_sensitive, latitude, longitude, place, hashtags, urls, user_mentions, media, is_quote_status, quoted_status_id, quoted_status) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (tweetid, userid, tweet_created_at, text, lang, in_reply_to_status_id, in_reply_to_user_id, in_reply_to_screen_name, retweet_count, favorite_count, source, possibly_sensitive, latitude, longitude, json.dumps(place), json.dumps(hashtags), json.dumps(urls), json.dumps(user_mentions), json.dumps(media), is_quote_status, quoted_status_id, json.dumps(quoted_status), ))
					cur2.close()
				except Exception as e:
					# print("Twitter Data insertion failed! " + str(e))
					# error_type = sys.exc_info()[0]
					# error_value = sys.exc_info()[1]
					# print('ERROR:', error_type, error_value)
					pass
			finally:
				conn.close()
	except TweepError as e:
		print('Error string: ' + e.response.text)	
		pass
	except RateLimitError:
		apikey_iter = (apikey_iter + 1) % apis_len
		pass