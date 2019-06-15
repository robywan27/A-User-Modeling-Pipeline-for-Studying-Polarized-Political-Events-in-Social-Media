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
import oauth2 as oauth

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



with open('/data/project/catalonia/userids_new.csv', encoding='utf8', mode='r') as userids_csv:
	reader = csv.reader(userids_csv)
	userids_list = list(reader)

userids = [int(userid[0]) for userid in userids_list]

iterations = len(userids)
c = 0
apikey_iter = 37


for i in range(c, iterations):	
	try:		
		user = apis[apikey_iter].get_user(userids[c])		

		c += 1
		print(str(c))

		if c % 300 == 0:
			apikey_iter = (apikey_iter + 1) % apis_len			
				
		userid = -1
		utc_offset = -1
		profile_image_url = None
		url = None
		protected = None

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
		created_at = None      
		time_zone = None
		geo_enabled = None  
		lang = None
		
		json_string = json.dumps(user._json)
		json_load = json.loads(json_string)

		if("id" in json_load):
			if(json_load["id"] != None):
				userid = json_load["id"]				
		if("utc_offset" in json_load):
			if(json_load["utc_offset"] != None):
				utc_offset = json_load["utc_offset"]
		if("profile_image_url" in json_load):
			if(json_load["profile_image_url"] != None):
				profile_image_url = json_load["profile_image_url"]
		if("url" in json_load):
			if(json_load["url"] != None):
				url = json_load["url"]
		if("protected" in json_load):
			if(json_load["protected"] != None):
				protected = json_load["protected"]

		if("name" in json_load):
			if(json_load["name"] != None):
				name = json_load["name"]
		if("screen_name" in json_load):
			if(json_load["screen_name"] != None):
				screen_name = json_load["screen_name"]				
		if("location" in json_load):
			if(json_load["location"] != None):
				location = json_load["location"]
		if("description" in json_load):
			if(json_load["description"] != None):
				description = json_load["description"]
		if("verified" in json_load):
			if(json_load["verified"] != None):
				verified = json_load["verified"]				
		if("followers_count" in json_load):
			if(json_load["followers_count"] != None):
				followers_count = json_load["followers_count"]
		if("friends_count" in json_load):
			if(json_load["friends_count"] != None):
				friends_count = json_load["friends_count"]
		if("listed_count" in json_load):
			if(json_load["listed_count"] != None):
				listed_count = json_load["listed_count"]
		if("favourites_count" in json_load):
			if(json_load["favourites_count"] != None):
				favourites_count = json_load["favourites_count"]
		if("statuses_count" in json_load):
			if(json_load["statuses_count"] != None):
				statuses_count = json_load["statuses_count"]				
		if("created_at" in json_load):
			if(json_load["created_at"] != None):
				created_at = json_load["created_at"]
		if("time_zone" in json_load):
			if(json_load["time_zone"] != None):
				time_zone = json_load["time_zone"]
		if("geo_enabled" in json_load):
			if(json_load["geo_enabled"] != None):
				geo_enabled = json_load["geo_enabled"]
		if("lang" in json_load):
			if(json_load["lang"] != None):
				lang = json_load["lang"]
		
		try:                
			conn = psycopg2.connect(conn_string)
			conn.autocommit = True
			cur = conn.cursor()    			
			cur.execute("INSERT INTO users_metadata(userid, name, screen_name, location, description, verified, followers_count, friends_count, listed_count, favourites_count, statuses_count, created_at, time_zone, geo_enabled, lang, utc_offset, profile_image_url, url, protected) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (userid, name, screen_name, location, description, verified, followers_count, friends_count, listed_count, favourites_count, statuses_count, created_at, time_zone, geo_enabled, lang, utc_offset, profile_image_url, url, protected, ))
			cur.close()			
		except Exception as e:
			print("Twitter User insertion failed! " + str(e))
			error_type = sys.exc_info()[0]
			error_value = sys.exc_info()[1]
			print('ERROR:', error_type, error_value)
			pass			
		finally:
			conn.close()
	except TweepError as e:
		print('Error string: ' + e.response.text)	
		pass
	except RateLimitError:
		apikey_iter = (apikey_iter + 1) % clients_len
		pass