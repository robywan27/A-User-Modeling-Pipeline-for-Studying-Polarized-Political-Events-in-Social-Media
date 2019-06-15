import json
import psycopg2
import io
import requests
from collections import OrderedDict
from urllib.parse import urlencode
import urllib

from datetime import datetime
from dateutil import relativedelta
import math

import operator
import time
import sys

import googlemaps


conn_string = "host='localhost' dbname='referendum_catalonia_db' user='postgres' password='########'"


users_table = "processed_users"
tweets_table = "tweets_metadata"


username = []
username_length = 2

username.append("robywan")
username.append("robywan27")



def getLocationFromGeonamesForProfiles():	
	select_string = "SELECT userid, location FROM " + users_table + " WHERE home_city is null and location <> ''"
	
	try:
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()
		cur.execute(select_string)
		rows = cur.fetchall()

		iter = 0
		counter = 0
		
		for row in rows:			
			userid = row[0]
			location = row[1]							

			user = {}
			user["userid"] = userid			
			user["country_code"] = 'None'
			user["country_name"] = 'None'			
			user["place_name"] = 'None'			
			user["admin_name"] = 'None'			
			user["latitude"] = None
			user["longitude"] = None		
						
			if(location != None and location != "" and location != "null"):
				location = location.encode('utf-8')
								
				try:					
					data = OrderedDict([('username', username[counter]), ('q', location), ('maxRows', 1)])
					r = requests.get("http://api.geonames.org/searchJSON", params=urlencode(data))
					
					json_load = r.json()
					print(json_load)
					
					if("geonames" in json_load):
						if(json_load["geonames"] != 'None'):
							for item in json_load["geonames"]:								
								if("countryCode" in item):
									if(item["countryCode"] != 'None'):
										user["country_code"] = item["countryCode"]								
								if("countryName" in item):
									if(item["countryName"] != 'None'):
										user["country_name"] = item["countryName"]								
								if("name" in item):
									if(item["name"] != 'None'):
										user["place_name"] = item["name"]																						
								if("adminName1" in item):
									if(item["adminName1"] != 'None'):
										user["admin_name"] = item["adminName1"]																							
								if("lat" in item):
									if(item["lat"] != 'None'):
										user["latitude"] = item["lat"]									
								if("lng" in item):
									if(item["lng"] != 'None'):
										user["longitude"] = item["lng"]					
					print(iter)
					iter += 1

					if iter % 2000 == 0:
						counter = (counter + 1) % username_length
						username[counter]					
					
					try:
						conn2 = psycopg2.connect(conn_string)
						cur = conn2.cursor()
						
						# if (user["admin_name"] != None or user["country_name"] != None or user["country_code"] != None or user["place_name"] != None or user["latitude"] != None or user["longitude"] != None):							
						cur.execute("UPDATE " + users_table + " SET home_admin_level = %s, home_country = %s, home_cc = %s, home_city = %s, latitude = %s, longitude = %s WHERE userid = %s", (user["admin_name"], user["country_name"], user["country_code"], user["place_name"], user["latitude"], user["longitude"], userid, ))							
						conn2.commit()
						cur.close()
					except Exception as e:
						print("Update Location Error: " + str(e))
				except Exception as e:
					print("Error: " + str(e))
					pass				
				
	except psycopg2.Error as e:
		print(e.pgerror)
		print("Connection failed")	






		


def getLocationFromGeolocatedTweets():	
	tmp_results = {}
		
	select_string = "SELECT t.userid, t.tweetid, t.place FROM " + tweets_table + " as t JOIN " + users_table + " as u ON t.userid = u.userid WHERE place <> 'null'"
	try:
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()
		cur.execute(select_string)
		rows = cur.fetchall()
		
		for row in rows:
			userid = row[0]
			tweet_id = row[1]
			place = row[2]			
	
			if(userid in tmp_results):
				tmp_results[userid]["places"].append(place)
			else:
				tmp_results[userid] = {}
				tmp_results[userid]["places"] = []
				tmp_results[userid]["places"].append(place)		
		
		iter = 0
		counter = 0

		for userid in tmp_results:
			user = {}
			user["userid"] = userid
			user["country"] = None			
			user["name"] = None
			user["full_name"] = None
			user["country_code"] = None			
			user["admin_name"] = None			
			user["latitude"] = None
			user["longitude"] = None

			tmp_places = {}
			
			for place in tmp_results[userid]["places"]:
				place_id = place["id"]
				if(place_id in tmp_places):
					tmp_places[place_id] += 1
				else:
					tmp_places[place_id] = 1
			# print("tmp_places " + str(tmp_places))
			most_tweeted_place_id = max(tmp_places.items(), key=operator.itemgetter(1))[0]
			# print("most_tweeted_place_id " + most_tweeted_place_id)
			
			for place in tmp_results[userid]["places"]:				
				if(place["id"] == most_tweeted_place_id):																
					if(place["country"] != None):
						user["country"] = place["country"]									
					if(place["name"] != None):
						user["name"] = place["name"]									
					if(place["full_name"] != None):
						user["full_name"] = place["full_name"]									
					if(place["country_code"] != None):
						user["country_code"] = place["country_code"]									
					if(place["bounding_box"] != None):						
						if place["bounding_box"]["coordinates"] != None:
							if place["bounding_box"]["coordinates"][0] != None:
								first_longitude = place["bounding_box"]["coordinates"][0][0][0]								
								second_longitude = place["bounding_box"]["coordinates"][0][1][0]
								first_latitude = place["bounding_box"]["coordinates"][0][0][1]
								fourth_latitude = place["bounding_box"]["coordinates"][0][3][1]								

								# find the center of the bounding box
								user["longitude"] = (first_longitude + second_longitude) / 2
								user["latitude"] = (first_latitude + fourth_latitude) / 2
					
					break			
				
			try:				
				conn2 = psycopg2.connect(conn_string)
				cur = conn2.cursor()
				cur.execute("SELECT location FROM " + users_table + " WHERE userid = %s", (userid, ))
				user_row = cur.fetchone()
				
				if (user_row != None):
					location = user_row[0].encode('utf-8')							
					
					try:
						data = OrderedDict([('username', username[counter]), ('q', location), ('maxRows', 1)])				
						r = requests.get("http://api.geonames.org/searchJSON", params=urlencode(data))
						
						json_load = r.json()
						# print(json_load)
						
						if("geonames" in json_load):
							if(json_load["geonames"] != None):
								for item in json_load["geonames"]:
									if("adminName1" in item):
										if(item["adminName1"] != None):
											user["admin_name"] = item["adminName1"]									
					except Exception as e:
						print("Error in geonames: " + str(e))				
						pass

					print(iter)
					iter += 1

					if iter % 2000 == 0:
						username[counter + 1]
						
					try:
						conn3 = psycopg2.connect(conn_string)
						cur = conn3.cursor()						
						if (user["country"] != None or user["name"] != None or user["full_name"] != None or user["country_code"] != None or user["admin_name"] != None or user["longitude"] != None or user["latitude"] != None):								
							cur.execute("UPDATE " + users_table + " SET home_admin_level = %s, home_country = %s, home_cc = %s, home_city = %s, home_city_full = %s, latitude = %s, longitude = %s WHERE userid = %s", (user["admin_name"], user["country"], user["country_code"], user["name"], user["full_name"], user["latitude"], user["longitude"], user["userid"], ))							
							print(user)
						conn3.commit()
						cur.close()
						conn3.close()
					except Exception as e:
						print("Update Location Error: " + str(e))							
				cur.close()
				conn2.close()
			except Exception as e:
				print("Update Location Error: " + str(e))
		
		cur.close()
		conn.close()
	except psycopg2.Error as e:
		print("Database Error: " + e.pgerror)	






	







# getLocationFromGeonamesForProfiles()
getLocationFromGeolocatedTweets()