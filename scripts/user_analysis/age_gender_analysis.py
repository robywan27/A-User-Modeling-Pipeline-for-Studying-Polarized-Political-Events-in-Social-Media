import tweepy
import json
import psycopg2
import time
import datetime
import requests
import smtplib
# import pycountry


# Notes:
# -2 (age) -> first elimination
# -1 (age) -> no age
# und (gender) -> no gender


conn_string = "host='localhost' dbname='referendum_catalonia_db' user='postgres' password='########'"



limit = 50
error_sleep_time = 10
initial_threshold = 0.4
api_keys_length = 2
table = "processed_users"




# given a user first name, return the gender and the probability associated to him
def get_demographics_by_genderize_with_lang_and_country(firstname, language, country):
	result = {}
	result["gender"] = None
	result["probability"] = None
	result["firstname"] = firstname
	result["wait"] = 0
	result["reset"] = None
	limit_count = None
	reset_time = None
	
	try:		
		api_params_string = "https://api.genderize.io/?" + "name=" + firstname		
		# if country != None:
		# 	country_code = countries.get(name=country).alpha_2
		# 	api_params_string += "&country_id=" + country_code
		if language != None:
			try:						
				api_params_string += "&language_id=" + language				
			except Exception as e:
				print("Exception, language not found: " + str(e))			
		
		r = requests.get(api_params_string)
		
		if("X-Rate-Limit-Remaining" in r.headers):
			if(r.headers["X-Rate-Limit-Remaining"] != None):
				limit_count = int(r.headers["X-Rate-Limit-Remaining"])
		if("X-Rate-Reset" in r.headers):
			if(r.headers["X-Rate-Reset"] != None):
				reset_time = int(r.headers["X-Rate-Reset"])
		
		if limit_count % 100 == 0:
			print("================================")
			print("Limit count: " + limit_count)
			print("================================")

		if(limit_count != None):
			if(limit_count <= 2):
				if(reset_time != None):
					print(reset_time)
					result["wait"] = 1
					result["reset"] = reset_time + 5
					#time.sleep(reset_time + 5)

		json_load = r.json()
		
		if("gender" in json_load):
			if(json_load["gender"] != None):
				result["gender"] = json_load["gender"]
				print("User: " + result["name"])
				print(result["gender"])
				if("probability" in json_load):
					if(json_load["probability"] != None):
						result["probability"] = json_load["probability"]
						print(result["probability"])
	except Exception as e:
		# print("Could not find gender for name: " + firstname)
		# print(str(e))
		pass
	# print(result)
	return result




def get_demographics_by_genderize(firstname):
	result = {}
	result["gender"] = None
	result["probability"] = None
	result["firstname"] = firstname

	try:
		api_params_string = "https://api.genderize.io/?" + "name=" + firstname		
		r = requests.get(api_params_string)

		print(r.headers["X-Rate-Reset"])
		
		if("X-Rate-Limit-Remaining" in r.headers):
			if(r.headers["X-Rate-Limit-Remaining"] != None):
				limit_count = int(r.headers["X-Rate-Limit-Remaining"])
		if("X-Rate-Reset" in r.headers):
			if(r.headers["X-Rate-Reset"] != None):
				reset_time = int(r.headers["X-Rate-Reset"])		
		
		if(limit_count != None):
			if(limit_count <= 2):
				if(reset_time != None):
					print(reset_time)
					result["wait"] = 1
					result["reset"] = reset_time + 5
					time.sleep(reset_time + 5)

		genderize_result = r.json()
		print(genderize_result)

		if("gender" in genderize_result):			
			if(genderize_result["gender"] != None):				
				result["gender"] = genderize_result["gender"]				
				print("User: " + result["name"])
				print(result["gender"])
				if("probability" in genderize_result):
					if(genderize_result["probability"] != None):
						result["probability"] = genderize_result["probability"]
						print(result["probability"])
	except requests.exceptions.RequestException as e:
		print(e)
	except Exception as e:
		# print("Could not find gender for name: " + firstname)
		# print(r.headers["X-Rate-Limit-Remaining"])
		# print(r.headers["X-Rate-Reset"])
		print(e)
		pass
	# print(result)
	return result




# given the profile image of a user, return for gender analysis the gender and probability; for age analysis the age and age range
def get_demographics_by_face_plus_plus(image_url, face_mod):
	result = {}
	result["gender"] = None
	result["probability"] = None
	result["age"] = None
	result["range"] = None
	result["ethnicity"] = None
	
	try:
		profile_image_url = image_url.replace('_normal', '')		
		if face_mod % 3 == 0:
			params = (('api_key','siYo2RbQxzFqc2CwcXHnRBHhYkUzP9YA'),('api_secret','zwBkEZyiYROBAb9U0C-KKHHGw9jTbs24'), ('image_url', profile_image_url), ('return_attributes', 'gender,age,ethnicity'))
		elif face_mod % 3 == 1:
			params = (('api_key','SOlrvAZvZY6kshRt0PKULNk_pkwYeuTS'),('api_secret','ayhD9D0uIvy6-BJhYhlUIwyUKoOavmIq'), ('image_url', profile_image_url), ('return_attributes', 'gender,age,ethnicity'))
		elif face_mod % 3 == 2:
			params = (('api_key','4a67vaUhnL9rz2wkaksBPgmZmt0Dv2js'),('api_secret','u3DQETlVcxZnaWpAppIYHOVh66ESxIw'), ('image_url', profile_image_url), ('return_attributes', 'gender,age,ethnicity'))
		
		r = requests.post("https://api-us.faceplusplus.com/facepp/v3/detect", params)
		json_load = r.json()
		print(json_load)
				
		if("faces" in json_load):
			if(json_load["faces"] != None):
				#if only 1 face in the profile pic
				if(len(json_load["faces"]) == 1):
					if(json_load["faces"][0] != None):
						if("attributes" in json_load["faces"][0]):
							if(json_load["faces"][0]["attributes"] != None):
								if("gender" in json_load["faces"][0]["attributes"]):
									if(json_load["faces"][0]["attributes"]["gender"] != None):
										if("value" in json_load["faces"][0]["attributes"]["gender"]):
											if(json_load["faces"][0]["attributes"]["gender"]["value"] != None):
												result["gender"] = json_load["faces"][0]["attributes"]["gender"]["value"]										
								if("age" in json_load["faces"][0]["attributes"]):
									if(json_load["faces"][0]["attributes"]["age"] != None):
										if("value" in json_load["faces"][0]["attributes"]["age"]):
											if(json_load["faces"][0]["attributes"]["age"]["value"] != None):
												result["age"] = json_load["faces"][0]["attributes"]["age"]["value"]																
		# time.sleep(1)
	except Exception as e:
		print(str(e))
		print("Error with Face++ for name: " + name)
		pass
	# print(result)
	return result



# select users from the db whose gender is unknown and update it with the gender and age information
def find_age_and_gender_faceplusplus():
	try:
		select_string = "SELECT userid, name, profile_image_url FROM " + table
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()
		cur.execute(select_string)
		rows = cur.fetchall()

		face_mod = 0
		iter = 0	
		
		for row in rows:
			userid = row[0]
			username = row[1]
			image_url = row[2]			

			gender = None
			probability = None
			age = None
			ethnicity = None
			
			if username.find(' ') != -1:	# if the name contains one space		
				input_name = username.split(' ')[0]
			else:
				input_name = username			
			
			wait = None
			reset = None
			print("Counter at: " + str(iter))
			print("face_mod: " + str(face_mod))	
			
								
			try:										
				face_result = get_demographics_by_face_plus_plus(image_url, face_mod)						

				if(face_result["gender"] != None):
					gender = face_result["gender"].title()						
				else:
					gender = "und"													
				
				if(face_result["age"] != None):
					age = face_result["age"]
				else:
					age = -1			
			except Exception as e:
				# print("Error occurred in find_age_and_gender: " + str(e))
				pass
				
			
			try:
				conn2 = psycopg2.connect(conn_string)
				cur2 = conn2.cursor()
				# print('gender: ' + str(gender) + '; age: ' + str(age) + '; ethnicity: ' + str(ethnicity))
				cur2.execute("UPDATE " + table + " SET age = %s, gender WHERE userid = %s", (age, gender, userid, ))				
				conn2.commit()
				cur2.close()
			except psycopg2.Error as e:
				print(e.pgerror)		
				pass
			except Exception as e:
				print("Update gender, age, ethnicity Error: " + str(e))
				pass
			conn2.close()

			# if(wait == 1):
			# 	if(reset != None):
			# 		time_to_sleep = reset
			# 		time.sleep(time_to_sleep)
			
			iter += 1
			if iter % 5000 == 0:
				face_mod = (face_mod + 1) % api_keys_length

		conn.close()
	except psycopg2.Error as e:
		print(e.pgerror)		
		pass
	# except Exception as e:
	# 	print("Error occurred in find_age_and_gender: " + str(e))
	# 	pass



# select users from the db whose gender is unknown and update it with the gender and age information
def find_age_and_gender_genderize():
	try:		
		select_string = "SELECT userid, name, lang FROM " + table + " WHERE gender = 'und' OR gender = 'None' OR gender is null"
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()
		cur.execute(select_string)
		rows = cur.fetchall()	

		face_mod = 0
		iter = 0	

		for row in rows:
			userid = row[0]
			username = row[1]			
			lang = row[2]			

			gender = None
			probability = None			
			
			if username.find(' ') != -1:	# if the name contains one space		
				input_name = username.split(' ')[0]
			else:
				input_name = username			
			
			wait = None
			reset = None
			print(iter)			
						
			try:				
				genderize_result = get_demographics_by_genderize(input_name)
				
				if(genderize_result["gender"] != None):
					gender = genderize_result["gender"].title()					
				else:
					gender = "undefined"				
									
				wait = genderize_result["wait"]
				reset = genderize_result["reset"]
			except Exception as e:
				# print("Error occurred in find_age_and_gender: " + str(e))
				pass			
			
			try:
				conn2 = psycopg2.connect(conn_string)
				cur2 = conn2.cursor()
				print('gender: ' + str(gender))
				cur2.execute("UPDATE " + table + " SET gender = %s WHERE userid = %s", (gender, userid, ))				
				conn2.commit()
				cur2.close()
			except psycopg2.Error as e:
				print(e.pgerror)		
				pass
			except Exception as e:
				print("Update gender, Error: " + str(e))
				pass
			conn2.close()

			if(wait == 1):
				if(reset != None):
					time_to_sleep = reset
					time.sleep(time_to_sleep)
			
			iter += 1			

		conn.close()
	except psycopg2.Error as e:
		print(e.pgerror)		
		pass
	except Exception as e:
		print("Error occurred in find_age_and_gender: " + str(e))
		pass






find_age_and_gender_faceplusplus()
# find_age_and_gender_genderize()