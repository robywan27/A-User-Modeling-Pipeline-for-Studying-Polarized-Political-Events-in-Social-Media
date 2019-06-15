import tweepy
import json
import psycopg2
import requests
import smtplib
import time



# Notes:
# None -> first elimination
# und -> no ethnicity 



conn_string = "host='localhost' dbname='referendum_catalonia_db' user='postgres' password='########'"


limit = 1000
error_sleep_time = 10
initial_threshold = 0.4
table = "users_ethnicity12"



# make an API call to textmap classifier passing the name of the user to analyze
def get_ethnicity_with_textmap(name):
	result = None
	try:
		r = requests.post("http://www.textmap.com/ethnicity_api/api", json={"names": [name]})
		a = r.json()
		# list(dict) returns a list of the keys in the dictionary
		a_len = len(a[list(a)[0]]) - 1		
		result = str(a[list(a)[0]][a_len]['best'])
		print(result)
	except Exception as e:
		print("Could not find ethnicity for name: " + name)
		pass
	return result



# retrieve a user whose ethnicity is unknown from the db, get the ethnicity, update the ethnicity value in the db for that user
def get_ethnicity():
	try:
		select_string = "SELECT userid, name FROM " + table + " WHERE ethnicity IS NULL OR ethnicity = 'und' OR ethnicity = 'None'"
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()		
		cur.execute(select_string, (limit, ))
		rows = cur.fetchall()
		count = 0

		for row in rows:
			userid = row[0]
			name = row[1]
			
			ethnicity = None

			# if(followers_count != 0 and statuses_count >= 100 and statuses_count <= 75000):
			# 	if((float (followers_count) / (followers_count + friends_count)) >= initial_threshold):
			count += 1
			print(count)
			ethnicity = get_ethnicity_with_textmap(name)
			# 	else:
			# 		ethnicity = "None"
			# else:
			# 	ethnicity = "None"
			if(ethnicity == None or ethnicity == ""):
				ethnicity = "undefined"
			
			try:
				conn2 = psycopg2.connect(conn_string)
				cur2 = conn2.cursor()
				print(ethnicity)
				cur2.execute("UPDATE " + table + " SET ethnicity = %s WHERE userid = %s", (ethnicity, userid, ))
				conn2.commit()
				cur2.close()				
			except Exception as e:
				print("Update Ethnicity Error: " + str(e))
				pass
			except psycopg2.Error as e:
				print(e.pgerror)
				print("Main Connection failed")
				pass
			conn2.close()
		conn.close()
	except Exception as e:
		print(e)
		pass








get_ethnicity()