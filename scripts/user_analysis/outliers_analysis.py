import json
import psycopg2
import numpy as np
from datetime import datetime
from dateutil import relativedelta




conn_string = "host='localhost' dbname='referendum_catalonia_db' user='postgres' password='########'"



table = "users_metadata"
upper_bound = 0




# Normalized Statuses Count = #Statuses Count / Account Age (months)
# then apply inter-quartile measure to detect outliers
def find_outliers():
	global upper_bound

	select_string = "SELECT userid, statuses_count, created_at FROM " + table
	statuses = None
	users = []

	try:		
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()
		cur.execute(select_string)
		rows = cur.fetchall()

		statuses = np.empty(shape = (len(rows)))
		counter = 0		
		cur_date = datetime.strptime("2017-10-10 23:59:59", "%Y-%m-%d %H:%M:%S")

		for row in rows:
			userid = row[0]
			statuses_count = row[1]					
			created_at =  row[2].replace(tzinfo = None)
			r = relativedelta.relativedelta(cur_date, created_at)
			denominator = (int(r.years) * 12) + (int(r.months))			
			
			if(denominator == 0):
				denominator = 1

			statuses[counter] = float(statuses_count) / denominator			
			user = {}
			user['id'] = userid
			user['statuses_count'] = statuses_count
			user['denominator'] = denominator
			users.append(user)
			counter += 1
	except psycopg2.Error as e:
		print(e.pgerror)
		print("Connection failed")

	stream = {}
	q1 = np.percentile(statuses, 25)
	q2 = np.percentile(statuses, 50)
	q3 = np.percentile(statuses, 75)
	iqr = q3 - q1
	lb = q1 - 1.5*iqr
	ub = q3 + 1.5*iqr	
	stream['q1'] = q1
	stream['q2'] = q2
	stream['q3'] = q3
	stream['iqr'] = iqr
	stream['lb'] = lb
	stream['ub'] = ub    
	
	upper_bound = ub

	print(stream)
	print("Total number of users: " + str(len(users)))
	number_of_valid_users = 0
	for user in users:
		if(float(user['statuses_count']) / user['denominator'] <= ub):
			number_of_valid_users += 1
	print("Number of valid users: " + str(number_of_valid_users))	






def update_outliers():
	users = {}
	select_string = "SELECT userid, statuses_count, created_at FROM " + table
	cur_date = datetime.strptime("2017-10-10 23:59:59", "%Y-%m-%d %H:%M:%S")
	# counter = 0
	
	try:
		conn = psycopg2.connect(conn_string)
		cur = conn.cursor()
		cur.execute(select_string)
		rows = cur.fetchall()
		
		for row in rows:
			userid = row[0]
			statuses_count = row[1]
			created_at =  row[2].replace(tzinfo = None)
			
			r = relativedelta.relativedelta(cur_date, created_at)
			denominator = (int(r.years) * 12) + (int(r.months))
			if(denominator == 0):
				denominator = 1
			users[userid] = float(statuses_count) / denominator
	except psycopg2.Error as e:
		print(e.pgerror)		
	
	outlier_thr = upper_bound
	
	for key in users:
		userid = int(key)
		userrole = None
		
		if(users[key] < outlier_thr):
			userrole = "Normal"
		else:
			userrole = "Outlier"
		
		try:
			conn = psycopg2.connect(conn_string)
			cur = conn.cursor()
			cur.execute("UPDATE " + table + " SET userrole = %s WHERE userid = %s", (userrole, userid, ))
			conn.commit()
			cur.close()
			conn.close()
			
			# counter += 1
			# if(counter % 10000 == 0):
			# 	print "User: " + str(counter)
		except Exception as e:
			print("Update Userrole Error: " + str(e))
			pass







find_outliers()
update_outliers()