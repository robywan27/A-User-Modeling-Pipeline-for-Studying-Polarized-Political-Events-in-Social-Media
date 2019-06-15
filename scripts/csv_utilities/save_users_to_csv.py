import psycopg2
import csv

conn_string = "host='localhost' dbname='referendum_catalonia_db' user='postgres' password='senzasenso'"

try:    
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cur = conn.cursor()    
    cur.execute("select user_mentions->10->'id' from tweets_metadata where user_mentions->10->'id' is not null")    
    rows = cur.fetchall()
    print("Number of results: ", cur.rowcount)
        
    csv_writer = csv.writer(open('/data/project/catalonia/userids_mention.csv', 'w'))
    for row in rows:
        csv_writer.writerow(row)                   

    cur.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()