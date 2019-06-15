import collections
import json
from datetime import datetime
import psycopg2
import logging
import sys

from query import query_tweets



# This string represents the parameters of the Postgres database in which to store the tweets. Any parameter (e.g. database name or password) 
# should be changed according to the employed database configuration
conn_string = "host='localhost' dbname='referendum_lombardia_db' user='postgres' password='########'"


# table = "tweets_copy"
table = "tweets_new"


class JSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if hasattr(obj, '__json__'):
            return obj.__json__()
        elif isinstance(obj, collections.Iterable):
            return list(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, '__getitem__') and hasattr(obj, 'keys'):
            return dict(obj)
        elif hasattr(obj, '__dict__'):
            return {member: getattr(obj, member)
                    for member in dir(obj)
                    if not member.startswith('_') and
                    not hasattr(getattr(obj, member), '__call__')}

        return json.JSONEncoder.default(self, obj)



# Saving batches of tweets
def main():
    limit = 1000 # this parameter defines the number of tweets to retrieve before storing them
    pos = None
    iter = 1

    query = "siautonomia since:2017-07-22 until:2017-10-22" # Define here the any query to perform as a string 

    while True:
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
        print("Iteration number: " + str(iter))
        iter += 1        

        try:            
            tweets, pos = query_tweets(query, pos, limit) 

            # To make it simple, the results could be stored in a JSON file instead of setting up a database. However, this solution is not advisable for
            # storing large amount of data.
            # with open("tweets.json", "w") as output:
            #     json.dump(tweets, output, cls=JSONEncoder)           

            tweets_json = json.loads(JSONEncoder().encode(tweets))
        except KeyboardInterrupt:
            logging.info("Program interrupted by user. Quitting...")
        
    
        tweetid_str = "-1"
        tweet_created_at = "0000-00-00 00:00:00+00"
        text = ""
        in_reply_to_status_ids_str = -1
        in_reply_to_user_id = None
        in_reply_to_screen_name = None
        retweet_count_str = -1
        favorite_count_str = -1    
        lang = ""
        latitude = None
        longitude = None
        country_code = None
        city_name = None
        hashtags = None
        urls = None
        user_mentions = None
        replies_count_str = -1
        userid_str = -1
        screen_name = ""
        fullname = ""    

        for tweet in tweets_json:
            
            tweetid_str = tweet["id"]            
            tweet_created_at = tweet["timestamp"]            
            text = tweet["text"]                
            retweet_count_str = tweet["retweet_count"]            
            favorite_count_str = tweet["favorite_count"]            
            lang = tweet["lang"]                        
            hashtags = tweet["hashtags"]            
            urls = tweet["urls"]            
            user_mentions = tweet["user_mentions"]            
            in_reply_to_status_id_str = tweet["in_reply_to_status_id"]            
            in_reply_to_user_ids = tweet["in_reply_to_user_id"]            
            in_reply_to_screen_name = tweet["in_reply_to_screen_name"]
            replies_count_str = tweet["replies"]
            
            userid_str = tweet["userid"]            
            screen_name = tweet["user"]
            fullname = tweet["fullname"]

            try:
                tweetid = int(tweetid_str)
            except ValueError:
                pass
            try:
                replies_count = int(replies_count_str)
            except ValueError:
                pass
            try:
                retweet_count = int(retweet_count_str)
            except ValueError:
                pass
            try:
                favorite_count = int(favorite_count_str)
            except ValueError:
                pass
            try:
                userid = int(userid_str)
            except ValueError:
                pass
            try:
                in_reply_to_status_id = int(in_reply_to_status_id_str)
            except ValueError:
                pass

            try:
                conn = psycopg2.connect(conn_string)
                conn.autocommit = True
                cur = conn.cursor()                
                cur.execute("INSERT INTO " + table + "(tweetid, userid, created_at, text, in_reply_to_status_id, retweet_count, favorite_count, lang, latitude, longitude, country_code, city_name, hashtags, urls, user_mentions, replies_count, screen_name, fullname, in_reply_to_user_ids, in_reply_to_screen_name) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (tweetid, userid, tweet_created_at, text, in_reply_to_status_id, retweet_count, favorite_count, lang, latitude, longitude, country_code, city_name, json.dumps(hashtags), json.dumps(urls), json.dumps(user_mentions), replies_count, screen_name, fullname, json.dumps(in_reply_to_user_ids), json.dumps(in_reply_to_screen_name), ))
                cur.close()
            except Exception as e:                
                print("Twitter Data insertion failed! " + str(e))
                error_type = sys.exc_info()[0]
                error_value = sys.exc_info()[1]
                print('ERROR:', error_type, error_value)
                pass
            finally:
                conn.close()

                                





main()