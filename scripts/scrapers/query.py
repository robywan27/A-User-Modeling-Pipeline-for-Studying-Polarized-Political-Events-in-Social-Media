import logging
import random
import requests
from fake_useragent import UserAgent

from tweet import Tweet


ua = UserAgent()
HEADERS_LIST = [ua.chrome, ua.google, ua['google chrome'], ua.firefox, ua.ff]

INIT_URL = "https://twitter.com/search?f=tweets&vertical=default&q={q}"
RELOAD_URL = "https://twitter.com/i/search/timeline?f=tweets&vertical=" \
             "default&include_available_features=1&include_entities=1&" \
             "reset_error_state=false&src=typd&max_position={pos}&q={q}"




def query_single_page(url, html_response=True, retry=3):
    """
    Returns tweets from the given URL.

    :param url: The URL to get the tweets from
    :param html_response: False, if the HTML is embedded in a JSON
    :param retry: Number of retries if something goes wrong.
    :return: The list of tweets, the pos argument for getting the next page.
    """
    headers = {'User-Agent': random.choice(HEADERS_LIST)}

    try:
        response = requests.get(url, headers=headers)
        if html_response:
            html = response.text
        else:        
            json_resp = response.json()
            html = json_resp['items_html']            

        tweets = list(Tweet.from_html(html))

        if not tweets:
            return [], None

        if not html_response:
            return tweets, json_resp['min_position']

        return tweets, "TWEET-{}-{}".format(tweets[-1].id, tweets[0].id)

    except requests.exceptions.HTTPError as e:
        logging.exception('HTTPError {} while requesting "{}"'.format(
            e, url))
    except requests.exceptions.ConnectionError as e:
        logging.exception('ConnectionError {} while requesting "{}"'.format(
            e, url))
    except requests.exceptions.Timeout as e:
        logging.exception('TimeOut {} while requesting "{}"'.format(
            e, url))

    if retry > 0:
        logging.info("Retrying...")
        return query_single_page(url, html_response, retry-1)

    logging.error("Giving up.")
    return [], None



def query_tweets_once(query, pos, limit=None, num_tweets=0):
    """    
    :param query: Any advanced query.
    :param pos: The Id of the last retrieved tweet, useful for reprising the
                iteration after stopping the retrieval.
    :param limit: Scraping will be stopped when at least ``limit`` number of
                  items are fetched.    
    :param num_tweets: Number of tweets fetched outside this function.
    :return:      A list of Tweet objects and the ID of the last tweet.
    """
    logging.info("Querying {}".format(query))
    query = query.replace(' ', '%20').replace("#", "%23").replace(":", "%3A")    
    tweets = []
    count = 0
    try:
        while True:            
            if pos is None:
                url = INIT_URL.format(q=query)
                new_tweets, pos = query_single_page(url)
            else:
                pos_old = pos
                url = RELOAD_URL.format(q=query, pos=pos)
                new_tweets, pos = query_single_page(url, False)

            if len(new_tweets) == 0:
                logging.info("Got {} tweets for {}.".format(
                    len(tweets), query))
                return tweets, pos_old

            tweets += new_tweets
            count += 1

            logging.info("Got {} tweets ({} new).".format(
                len(tweets) + num_tweets, len(new_tweets)))

            print("pos: {}".format(pos))
            print("len tweets + num_tweets: {}".format(len(tweets) + num_tweets))
            if limit is not None and len(tweets) + num_tweets >= limit:
                print("pos_to return: {}".format(pos))
                return tweets, pos    

    except KeyboardInterrupt:
        logging.info("Program interrupted by user. Returning tweets gathered "
                     "so far...")
    except BaseException:
        logging.exception("An unknown error occurred! Returning tweets "
                          "gathered so far.")

    return tweets, pos



def query_tweets(query, pos, limit=None):
    tweets = []
    iteration = 1

    while limit is None or len(tweets) < limit:        
        new_tweets, pos = query_tweets_once(query, pos, limit, len(tweets))
        tweets.extend(new_tweets)

        if not new_tweets:
            break

        mindate = min(map(lambda tweet: tweet.timestamp, new_tweets)).date()
        maxdate = max(map(lambda tweet: tweet.timestamp, new_tweets)).date()
        logging.info("Got tweets ranging from {} to {}".format(
            mindate.isoformat(), maxdate.isoformat()))    
        
    # Eliminate duplicates
    return list(eliminate_duplicates(tweets)), pos



def eliminate_duplicates(iterable):
    """
    Yields all unique elements of an iterable sorted. Elements are considered
    non unique if the equality comparison to another element is true. (In those
    cases, the set conversion isn't sufficient as it uses identity comparison.)
    """
    class NoElement: pass

    prev_elem = NoElement
    for elem in sorted(iterable):
        if prev_elem is NoElement:
            prev_elem = elem
            yield elem
            continue

        if prev_elem != elem:
            prev_elem = elem
            yield elem