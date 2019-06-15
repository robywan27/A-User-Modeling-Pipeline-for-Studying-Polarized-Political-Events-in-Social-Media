from datetime import datetime

from bs4 import BeautifulSoup
from coala_utils.decorators import generate_ordering
import re


@generate_ordering('timestamp', 'id', 'text', 'user', 'replies', 'retweet_count', 'favorite_count', 'userid', 'hashtags', 'user_mentions', 'lang', 'in_reply_to_user_id', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'urls')
class Tweet:
    def __init__(self, user, id, timestamp, fullname, text, replies, retweet_count, favorite_count, userid, hashtags, user_mentions, lang, in_reply_to_user_id, in_reply_to_screen_name, in_reply_to_status_id, urls):
        self.user = user
        self.id = id
        self.timestamp = timestamp
        self.fullname = fullname
        self.text = text
        self.replies = replies
        self.retweet_count = retweet_count
        self.favorite_count = favorite_count
        self.userid = userid
        self.hashtags = hashtags
        self.user_mentions = user_mentions        
        self.lang = lang
        self.in_reply_to_user_id = in_reply_to_user_id
        self.in_reply_to_status_id = in_reply_to_status_id
        self.in_reply_to_screen_name = in_reply_to_screen_name
        self.urls = urls

    @classmethod
    def from_soup(cls, tweet):
        return cls(
            user=tweet.find('span', 'username').text[1:],
            id=tweet['data-item-id'],
            timestamp=datetime.utcfromtimestamp(
                int(tweet.find('span', '_timestamp')['data-time'] if tweet.find('span', '_timestamp') is not None else "0000000000000")),
            fullname=tweet.find('strong', 'fullname').text or "",
            text=tweet.find('p', 'tweet-text').text or "",
            replies = tweet.find('div', 'ProfileTweet-action--reply').find('span', 'ProfileTweet-actionCountForPresentation').text or '0',
            retweet_count = tweet.find('div', 'ProfileTweet-action--retweet').find('span', 'ProfileTweet-actionCountForPresentation').text or '0',
            favorite_count = tweet.find('div', 'ProfileTweet-action--favorite').find('span', 'ProfileTweet-actionCountForPresentation').text or '0',
            userid = tweet.find('div', 'js-stream-tweet')['data-user-id'],
            hashtags = [hashtag.text[1:] for hashtag in tweet('a', 'twitter-hashtag')],
            user_mentions = [mention.text[1:] for mention in tweet('a', 'twitter-atreply')],            
            lang = tweet.find('p', 'tweet-text')['lang'] or "",
            in_reply_to_user_id = [re.split('","', reply_to_user_id)[0][12:] for reply_to_user_id in re.split('}}[^]]', tweet.find('div', 'js-stream-tweet')['data-reply-to-users-json'])[1:]],
            in_reply_to_screen_name = [reply_to_screen_name.split('","')[1][14:] for reply_to_screen_name in re.split('}}[^]]', tweet.find('div', 'js-stream-tweet')['data-reply-to-users-json'])[1:]],
            in_reply_to_status_id = tweet.find('div', 'js-stream-tweet')['data-conversation-id'] or '0',
            urls = [url for url in tweet('span', 'js-display-url')]
        )

    @classmethod
    def from_html(cls, html):
        soup = BeautifulSoup(html, "lxml")
        tweets = soup.find_all('li', 'js-stream-item')
        if tweets:
            for tweet in tweets:
                try:
                    yield cls.from_soup(tweet)
                except AttributeError:
                    pass  # Incomplete info? Discard!
