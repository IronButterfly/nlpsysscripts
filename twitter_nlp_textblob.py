import json
import urllib3
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
from elasticsearch import Elasticsearch
from datetime import datetime
urllib3.disable_warnings()

# Import twitter keys and tokens from config
from config import *

# Create instance of elasticsearch
es = Elasticsearch(
['your_elasticsearch_server.com'],
    http_auth=('username', 'password'),
    port=9200,
)

indexName = "twsent"

class TweetStreamListener(StreamListener):

    # On success
    def on_data(self, data):

        # Decode json
        dict_data = json.loads(data) # data is a json string
        http = urllib3.PoolManager()

        # Filter out tweets w/o user:
        if 'user' not in dict_data:
                return True
        
        # Filter out retweets
        if 'retweeted' in dict_data and 'text' in dict_data and (dict_data["retweeted"] or 'RT @' in dict_data["text"]):
                return True

        # Country code extraction
        country = None
        if 'location' in dict_data['user'] and dict_data['user']['location'] is not None:

          try:
                r = http.request('GET', "https://maps.googleapis.com/maps/api/geocode/json?address=" + dict_data['user']['location'].encode('utf-8', 'ignore').replace(' ', '+') + "&key="+Google_API)
          except:
            return True

          for result in json.loads(r.data.decode('utf-8', 'ignore'))['results']: # Loop through the results (and look for Country)
             for k in result["address_components"]:
                if  'country' in k["types"] :
                  country = k["short_name"]
                  break
             if country is not None:
               break

        print(country)

        # Pass tweet into TextBlob
        tweet = TextBlob(dict_data["text"])

        # print (tweet.sentiment.polarity) # Output sentiment polarity

        # Determine if sentiment is positive, negative, or neutral
        if tweet.sentiment.polarity < 0:
            sentiment = "negative"
        elif tweet.sentiment.polarity == 0:
            sentiment = "neutral"
        else:
            sentiment = "positive"

        # Output polarity sentiment and tweet text
        print (str(tweet.sentiment.polarity) + " " + sentiment + " " + dict_data["text"])

        # Add text and sentiment info to Elasticsearch
        es.index(index=indexName,
                 doc_type="test-type",
                 body={"author": dict_data["user"]["screen_name"],
                       "date": dict_data["created_at"], # unfortunately this gets stored as a string
                       "location": dict_data["user"]["location"], # user location
                       "followers": dict_data["user"]["followers_count"],
                       "friends": dict_data["user"]["friends_count"],
                       "time_zone": dict_data["user"]["time_zone"],
                       "lang": dict_data["user"]["lang"],
                       "timestamp": dict_data["timestamp_ms"],
                       "datetime": datetime.now(),
                       "message": dict_data["text"],
                       "polarity": tweet.sentiment.polarity,
                       "subjectivity": tweet.sentiment.subjectivity,
                       # Handle Contry code
                       "country": country,
                       "sentiment": sentiment})
        return True

    # On failure
    def on_error(self, status):
        print (status)

if __name__ == '__main__':

    # Create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()

    # Set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # Create instance of the tweepy stream
    stream = Stream(auth, listener)

    # Search twitter for these keywords
    stream.filter(track=['putin', 'trump'], languages=['en'], async=True)
