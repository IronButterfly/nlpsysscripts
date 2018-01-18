import json
import urllib3
import sys
import boto3 
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
from elasticsearch import Elasticsearch
from datetime import datetime
urllib3.disable_warnings()

# Import twitter keys and tokens from config
from config import *

# Create instance of Elasticsearch
es = Elasticsearch(
['your_elastic_server.com'],
    http_auth=('username', 'password'),
    port=9200,
)

indexName = "twcomprehend"

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

        # Contry code extraction
        country = None
        if 'location' in dict_data['user'] and dict_data['user']['location'] is not None:
          print dict_data['user']['location'].encode('utf-8', 'ignore').replace(' ', '+')

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
 
        client_comprehend = boto3.client(
        'comprehend',
        region_name = 'eu-west-1',
        aws_access_key_id = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY    
        )

        dominant_language_response = client_comprehend.detect_dominant_language(
                Text=dict_data['text']
        )
        dominant_language = sorted(dominant_language_response['Languages'], key=lambda k: k['LanguageCode'])[0]['LanguageCode']
   
    # The service now only supports English and Spanish. In future more languages will be available.
        if dominant_language not in ['en','es']:
                dominant_language = 'en'
    

        response = client_comprehend.detect_entities(
                Text=dict_data['text'],
                LanguageCode=dominant_language
        )
        entites = list(set([x['Type'] for x in response['Entities']]))

        response_key_phrases = client_comprehend.detect_key_phrases(
                Text=dict_data['text'],
                LanguageCode=dominant_language
        )
        key_phrases = list(set([x['Text'] for x in response_key_phrases['KeyPhrases']]))

        response_sentiment = client_comprehend.detect_sentiment(
                Text=dict_data['text'],
                LanguageCode=dominant_language
        )
        sentiment = response_sentiment['Sentiment']

        print(country)
        print(dict_data['text'])
        print(entites)
        print(key_phrases)
        print(sentiment)


# Add analyse results to Elasticsearch
        es.index(index=indexName,
                 doc_type="amazoncomprehend",
                 body={"author": dict_data["user"]["screen_name"],
                       "location": dict_data["user"]["location"], # User location
                       "followers": dict_data["user"]["followers_count"],
                       "friends": dict_data["user"]["friends_count"],
                       "time_zone": dict_data["user"]["time_zone"],
                       "lang": dict_data["user"]["lang"],
                       "datetime": datetime.now(),
                       "message": dict_data["text"],
                       "country": country,
                       "entites": entites,
                       "key_phrases": key_phrases,
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
    stream.filter(track=['putin','trump'], languages=['en','es'], async=True)
