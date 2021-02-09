import sys
import requests
import json
import pandas as pd
import datetime
import os
from google.cloud import bigquery
import google.auth

def fetch_twitter_metrics(event, context):

     API_KEY = os.environ.get('API_KEY')
     API_SECRET= os.environ.get('API_SECRET')
     ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN')
     ACCESS_TOKEN_SECRET = os.environ.get('ACCESS_TOKEN_SECRET')

     import requests
     from requests_oauthlib import OAuth1
     auth = OAuth1(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

     tweet_ids = []
     public_metrics_table = []
     non_public_metrics_table = []
     screen_name = ""

     results_length = -1
     max_id_str = ''
     loop_count = 0

     while (results_length !=0 and loop_count < 10000):
          try:
               current_tweet_ids = []
               recent_tweet_ids = []

               url = f'https://api.twitter.com/1.1/statuses/user_timeline.json?count=100{max_id_str}'
               print(url)
               response = requests.get(url, auth=auth)
               response.raise_for_status()
               tweets = response.json()

               results_length = len(tweets)

               for index, tweet in enumerate(tweets):
                    if index == 0:
                         screen_name = tweet['user']['screen_name']

                    age = datetime.datetime.today() - datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y') 

                    if age.days <= 30:
                         recent_tweet_ids.append(str(tweet['id']))

                    tweet_ids.append(tweet['id'])
                    current_tweet_ids.append(tweet['id'])

               tweet_string_ids = map(str, current_tweet_ids)
               
               # Get all public metrics
               if len(current_tweet_ids):
                    ids = ','.join(tweet_string_ids)
                    url = f'https://api.twitter.com/2/tweets?ids={ids}&tweet.fields=created_at,public_metrics&media.fields=public_metrics'
                    response = requests.get(url, auth=auth)
                    response.raise_for_status()
                    public_metrics = response.json()

                    if len(public_metrics) and 'data' in public_metrics:
                         # Add to aggegated list and set up the next batch
                         public_metrics_table.extend(public_metrics['data'])
                         max_id = min(tweet_ids) - 1
                         max_id_str=f'&max_id={max_id}'
                    else:
                         # If there aren't any more this will be the last loop
                         print(json.dumps(public_metrics, indent=2))
                         results_length = 0

               # Get non public and organic metrics if the tweets are in the last 30 days
               if len(recent_tweet_ids):
                    ids = ','.join(recent_tweet_ids)
                    url = f'https://api.twitter.com/2/tweets?ids={ids}&tweet.fields=created_at,non_public_metrics,organic_metrics&user.fields=username&media.fields=non_public_metrics,organic_metrics'
                    response = requests.get(url, auth=auth)
                    response.raise_for_status()
                    non_public_metrics = response.json()

                    if len(non_public_metrics) and 'data' in non_public_metrics:
                         non_public_metrics_table.extend(non_public_metrics['data'])
                    else:
                         print(json.dumps(non_public_metrics, indent=2))

               loop_count += 1
          except Exception as e:
               print(e)
               results_length = 0


     credentials, project = google.auth.default()

     public_df = pd.json_normalize(public_metrics_table)
     public_df['url'] = public_df['id'].map(lambda x: f'https://twitter.com/{screen_name}/status/{x}')
     public_df['created_at'] = public_df['created_at'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ'))
     public_df['fetch_date'] = datetime.datetime.today()

     newcolumns = []
     for column in public_df.columns:
          newcolumns.append(column.replace('public_metrics.',''))
     public_df.columns = newcolumns
     public_df.to_gbq('twitter.public_metrics', project, if_exists='append', credentials=credentials)

     non_public_df = pd.json_normalize(non_public_metrics_table)
     non_public_df['url'] = non_public_df['id'].map(lambda x: f'https://twitter.com/{screen_name}/status/{x}')
     non_public_df['created_at'] = non_public_df['created_at'].map(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ'))
     non_public_df['fetch_date'] = datetime.datetime.today()

     newcolumns = []
     for column in non_public_df.columns:
          newcolumns.append(column.replace('.','_'))
     non_public_df.columns = newcolumns
     non_public_df.to_gbq('twitter.non_public_organic_metrics', project, if_exists='append', credentials=credentials)
