# Twitter Metrics to Google BigQuery

This script extracts public, non-public, and organic metrics using the Twitter API and writes the data to Google BigQuery. 
The script is designed to be run as a Google Cloud function (or anywhere if you [set up Google authentication](https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python).

The script creates a dataset called `twitter` and two tables: `public_metrics` and `non_public_organic_metrics` with the following fields:


####`public_metrics`
================
* id
* created_at	
* text	
* retweet_count	
* reply_count	
* like_count
* quote_count
* url
* fetch_date

####`non_public_organic_metrics`
============================
* id
* text
* created_at
* organic_metrics_retweet_count	
* organic_metrics_reply_count	
* organic_metrics_like_count	
* organic_metrics_user_profile_clicks	
* organic_metrics_impression_count	
* non_public_metrics_impression_count
* non_public_metrics_user_profile_clicks
* organic_metrics_url_link_clicks	
* non_public_metrics_url_link_clicks
* url	
* fetch_date


### Installation

``pip3 install -r requirements.txt``

### Setting Environment Variables

You must configure the following environment variables:

``API_KEY - Twitter API key  
API_SECRET - Twitter API Secret  
ACCESS_TOKEN - Twitter Access Token (for OAuth)  
ACCESS_TOKEN_SECRET - Twitter Access Token (for OAuth)``

### Running the Script

``python3 main.py``
