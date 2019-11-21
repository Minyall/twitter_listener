import tweepy
from credentials import CREDENTIALS_LIBRARY
from SETTINGS import QUERY_LIST, GATHER_RETWEETS, WORKER_LIMIT
from functions import chunks
from listening_functions import initiate_query_monitor

if WORKER_LIMIT > 1:
    num_credentials = len(CREDENTIALS_LIBRARY)
    num_workers = min([num_credentials, WORKER_LIMIT])
    num_chunks = len(QUERY_LIST) // num_workers + 1
    query_chunks = list(chunks(QUERY_LIST, num_chunks))
else:
    num_workers = 1
    query_chunks = QUERY_LIST


test = list(zip(query_chunks, CREDENTIALS_LIBRARY))
print()
# auth = tweepy.OAuthHandler(CONSUMER_KEY,CONSUMER_SECRET)
# auth.set_access_token(ACCESS_KEY,ACCESS_SECRET)

# api_live = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
# session_data = []

# initiate_query_monitor(query, api=api_live)