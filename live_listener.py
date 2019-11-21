import dataset
import json
import tweepy
import argparse
from time import sleep
from urllib3.exceptions import ReadTimeoutError
from my_credentials import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_SECRET, ACCESS_KEY

def stringify_nested_dict(data, stringify_types=[list, dict, None]):

    for key, val in data.items():
        if type(val) in stringify_types:
            data[key] = json.dumps(data[key])
    return data

def flatten_status(status):
    data = status._json
    id_val = data.pop('id')
    data['tweet_id'] = id_val

    flat_data = stringify_nested_dict(data)
    print()
    return flat_data


class MyListener(tweepy.StreamListener):

    def on_status(self, status):
        try:
            if retweets:
                print(status.text)
                flat_data = flatten_status(status)
                table.insert(flat_data)
            else:
                if not 'retweeted_status' in status._json and not status._json['text'].startswith('RT'):
                    print(status.text)
                    flat_data = flatten_status(status)
                    table.insert(flat_data)

                else:
                    pass
        except Exception as e:
            print('*'*200)
            print('ERROR! {}'.format(str(e)))

    def on_error(self,status_code):
        if status_code == 420:
            print(f'Rate Limited: {status_code}')
            return False
    def on_timeout(self):
        raise ReadTimeoutError





parser = argparse.ArgumentParser(description='A general pupose Twitter Monitor')
parser.add_argument('-db', '--dbname', action='store', type=str, required=True)
parser.add_argument('-q', '--query', action='store', nargs='+', required=True)
parser.add_argument('-r', '--retweets', help='Boolean: Collect Retweets - Default False',
                    action='store_true', default=False)


args = vars(parser.parse_args())

query = args['query']
retweets = args['retweets']
dbname = args['dbname']

db = dataset.connect(f"sqlite:///{dbname}.db")
table = db['tweets']
auth = tweepy.OAuthHandler(CONSUMER_KEY,CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY,ACCESS_SECRET)

api_live = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
session_data = []


def initiate_query_monitor(query):
    print('Listening for Tweets matching {}'.format(query))
    try:
        stream = tweepy.Stream(auth=api_live.auth, listener=MyListener())
        stream.filter(track=query, is_async=True)
    except ReadTimeoutError:
        print('Connection Lost...')
        stream.disconnect()
        sleep(4)
        print('Restarting...')
        sleep(1)
        initiate_query_monitor(query)
    except KeyboardInterrupt:
        stream.disconnect()
        raise
    except Exception as e:
        print('Fatal Error', e)
        initiate_query_monitor(query)
    return

try:
    initiate_query_monitor(query)
except KeyboardInterrupt:
    print('Shutdown signal recieved...')
finally:
    print('Shutdown complete. Have a nice day!')