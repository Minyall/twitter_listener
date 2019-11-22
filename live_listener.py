import dataset
import json
import tweepy
import argparse
from time import sleep, time
from functions import minute_passed
from credentials import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_SECRET, ACCESS_KEY

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
    return flat_data

class Reporter(object):
    def __init__(self):
        self.tracker = {}
    def init_report(self, name):
        self.tracker[name] = 0
    def increment(self, name):
        self.tracker[name] +=1
    def report(self,name):
        return self.tracker[name]


class MyListener(tweepy.StreamListener):

    def on_connect(self):
        self.name = ','.join(query)
        self.ticker = time()
        self.reporter = Reporter()
        self.reporter.init_report(self.name)

    def on_status(self, status):
        try:
            if minute_passed(self.ticker):
                print(f'Total statuses gathered for {query}: {self.reporter.report(self.name)}')
                self.ticker = time()
            if retweets:
                flat_data = flatten_status(status)
                table.insert(flat_data)
            else:
                if not 'retweeted_status' in status._json and not status._json['text'].startswith('RT'):
                    flat_data = flatten_status(status)
                    table.insert(flat_data)


                else:
                    pass
            self.reporter.increment(self.name)
        except Exception as e:
            print('*'*200)
            print('ERROR! {}'.format(str(e)))

    def on_error(self,status_code):
        if status_code == 420:
            print(f'Rate Limited: {status_code}')
            sleep(60)
            return True
    def on_timeout(self):
        return True

    def on_exception(self, exception):
        return False

    def keep_alive(self):
        if minute_passed(self.ticker):
            print("Connection still live...")
            self.ticker = time()


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


def initiate_query_monitor(query):
    print('Listening for Tweets matching {}'.format(query))
    try:
        stream = tweepy.Stream(auth=api_live.auth, listener=MyListener())
        stream.filter(track=query, is_async=True)

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