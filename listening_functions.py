import json
import tweepy
import dataset
from SETTINGS import GATHER_RETWEETS, DB_NAME

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

    def on_connect(self):
        self.db = dataset.connect(f"sqlite:///{DB_NAME}.db")


    def on_status(self, status):
        table = self.db['tweets']

        try:
            if GATHER_RETWEETS:
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


def initiate_query_monitor(query, api):
    print('Listening for Tweets matching {}'.format(query))
    stream = tweepy.Stream(auth=api.auth, listener=MyListener())
    stream.filter(track=query, is_async=True)
    return