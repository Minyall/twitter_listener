import dataset
import tweepy
import argparse
from time import sleep, time
from functions import duration_passed, build_logger, flatten_status, RingBuffer
from credentials import CONSUMER_KEY, CONSUMER_SECRET, ACCESS_SECRET, ACCESS_KEY

def clean_tweet(tweet_text, truncate_length=100):
    tweet_text = ' '.join(tweet_text.split())
    tweet_text.replace('\n', '')
    tweet_text = tweet_text[:truncate_length]
    tweet_text = tweet_text.strip()
    return tweet_text + '...'

def initiate_query_monitor(query):
    try:
        stream = tweepy.Stream(auth=api_live.auth, listener=MyListener())
        stream.filter(track=query, is_async=True)

    except KeyboardInterrupt:
        stream.disconnect()
        raise
    except Exception as e:
        main_logger.error(f'Fatal Error: {e}')
        initiate_query_monitor(query)
    return

class MyListener(tweepy.StreamListener):
    def _initialise_logging(self):
        self.ticker = time()
        self.name = ','.join(query)
        self.tracker = 0
        self.logger = build_logger(self.name, f"{self.name}.log")
        if show_sample:
            self.sample_tweets = RingBuffer(size=sample_n)

    def _handle_status(self, status):
        flat_data = flatten_status(status)
        table.insert(flat_data)
        if show_sample:
            self.sample_tweets.append(status.text.strip())
        self.tracker += 1
        return

    def display_sample(self):
        self.logger.info("")
        self.logger.info(f'[***] Current Query: {query} [***]')
        self.logger.info(f'[***] Statuses Gathered: {self.tracker} [***]')
        for i, tweet in enumerate(self.sample_tweets.get(), start=1):
            self.logger.info(f'Sample {i} of {sample_n} | {clean_tweet(tweet, sample_len)}')

    def on_connect(self):
        self._initialise_logging()
        self.logger.info('Succesfully connected to Streaming API - We are Listening.')
        self.logger.info(f"Your query is: {query} |"
                         f" Storing in Database {dbname}.db |"
                         f" Log Updates every {logging_interval} seconds.")
        if historical:
            self.retrieve_historical()

    def retrieve_historical(self):
        c = tweepy.Cursor(api_live.search, count=100, q=query).items()
        for tweet in c:
            self.on_status(tweet)
        return True

    def on_status(self, status):
        try:
            if duration_passed(self.ticker, logging_interval):
                if show_sample:
                    self.display_sample()
                else:
                    self.logger.info(f'Current Query: {query} | Statuses Gathered: {self.tracker}')
                self.ticker = time()
            if retweets:
                self._handle_status(status)
            else:
                if not 'retweeted_status' in status._json and not status._json['text'].startswith('RT'):
                    self._handle_status(status)
                else:
                    pass
            return True
        except Exception as e:
            self.logger.error('*'*200)
            self.logger.error('ERROR! {}'.format(str(e)))
            return False

    def on_error(self,status_code):
        if status_code == 420:
            self.logger.error(f'Rate Limited: {status_code}')
            sleep(60)
            return True
        elif status_code == 401:
            self.logger.error(f'Twitter Error 401: Incorrect Credentials')
            return False

    def on_timeout(self):
        self.logger.info("Timeout Detected - Attempting to handle...")
        return True

    def on_exception(self, exception):
        if exception == KeyboardInterrupt:
            raise KeyboardInterrupt
        else:
            self.logger.error(f'Unhandled Exception {exception}')
        return False

    def keep_alive(self):
        if duration_passed(self.ticker, logging_interval):
            self.logger.info("Connection still live...")
            self.ticker = time()


if __name__ == '__main__':

    main_logger = build_logger('main_logger','main.log')
    parser = argparse.ArgumentParser(description='A general pupose Twitter Monitor')
    parser.add_argument('-db', '--dbname', action='store', type=str, required=True)
    parser.add_argument('-q', '--query', action='store', nargs='+', required=True)
    parser.add_argument('-r', '--retweets', help='Boolean: Collect Retweets - Default False',
                        action='store_true', default=False)
    parser.add_argument('-v', '--verbosity', action='store', required=False, type=int, default=60,
                        help='Integer - Duration between logging updates in seconds')
    parser.add_argument('-s', '--show_sample', action='store_true', required=False, default=False, help='Switch: Include argument'
                        ' to have logging return a sample of status texts')
    parser.add_argument('-sn', '--sample_n', action='store', type=int, default=5, required=False, help=
                        "If using --show_sample, sets the number of sample items to show.")
    parser.add_argument('-sl', '--sample_len', action='store', type=int, default=100, required=False, help=
                        "Character limit of displayed samples - good for ensuring a clean fit on the screen. Default 100.")
    parser.add_argument('-hr', '--historical', action='store_true', required=False, default=False, help='Retrieve tweets from the past 7 days prior to initiating the listener')

    try:
        args = vars(parser.parse_args())

        query = args['query']
        retweets = args['retweets']
        dbname = args['dbname']
        logging_interval = args['verbosity']
        show_sample = args['show_sample']
        sample_n = args['sample_n']
        sample_len = args['sample_len']
        historical = args['historical']

        db = dataset.connect(f"sqlite:///{dbname}.db")
        table = db['tweets']

        auth = tweepy.OAuthHandler(CONSUMER_KEY,CONSUMER_SECRET)
        auth.set_access_token(ACCESS_KEY,ACCESS_SECRET)

        api_live = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        initiate_query_monitor(query)
    except KeyboardInterrupt:
        main_logger.info('Shutdown signal recieved...')
        main_logger.info('Shutdown Complete. Have a nice day!')