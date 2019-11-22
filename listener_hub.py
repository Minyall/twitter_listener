from time import time, sleep
import tweepy
import dataset
from functions import build_logger, RingBuffer, flatten_status, clean_tweet, duration_passed


class Core_Listener(object):
    def __init__(self, args,CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET, logger):
        self.query = args['query']
        self.retweets = args['retweets']
        self.dbname = args['dbname']
        self.logging_interval = args['verbosity']
        self.show_sample = args['show_sample']
        self.sample_n = args['sample_n']
        self.sample_len = args['sample_len']

        self.db = dataset.connect(f"sqlite:///{self.dbname}.db")
        self.table = self.db['tweets']

        self.auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        self.auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)

        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        self.logger = logger

    def initiate_core(self):
        try:
            stream = tweepy.Stream(auth=self.api.auth, listener=self.MyListener())
            stream.filter(track=self.query, is_async=True)

        except KeyboardInterrupt:
            stream.disconnect()
            raise
        except Exception as e:
            self.logger.error(f'Fatal Error: {e}')
            self.initiate_core()
        return

    class MyListener(tweepy.StreamListener):
        def _initialise_logging(self):
            self.ticker = time()
            self.name = ','.join(self.query)
            self.tracker = 0
            self.logger = build_logger(self.name, f"{self.name}.log")
            if self.show_sample:
                self.sample_tweets = RingBuffer(size=self.sample_n)

        def _handle_status(self, status):
            flat_data = flatten_status(status)
            self.table.insert(flat_data)
            if self.show_sample:
                self.sample_tweets.append(status.text.strip())
            self.tracker += 1
            return

        def display_sample(self):
            self.logger.info("")
            self.logger.info(f'[***] Current Query: {self.query} [***]')
            self.logger.info(f'[***] Statuses Gathered: {self.tracker} [***]')
            for i, tweet in enumerate(self.sample_tweets.get(), start=1):
                self.logger.info(f'Sample {i} of {self.sample_n} | {clean_tweet(tweet, self.sample_len)}')

        def on_connect(self):
            self._initialise_logging()
            self.logger.info('Succesfully connected to Streaming API - We are Listening.')
            self.logger.info(f"Your query is: {self.query} |"
                             f" Storing in Database {self.dbname}.db |"
                             f" Log Updates every {self.logging_interval} seconds.")

        def on_status(self, status):
            try:
                if duration_passed(self.ticker, self.logging_interval):
                    if self.show_sample:
                        self.display_sample()
                    else:
                        self.logger.info(f'Current Query: {self.query} | Statuses Gathered: {self.tracker}')
                    self.ticker = time()
                if self.retweets:
                    self._handle_status(status)
                else:
                    if not 'retweeted_status' in status._json and not status._json['text'].startswith('RT'):
                        self._handle_status(status)
                    else:
                        pass
                return True
            except Exception as e:
                self.logger.error('*' * 200)
                self.logger.error('ERROR! {}'.format(str(e)))
                return False

        def on_error(self, status_code):
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
            if duration_passed(self.ticker, self.logging_interval):
                self.logger.info("Connection still live...")
                self.ticker = time()
