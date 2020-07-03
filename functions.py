import logging
from collections import deque
import json
from time import time

import dataset
import pandas as pd
from sqlalchemy.exc import OperationalError


def if_no_dir_make(path):
    import os
    try:
        os.makedirs(path)
    except OSError:
        if not os.path.isdir(path):
            raise
    finally:
        return path


def duration_passed(oldepoch, duration_seconds=60):
    return time() - oldepoch >= duration_seconds


def build_logger(name, filename):
    import os
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')
    log_path = os.path.join(if_no_dir_make('logs'), filename)
    filehandler = logging.FileHandler(log_path)
    filehandler.setFormatter(formatter)
    streamhandler = logging.StreamHandler()
    streamhandler.setFormatter(formatter)

    logger.addHandler(filehandler)
    logger.addHandler(streamhandler)

    return logger


def flatten_status(status):
    data = status._json
    id_val = data.pop('id')
    data['tweet_id'] = id_val

    flat_data = stringify_nested_dict(data)
    return flat_data


def stringify_nested_dict(data, stringify_types=[list, dict, None]):

    for key, val in data.items():
        if type(val) in stringify_types:
            data[key] = json.dumps(data[key])
    return data


class RingBuffer(deque):
    """
    inherits deque, pops the oldest data to make room
    for the newest data when size is reached
    """

    def __init__(self, size):
        deque.__init__(self)
        self.size = size

    def full_append(self, item):
        deque.append(self, item)
        # full, pop the oldest item, left most item
        self.popleft()

    def full_extend(self, item):
        deque.extend(self, item)
        self.popleft()

    def append(self, item):
        deque.append(self, item)
        # max size reached, append becomes full_append
        if len(self) == self.size:
            self.append = self.full_append

    def extend(self, item):
        deque.extend(self,item)
        if len(self) == self.size:
            self.extend = self.full_extend

    def get(self):
        """returns a list of size items (newest items)"""
        return list(self)


def most_recent_id(db):
    try:
        res = list(x for x in db.query('SELECT MAX(tweet_id) FROM tweets'))
        return res[0]['MAX(tweet_id)']
    except OperationalError:
        return None


def unpack_column(df, col, isolate=False, drop_prefix=False):
    if col in df.columns:
        unpacked = df[df[col].notna()][col].apply(lambda x: pd.Series(json.loads(x)))
        unpacked = unpacked.add_prefix(col + '.')
        df.drop(columns=[col], inplace=True)
        df = df.merge(unpacked, left_index=True, right_index=True, how='left')
        if isolate:
            isolate_cols = [c for c in df.columns if c.startswith(col)]
            df = df[isolate_cols].copy()
            if drop_prefix:
                remap = {c:c.split('.')[1] for c in isolate_cols}
                df.rename(columns=remap, inplace=True)
    return df


def load_as_dataframe(dbname, cols_to_unpack=None):
    if not dbname.endswith('.db'):
        dbname = dbname+'.db'
    db = dataset.connect(f"sqlite:///{dbname}")
    result = db['tweets'].all()
    df = pd.DataFrame(result)
    if cols_to_unpack != None:

        for col in cols_to_unpack:
            df = unpack_column(df, col)

    return df

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

if __name__ == '__main__':
    pass


def unwind_retweets(df):
    rt_cols = [col for col in df.columns if col.startswith('retweeted_status')]
    retweets = df[rt_cols].copy()
    col_remaps = {col: col.split('.')[1] for col in retweets.columns}
    retweets.rename(columns=col_remaps)
    df = pd.concat([df,retweets], axis=0, sort=False)
    df = df.drop_duplicates(subset='tweet_id')
    df.reset_index(inplace=True)
    return df