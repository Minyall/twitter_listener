import logging
from collections import deque

import json
from time import time


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

    def append(self, item):
        deque.append(self, item)
        # max size reached, append becomes full_append
        if len(self) == self.size:
            self.append = self.full_append

    def get(self):
        """returns a list of size items (newest items)"""
        return list(self)