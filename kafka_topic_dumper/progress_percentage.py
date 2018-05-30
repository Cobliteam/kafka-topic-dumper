import logging
import os
import threading


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ProgressPercentage(object):
    def __init__(self, filename, filesize=None):
        self._filename = filename
        if filesize is not None:
            self._size = filesize
        else:
            self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            msg = "{:40s}  {:10} / {:10}  ({:03g}%)"
            logger.info(msg.format(
                self._filename, self._seen_so_far, self._size,
                percentage))
