"""Cache items into sqlite."""
import os
import sqlite3
import json
import functools
import logging
from datetime import date, datetime, timedelta
import sys

# TODO refactor cache to test configure path
CACHE_PATH = os.getenv('PYANVIL_CACHE_PATH', '/tmp/pyanvil-cache.sqlite')


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class Cache():
    """Cache items in sqlite."""

    def __init__(self, path=CACHE_PATH, timeout=60 * 60 * 24 * 365):
        """Set up sqlite db."""
        # works better w/ flask
        self._path = path
        self._conn = sqlite3.connect(path, check_same_thread=True)
        self._timeout = timeout
        cur = self._conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS items (
            key text PRIMARY KEY,
            expiry TIMESTAMP,
            json text NOT NULL
        );""")
        self._conn.commit()
        # now = datetime.now().replace(microsecond=0).isoformat()
        # cur.execute("DELETE FROM items where expiry < ?", (now,))
        # if cur.rowcount > 0:
        #     self._logger.debug(f"expired {cur.rowcount} rows")
        # self._conn.commit()

        # optimize for single thread speed
        self._conn.execute('PRAGMA synchronous = OFF')
        self._conn.execute('PRAGMA journal_mode = OFF')
        # double cache size
        self._conn.execute('PRAGMA cache_size = -6000')
        self._conn.commit()
        self._conn.close()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level='DEFERRED')
        logging.getLogger(__name__).info(f"Initialized cache {path}")
        print(f"Initialized cache {path}", file=sys.stderr)

    def get(self, key):
        """Retrieve an item."""
        _logger = logging.getLogger(__name__)

        _logger.debug(f"? {key}")
        now = datetime.now().replace(microsecond=0).isoformat()
        cur = self._conn.cursor()
        data = cur.execute("SELECT json FROM items where key=? and expiry > ?", (key, now)).fetchone()
        if data:
            data = json.loads(data[0])
            _logger.debug(f"hit {key}")
        else:
            _logger.debug(f"miss {key}")
        cur.close()
        return data

    def put(self, key, data):
        """Save an item."""
        logging.getLogger(__name__).debug(f"put {key}")
        expiry = (datetime.now() + timedelta(seconds=self._timeout)).replace(microsecond=0).isoformat()
        cur = self._conn.cursor()
        cur.execute("REPLACE into items values (?, ?, ?);", (key, expiry, json.dumps(data, default=json_serial)))
        self._conn.commit()
        cur.close()


def memoize(func):
    """Cache decorator."""

    @functools.wraps(func)
    def memoized_func(*args, **kwargs):
        """Retrieve from cache, or execute func and save results."""
        # special case the first argument (self)
        _id = ''
        if len(args) > 0:
            _id = str(args[0])
            if 'class' in str(type(args[0])):
                if hasattr(args[0], 'id'):
                    _id = args[0].id
        key = func.__name__ + ":" + _id + "/" + "/".join(args[1:]) + "/" + str(kwargs)
        data = cache.get(key)
        # empty list is OK
        if isinstance(data, list) and len(data) == 0:
            return data
        if not data:
            logging.getLogger(__name__).debug(f"running {key} {data}")
            data = func(*args, **kwargs)
            cache.put(key, data)
        return data

    return memoized_func


# TODO -- initialize when needed, don't just blindly instantitate
cache = Cache()
