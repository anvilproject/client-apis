"""Cache items into sqlite."""
import sqlite3
import json
import logging
from datetime import date, datetime
from collections import defaultdict

logger = logging.getLogger(__name__)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class Entities():
    """Cache items in sqlite."""

    def __init__(self, path):
        """Set up sqlite db."""
        # works better w/ flask
        self._path = path
        self._conn = sqlite3.connect(path, check_same_thread=True)
        self._conn.row_factory = dict_factory
        cur = self._conn.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS vertices (
            key text PRIMARY KEY,
            label text NOT NULL,
            json text NOT NULL
        );
        CREATE TABLE IF NOT EXISTS edges (
            src text,
            dst text,
            src_name text,
            dst_name text
        );
        """)
        self._conn.commit()

        # optimize for single thread speed
        self._conn.execute('PRAGMA synchronous = OFF')
        self._conn.execute('PRAGMA journal_mode = OFF')
        # double cache size
        self._conn.execute('PRAGMA cache_size = -6000')
        self._conn.commit()
        self._conn.close()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level='DEFERRED')
        self._conn.row_factory = dict_factory
        self._cursor = None
        self._put_count = 0

    def get(self, key):
        """Retrieve an item."""

        logger.debug(f"? {key}")
        cur = self._conn.cursor()
        data = cur.execute("SELECT json FROM vertices where key=? ", (key,)).fetchone()
        if data:
            data = json.loads(data['json'])
            logger.debug(f"hit {key}")
        else:
            logger.debug(f"miss {key}")
        cur.close()
        return data

    @property
    def cursor(self):
        if not self._cursor:
            self._cursor = self._conn.cursor()
        return self._cursor

    def get_by_label(self, label):
        """Retrieve all items."""

        logger.debug(f"? {label}")
        rows = self.cursor.execute("SELECT json FROM vertices where label=? order  by key ", (label,)).fetchall()
        for row in rows:
            yield json.loads(row['json'])

    def commit(self, force=False):
        """Commit if put count over limit."""
        if self._put_count > 1000 or force:
            self._conn.commit()
            self._cursor = None
            self._put_count = 0

    def put(self, key, label, data):
        """Save an item."""
        logger.debug(f"put {key}")
        self.cursor.execute("REPLACE into vertices values (?, ?, ?);", (key, label, json.dumps(data, default=json_serial)))
        self._put_count += 1
        self.commit()

    def put_edge(self, src, dst, src_name, dst_name):
        """Save edge."""
        logger.debug(f"put edge {src_name}/{src} -> {dst_name}/{dst}")
        self.cursor.execute("REPLACE into edges values (?, ?, ?, ?);", (src, dst, src_name, dst_name))
        self._put_count += 1
        self.commit()

    def get_edges(self, src, src_name):
        """Retrieves all edges."""
        destination_edges = self.cursor.execute("SELECT dst, dst_name FROM edges where src= ? and src_name= ? ", (src, src_name, )).fetchall()
        destination_vertices = defaultdict(list)
        for de in destination_edges:
            destination_vertices[de['dst_name']].append(self.get(de['dst']))
        return destination_vertices

    def get_edges_by_label(self, src, src_name, dst_name, limit=None):
        """Retrieves all edges for a particular label."""
        if limit:
            limit = f"LIMIT {limit}"
        else:
            limit = ''
        destination_edges = self.cursor.execute(f"SELECT dst, dst_name FROM edges where src = ? and src_name = ? and dst_name = ? {limit}", (src, src_name, dst_name, )).fetchall()
        destination_vertices = {de['dst_name']: self.get(de['dst']) for de in destination_edges}
        return destination_vertices

    def index(self):
        logger.info('Indexing')
        cur = self._conn.cursor()
        cur.executescript("""
        CREATE  INDEX IF NOT EXISTS items_key ON vertices(key);
        CREATE  INDEX IF NOT EXISTS label_key ON vertices(label);
        CREATE UNIQUE INDEX IF NOT EXISTS edges_src_dst ON edges(src, dst, src_name, dst_name);
        CREATE  INDEX IF NOT EXISTS edges_dst ON edges(dst);
        """)
        self._conn.commit()
