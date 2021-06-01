"""Render workspaces into a sqlite graph."""
import sqlite3
import json
import logging


from datetime import date, datetime


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class WorkspaceGraph:
    """Represent workspace as in a graph."""

    def __init__(self, path):
        """Initialize db."""
        self.path = path
        assert self.path, "Path must be set"
        self._conn = sqlite3.connect(self.path)
        cur = self._conn.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS vertices (
            key text PRIMARY KEY,
            submitter_id text,
            name text,
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

    def put(self, key, submitter_id, name, data, cur):
        """Save an item."""
        if hasattr(data, 'attributes'):
            _json = json.dumps(data.attributes, default=json_serial)
        else:
            _json = json.dumps(data, default=json_serial)
        cur.execute("REPLACE into vertices values (?, ?, ?, ?);", (key, submitter_id, name, _json))
        # self._logger.debug(f"put {key}")

    def get(self, key):
        """Retrieve an item."""
        cur = self._conn.cursor()
        data = cur.execute("SELECT json, key, submitter_id, name FROM vertices where key=?", (key,)).fetchone()
        if data:
            v = json.loads(data[0])
            v['_key'] = data[1]
            v['_submitter_id'] = data[2]
            v['_name'] = data[3]
            src_edges = [(d[0], d[1]) for d in cur.execute("SELECT src, src_name FROM edges where dst=?", (key,)).fetchall()]
            edges = {}
            for src in src_edges:
                if src[1] not in edges:
                    edges[src[1]] = []
                for d in cur.execute("SELECT json, key, name, submitter_id FROM vertices where key=?", (src[0],)).fetchall():
                    e = json.loads(d[0])
                    e['_key'] = d[1]
                    e['_name'] = d[2]
                    e['_submitter_id'] = d[3]
                    edges[src[1]].append(e)

            return {'vertex': v, 'edges': edges}
        assert False, f"NOT FOUND {key}"

    def get_by_name(self, name=None):
        """Retrieve all items with name."""
        cur = self._conn.cursor()
        data = cur.execute("SELECT key FROM vertices where name=?", (name,)).fetchall()
        return [self.get(d[0]) for d in data]

    def put_edge(self, src, dst, src_name, dst_name, cur):
        """Save edge."""
        cur.execute("REPLACE into edges values (?, ?, ?, ?);", (src, dst, src_name, dst_name))

    def save(self, workspace):
        """Save workspace to graph."""
        cur = self._conn.cursor()
        logging.getLogger(__name__).info(f'Loading {workspace.name}')
        self.put(workspace.id, workspace.name, 'workspace', workspace, cur)
        for subject in workspace.subjects:
            self.put(subject.id, subject.id, 'subject', subject, cur)
            self.put_edge(subject.id, workspace.id, 'subject', 'workspace', cur)
            for sample in subject.samples:
                self.put(sample.id, sample.id, 'sample', sample, cur)
                self.put_edge(sample.id, subject.id, 'sample', 'subject', cur)
                for blob_id, blob in sample.blobs.items():
                    self.put(blob_id, blob_id, 'blob', blob, cur)
                    self.put_edge(blob_id, sample.id, 'blob', 'sample', cur)
                    if 'ga4gh_drs_uri' in blob:
                        drs = {'uri': blob['ga4gh_drs_uri']}
                        self.put(drs['uri'], sample.id, 'drs', drs, cur)
                        self.put_edge(drs['uri'], sample.id, 'drs', 'sample', cur)

        self._conn.commit()

    def index(self):
        """Index the vertices and edges."""
        logging.getLogger(__name__).info('Indexing')
        cur = self._conn.cursor()
        cur.executescript("""
        CREATE  INDEX IF NOT EXISTS vertices_submitter_id ON vertices(submitter_id);
        CREATE UNIQUE INDEX IF NOT EXISTS edges_src_dst ON edges(src, dst, src_name, dst_name);
        CREATE  INDEX IF NOT EXISTS edges_dst ON edges(dst);
        """)
        self._conn.commit()
