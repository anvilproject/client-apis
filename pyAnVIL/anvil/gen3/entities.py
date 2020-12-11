"""Parse PFB avro, write sqlite, summarize."""
from fastavro import reader
import json
from datetime import date, datetime
import sqlite3
import logging


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class Entities:
    """Represent gen3 objects."""

    def __init__(self, path):
        """Simplify blob."""
        self.path = path
        self._conn = sqlite3.connect('/tmp/gen3-drs.sqlite')
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
        CREATE TABLE IF NOT EXISTS history (
            key text
        );
        """)
        self._conn.commit()

    def put(self, key, submitter_id, name, data, cur):
        """Save an item."""
        cur.execute("REPLACE into vertices values (?, ?, ?, ?);", (key, submitter_id, name, json.dumps(data, default=json_serial)))
        for r in data['relations']:
            cur.execute("REPLACE into edges values (?, ?, ?, ?);", (key, r['dst_id'], data['name'], r['dst_name']))
        # self._logger.debug(f"put {key}")

    def get(self, key=None, submitter_id=None):
        """Retrieve an item."""
        cur = self._conn.cursor()
        data = cur.execute("SELECT json FROM vertices where key=? or submitter_id =?", (key, submitter_id)).fetchone()
        if data:
            return json.loads(data[0])
        assert False, f"NOT FOUND {key} {submitter_id}"

    def load(self):
        """Load sqlite db from file."""
        cur = self._conn.cursor()
        ids = {}
        logging.getLogger(__name__).info(f'Loading {self.path}')

        loaded_already = cur.execute("SELECT count(*) FROM history WHERE key=?;", (self.path,)).fetchone()[0]
        if loaded_already == 1:
            logging.getLogger(__name__).info(f'Already indexed {self.path}')
            return

        # index_count = cur.execute("SELECT count(*) FROM sqlite_master WHERE type='index' and name='vertices_submitter_id';").fetchone()[0]
        # if index_count == 1:
        #     logging.getLogger(__name__).info('Already indexed')
        #     return

        with open(self.path, 'rb') as fo:
            for record in reader(fo):
                if record['id'] not in ids:
                    self.put(record['id'], self._submitter_id(record), record['name'], record, cur)
                    ids[record['id']] = None
        self._conn.commit()

        logging.getLogger(__name__).info('Indexing')
        cur.executescript("""
        CREATE  INDEX IF NOT EXISTS vertices_submitter_id ON vertices(submitter_id);
        CREATE UNIQUE INDEX IF NOT EXISTS edges_src_dst ON edges(src, dst, src_name, dst_name);
        CREATE  INDEX IF NOT EXISTS edges_dst ON edges(dst);
        """)
        self._conn.commit()

        logging.getLogger(__name__).info('Flattening')
        cur.executescript("""
        drop table if exists flattened ;
        create table flattened
        as
        select
            json_extract(su.json, '$.object.project_id') as "project_id",
            json_extract(su.json, '$.object.anvil_project_id') as "anvil_project_id",
            su.name as "subject_type",
            su.key as "subject_id",
            json_extract(su.json, '$.object.participant_id') as "participant_id",
            json_extract(su.json, '$.object.submitter_id') as "subject_submitter_id",
            sa.name as "sample_type",
            sa.key  as "sample_id",
            json_extract(sa.json, '$.object.sample_id') as "sample_sample_id",
            json_extract(sa.json, '$.object.submitter_id') as "sample_submitter_id",
            'sequencing' as "sequencing_type",
            sequencing_edge.src  as "sequencing_id",
            json_extract(sq.json, '$.object.submitter_id') as "sequencing_submitter_id",
            json_extract(sq.json, '$.object.ga4gh_drs_uri') as "ga4gh_drs_uri"
            from vertices as su
                join edges as sample_edge on sample_edge.dst = su.key and sample_edge.src_name = 'sample'
                    join vertices as sa on sample_edge.src = sa.key
                        left join edges as sequencing_edge on sequencing_edge.dst = sa.key and sequencing_edge.src_name = 'sequencing'
                            join vertices as sq on sequencing_edge.src = sq.key

            where
            su.name = 'subject'            ;
        """)
        self._conn.commit()

        logging.getLogger(__name__).info('Summarizing')
        cur.executescript("""
            drop table if exists summary ;
            create table summary
            as
            select f.project_id, f.anvil_project_id,
                count(distinct f.subject_id) as "subject_count",
                count(distinct f.sample_id) as "sample_count",
                count(distinct m.sequencing_id) as "sequencing_count",
                count(distinct m.ga4gh_drs_uri) as "ga4gh_drs_uri_count"
                from flattened as f
                    left join flattened as m on f.project_id = m.project_id and f.anvil_project_id = m.anvil_project_id
                group by f.project_id, f.anvil_project_id;
        """)
        self._conn.commit()

        logging.getLogger(__name__).info('Updating history')
        cur.execute("""
            insert into history(key) values(?);
        """, (self.path,))
        self._conn.commit()

    def _submitter_id(self, record):
        """Deduce 'natural' key.

        See https://docs.google.com/spreadsheets/d/1MxfcWDXhTfFNFKsbRGjGTQkBoTirNktj04lf6L9_jmk/edit#gid=0
        """
        name = record['name']
        if 'specimen_id' in record['object'] and name == 'sample':
            return record['object']['specimen_id']
        if 'participant_id' in record['object'] and name == 'subject':
            return record['object']['participant_id']
        if 'file_name' in record['object'] and name == 'sequencing':
            return record['object']['file_name']
        if 'dbgap_accession_number' in record['object'] and name == 'program':
            return record['object']['dbgap_accession_number']
        if 'code' in record['object'] and name == 'project':
            return record['object']['code']
        if record['name'] == 'Metadata':
            return f"{record['name']}/schema"
        if 'submitter_id' in record['object']:
            return record['object']['submitter_id']
        assert False, record
