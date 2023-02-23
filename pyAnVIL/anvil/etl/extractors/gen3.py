import json
import logging
import sqlite3
from gen3.auth import Gen3Auth
from anvil.clients.gen3_auth import Gen3TerraAuth
from anvil.clients.gen3_auth import TERRA_TOKEN_URL
from gen3.query import Gen3Query
from tabulate import tabulate

from anvil.etl.utilities.entities import dict_factory


def drs_extractor(gen3_credentials_path, output_path, use_terra_credentials, expected_row_count):
    """Retrieve DRS url from Gen3's flat file index."""
    # TODO - consider wrapping this using the Entities class
    gen3_endpoint = "https://gen3.theanvil.io"

    # Install n API Key downloaded from the
    # commons' "Profile" page at ~/.gen3/credentials.json

    if use_terra_credentials:
        auth = Gen3TerraAuth(endpoint=gen3_endpoint, terra_auth_url=TERRA_TOKEN_URL, user_email=None)
    else:
        auth = Gen3Auth(endpoint=gen3_endpoint, refresh_file=gen3_credentials_path)

    logger = logging.getLogger(__name__)

    query_client = Gen3Query(auth)
    logger.info('Starting export of data from gen3.')
    raw_data = query_client.raw_data_download(data_type='file', fields='node_id,project_id,anvil_project_id,subject_submitter_id,sample_submitter_id,sequencing_assay_submitter_id,file_name,file_size,md5sum,submitter_id,md5sum,drs_id,_subject_id'.split(','))

    assert len(raw_data) > expected_row_count, f"Expected over {expected_row_count} file records, got {len(raw_data)} instead.  Projects {set(sorted([(r['project_id']) for r in raw_data]))}"

    logger.info(f"retrieved {len(raw_data)} file records from gen3.")

    sqlite_path = f'{output_path}/drs_file.sqlite'
    _conn = sqlite3.connect(sqlite_path)
    cur = _conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS drs_file (
        md5sum text,
        sequencing_id text PRIMARY KEY,
        file_name text,
        ga4gh_drs_uri text,
        sample_submitter_id text,
        subject_submitter_id text,
        subject_id text,
        project_id text,
        anvil_project_id text
    );
    """)
    _conn.commit()
    # optimize for single thread speed
    _conn.execute('PRAGMA synchronous = OFF')
    _conn.execute('PRAGMA journal_mode = OFF')
    _conn.commit()
    _conn.close()

    _conn = sqlite3.connect(sqlite_path, check_same_thread=False, isolation_level='DEFERRED')
    cur = _conn.cursor()
    logger.info(f'Starting import of data into sqlite {sqlite_path}.')

    def _first(_array):
        """Return first element in array, or none if empty."""
        if not _array or len(_array) == 0:
            return None
        return _array[0]

    commit_threshold = 1000
    c = 0
    for row in raw_data:
        try:
            cur.execute(
                "INSERT into drs_file values (?, ?, ?, ?, ?, ?, ?, ?, ?);", (
                    row['md5sum'],
                    row['node_id'],
                    row['file_name'],
                    row['drs_id'],
                    _first(row['sample_submitter_id']),
                    _first(row['subject_submitter_id']),
                    _first(row['_subject_id']),
                    row['project_id'],
                    _first(row['anvil_project_id']),
                )
            )
        except Exception:
            logger.error(row)
            logger.error("Fatal error writing row above to sqlite.", exc_info=True)
            break
        c += 1
        if c > commit_threshold:
            _conn.commit()
            c = 0
    _conn.commit()

    logger.info('Indexing')
    cur.executescript("""
    CREATE INDEX IF NOT EXISTS drs_file_md5sum ON drs_file(md5sum);
    CREATE  INDEX IF NOT EXISTS drs_file_file_name ON drs_file(file_name);
    """)
    _conn.commit()
    logger.info(f'Created {sqlite_path}')

    def _dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    _conn = sqlite3.connect(sqlite_path)
    _conn.row_factory = _dict_factory
    cur = _conn.cursor()

    dataset = cur.execute('SELECT project_id as "gen3_project_id", anvil_project_id, count(*) as "file_count" FROM drs_file group by  project_id, anvil_project_id').fetchall()

    header = dataset[0].keys()
    rows = [x.values() for x in dataset]
    logger.info(f"\nExtracted File Counts\n{tabulate(rows, header)}")


class DRSReader:
    """Read items in sqlite."""

    def __init__(self, output_path):
        """Set up sqlite db."""
        self._path = f'{output_path}/drs_file.sqlite'
        self._conn = sqlite3.connect(self._path, check_same_thread=True)
        self._conn.row_factory = dict_factory

    def get(self, file_name=None, md5sum=None):
        """Retrieve an item."""
        assert file_name or md5sum, "Please provide either file_name or md5sum"
        cur = self._conn.cursor()
        data = cur.execute("SELECT * FROM drs_file where file_name=? or md5sum=?", (file_name, md5sum, )).fetchone()
        cur.close()
        return data
