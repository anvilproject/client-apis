"""Query sqlite for gen3 sequencing records, for more see bin/anvil_extract."""

import sqlite3


def dict_factory(cursor, row):
    """Return results as a dict."""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class DRSFiles:
    """Lookup from gen3 sequencing."""

    def __init__(self, sqlite_path):
        """Lookup from gen3 sequencing."""
        self._conn = sqlite3.connect(sqlite_path)
        self._conn.row_factory = dict_factory
        self._cur = self._conn.cursor()

    def find_by_file_name(self, file_name):
        """Find using file_name."""
        return self._cur.execute("SELECT * FROM drs_file where file_name =?", (file_name, )).fetchone()

    def find_by_md5sum(self, md5sum):
        """Find using md5sum."""
        return self._cur.execute("SELECT * FROM drs_file where md5sum =?", (md5sum, )).fetchone()
