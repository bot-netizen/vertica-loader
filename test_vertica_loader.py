"""
Offline unit tests for vertica_loader — no Vertica or SSH needed (all I/O mocked).

Run:
    python test_vertica_loader.py
    # or
    python -m unittest test_vertica_loader -v
"""

import unittest
from unittest import mock

import vertica_loader as vl


# A minimal, valid config used by every test.
CFG = {
    "vertica": {"host": "h", "port": 5433, "user": "dbadmin",
                "password": "x", "database": "db"},
    "cluster": {"ssh_user": "dbadmin",
                "nodes": [{"ssh_host": "n1", "db_node": "v1"}]},
    "paths": {"source_dir": "/data", "backup_dir": "/bak", "file_glob": "*.tar.gz"},
    "schema": {"target_schema": "public", "schema_files": "./schemas"},
    "load": {"target_table": "weather_fact", "reject_table": "weather_fact_rej",
             "audit_table": "load_audit", "delimiter": "\\t"},
}


class TestConfig(unittest.TestCase):
    def test_valid_config_passes(self):
        vl.validate_config(CFG)                       # should not raise

    def test_missing_key_raises(self):
        bad = {**CFG, "load": {k: v for k, v in CFG["load"].items()
                               if k != "target_table"}}
        with self.assertRaises(ValueError):
            vl.validate_config(bad)


class TestSQLBuilders(unittest.TestCase):
    def test_fact_copy_sql(self):
        stream = "vload_20260101_120000_0000"
        sql = vl.build_fact_copy_sql(CFG, stream, "/tmp/work", "v1")
        self.assertIn("COPY public.weather_fact", sql)
        self.assertIn("FROM '/tmp/work/*.gz' ON v1 GZIP", sql)  # server-side, no stdin pipe
        self.assertIn("DELIMITER E'\\t'", sql)
        self.assertIn(f"STREAM NAME '{stream}'", sql)
        self.assertIn("rejectted_" + stream, sql)              # per-stream reject table
        self.assertTrue(sql.strip().endswith("DIRECT"))


class TestDedup(unittest.TestCase):
    def test_skips_already_loaded(self):
        cur = mock.Mock()
        cur.fetchall.return_value = [("a.tar.gz",)]            # already in audit table
        conn = mock.Mock()
        conn.cursor.return_value = cur

        manifest = [{"name": "a.tar.gz"}, {"name": "b.tar.gz"}]
        fresh = vl.filter_already_loaded(conn, CFG, manifest)
        self.assertEqual([e["name"] for e in fresh], ["b.tar.gz"])


class TestAtomicLoad(unittest.TestCase):
    """Core guarantee: an archive commits only if BOTH untar and COPY succeed."""

    ENTRY  = {"db_node": "v1", "ssh_host": "n1", "path": "/d/a.tar.gz", "name": "a.tar.gz"}
    STREAM = "vload_20260101_120000_0000"

    def _load(self, untar_error=False, copy_error=False):
        conn = mock.Mock()
        if copy_error:
            conn.cursor.return_value.execute.side_effect = RuntimeError("copy failed")

        ssh_effect = RuntimeError("untar failed") if untar_error else None
        with mock.patch.object(vl, "ssh_run", side_effect=ssh_effect), \
             mock.patch.object(vl, "get_connection", return_value=conn), \
             mock.patch.object(vl, "_stream_stats",
                               return_value={"accepted": 100, "rejected": 2,
                                             "duration_sec": 1.0}):
            result = vl.load_one_archive(CFG, self.ENTRY, self.STREAM)
        return conn, result

    def test_commit_on_success(self):
        conn, result = self._load()
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["accepted"], 100)

    def test_rollback_when_untar_fails(self):
        conn, result = self._load(untar_error=True)
        conn.rollback.assert_called_once()   # untar raises inside try, rollback fires
        conn.commit.assert_not_called()
        self.assertEqual(result["status"], "FAILED")
        self.assertIn("untar failed", result["error"])

    def test_rollback_when_copy_fails(self):
        conn, result = self._load(copy_error=True)
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()
        self.assertEqual(result["status"], "FAILED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
