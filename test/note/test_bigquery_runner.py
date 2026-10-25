"""BigQuery workflow rendering and failure propagation, without cloud calls."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[2]


class BigQueryRunnerTests(unittest.TestCase):
    def execute(self, source='discharge', fail=False):
        with tempfile.TemporaryDirectory(prefix='note-bq-') as folder:
            root=Path(folder)
            executable=root/'bq'
            executable.write_text('#!/usr/bin/env python3\nimport os,sys\nfrom pathlib import Path\n'
                "Path(os.environ['BQ_TEST_QUERY']).write_text(sys.stdin.read())\n"
                "sys.exit(int(os.environ.get('BQ_TEST_EXIT','0')))\n")
            executable.chmod(0o700)
            name='note' if source=='discharge' else 'note_'+source
            workflow='workflow_note_echo.conf' if source=='echo' else 'workflow_note.conf'
            env=dict(os.environ,PATH=str(root)+os.pathsep+os.environ['PATH'],
                BQ_TEST_QUERY=str(root/'query.sql'),BQ_TEST_EXIT='7' if fail else '0')
            result=subprocess.run([sys.executable,'scripts/run_workflow.py','-e','conf/'+name+'.etlconf',
                '-c','conf/'+workflow],cwd=ROOT,env=env,text=True,capture_output=True)
            return result,(root/'query.sql').read_text()

    def test_sources_render_one_atomic_note_only_script(self):
        for source in ['discharge','radiology','cxr','ecg_machine']:
            with self.subTest(source=source):
                result,query=self.execute(source)
                self.assertEqual(result.returncode,0,result.stderr)
                self.assertNotIn('@',query)
                self.assertEqual(query.count('BEGIN TRANSACTION;'),1)
                self.assertEqual(query.count('COMMIT TRANSACTION;'),1)
                self.assertNotIn('CREATE OR REPLACE TABLE',query)
                self.assertNotIn('ext_note',query)
                self.assertNotIn('DELETE FROM',query)
                self.assertIn('INSERT INTO YOUR_ETL_PROJECT.YOUR_EXISTING_ETL_DATASET.cdm_note',query)
                self.assertLess(query.index("AS 'loaded_rows_match_source'"),query.index('COMMIT TRANSACTION;'))
                self.assertIn("REGEXP_CONTAINS(s.note_text, r'[^ \\t\\r\\n]')",query)

    def test_failure_propagates(self):
        result,_=self.execute(fail=True)
        self.assertEqual(result.returncode,7)

    def test_echo_workflow_is_read_only(self):
        result,query=self.execute('echo')
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertNotIn('@',query)
        self.assertIn('NO_REPORT_BODY',query)
        for keyword in ['INSERT INTO','UPDATE ','DELETE FROM','CREATE TABLE']:
            self.assertNotIn(keyword,query)


if __name__=='__main__':
    unittest.main()
