"""Load prepared raw modality CSVs and verify every field on PostgreSQL.

Connection parameters are standard PG* environment variables. Requires Docker
and postgres:16. All table data stays outside the repository. Reruns require
the same source file and exact row multiset; changed releases need reconciliation.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


def ident(value):
    if not re.fullmatch(r'[a-z_][a-z0-9_]*', value):
        raise ValueError('Invalid SQL identifier')
    return '"'+value+'"'


def command(folder):
    args = ['docker','run','--rm','-i','--network','host',
            '--mount',f'type=bind,src={folder.resolve()},dst=/input,readonly']
    for key in ['PGHOST','PGPORT','PGUSER','PGDATABASE','PGPASSWORD','PGSSLMODE','PGCONNECT_TIMEOUT','PGOPTIONS']:
        if key in os.environ:
            args += ['-e',key]
    return args+['postgres:16','psql','-X','-qAt','-v','ON_ERROR_STOP=1']


def run_sql(folder, sql):
    result = subprocess.run(command(folder), input=sql, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout


def verify_table(folder, schema, record):
    columns = ','.join(ident(c) for c in record['columns'])
    digest_expr = "encode(sha256(convert_to(jsonb_build_array("+columns+")::text,'UTF8')),'hex')"
    sql = f'COPY (SELECT h FROM (SELECT {digest_expr} AS h FROM {ident(schema)}.{ident(record["table"])}) hashes ORDER BY h COLLATE "C") TO STDOUT;'
    process = subprocess.Popen(command(folder)+['-c',sql], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    digest = hashlib.sha256()
    count = 0
    for line in process.stdout:
        digest.update(line.rstrip(b'\r\n')+b'\n')
        count += 1
    process.stdout.close()
    stderr = process.stderr.read().decode()
    if process.wait():
        raise RuntimeError(stderr)
    actual = digest.hexdigest()
    if count != record['rows'] or actual != record['row_multiset_sha256']:
        raise ValueError('Raw field comparison failed: '+record['table'])
    return {'rows': count, 'row_multiset_sha256': actual, 'status': 'PASS'}


def load(folder, schema, report_file):
    manifest = json.loads((folder/'prepared_manifest.json').read_text())
    report = {'tables': {}, 'host': os.environ.get('PGHOST'),
              'port': os.environ.get('PGPORT','5432'), 'database': os.environ.get('PGDATABASE'),
              'schema': schema, 'started_at_utc': datetime.now(timezone.utc).isoformat()}
    source_schema = ident(schema)
    run_sql(folder, f'''CREATE SCHEMA IF NOT EXISTS {source_schema};
        CREATE TABLE IF NOT EXISTS {source_schema}.source_file (
            table_name text PRIMARY KEY, source_file text NOT NULL,
            source_version text NOT NULL, source_sha256 text NOT NULL,
            published_sha256 text NOT NULL, checksum_status text NOT NULL,
            source_rows bigint NOT NULL, row_multiset_sha256 text NOT NULL
        );''')
    for record in manifest['tables']:
        table = ident(record['table'])
        columns = ','.join(ident(c) for c in record['columns'])
        existing = run_sql(folder, f"SELECT source_sha256 FROM {source_schema}.source_file WHERE table_name='{record['table']}';").strip()
        if existing and existing != record['source_sha256']:
            raise ValueError('Raw source changed; reconcile before replacing: '+record['table'])
        if not existing:
            definition = ', '.join(ident(c)+' text NOT NULL' for c in record['columns'])
            key = 'dicom_id' if record['table'] in ('cxr_metadata','cxr_record_list') else 'study_id'
            pk = '' if record['table']=='ecg_waveform_note_links' else f', PRIMARY KEY ({ident(key)})'
            values = [record[k] for k in ['table','source_file','source_version','source_sha256','published_sha256','checksum_status']]
            literals = ','.join("'"+v.replace("'","''")+"'" for v in values)
            sql = f'''BEGIN;
                CREATE TABLE {source_schema}.{table} ({definition}{pk});
                \\copy {source_schema}.{table} ({columns}) FROM '/input/{record['table']}.csv' WITH (FORMAT CSV, HEADER, FORCE_NOT_NULL ({columns}))
                INSERT INTO {source_schema}.source_file VALUES ({literals},{record['rows']},'{record['row_multiset_sha256']}');
                COMMIT;
                ANALYZE {source_schema}.{table};'''
            run_sql(folder, sql)
        report['tables'][record['table']] = verify_table(folder, schema, record)
        report_file.write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps({'table':record['table'],**report['tables'][record['table']]}),flush=True)
    report['status'] = 'PASS'
    report['finished_at_utc'] = datetime.now(timezone.utc).isoformat()
    report_file.write_text(json.dumps(report,indent=2)+'\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',type=Path,required=True)
    parser.add_argument('--schema',default='mimiciv_modality')
    parser.add_argument('--report',type=Path,required=True)
    args = parser.parse_args()
    load(args.input,args.schema,args.report)
