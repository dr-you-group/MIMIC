"""Execute NOTE-only SQL with PG* environment credentials and external audits.

Only NOTE rows and optional NOTE indexes can change. No source/raw loading,
permanent audit tables, CDM extensions, or migration of existing column types.
"""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess

from load_modality_raw import ident

ROOT = Path(__file__).resolve().parents[1]


def now():
    return datetime.now(timezone.utc).isoformat()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def command(args):
    cmd = ['docker','run','--rm','-i','--network','host',
           '--mount',f'type=bind,src={ROOT},dst=/work,readonly',
           '--mount',f'type=bind,src={args.audit.resolve()},dst=/audit']
    for key in ['PGHOST','PGPORT','PGUSER','PGDATABASE','PGPASSWORD','PGSSLMODE','PGCONNECT_TIMEOUT','PGOPTIONS']:
        if key in os.environ:
            cmd += ['-e',key]
    return cmd + ['postgres:16','psql','-X','-qAt','-v','ON_ERROR_STOP=1',
                  '-v','cdm_schema='+args.cdm_schema,'-v','radiology_schema='+args.radiology_schema]


def fingerprint(args, name):
    fields = ['note_id','person_id','note_date','note_datetime','note_type_concept_id',
              'note_class_concept_id','note_title',"encode(sha256(convert_to(note_text,'UTF8')),'hex')",
              'encoding_concept_id','language_concept_id','provider_id','visit_detail_id','note_source_value']
    expr = "encode(sha256(convert_to(jsonb_build_array("+','.join(fields)+")::text,'UTF8')),'hex')"
    sql = f'COPY (SELECT note_id,{expr},visit_occurrence_id FROM {ident(args.cdm_schema)}.note ORDER BY note_id) TO STDOUT WITH (FORMAT CSV);'
    digest = hashlib.sha256()
    rows = 0
    with (args.audit/(name+'.csv')).open('wb') as out, (args.audit/(name+'.log')).open('w') as log:
        process = subprocess.Popen(command(args)+['-c',sql],stdout=subprocess.PIPE,stderr=log)
        for line in process.stdout:
            out.write(line); digest.update(line); rows += 1
        process.stdout.close()
        if process.wait():
            raise RuntimeError('NOTE fingerprint failed: '+name)
    return {'rows':rows,'sha256':digest.hexdigest()}


def preserved(before, after, changes):
    count = 0
    seen = set()
    with before.open() as left, after.open() as right:
        new = iter(csv.reader(right)); current = next(new,None)
        for old in csv.reader(left):
            while current is not None and int(current[0]) < int(old[0]):
                current = next(new,None)
            if current is None or current[:2] != old[:2]:
                raise ValueError('Pre-existing NOTE ID/text/descriptive fields changed')
            if current[2] != old[2]:
                if old[2] or changes.get(old[0]) != current[2]:
                    raise ValueError('Visit changed without an audited null-to-confirmed mapping')
                seen.add(old[0])
            count += 1
    if seen != set(changes):
        raise ValueError('Visit update audit does not match the actual changes')
    return count


def main(args):
    for schema in [args.source_schema,args.radiology_schema,args.cdm_schema]:
        ident(schema)
    args.audit.mkdir(parents=True,exist_ok=True)
    if (args.audit/'run.json').exists():
        raise ValueError('Use a new audit directory for each execution')
    os.environ['PGOPTIONS'] = os.environ.get('PGOPTIONS','')+' -c TimeZone=UTC -c DateStyle=ISO,YMD'
    files = ['note.sql','st_cxr_note.sql','st_ecg_note.sql','audit_echo_note.sql','qa_note.sql','index_note.sql']
    hashes = {f:hashlib.sha256((ROOT/'etl/postgres'/f).read_bytes()).hexdigest() for f in files}
    state = {'status':'RUNNING','started_at_utc':now(),'host':os.environ.get('PGHOST'),
             'database':os.environ.get('PGDATABASE'),'sql_sha256':hashes,'sources':args.sources,'steps':[]}
    save(args.audit/'run.json',state)
    try:
        state['before'] = fingerprint(args,'note_before'); save(args.audit/'run.json',state)
        changes = {}
        for source in args.sources:
            for f, digest in hashes.items():
                if hashlib.sha256((ROOT/'etl/postgres'/f).read_bytes()).hexdigest() != digest:
                    raise ValueError('SQL changed during execution: '+f)
            schema = args.radiology_schema if source in ['discharge','radiology'] else args.source_schema
            file = 'audit_echo_note.sql' if source == 'echo' else 'note.sql'
            step = {'source':source,'started_at_utc':now()}; state['steps'].append(step)
            save(args.audit/'run.json',state)
            with (args.audit/(source+'.log')).open('w') as log:
                subprocess.run(command(args)+['-v','source_schema='+schema,'-v','source_table='+source,
                    '-v','audit_file=/audit/'+source+'.csv','-f','/work/etl/postgres/'+file],
                    stdout=log,stderr=subprocess.STDOUT,check=True)
            lines = (args.audit/(source+'.log')).read_text().splitlines()
            step['report'] = json.loads(next(x.removeprefix('NOTE_ETL_REPORT ') for x in lines if x.startswith('NOTE_ETL_REPORT ')))
            with (args.audit/(source+'.csv')).open() as stream:
                for row in csv.DictReader(stream):
                    if row.get('action') == 'VISIT_UPDATE':
                        changes[row['note_id']] = row['visit_occurrence_id']
            step.update(status='PASS',finished_at_utc=now())
            save(args.audit/'run.json',state); print(json.dumps(step),flush=True)
        state['after'] = fingerprint(args,'note_after')
        state['preexisting_notes_preserved'] = preserved(args.audit/'note_before.csv',args.audit/'note_after.csv',changes)
        with (args.audit/'qa.log').open('w') as log:
            subprocess.run(command(args)+['-f','/work/etl/postgres/qa_note.sql'],stdout=log,stderr=subprocess.STDOUT,check=True)
        state['qa'] = json.loads(next(x.removeprefix('NOTE_CONVENTION_REPORT ') for x in (args.audit/'qa.log').read_text().splitlines() if x.startswith('NOTE_CONVENTION_REPORT ')))
        if args.indexes:
            with (args.audit/'indexes.log').open('w') as log:
                subprocess.run(command(args)+['-f','/work/etl/postgres/index_note.sql'],stdout=log,stderr=subprocess.STDOUT,check=True)
        state.update(status='PASS',finished_at_utc=now())
    except Exception:
        state.update(status='FAILED',finished_at_utc=now())
        raise
    finally:
        save(args.audit/'run.json',state)
    print(json.dumps(state),flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--audit',type=Path,required=True)
    p.add_argument('--source-schema',default='mimiciv_modality')
    p.add_argument('--radiology-schema',default='mimiciv_note')
    p.add_argument('--cdm-schema',default='mimiciv')
    p.add_argument('--sources',nargs='+',choices=['discharge','radiology','cxr','ecg_machine','echo'],
                   default=['discharge','radiology','cxr','ecg_machine','echo'])
    p.add_argument('--indexes',action='store_true',help='Create indexes on existing NOTE columns after validation')
    main(p.parse_args())
