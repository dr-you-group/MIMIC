"""Run the modality SQL add-on against an existing PostgreSQL CDM.

Uses standard PG* environment variables, Docker postgres:16 and verified raw
inputs. Captures SQL hashes and every NOTE row before/after. An explicit
one-time correction can clear CXR proxy datetimes; all other fields must
remain identical. No credentials are written to the audit.
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
    path.write_text(json.dumps(value, indent=2)+'\n')


def command(args):
    cmd=['docker','run','--rm','-i','--network','host',
         '--mount',f'type=bind,src={ROOT},dst=/work,readonly']
    for key in ['PGHOST','PGPORT','PGUSER','PGDATABASE','PGPASSWORD','PGSSLMODE','PGCONNECT_TIMEOUT','PGOPTIONS']:
        if key in os.environ:cmd+=['-e',key]
    return cmd+['postgres:16','psql','-X','-qAt','-v','ON_ERROR_STOP=1',
        '-v','source_schema='+args.source_schema, '-v','radiology_schema='+args.radiology_schema,
        '-v','cdm_schema='+args.cdm_schema, '-v','etl_schema='+args.etl_schema]


def fingerprint(args, name, clear_cxr_datetime=False):
    time_expr=("CASE WHEN note_source_value IN ('CXR','CXR_SUPPLEMENT','CXR_VARIANT') THEN NULL::timestamp ELSE note_datetime END"
               if clear_cxr_datetime else 'note_datetime')
    fields=['note_id','person_id','note_date',time_expr,'note_type_concept_id',
        'note_class_concept_id','note_title',"encode(sha256(convert_to(note_text,'UTF8')),'hex')",
        'encoding_concept_id','language_concept_id','provider_id','visit_occurrence_id',
        'visit_detail_id','note_source_value']
    expr="encode(sha256(convert_to(jsonb_build_array("+','.join(fields)+")::text,'UTF8')),'hex')"
    sql=f'COPY (SELECT note_id,{expr} FROM {ident(args.cdm_schema)}.note ORDER BY note_id) TO STDOUT WITH (FORMAT CSV);'
    digest=hashlib.sha256();rows=0
    with (args.audit/(name+'.csv')).open('wb') as out, (args.audit/(name+'.log')).open('w') as log:
        process=subprocess.Popen(command(args)+['-c',sql],stdout=subprocess.PIPE,stderr=log)
        for line in process.stdout:
            canonical=line.rstrip(b'\r\n')+b'\n';out.write(canonical);digest.update(canonical);rows+=1
        process.stdout.close()
        if process.wait():raise RuntimeError('NOTE fingerprint query failed: '+name)
    return {'rows':rows,'sha256':digest.hexdigest()}


def preserved(before, after):
    count=0
    with before.open() as left, after.open() as right:
        old=csv.reader(left);new=iter(csv.reader(right));current=next(new,None)
        for row in old:
            while current is not None and int(current[0]) < int(row[0]):current=next(new,None)
            if current != row:raise ValueError('A pre-existing NOTE was removed or changed')
            count+=1
    return count


def main(args):
    for schema in [args.source_schema,args.radiology_schema,args.cdm_schema,args.etl_schema]:ident(schema)
    args.audit.mkdir(parents=True,exist_ok=True)
    manifest=json.loads((args.input/'prepared_manifest.json').read_text())
    verification=json.loads(args.raw_report.read_text())
    if verification.get('status')!='PASS':raise ValueError('Raw inputs are not verified')
    if verification.get('host')!=os.environ.get('PGHOST') or verification.get('database')!=os.environ.get('PGDATABASE') or verification.get('schema')!=args.source_schema:
        raise ValueError('Raw verification belongs to another database/schema')
    for record in manifest['tables']:
        checked=verification['tables'][record['table']]
        if checked['rows']!=record['rows'] or checked['row_multiset_sha256']!=record['row_multiset_sha256']:
            raise ValueError('Raw verification belongs to another input release')
    # Pin date serialization independently of the server/client defaults.
    os.environ['PGOPTIONS']=os.environ.get('PGOPTIONS','')+' -c TimeZone=UTC -c DateStyle=ISO,YMD'
    files=['st_modality_note.sql','note.sql','ext_modality_note.sql']
    hashes={f:hashlib.sha256((ROOT/'etl/postgres'/f).read_bytes()).hexdigest() for f in files}
    state={'started_at_utc':now(),'host':os.environ.get('PGHOST'),'database':os.environ.get('PGDATABASE'),
           'sql_sha256':hashes,'steps':[]}
    save(args.audit/'run.json',state)
    state['before']=fingerprint(args,'note_before');save(args.audit/'run.json',state)
    if args.clear_cxr_proxy_datetime:
        if 'cxr' not in args.sources:raise ValueError('CXR datetime correction requires --sources cxr')
        state['expected_before_after_datetime_correction']=fingerprint(args,'note_expected',True)
        result=subprocess.run(command(args)+['-c',
            f"SELECT count(*) FROM {ident(args.cdm_schema)}.note WHERE note_source_value IN ('CXR','CXR_SUPPLEMENT','CXR_VARIANT') AND note_datetime IS NOT NULL;"],
            text=True,capture_output=True,check=True)
        state['allowed_cxr_datetime_corrections']=int(result.stdout.strip())
        save(args.audit/'run.json',state)
    sources={r['table']:r for r in manifest['tables']}
    source_rules={'cxr':('2.1.0','cxr_reports'), 'ecg_machine':('1.0','ecg_machine_measurements')}
    phases=[('st_modality_note.sql',[])]
    for source in args.sources:
        version,raw=source_rules[source]
        phases.append(('note.sql',['-v','source_table='+source,'-v','source_version='+version,
                                  '-v','source_sha256='+sources[raw]['source_sha256']]))
    phases.append(('ext_modality_note.sql',[]))
    state['sources']=args.sources
    for index,(file,variables) in enumerate(phases):
        if hashlib.sha256((ROOT/'etl/postgres'/file).read_bytes()).hexdigest()!=hashes[file]:
            raise ValueError('SQL changed during the run')
        step={'script':file,'started_at_utc':now()};state['steps'].append(step);save(args.audit/'run.json',state)
        logfile=args.audit/f'{index:02d}_{file}.log'
        with logfile.open('w') as log:
            result=subprocess.run(command(args)+variables+['-f','/work/etl/postgres/'+file],stdout=log,stderr=subprocess.STDOUT)
        if result.returncode:
            step['status']='FAILED';save(args.audit/'run.json',state)
            raise RuntimeError('SQL failed; see '+str(logfile))
        step.update(status='PASS',finished_at_utc=now())
        reports=[line.strip().removeprefix('NOTE_ETL_REPORT ') for line in logfile.read_text().splitlines()
                 if line.strip().startswith('NOTE_ETL_REPORT ')]
        if reports:step['report']=json.loads(reports[-1])
        save(args.audit/'run.json',state);print(json.dumps(step),flush=True)
    state['after']=fingerprint(args,'note_after')
    expected='note_expected.csv' if args.clear_cxr_proxy_datetime else 'note_before.csv'
    state['preexisting_notes_preserved']=preserved(args.audit/expected,args.audit/'note_after.csv')
    schema=ident(args.etl_schema)
    query=f"SELECT jsonb_agg(r) FROM (SELECT source_table,load_status,checksum_status,count(*) AS rows FROM {schema}.modality_note_link GROUP BY 1,2,3 ORDER BY 1,2,3) r;"
    result=subprocess.run(command(args)+['-c',query],text=True,capture_output=True,check=True)
    state['modality_counts']=json.loads(result.stdout)
    state.update(status='PASS',finished_at_utc=now());save(args.audit/'run.json',state)
    print(json.dumps(state),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',type=Path,required=True)
    p.add_argument('--raw-report',type=Path,required=True)
    p.add_argument('--audit',type=Path,required=True)
    p.add_argument('--source-schema',default='mimiciv_modality')
    p.add_argument('--radiology-schema',default='mimiciv_note')
    p.add_argument('--cdm-schema',default='mimiciv')
    p.add_argument('--etl-schema',default='mimiciv_note_etl')
    p.add_argument('--sources',nargs='+',choices=['cxr','ecg_machine'],default=['cxr','ecg_machine'],
                   help='NOTE sources to load; omitted sources must already have completed their NOTE ETL')
    p.add_argument('--clear-cxr-proxy-datetime',action='store_true',
                   help='Allow only clearing CXR note_datetime; preserve source acquisition time in the extension')
    main(p.parse_args())
