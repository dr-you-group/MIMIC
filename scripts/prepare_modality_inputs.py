"""Prepare note-only PhysioNet files for raw SQL loading; no CDM mapping here.

The download manifest supplies the published checksums. Echo metadata with a
known mismatch may be retained explicitly for quarantine, never as NOTE text.
No credentials, patient data, or prepared files belong in the Git repository.
"""
import argparse
import csv
import gzip
import hashlib
import json
from pathlib import Path
import re
import zipfile

SOURCES = {
    'cxr_reports': 'mimic-cxr/2.1.0/mimic-cxr-reports.zip',
    'cxr_study_list': 'mimic-cxr/2.1.0/cxr-study-list.csv.gz',
    'cxr_record_list': 'mimic-cxr/2.1.0/cxr-record-list.csv.gz',
    'cxr_provider_list': 'mimic-cxr/2.1.0/cxr-provider-list.csv.gz',
    'cxr_metadata': 'mimic-cxr-jpg/2.1.0/mimic-cxr-2.0.0-metadata.csv.gz',
    'ecg_machine_measurements': 'mimic-iv-ecg/1.0/machine_measurements.csv',
    'ecg_record_list': 'mimic-iv-ecg/1.0/record_list.csv',
    'ecg_waveform_note_links': 'mimic-iv-ecg/1.0/waveform_note_links.csv',
    'echo_study_list': 'mimic-iv-echo/1.0.1/echo-study-list.csv',
}


def file_hash(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def row_hash(row):
    # PostgreSQL jsonb_build_array(text, ...) uses this same representation.
    return hashlib.sha256(json.dumps(row, ensure_ascii=False).encode()).hexdigest()


def write_bigquery_loading_files(output, manifest):
    for record in manifest['tables']:
        schema = [dict(name=c, type='STRING', mode='REQUIRED') for c in record['columns']]
        (output/(record['table']+'.schema.json')).write_text(json.dumps(schema,indent=2)+'\n')
    fields = ['table_name','source_file','source_version','source_sha256','published_sha256',
              'checksum_status','source_rows','row_multiset_sha256']
    with (output/'source_file.csv').open('w',newline='') as f:
        writer=csv.writer(f,lineterminator='\n');writer.writerow(fields)
        for r in manifest['tables']:
            writer.writerow([r['table'],r['source_file'],r['source_version'],r['source_sha256'],
                             r['published_sha256'],r['checksum_status'],r['rows'],r['row_multiset_sha256']])
    schema=[dict(name=c,type='INTEGER' if c=='source_rows' else 'STRING',mode='REQUIRED') for c in fields]
    (output/'source_file.schema.json').write_text(json.dumps(schema,indent=2)+'\n')


def prepare(data, output, retain_echo=False):
    manifest = json.loads((data/'DOWNLOAD_MANIFEST.json').read_text())
    inventory = manifest['inventory']
    if 'files' in inventory:
        inventory = inventory['files']
    output.mkdir(parents=True, exist_ok=True)
    result = {'tables': [], 'scope': 'raw note text and linkage only; no CDM ETL'}
    csv.field_size_limit(10_000_000)
    for table, relative in SOURCES.items():
        source = data/relative
        item = inventory[relative]
        expected = item.get('published_sha256', item.get('sha256'))
        actual = file_hash(source)
        checksum_status = 'VERIFIED' if actual == expected else 'CHECKSUM_MISMATCH'
        if checksum_status != 'VERIFIED' and not (
            table == 'echo_study_list' and retain_echo
            and item.get('independent_downloads_match') is True
            and actual == item.get('actual_sha256')
        ):
            raise ValueError('Unverified source: ' + relative)
        target = output/(table+'.csv')
        hashes = []
        with target.open('w', encoding='utf-8', newline='') as out:
            writer = csv.writer(out, lineterminator='\n')
            if table == 'cxr_reports':
                columns = ['subject_id', 'study_id', 'text']
                writer.writerow(columns)
                with zipfile.ZipFile(source) as archive:
                    for name in sorted(archive.namelist()):
                        match = re.search(r'p(\d{8})/s(\d{8})\.txt$', name)
                        if not match:
                            continue
                        row = [*match.groups(), archive.read(name).decode('utf-8')]
                        writer.writerow(row)
                        hashes.append(row_hash(row))
            else:
                opener = gzip.open if source.suffix == '.gz' else open
                with opener(source, 'rt', encoding='utf-8', newline='') as inp:
                    reader = csv.reader(inp)
                    source_columns = next(reader)
                    columns = [re.sub(r'[^a-z0-9_]', '', c.lower()) for c in source_columns]
                    if len(set(columns)) != len(columns):
                        raise ValueError('Duplicate normalized column name')
                    writer.writerow(columns)
                    for row in reader:
                        if len(row) != len(columns):
                            raise ValueError('Malformed source row: ' + relative)
                        writer.writerow(row)
                        hashes.append(row_hash(row))
        rows_digest = hashlib.sha256()
        for digest in sorted(hashes):
            rows_digest.update((digest+'\n').encode())
        record = dict(table=table, file=str(target.resolve()), columns=columns,
                      rows=len(hashes), row_multiset_sha256=rows_digest.hexdigest(),
                      prepared_sha256=file_hash(target), source_file=relative,
                      source_version=relative.split('/')[1], source_sha256=actual,
                      published_sha256=expected, checksum_status=checksum_status)
        result['tables'].append(record)
        (output/'prepared_manifest.json').write_text(json.dumps(result, indent=2)+'\n')
        print(json.dumps({k: record[k] for k in ['table','rows','checksum_status']}), flush=True)
    write_bigquery_loading_files(output,result)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--retain-unverified-echo-links', action='store_true')
    args = parser.parse_args()
    prepare(args.data, args.output, args.retain_unverified_echo_links)
