"""Isolated PostgreSQL tests: Docker postgres:16, no production connections."""
import hashlib
import json
from pathlib import Path
import subprocess
import time
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[2]


def note_id(table, source_id):
    dataset = {'cxr':'mimic-cxr','ecg_machine':'mimic-iv-ecg'}.get(table,'mimic-iv-note')
    return int(hashlib.sha256(f'{dataset}|{table}|{source_id}'.encode()).hexdigest()[:15],16)


class PostgresNoteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container = 'note-only-test-'+uuid.uuid4().hex[:10]
        subprocess.run(['docker','run','-d','--rm','--network','none','--name',cls.container,
            '-e','POSTGRES_HOST_AUTH_METHOD=trust','--mount',f'type=bind,src={ROOT},dst=/work,readonly',
            'postgres:16'],check=True,stdout=subprocess.DEVNULL)
        cls.addClassCleanup(lambda: subprocess.run(['docker','rm','-f',cls.container],stdout=subprocess.DEVNULL))
        for _ in range(60):
            if subprocess.run(['docker','exec',cls.container,'pg_isready','-h','127.0.0.1','-U','postgres'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode == 0:
                return
            time.sleep(.5)
        raise RuntimeError('Test database did not start')

    def sql(self, sql):
        r = subprocess.run(['docker','exec','-i',self.container,'psql','-X','-qAt','-U','postgres','-v','ON_ERROR_STOP=1'],input=sql,text=True,capture_output=True)
        self.assertEqual(r.returncode,0,r.stderr)
        return r.stdout.strip()

    def run_etl(self, source='discharge', success=True):
        r = subprocess.run(['docker','exec',self.container,'psql','-X','-qAt','-U','postgres',
            '-v','source_schema=raw','-v','radiology_schema=raw','-v','cdm_schema=cdm',
            '-v','source_table='+source,'-v','audit_file=/tmp/'+source+'.csv',
            '-f','/work/etl/postgres/note.sql'],text=True,capture_output=True)
        if not success:
            self.assertNotEqual(r.returncode,0,r.stdout)
            return r
        self.assertEqual(r.returncode,0,r.stderr)
        return json.loads(next(x.removeprefix('NOTE_ETL_REPORT ') for x in r.stdout.splitlines() if x.startswith('NOTE_ETL_REPORT ')))

    def setUp(self):
        self.sql('''DROP SCHEMA IF EXISTS raw CASCADE; DROP SCHEMA IF EXISTS cdm CASCADE;
        CREATE SCHEMA raw; CREATE SCHEMA cdm;
        CREATE TABLE cdm.person(person_id bigint PRIMARY KEY,person_source_value text);
        INSERT INTO cdm.person VALUES(1001,'101'),(1002,'102');
        CREATE TABLE cdm.visit_occurrence(visit_occurrence_id bigint PRIMARY KEY,person_id bigint,
            visit_source_value text,visit_start_datetime timestamp,visit_end_datetime timestamp);
        INSERT INTO cdm.visit_occurrence VALUES
            (2001,1001,'101|201','2100-01-01','2100-01-03'),
            (2002,1002,'102|301','2100-01-01','2100-01-03'),
            (2003,1001,'101|2100-01-04','2100-01-04','2100-01-05');
        CREATE TABLE cdm.visit_detail(visit_detail_id bigint,person_id bigint,visit_occurrence_id bigint);
        CREATE TABLE cdm.provider(provider_id bigint);
        CREATE TABLE cdm.procedure_occurrence(procedure_occurrence_id bigint,person_id bigint);
        INSERT INTO cdm.procedure_occurrence VALUES(7,1001);
        CREATE TABLE cdm.concept(concept_id integer,domain_id text,vocabulary_id text,concept_code text,
            standard_concept text,invalid_reason text,valid_start_date date DEFAULT '1970-01-01',
            valid_end_date date DEFAULT '2099-12-31');
        INSERT INTO cdm.concept(concept_id,domain_id,vocabulary_id,concept_code,standard_concept) VALUES
            (32817,'Type Concept','Type Concept','EHR','S'),(32678,'Metadata','Metadata','UTF8','S'),
            (4180186,'Language','SNOMED','297487008','S'),(3020091,'Note','LOINC','18842-5','S'),
            (40771183,'Note','LOINC','68604-8','S'),(99999,'Note','LOINC','75425-9','S');
        CREATE TABLE cdm.note(note_id bigint PRIMARY KEY,person_id bigint NOT NULL,note_date date NOT NULL,
            note_datetime timestamp,note_type_concept_id integer NOT NULL,note_class_concept_id integer NOT NULL,
            note_title varchar(250),note_text text NOT NULL,encoding_concept_id integer NOT NULL,
            language_concept_id integer NOT NULL,provider_id integer,visit_occurrence_id bigint,visit_detail_id bigint,
            note_source_value varchar(50));
        CREATE TABLE cdm.note_nlp(note_nlp_id integer PRIMARY KEY,note_id integer);
        CREATE TABLE raw.discharge(note_id text,subject_id integer,hadm_id integer,note_type text,
            charttime timestamp,storetime timestamp,text text);
        CREATE TABLE raw.radiology(LIKE raw.discharge);
        CREATE TABLE raw.radiology_detail(note_id text,field_name text,field_value text);
        INSERT INTO raw.discharge VALUES
            ('101-DS-1',101,201,'DS','2100-01-02 12:00','2100-01-03',E'한글, spaces  \\r\\ntext  '),
            ('101-AD-1',101,999,'AD','2100-01-02',NULL,'Unmatched explicit admission'),
            ('999-DS-1',999,201,'DS','2100-01-02',NULL,'Missing person');
        INSERT INTO raw.radiology VALUES
            ('101-RR-1',101,NULL,'RR','2100-01-02 12:00',NULL,'FINDINGS: No acute disease.'),
            ('101-RR-2',101,NULL,'RR','2100-01-04 12:00',NULL,'No numeric visit.'),
            ('101-AR-1',101,201,'AR','2100-01-05',NULL,'Outside time but explicit admission.');
        CREATE TABLE raw.source_file(table_name text,checksum_status text);
        INSERT INTO raw.source_file VALUES('cxr_reports','VERIFIED'),('cxr_metadata','VERIFIED'),
            ('ecg_machine_measurements','VERIFIED');
        CREATE TABLE raw.cxr_reports(subject_id text,study_id text,text text);
        INSERT INTO raw.cxr_reports VALUES
            ('101','501','FINAL REPORT
        FINDINGS: No acute disease.'),
            ('101','502','INDICATION: fever. FINDINGS: No acute disease.'),
            ('101','503','A genuinely different document.');
        CREATE TABLE raw.cxr_metadata(subject_id text,study_id text,studydate text,studytime text);
        INSERT INTO raw.cxr_metadata VALUES('101','501','21000102','120000'),
            ('101','502','21000102','120001'),('101','503','21000102','120002');
        ''')
        columns = ','.join(f'report_{i} text' for i in range(18))
        self.sql(f"CREATE TABLE raw.ecg_machine_measurements(subject_id text,study_id text,ecg_time text,{columns});")
        self.sql("INSERT INTO raw.ecg_machine_measurements(subject_id,study_id,ecg_time,report_0,report_2) VALUES ('101','701','2100-01-02 12:00','Sinus rhythm','Normal ECG'),('101','702','2100-01-02 12:01','Sinus rhythm','Normal ECG'),('101','703','2100-01-02 12:02','','');")

    def test_text_identity_incremental_and_idempotent(self):
        a = self.run_etl()
        self.assertEqual(a['inserted_rows'],2)
        self.assertEqual(a['load_status']['MISSING_PERSON'],1)
        self.assertEqual(self.run_etl()['changed_note_rows'],0)
        self.run_etl('radiology')
        self.assertEqual(self.sql('SELECT count(*) FROM cdm.note'),'5')
        self.assertEqual(self.sql(f"SELECT n.note_text=r.text FROM cdm.note n JOIN raw.discharge r ON r.note_id='101-DS-1' WHERE n.note_id={note_id('discharge','101-DS-1')}"),'t')
        self.assertEqual(self.sql("SELECT count(*) FROM pg_namespace WHERE nspname='mimiciv_note_etl'"),'0')
        self.assertEqual(self.sql('SELECT count(*) FROM cdm.procedure_occurrence'),'1')
        self.assertEqual(self.sql("SELECT data_type FROM information_schema.columns WHERE table_schema='cdm' AND table_name='note_nlp' AND column_name='note_id'"),'integer')

    def test_explicit_hadm_no_fallback_and_document_interval(self):
        self.run_etl(); self.run_etl('radiology')
        self.assertEqual(self.sql("SELECT coalesce(visit_occurrence_id::text,'NULL') FROM cdm.note WHERE note_source_value='AD'"),'NULL')
        self.assertEqual(self.sql(f"SELECT visit_occurrence_id FROM cdm.note WHERE note_id={note_id('radiology','101-RR-1')}"),'2001')
        self.assertEqual(self.sql(f"SELECT coalesce(visit_occurrence_id::text,'NULL') FROM cdm.note WHERE note_id={note_id('radiology','101-RR-2')}"),'NULL')
        self.assertEqual(self.sql("SELECT visit_occurrence_id FROM cdm.note WHERE note_source_value='AR'"),'2001')

    def test_ambiguous_interval_is_null(self):
        self.sql("INSERT INTO cdm.visit_occurrence VALUES(2010,1001,'101|202','2100-01-01','2100-01-03')")
        r=self.run_etl('radiology')
        self.assertEqual(r['visit_status']['AMBIGUOUS_ADMISSION_INTERVAL'],1)
        self.assertEqual(self.sql(f"SELECT visit_occurrence_id IS NULL FROM cdm.note WHERE note_id={note_id('radiology','101-RR-1')}"),'t')

    def test_rerun_fills_only_null_visit(self):
        self.run_etl('radiology')
        self.sql(f"UPDATE cdm.note SET visit_occurrence_id=NULL WHERE note_id={note_id('radiology','101-RR-1')}")
        self.assertEqual(self.run_etl('radiology')['visit_updated_rows'],1)
        self.assertEqual(self.run_etl('radiology')['changed_note_rows'],0)

    def test_ecg_slots_and_device_clock(self):
        r=self.run_etl('ecg_machine')
        self.assertEqual(r['inserted_rows'],2)
        self.assertEqual(r['load_status']['EMPTY_TEXT'],1)
        self.assertEqual(self.sql('SELECT count(*) FROM cdm.note WHERE visit_occurrence_id IS NOT NULL'),'0')
        self.assertEqual(self.sql("SELECT bool_and(length(note_text)-length(replace(note_text,E'\\n',''))=17) FROM cdm.note"),'t')
        self.assertEqual(self.sql('SELECT count(DISTINCT note_id) FROM cdm.note'),'2')
        self.assertEqual(self.run_etl('ecg_machine')['changed_note_rows'],0)

    def test_cxr_only_complete_match_and_no_acquisition_date_insertion(self):
        self.run_etl('radiology')
        r=self.run_etl('cxr')
        self.assertEqual(r['inserted_rows'],0)
        self.assertEqual(r['load_status']['REUSED_EXISTING_RADIOLOGY'],1)
        self.assertEqual(r['load_status']['MISSING_DOCUMENT_DATE'],2)

    def test_cxr_legacy_date_rows_preserved(self):
        self.sql(f"INSERT INTO cdm.note VALUES({note_id('cxr','101-CXR-503')},1001,'2100-01-02',NULL,32817,40771183,'Chest radiograph report','A genuinely different document.',32678,4180186,NULL,NULL,NULL,'CXR')")
        r=self.run_etl('cxr')
        self.assertEqual(r['load_status']['LEGACY_DATE_PROXY_RETAINED'],1)
        self.assertEqual(r['changed_note_rows'],0)

    def test_cxr_prefix_retained_but_not_given_final_report_date(self):
        self.run_etl('radiology')
        self.sql("UPDATE raw.cxr_reports SET text=E'WET READ: Different findings.\\nFINAL REPORT\\nFINDINGS: No acute disease.' WHERE study_id='501'")
        r=self.run_etl('cxr')
        self.assertEqual(r['source_rows'],4)
        self.assertEqual(r['load_status']['REUSED_EXISTING_RADIOLOGY'],1)
        self.assertEqual(r['load_status']['MISSING_DOCUMENT_DATE'],3)

    def test_existing_text_change_rejected(self):
        self.run_etl()
        self.sql("UPDATE raw.discharge SET text='Changed' WHERE note_id='101-DS-1'")
        self.assertIn('existing_identity_and_text',self.run_etl(success=False).stderr)

    def test_missing_date_held_without_rejecting_other_rows(self):
        self.sql("UPDATE raw.discharge SET charttime=NULL WHERE note_id='101-AD-1'")
        r=self.run_etl()
        self.assertEqual(r['load_status']['MISSING_DOCUMENT_DATE'],1)
        self.assertEqual(r['inserted_rows'],1)

    def test_fanout_and_invalid_concept_roll_back(self):
        self.sql("INSERT INTO cdm.visit_occurrence VALUES(2010,1001,'101|201','2100-01-01','2100-01-03')")
        self.assertIn('unambiguous_explicit_hadm',self.run_etl(success=False).stderr)
        self.assertEqual(self.sql('SELECT count(*) FROM cdm.note'),'0')
        self.sql("DELETE FROM cdm.visit_occurrence WHERE visit_occurrence_id=2010; UPDATE cdm.concept SET domain_id='Observation' WHERE concept_code='18842-5'")
        self.assertIn('document_class_mapping',self.run_etl(success=False).stderr)

    def test_no_column_migration(self):
        self.sql('ALTER TABLE cdm.note ALTER COLUMN note_id TYPE integer')
        self.assertIn('existing_bigint_identifiers',self.run_etl(success=False).stderr)

    def test_trigger_modification_of_other_table_rolls_back(self):
        self.sql('''CREATE FUNCTION cdm.bad_trigger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          INSERT INTO cdm.procedure_occurrence VALUES(8,NEW.person_id); RETURN NEW; END $$;
          CREATE TRIGGER bad_trigger AFTER INSERT ON cdm.note FOR EACH ROW EXECUTE FUNCTION cdm.bad_trigger();''')
        self.assertIn('only_note_permanent_rows_changed',self.run_etl(success=False).stderr)
        self.assertEqual(self.sql('SELECT count(*) FROM cdm.note'),'0')
        self.assertEqual(self.sql('SELECT count(*) FROM cdm.procedure_occurrence'),'1')

    def test_source_type_cannot_move_between_tables(self):
        self.sql("UPDATE raw.discharge SET note_type='RR' WHERE note_id='101-DS-1'")
        self.assertIn('document_class_mapping',self.run_etl(success=False).stderr)

    def test_deleted_entire_document_type_is_detected(self):
        self.run_etl()
        self.sql("DELETE FROM raw.discharge WHERE note_type='AD'")
        self.assertIn('no_unaccounted_existing_source',self.run_etl(success=False).stderr)
        self.assertEqual(self.sql('SELECT count(*) FROM cdm.note'),'2')

    def test_existing_whole_cxr_does_not_make_a_redundant_prefix(self):
        self.run_etl('radiology')
        self.sql("UPDATE raw.cxr_reports SET text=E'WET READ: Different findings.\\nFINAL REPORT\\nFINDINGS: No acute disease.' WHERE study_id='501'")
        value=note_id('cxr','101-CXR-501')
        self.sql(f"INSERT INTO cdm.note SELECT {value},1001,'2100-01-02',NULL,32817,40771183,'Chest radiograph source report (possible duplicate or variant)',text,32678,4180186,NULL,NULL,NULL,'CXR_VARIANT' FROM raw.cxr_reports WHERE study_id='501'")
        report=self.run_etl('cxr')
        self.assertEqual(report['source_rows'],3)
        self.assertEqual(report['load_status']['LEGACY_DATE_PROXY_RETAINED'],1)
        self.assertEqual(report['changed_note_rows'],0)


if __name__ == '__main__':
    unittest.main()
