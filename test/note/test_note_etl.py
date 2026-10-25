"""Isolated PostgreSQL integration tests and BigQuery runner contract tests.

Run: python -m unittest discover -s test/note -v
Requires Docker and the postgres:16 image. No production DB is contacted.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[2]


def note_id(table, source_id):
    key = f"mimic-iv-note|{table}|{source_id}".encode("utf-8")
    return int(hashlib.sha256(key).hexdigest()[:15], 16)


class PostgresNoteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("docker"):
            raise RuntimeError("Docker is required; integration tests must not silently skip")
        cls.container = "mimic-note-test-" + uuid.uuid4().hex[:10]
        subprocess.run([
            "docker", "run", "-d", "--rm", "--network", "none", "--name", cls.container,
            "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
            "--mount", f"type=bind,src={ROOT},dst=/work,readonly", "postgres:16"
        ], check=True, stdout=subprocess.DEVNULL)
        cls.addClassCleanup(lambda: subprocess.run(
            ["docker", "rm", "-f", cls.container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        for _ in range(60):
            if subprocess.run(["docker", "exec", cls.container, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
                return
            time.sleep(0.5)
        raise RuntimeError("Test PostgreSQL did not become ready")

    def sql(self, sql):
        result = subprocess.run([
            "docker", "exec", "-i", self.container, "psql", "-X", "-qAt",
            "-U", "postgres", "-v", "ON_ERROR_STOP=1"
        ], input=sql, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def run_etl(self, table="discharge", success=True, source_schema="raw"):
        result = subprocess.run([
            "docker", "exec", self.container, "psql", "-X", "-qAt", "-U", "postgres",
            "-v", "source_schema=" + source_schema, "-v", "cdm_schema=cdm", "-v", "etl_schema=note_audit",
            "-v", "source_table=" + table, "-f", "/work/etl/postgres/note.sql"
        ], text=True, capture_output=True)
        if not success:
            self.assertNotEqual(result.returncode, 0, result.stdout)
            return result
        self.assertEqual(result.returncode, 0, result.stderr)
        reports = [line.removeprefix("NOTE_ETL_REPORT ") for line in result.stdout.splitlines()
                   if line.startswith("NOTE_ETL_REPORT ")]
        self.assertEqual(len(reports), 1, result.stdout)
        return json.loads(reports[0])

    def setUp(self):
        self.sql("""
            DROP SCHEMA IF EXISTS modality CASCADE;
            DROP SCHEMA IF EXISTS raw CASCADE;
            DROP SCHEMA IF EXISTS cdm CASCADE;
            DROP SCHEMA IF EXISTS note_audit CASCADE;
            CREATE SCHEMA raw; CREATE SCHEMA cdm;
            CREATE TABLE cdm.person(person_id bigint PRIMARY KEY, person_source_value text);
            CREATE TABLE cdm.visit_occurrence(
                visit_occurrence_id bigint PRIMARY KEY, person_id bigint, visit_source_value text);
            CREATE TABLE cdm.concept(concept_id integer, domain_id text, vocabulary_id text,
                concept_code text, standard_concept text, invalid_reason text,
                valid_start_date date DEFAULT '1970-01-01', valid_end_date date DEFAULT '2099-12-31');
            INSERT INTO cdm.concept(concept_id,domain_id,vocabulary_id,concept_code,standard_concept) VALUES
                (32817,'Type Concept','Type Concept','EHR','S'),
                (32678,'Metadata','Metadata','UTF8','S'),
                (4180186,'Language','SNOMED','297487008','S'),
                (3020091,'Note','LOINC','18842-5','S'),
                (40771183,'Note','LOINC','68604-8','S');
            INSERT INTO cdm.person VALUES (1125899906842625,'101'),(-1099511627776,'102');
            INSERT INTO cdm.visit_occurrence VALUES
                (2251799813685249,1125899906842625,'101|201'),
                (-2199023255552,-1099511627776,'102|301');
            CREATE TABLE cdm.note (
                note_id integer PRIMARY KEY, person_id integer NOT NULL,
                note_date date NOT NULL, note_datetime timestamp, note_type_concept_id integer NOT NULL,
                note_class_concept_id integer NOT NULL, note_title varchar(250), note_text text NOT NULL,
                encoding_concept_id integer NOT NULL, language_concept_id integer NOT NULL,
                provider_id integer, visit_occurrence_id integer, visit_detail_id integer,
                note_source_value varchar(50));
            CREATE TABLE cdm.note_nlp(note_nlp_id integer PRIMARY KEY, note_id integer);
            CREATE TABLE raw.discharge (
                note_id text PRIMARY KEY, subject_id integer, hadm_id integer, note_type text,
                note_seq integer, charttime timestamp, storetime timestamp, text text);
            CREATE TABLE raw.radiology (LIKE raw.discharge INCLUDING ALL);
            CREATE TABLE raw.discharge_detail (
                note_id text, subject_id integer, field_name text, field_value text, field_ordinal integer);
            CREATE TABLE raw.radiology_detail (LIKE raw.discharge_detail);
            INSERT INTO raw.discharge VALUES
                ('101-DS-1',101,201,'DS',1,'2100-01-01 12:34:56.123456','2100-01-02',
                 E'UTF-8 한글, commas\\r\\n  spaces\\nfinal line  '),
                ('101-DS-2',101,202,'DS',2,'2100-01-03','2100-01-01','Unmatched visit'),
                ('999-DS-1',999,900,'DS',1,'2100-01-01',NULL,'Missing person'),
                ('102-AD-1',102,301,'AD',1,'2100-01-04',NULL,'Discharge addendum');
            INSERT INTO raw.discharge_detail VALUES
                ('101-DS-1',101,'author','Anonymous author',1),
                ('101-DS-1',101,'author','Anonymous author',1),
                ('999-DS-1',999,'author','Rejected document author',1);
            INSERT INTO raw.radiology VALUES
                ('101-RR-1',101,NULL,'RR',1,'2100-01-01',NULL,E'Radiology\\nreport'),
                ('101-AR-1',101,201,'AR',1,'2100-01-02','2100-01-03','Radiology addendum');
            INSERT INTO raw.radiology_detail VALUES
                ('101-RR-1',101,'exam_name','Synthetic image study',1),
                ('101-AR-1',101,'parent_note_id','101-RR-1',1);
        """)

    def test_incremental_sources_exact_text_and_idempotence(self):
        first = self.run_etl()
        self.assertEqual((first["source_rows"], first["loaded_rows"], first["rejected_person_rows"],
                          first["missing_visit_rows"], first["detail_rows"], first["changed_note_rows"]),
                         (4, 3, 1, 1, 3, 3))
        expected_id = note_id("discharge", "101-DS-1")
        self.assertEqual(self.sql("SELECT note_id FROM note_audit.note_id_map "
                                  "WHERE source_note_id='101-DS-1'"), str(expected_id))
        self.assertEqual(self.sql(f"SELECT n.note_text=s.text FROM cdm.note n "
                                  f"JOIN raw.discharge s ON s.note_id='101-DS-1' WHERE n.note_id={expected_id}"), "t")
        self.assertEqual(self.sql("SELECT count(*) FROM note_audit.note_detail "
                                  "WHERE source_note_id='101-DS-1'"), "2")
        radiology = self.run_etl("radiology")
        self.assertEqual((radiology["loaded_rows"], radiology["no_hadm_rows"]), (2, 1))
        rerun = self.run_etl()
        self.assertEqual(rerun["changed_note_rows"], 0)
        self.assertEqual(self.sql("SELECT count(*) FROM cdm.note"), "5")
        self.assertEqual(self.sql("SELECT count(DISTINCT note_id) FROM cdm.note"), "5")
        self.assertEqual(self.sql("SELECT note_class_concept_id FROM cdm.note "
                                  "WHERE note_source_value='RR'"), "40771183")

    def test_incremental_load_preserves_views_on_bigint_ids(self):
        self.run_etl()
        self.sql("""CREATE SCHEMA note_views;
            CREATE VIEW note_views.note AS SELECT * FROM cdm.note WHERE person_id=1125899906842625;
            CREATE VIEW note_views.note_nlp AS SELECT * FROM cdm.note_nlp;
        """)
        self.addCleanup(lambda: self.sql("DROP SCHEMA IF EXISTS note_views CASCADE"))
        self.assertEqual(self.run_etl("radiology")["loaded_rows"], 2)
        self.assertEqual(self.run_etl("radiology")["changed_note_rows"], 0)
        self.assertEqual(self.sql("SELECT count(*) FROM note_views.note"), "4")
        self.assertEqual(self.sql("SELECT count(*) FROM note_views.note_nlp"), "0")

    def test_join_fanout_is_rejected(self):
        self.sql("INSERT INTO cdm.visit_occurrence VALUES (8888,1125899906842625,'101|201')")
        result = self.run_etl(success=False)
        self.assertIn("no_join_fanout", result.stderr)
        self.assertEqual(self.sql("SELECT count(*) FROM cdm.note"), "0")

    def test_invalid_document_concept_is_rejected(self):
        self.sql("UPDATE cdm.concept SET domain_id='Observation' WHERE concept_code='18842-5'")
        self.assertIn("document_class_mapping", self.run_etl(success=False).stderr)
        self.assertEqual(self.sql("SELECT count(*) FROM cdm.note"), "0")

    def test_unowned_note_id_collision_is_rejected(self):
        value = note_id("discharge", "101-DS-1")
        self.sql(f"""ALTER TABLE cdm.note ALTER COLUMN note_id TYPE bigint;
            INSERT INTO cdm.note(note_id,person_id,note_date,note_type_concept_id,
                note_class_concept_id,note_text,encoding_concept_id,language_concept_id)
            VALUES ({value},1,'2100-01-01',32817,3020091,'Other ETL',32678,4180186)""")
        self.assertIn("no_unowned_target_id_collision", self.run_etl(success=False).stderr)
        self.assertEqual(self.sql("SELECT note_text FROM cdm.note"), "Other ETL")

    def test_integrity_failure_rolls_back_data_and_type_migration(self):
        self.sql("""CREATE FUNCTION cdm.corrupt_test_text() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN NEW.note_text := 'corrupted'; RETURN NEW; END $$;
            CREATE TRIGGER corrupt_test BEFORE INSERT ON cdm.note
            FOR EACH ROW EXECUTE FUNCTION cdm.corrupt_test_text();""")
        self.assertIn("every_loaded_field_matches", self.run_etl(success=False).stderr)
        self.assertEqual(self.sql("SELECT count(*) FROM cdm.note"), "0")
        self.assertEqual(self.sql("SELECT data_type FROM information_schema.columns "
                                  "WHERE table_schema='cdm' AND table_name='note' AND column_name='note_id'"),
                         "integer")
        self.assertEqual(self.sql("SELECT count(*) FROM pg_namespace WHERE nspname='note_audit'"), "0")

    def test_source_deletions_require_explicit_reconciliation(self):
        self.run_etl()
        self.sql("DELETE FROM raw.discharge_detail WHERE note_id='101-DS-1';"
                 "DELETE FROM raw.discharge WHERE note_id='101-DS-1'")
        self.assertIn("no_implicit_source_deletions", self.run_etl(success=False).stderr)
        self.assertEqual(self.sql("SELECT count(*) FROM cdm.note"), "3")

    def test_detail_subject_mismatch_is_rejected(self):
        self.sql("UPDATE raw.discharge_detail SET subject_id=102 WHERE note_id='101-DS-1'")
        self.assertIn("detail_parent_and_subject", self.run_etl(success=False).stderr)

    def test_details_without_source_document_are_retained_without_a_fake_note(self):
        self.sql("""INSERT INTO raw.radiology_detail VALUES
            ('999-RR-999',999,'exam_name','Unavailable document',1),
            ('999-RR-999',999,'exam_name','Unavailable document',1);""")
        result = self.run_etl('radiology')
        self.assertEqual((result['detail_rows'], result['unmatched_detail_rows']), (4, 2))
        self.assertEqual(self.sql("SELECT count(*) FROM note_audit.note_detail "
                                  "WHERE source_note_id='999-RR-999' AND note_id IS NULL"), '2')
        self.assertEqual(self.sql("SELECT count(*) FROM note_audit.note_id_map "
                                  "WHERE source_note_id='999-RR-999'"), '0')
        self.assertEqual(self.sql("SELECT count(*) FROM cdm.note"), '2')
        repeat = self.run_etl('radiology')
        self.assertEqual((repeat['changed_note_rows'], repeat['unmatched_detail_rows']), (0, 2))

    def modality_fixture(self):
        self.run_etl('discharge')
        self.run_etl('radiology')
        reports = ','.join(f'report_{i} text' for i in range(18))
        self.sql(f"""
            INSERT INTO cdm.concept(concept_id,domain_id,vocabulary_id,concept_code,standard_concept)
                VALUES (46235236,'Note','LOINC','75425-9','S');
            CREATE SCHEMA modality;
            CREATE TABLE modality.source_file(table_name text, source_version text,
                source_sha256 text, checksum_status text);
            INSERT INTO modality.source_file
                SELECT t,'test','synthetic',CASE WHEN t='echo_study_list' THEN 'CHECKSUM_MISMATCH' ELSE 'VERIFIED' END
                FROM unnest(ARRAY['cxr_reports','cxr_study_list','cxr_record_list','cxr_provider_list',
                    'cxr_metadata','ecg_machine_measurements','ecg_record_list','ecg_waveform_note_links','echo_study_list']) t;
            CREATE TABLE modality.cxr_reports(subject_id text,study_id text,text text);
            INSERT INTO modality.cxr_reports VALUES
                ('101','501',E'FINAL REPORT\\nRadiology\\nreport'),
                ('101','502','Unresolved overlap'),
                ('102','503',E'New UTF-8 한글 CXR\\n  original text '),
                ('999','504','No CDM person');
            CREATE TABLE modality.cxr_study_list AS SELECT subject_id,study_id FROM modality.cxr_reports;
            CREATE TABLE modality.cxr_metadata AS
                SELECT subject_id,study_id,'21000101'::text AS studydate,'000000.125'::text AS studytime
                FROM modality.cxr_reports;
            CREATE TABLE modality.ecg_machine_measurements(subject_id text,study_id text,ecg_time text,{reports});
            INSERT INTO modality.ecg_machine_measurements(subject_id,study_id,ecg_time,report_0,report_1,report_2) VALUES
                ('101','601','2100-01-03 01:02:03','Machine first','','Machine third'),
                ('999','602','2100-01-03 01:02:03','Missing person','',''),
                ('102','603','2100-01-03 01:02:03','','','');
            CREATE TABLE modality.echo_study_list(subject_id text,study_id text,study_datetime text,note_id text);
            INSERT INTO modality.echo_study_list VALUES
                ('101','701','2100-01-03','101-EC-1'),('999','702','2100-01-03','');
        """)

    def run_modality(self, filename, success=True):
        result = subprocess.run([
            'docker','exec',self.container,'psql','-X','-qAt','-U','postgres',
            '-v','source_schema=modality','-v','radiology_schema=raw',
            '-v','cdm_schema=cdm','-v','etl_schema=note_audit',
            '-f','/work/etl/postgres/'+filename
        ],text=True,capture_output=True)
        if success:
            self.assertEqual(result.returncode,0,result.stderr)
        else:
            self.assertNotEqual(result.returncode,0,result.stdout)
        return result

    def test_modality_incremental_reuse_machine_lines_and_echo_quarantine(self):
        self.modality_fixture()
        self.run_modality('st_modality_note.sql')
        cxr = self.run_etl('cxr',source_schema='modality')
        ecg = self.run_etl('ecg_machine',source_schema='modality')
        self.assertEqual((cxr['source_rows'],cxr['loaded_rows']),(3,2))
        self.assertEqual((ecg['source_rows'],ecg['loaded_rows']),(2,1))
        self.run_modality('ext_modality_note.sql')
        self.assertEqual(self.sql('SELECT count(*) FROM cdm.note'),'8')
        self.assertEqual(self.sql("SELECT count(*) FROM cdm.note WHERE note_source_value IN ('DS','AD','RR','AR')"),'5')
        self.assertEqual(self.sql("SELECT load_status FROM note_audit.modality_note_link WHERE study_id=501"),'LINKED_EXISTING')
        self.assertEqual(self.sql("SELECT load_status FROM note_audit.modality_note_link WHERE study_id=502"),'LOADED_SOURCE_VARIANT')
        self.assertEqual(self.sql("SELECT note_id FROM note_audit.modality_note_link WHERE study_id=501"),str(note_id('radiology','101-RR-1')))
        self.assertEqual(self.sql("SELECT count(*) FROM note_audit.modality_note_link WHERE source_table='echo' AND note_id IS NULL AND checksum_status='CHECKSUM_MISMATCH'"),'2')
        expected = int(hashlib.sha256(b'mimic-iv-ecg|ecg_machine|101-ECG_MACHINE-601').hexdigest()[:15],16)
        self.assertEqual(self.sql("SELECT note_id FROM cdm.note WHERE note_source_value='ECG_MACHINE'"),str(expected))
        machine = 'Machine first\n\nMachine third'+'\n'*15
        expected_hash = hashlib.sha256(machine.encode()).hexdigest()
        self.assertEqual(self.sql("SELECT encode(sha256(convert_to(note_text,'UTF8')),'hex') FROM cdm.note WHERE note_source_value='ECG_MACHINE'"),expected_hash)
        self.assertEqual(self.sql("SELECT count(*) FROM cdm.note WHERE note_source_value IN ('CXR','ECG_MACHINE') AND visit_occurrence_id IS NULL AND provider_id IS NULL"),'2')
        self.run_modality('st_modality_note.sql')
        self.assertEqual(self.run_etl('cxr',source_schema='modality')['changed_note_rows'],0)
        self.assertEqual(self.run_etl('ecg_machine',source_schema='modality')['changed_note_rows'],0)
        self.run_modality('ext_modality_note.sql')
        self.assertEqual(self.sql('SELECT count(*) FROM note_audit.modality_note_link'),'9')

    def test_cxr_ambiguous_repeated_text_is_preserved_without_a_false_alias(self):
        self.modality_fixture()
        self.sql("""INSERT INTO modality.cxr_reports SELECT subject_id,'505',text FROM modality.cxr_reports WHERE study_id='501';
            INSERT INTO modality.cxr_study_list VALUES ('101','505');
            INSERT INTO modality.cxr_metadata VALUES ('101','505','21000101','000000.125');""")
        self.run_modality('st_modality_note.sql')
        self.assertEqual(self.sql("SELECT count(*) FROM note_audit.cxr_reconciliation WHERE study_id IN (501,505) AND decision='NEW_SOURCE_VARIANT' AND radiology_note_id IS NULL"),'2')
        self.run_etl('cxr',source_schema='modality')
        self.run_etl('ecg_machine',source_schema='modality')
        self.run_modality('ext_modality_note.sql')
        self.assertEqual(self.sql("SELECT count(DISTINCT source_variant_note_id) FROM note_audit.cxr_overlap_note_link WHERE study_id IN (501,505) AND candidate_note_id IS NOT NULL AND relationship_status='POSSIBLE_DUPLICATE_NOT_CONFIRMED'"),'2')

    def test_cxr_clinical_body_requires_exam_time_and_unique_pair(self):
        self.modality_fixture()
        body = 'FINDINGS: No acute pulmonary changes; normal cardiomediastinal silhouette.'
        self.sql(f"""UPDATE raw.radiology SET text='Different indication. {body}',charttime='2100-01-01 00:00:00' WHERE note_id='101-RR-1';
            UPDATE raw.radiology_detail SET field_value='CHEST (PA AND LAT)' WHERE field_name='exam_name';
            UPDATE modality.cxr_reports SET text='FINAL REPORT Different deidentification. {body}' WHERE study_id='501';""")
        self.run_modality('st_modality_note.sql')
        self.assertEqual(self.sql("SELECT match_method FROM note_audit.cxr_reconciliation WHERE study_id=501"),'CLINICAL_BODY_CHEST_WITHIN_1H')
        self.sql("UPDATE modality.cxr_metadata SET studytime='080000' WHERE study_id='501'")
        self.assertIn('stable_existing_resolution',self.run_modality('st_modality_note.sql',success=False).stderr)

    def test_cxr_wet_read_is_preserved_separately_and_final_is_reused(self):
        self.modality_fixture()
        prefix = 'WET READ: preliminary 한글\n  No pleural effusion.\n____________________\n'
        text = prefix + ' FINAL REPORT\nRadiology\nreport'
        self.sql("UPDATE modality.cxr_reports SET text='" + text + "' WHERE study_id='501'")
        self.run_modality('st_modality_note.sql')
        self.run_etl('cxr',source_schema='modality')
        self.run_etl('ecg_machine',source_schema='modality')
        self.run_modality('ext_modality_note.sql')
        expected_hash = hashlib.sha256(prefix.encode()).hexdigest()
        self.assertEqual(self.sql("SELECT text_sha256 FROM note_audit.cxr_note_component WHERE study_id=501 AND component_kind='PREFIX'"),expected_hash)
        self.assertEqual(self.sql("SELECT note_source_value FROM cdm.note WHERE note_title LIKE '%supplementary%'"),'CXR_SUPPLEMENT')
        self.assertEqual(self.sql("SELECT parent_note_id FROM note_audit.cxr_note_component WHERE study_id=501 AND component_kind='PREFIX'"),str(note_id('radiology','101-RR-1')))
        self.assertEqual(self.sql("SELECT source_text='" + prefix + "' FROM note_audit.cxr_component_text WHERE study_id=501 AND component_kind='PREFIX'"),'t')
        self.assertEqual(self.sql("SELECT note_text FROM cdm.note WHERE note_id="+str(note_id('radiology','101-RR-1'))),'Radiology\nreport')
        self.run_modality('st_modality_note.sql')
        self.assertEqual(self.run_etl('cxr',source_schema='modality')['changed_note_rows'],0)
        self.run_modality('ext_modality_note.sql')

    def test_cxr_exact_parent_linked_addendum_reuses_both_notes(self):
        self.modality_fixture()
        text='FINAL ADDENDUM\nADDENDUM: Radiology addendum\n____________________\nFINAL REPORT\nRadiology\nreport'
        self.sql("UPDATE modality.cxr_reports SET text='"+text+"' WHERE study_id='501'")
        self.run_modality('st_modality_note.sql')
        self.run_etl('cxr',source_schema='modality')
        self.run_etl('ecg_machine',source_schema='modality')
        self.run_modality('ext_modality_note.sql')
        self.assertEqual(self.sql("SELECT note_id FROM note_audit.cxr_note_component WHERE study_id=501 AND component_kind='PREFIX'"),str(note_id('radiology','101-AR-1')))
        self.assertEqual(self.sql("SELECT count(*) FROM cdm.note WHERE note_source_value='CXR_SUPPLEMENT'"),'0')
        self.assertEqual(self.sql("SELECT count(*) FROM note_audit.cxr_note_component WHERE study_id=501 AND load_status='LINKED_EXISTING'"),'2')

    def test_cxr_midnight_images_match_one_final_report(self):
        self.modality_fixture()
        body='FINDINGS: No acute pulmonary changes; the cardiomediastinal silhouette remains normal.'
        self.sql(f"""UPDATE raw.radiology SET text='Indication. {body}',charttime='2100-01-02 00:06:00' WHERE note_id='101-RR-1';
            UPDATE raw.radiology_detail SET field_value='CHEST (PA AND LAT)' WHERE field_name='exam_name';
            UPDATE modality.cxr_reports SET text='FINAL REPORT Different indication. {body}' WHERE study_id='501';
            UPDATE modality.cxr_metadata SET studytime='235600.000000004' WHERE study_id='501';
            INSERT INTO modality.cxr_metadata VALUES ('101','501','21000102','001400.125000004');""")
        self.run_modality('st_modality_note.sql')
        self.assertEqual(self.sql("SELECT decision FROM note_audit.cxr_reconciliation WHERE study_id=501"),'MATCHED_RADIOLOGY')
        self.assertEqual(self.sql("SELECT match_method FROM note_audit.cxr_reconciliation WHERE study_id=501"),'FINAL_BODY_CHEST_WITHIN_6H')

    def test_cxr_legacy_link_survives_a_late_image_in_the_same_study(self):
        self.modality_fixture()
        self.sql("INSERT INTO modality.cxr_metadata VALUES ('101','501','21000101','120000')")
        self.run_modality('st_modality_note.sql')
        self.assertEqual(self.sql("SELECT decision FROM note_audit.cxr_reconciliation WHERE study_id=501"),'MATCHED_RADIOLOGY')
        self.run_modality('st_modality_note.sql')
        self.assertEqual(self.sql("SELECT radiology_note_id FROM note_audit.cxr_reconciliation WHERE study_id=501"),'101-RR-1')

    def test_cxr_mask_rule_keeps_negation_numbers_and_punctuation(self):
        body='FINDINGS: No pleural effusion. Cardiac size is normal. A 3.1 cm opacity in the left lung is unchanged.'
        cases=[(body.replace('effusion.','effusion. ___'),'MATCHED_RADIOLOGY'),
               (body.replace('No pleural','___ pleural'),'NEW_SOURCE_VARIANT'),
               (body.replace('3.1','3.___1'),'NEW_SOURCE_VARIANT'),
               (body.replace('3.1','3.2'),'NEW_SOURCE_VARIANT')]
        for variant,expected in cases:
            with self.subTest(expected=expected,variant=variant):
                self.setUp()
                self.modality_fixture()
                self.sql(f"""UPDATE raw.radiology SET text='Indication. {body}' WHERE note_id='101-RR-1';
                    UPDATE raw.radiology_detail SET field_value='CHEST (PA AND LAT)' WHERE field_name='exam_name';
                    UPDATE modality.cxr_reports SET text='Different indication. {variant}' WHERE study_id='501';""")
                self.run_modality('st_modality_note.sql')
                self.assertEqual(self.sql("SELECT decision FROM note_audit.cxr_reconciliation WHERE study_id=501"),expected)
                if expected=='MATCHED_RADIOLOGY':
                    self.assertEqual(self.sql("SELECT match_method FROM note_audit.cxr_reconciliation WHERE study_id=501"),'MASK_ONLY_BODY_CHEST_WITHIN_1H')
                else:
                    self.run_etl('cxr',source_schema='modality')
                    self.assertEqual(self.sql("SELECT note_text FROM cdm.note n JOIN note_audit.note_id_map m USING(note_id) WHERE m.source_note_id='101-CXR-501'"),'Different indication. '+variant)

    def test_cxr_new_report_requires_temporal_and_text_exclusion(self):
        self.modality_fixture()
        self.sql("""UPDATE modality.cxr_reports SET text='Distinct report with a newly described finding.' WHERE study_id='501';
            UPDATE modality.cxr_reports SET text=E'Radiology\\nreport' WHERE study_id='502';
            UPDATE modality.cxr_metadata SET studydate='21000201' WHERE study_id IN ('501','502');""")
        self.run_modality('st_modality_note.sql')
        self.assertEqual(self.sql("SELECT decision FROM note_audit.cxr_reconciliation WHERE study_id=501"),'NEW_NO_NEARBY_RADIOLOGY')
        self.assertEqual(self.sql("SELECT decision FROM note_audit.cxr_reconciliation WHERE study_id=502"),'NEW_SOURCE_VARIANT')

    def test_zero_new_cxr_documents_is_valid(self):
        self.modality_fixture()
        self.sql("DELETE FROM modality.cxr_reports WHERE study_id IN ('502','503','504'); DELETE FROM modality.cxr_study_list WHERE study_id IN ('502','503','504');")
        self.run_modality('st_modality_note.sql')
        result=self.run_etl('cxr',source_schema='modality')
        self.assertEqual((result['source_rows'],result['loaded_rows']),(0,0))

    def test_cxr_delayed_exact_full_text_requires_unique_unowned_report(self):
        self.modality_fixture()
        self.sql("UPDATE modality.cxr_metadata SET studydate='21000102',studytime='120000' WHERE study_id='501'")
        self.run_modality('st_modality_note.sql')
        self.assertEqual(self.sql("SELECT match_method FROM note_audit.cxr_reconciliation WHERE study_id=501"),'FULL_TEXT_WITHIN_48H')
        self.sql("UPDATE modality.cxr_reports SET text=E'FINAL REPORT\\nRadiology\\nreport' WHERE study_id='502'")
        # The original 501 now competes with a higher-priority same-day study.
        # Established links may not be reassigned on a later run.
        self.assertIn('stable_existing_resolution',self.run_modality('st_modality_note.sql',success=False).stderr)

    def test_cxr_composite_variant_is_retained_whole_with_unknown_document_time(self):
        self.modality_fixture()
        value='WET READ: Small pleural effusion.\n__________\nFINAL REPORT\nFINDINGS: No pleural effusion.'
        self.sql("UPDATE modality.cxr_reports SET text='"+value+"' WHERE study_id='502'")
        self.run_modality('st_modality_note.sql')
        self.run_etl('cxr',source_schema='modality')
        self.run_etl('ecg_machine',source_schema='modality')
        self.run_modality('ext_modality_note.sql')
        self.assertEqual(self.sql("SELECT component_kind FROM note_audit.cxr_note_component WHERE study_id=502"),'REPORT')
        self.assertEqual(self.sql("SELECT note_text FROM cdm.note WHERE note_source_value='CXR_VARIANT'"),value)
        self.assertEqual(self.sql("SELECT count(*) FROM cdm.note WHERE note_source_value IN ('CXR','CXR_VARIANT','CXR_SUPPLEMENT') AND note_datetime IS NOT NULL"),'0')
        self.assertEqual(self.sql("SELECT count(*) FROM note_audit.note_metadata WHERE source_table='cxr' AND charttime IS NULL"),'0')
        self.assertEqual(self.sql("SELECT field_value FROM note_audit.note_detail WHERE field_name='duplicate_status'"),'UNRESOLVED_POSSIBLE_DUPLICATE')
        # One-time correction of the former acquisition-time proxy is limited
        # to note_datetime and remains idempotent after applying the new rule.
        self.sql("UPDATE cdm.note SET note_datetime=note_date::timestamp WHERE note_source_value IN ('CXR','CXR_VARIANT')")
        self.assertEqual(self.run_etl('cxr',source_schema='modality')['changed_note_rows'],2)
        self.assertEqual(self.run_etl('cxr',source_schema='modality')['changed_note_rows'],0)

    def test_final_convention_audit_detects_invalid_visit_patient(self):
        self.modality_fixture()
        self.sql('CREATE TABLE cdm.provider(provider_id bigint); CREATE TABLE cdm.visit_detail(visit_detail_id bigint,person_id bigint);')
        self.run_modality('st_modality_note.sql')
        self.run_etl('cxr',source_schema='modality')
        self.run_etl('ecg_machine',source_schema='modality')
        self.run_modality('ext_modality_note.sql')
        result=self.run_modality('qa_note.sql')
        self.assertIn('NOTE_CONVENTION_REPORT',result.stdout)
        self.sql("UPDATE cdm.visit_occurrence SET person_id=-1099511627776 WHERE visit_source_value='101|201'")
        rejected=self.run_modality('qa_note.sql',success=False)
        self.assertIn('"status": "FAIL"',rejected.stdout)
        report=json.loads(next(line.removeprefix('NOTE_CONVENTION_REPORT ') for line in rejected.stdout.splitlines()
            if line.startswith('NOTE_CONVENTION_REPORT ')))
        self.assertGreater(report['checks']['visit_fk_or_person'],0)

    def test_raw_fingerprint_encoding_matches_python(self):
        row = ['quotes " and \\', '한글\r\n  spaces', '']
        expected=hashlib.sha256(json.dumps(row,ensure_ascii=False).encode()).hexdigest()
        actual=self.sql("SELECT encode(sha256(convert_to(jsonb_build_array("+
            ','.join("'"+v.replace("'","''")+"'::text" for v in row)+")::text,'UTF8')),'hex')")
        self.assertEqual(actual,expected)


class BigQueryRunnerTests(unittest.TestCase):
    def execute(self, script_mode, fail=False, real_workflow=False, source='discharge'):
        with tempfile.TemporaryDirectory(prefix="note-bq-runner-") as folder:
            root = Path(folder)
            executable = root / "bq"
            executable.write_text(
                "#!/usr/bin/env python3\nimport json,os,sys\n"
                "query=sys.stdin.read() if len(sys.argv)==3 else sys.argv[-1]\n"
                "with open(os.environ['BQ_TEST_LOG'],'a') as f: "
                "f.write(json.dumps(query)+'\\n')\n"
                "sys.exit(int(os.environ.get('BQ_TEST_EXIT','0')))\n")
            executable.chmod(0o700)
            script = root / "first.sql"
            script.write_text("CREATE TEMP TABLE t AS SELECT '@value' AS x;\nSELECT * FROM t;\n")
            second = root / "second.sql"
            second.write_text("SELECT 'must not run after failure';")
            config = root / "workflow.json"
            workflow = {"script_mode": script_mode, "variables": {"@value": "literal"},
                        "scripts": [{"script": str(script)}, {"script": str(second)}]}
            if real_workflow:
                name = 'note' if real_workflow is True else real_workflow
                workflow = json.loads((ROOT / f"conf/workflow_{name}.conf").read_text())
                config_name = 'modality_note' if name.startswith('modality') else ('note' if source=='discharge' else 'note_'+source)
                workflow["variables"] = json.loads((ROOT / f"conf/{config_name}.etlconf").read_text())["variables"]
            config.write_text(json.dumps(workflow))
            env = os.environ.copy()
            env.update(PATH=str(root) + os.pathsep + env["PATH"], BQ_TEST_LOG=str(root / "queries.jsonl"),
                       BQ_TEST_EXIT="7" if fail else "0")
            result = subprocess.run([sys.executable, "scripts/run_workflow.py", "-c", str(config)],
                                    cwd=ROOT, env=env, text=True, capture_output=True)
            queries = [json.loads(line) for line in (root / "queries.jsonl").read_text().splitlines()]
            return result, queries

    def test_script_mode_keeps_transaction_and_temporary_table_session(self):
        result, queries = self.execute(True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(queries), 1)
        self.assertIn("CREATE TEMP TABLE t AS SELECT 'literal' AS x;\nSELECT * FROM t;", queries[0])
        self.assertIn("SELECT 'must not run after failure';", queries[0])

    def test_script_failure_reaches_workflow_caller(self):
        result, queries = self.execute(True, fail=True)
        self.assertEqual(result.returncode, 7)
        self.assertEqual(len(queries), 1)

    def test_legacy_mode_still_executes_individual_statements(self):
        result, queries = self.execute(False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(queries), 3)

    def test_legacy_failure_reaches_workflow_caller(self):
        result, queries = self.execute(False, fail=True)
        self.assertEqual(result.returncode, 7)
        self.assertEqual(len(queries), 1)

    def test_real_note_workflow_renders_one_atomic_script(self):
        result, queries = self.execute(True, real_workflow=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(queries), 1)
        for query in queries:
            self.assertNotIn("@note_", query)
            self.assertNotIn("@etl_", query)
        self.assertIn("ELSE 'mimic-iv-note'", queries[0])
        query = queries[0]
        self.assertEqual(query.count("BEGIN TRANSACTION;"), 1)
        self.assertEqual(query.count("COMMIT TRANSACTION;"), 1)
        ordered = [
            "CREATE OR REPLACE TABLE YOUR_ETL_PROJECT.YOUR_EXISTING_ETL_DATASET.src_note AS",
            "CREATE OR REPLACE TABLE YOUR_ETL_PROJECT.YOUR_EXISTING_ETL_DATASET.lk_note_mapped AS",
            "CREATE TABLE IF NOT EXISTS YOUR_ETL_PROJECT.YOUR_EXISTING_ETL_DATASET.ext_note_id_map",
            "BEGIN TRANSACTION;",
            "INSERT INTO YOUR_ETL_PROJECT.YOUR_EXISTING_ETL_DATASET.cdm_note",
            "INSERT INTO YOUR_ETL_PROJECT.YOUR_EXISTING_ETL_DATASET.ext_note_metadata",
            "AS 'Loaded NOTE differs from source or mapping'",
            "COMMIT TRANSACTION;",
        ]
        positions = [query.index(fragment) for fragment in ordered]
        self.assertEqual(positions, sorted(positions))

    def test_modality_workflows_and_note_sources_render_without_parameters(self):
        for name in ['modality_note_staging','modality_note_links']:
            with self.subTest(workflow=name):
                result,queries=self.execute(True,real_workflow=name)
                self.assertEqual(result.returncode,0,result.stderr)
                self.assertEqual(len(queries),1)
                self.assertNotIn('@',queries[0])
                self.assertEqual(queries[0].count('BEGIN TRANSACTION;'),1)
                self.assertEqual(queries[0].count('COMMIT TRANSACTION;'),1)
        for source in ['cxr','ecg_machine']:
            with self.subTest(source=source):
                result,queries=self.execute(True,real_workflow=True,source=source)
                self.assertEqual(result.returncode,0,result.stderr)
                self.assertNotIn('@',queries[0])
                self.assertIn('YOUR_SOURCE_PROJECT.mimiciv_modality.'+source,queries[0])


if __name__ == "__main__":
    unittest.main()
