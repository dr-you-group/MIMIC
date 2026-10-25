-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- NOTE-only mapping; same rules as etl/postgres/note.sql.
-- Temporary work tables only. Run in one workflow_note.conf script.
-- -------------------------------------------------------------------

CREATE TEMP FUNCTION note_key(dataset STRING, source_table STRING, source_note_id STRING) AS (
    CAST(CONCAT('0x',SUBSTR(TO_HEX(SHA256(CONCAT(dataset,'|',source_table,'|',source_note_id))),1,15)) AS INT64)
);
CREATE TEMP FUNCTION note_normalize(value STRING) AS (
    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(value,r'^[[:space:]]*FINAL REPORT[[:space:]]*',''),r'[[:space:]]+',' '))
);
CREATE TEMP FUNCTION note_addendum(value STRING) AS (
    note_normalize(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(value,
        r'(?m)^[ \t]*_{10,}[ \t]*\r?$',''),r'(?is)^[[:space:]]*FINAL ADDENDUM[[:space:]]*',''),
        r'(?is)^[[:space:]]*ADDENDUM[[:space:]]*:?[[:space:]]*',''))
);
BEGIN TRANSACTION;
ASSERT '@note_source_table' IN ('discharge','radiology','cxr','ecg_machine') AS 'supported_source';

IF '@note_source_table' IN ('discharge','radiology') THEN
CREATE TEMPORARY TABLE src_note_text AS
SELECT
  CAST(note_id AS STRING) AS source_note_id,
  subject_id,
  hadm_id,
  note_type,
  CAST(charttime AS DATETIME) AS charttime,
  CAST(storetime AS DATETIME) AS storetime,
  text AS note_text,
  CAST(NULL AS STRING) AS study_id,
  CAST(NULL AS INT64) AS reuse_note_id,
  CAST('DOCUMENT_CHARTTIME' AS STRING) AS timestamp_basis,
  CAST('REPORT' AS STRING) AS component_kind,
  CAST(NULL AS STRING) AS match_method
FROM @note_project.@note_dataset.@note_source_table;

ELSEIF '@note_source_table' = 'ecg_machine' THEN
ASSERT (SELECT COUNT(*) = 1 AND LOGICAL_AND(checksum_status = 'VERIFIED') FROM @note_project.@note_dataset.source_file WHERE table_name = 'ecg_machine_measurements') AS 'verified_ecg_source';
CREATE TEMPORARY TABLE src_note_text AS
SELECT
  subject_id || '-ECG_MACHINE-' || study_id AS source_note_id,
  CAST(subject_id AS INT64) AS subject_id,
  CAST(NULL AS INT64) AS hadm_id,
  CAST('ECG_MACHINE' AS STRING) AS note_type,
  CAST(NULLIF(ecg_time, '') AS DATETIME) AS charttime,
  CAST(NULL AS DATETIME) AS storetime,
  ARRAY_TO_STRING(
    [
      COALESCE(report_0, ''),
      COALESCE(report_1, ''),
      COALESCE(report_2, ''),
      COALESCE(report_3, ''),
      COALESCE(report_4, ''),
      COALESCE(report_5, ''),
      COALESCE(report_6, ''),
      COALESCE(report_7, ''),
      COALESCE(report_8, ''),
      COALESCE(report_9, ''),
      COALESCE(report_10, ''),
      COALESCE(report_11, ''),
      COALESCE(report_12, ''),
      COALESCE(report_13, ''),
      COALESCE(report_14, ''),
      COALESCE(report_15, ''),
      COALESCE(report_16, ''),
      COALESCE(report_17, '')
    ],
    CAST(b'\n' AS STRING)
  ) AS note_text,
  CAST(study_id AS STRING) AS study_id,
  CAST(NULL AS INT64) AS reuse_note_id,
  CAST('DEVICE_CLOCK_UNSYNCHRONIZED' AS STRING) AS timestamp_basis,
  CAST('REPORT' AS STRING) AS component_kind,
  CAST(NULL AS STRING) AS match_method
FROM @note_project.@note_dataset.ecg_machine_measurements;
END IF;
