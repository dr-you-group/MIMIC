-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- Prepare canonical modality NOTE inputs; same rules as PostgreSQL.
-- Dependencies: modality raw tables/source_file; complete raw radiology.
-- Run this workflow, NOTE for cxr, NOTE for ecg_machine, then modality links.
-- -------------------------------------------------------------------

ASSERT (SELECT count(*) = 8 AND LOGICAL_AND(checksum_status = 'VERIFIED')
    FROM @modality_project.@modality_dataset.source_file WHERE table_name <> 'echo_study_list')
    AS 'Unverified non-Echo source';
ASSERT (SELECT count(*) > 0 FROM @note_project.@note_dataset.radiology)
    AS 'Radiology source is empty';
ASSERT (SELECT count(*) = count(DISTINCT study_id) FROM @modality_project.@modality_dataset.cxr_reports)
    AS 'Duplicate CXR study';
ASSERT NOT EXISTS (
    (SELECT subject_id, study_id FROM @modality_project.@modality_dataset.cxr_reports
     EXCEPT DISTINCT SELECT subject_id, study_id FROM @modality_project.@modality_dataset.cxr_study_list)
    UNION ALL
    (SELECT subject_id, study_id FROM @modality_project.@modality_dataset.cxr_study_list
     EXCEPT DISTINCT SELECT subject_id, study_id FROM @modality_project.@modality_dataset.cxr_reports)
) AS 'CXR study inventory mismatch';

-- Comparison only: raw text and the NOTE text are never normalized.
CREATE TEMP FUNCTION note_normalize(value STRING) AS (
    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(value,
        r'^[[:space:]]*FINAL REPORT[[:space:]]*', ''), r'[[:space:]]+', ' '))
);
CREATE TEMP FUNCTION note_body(value STRING) AS (
    note_normalize(REGEXP_EXTRACT(value,
        r'(?is)((?:FINDINGS(?: AND IMPRESSION)?|IMPRESSION):.*)'))
);

-- -------------------------------------------------------------------
-- src_cxr_study
-- One acquisition timestamp per study, retaining all raw image metadata
-- -------------------------------------------------------------------

CREATE TEMP TABLE src_cxr_time AS
SELECT
    CAST(subject_id AS INT64)                 AS subject_id,
    CAST(study_id AS INT64)                   AS study_id,
    CASE WHEN studydate <> '' AND studytime <> '' THEN
        PARSE_DATETIME('%Y%m%d%H%M%E*S', CONCAT(studydate,
            LPAD(SPLIT(studytime, '.')[OFFSET(0)], 6, '0'),
            CASE WHEN STRPOS(studytime, '.') > 0
                THEN CONCAT('.', SPLIT(studytime, '.')[SAFE_OFFSET(1)]) ELSE '' END))
    END                                         AS study_datetime
FROM
    @modality_project.@modality_dataset.cxr_metadata
;
CREATE TEMP TABLE src_cxr_study AS
SELECT
    CAST(raw.subject_id AS INT64)             AS subject_id,
    CAST(raw.study_id AS INT64)               AS study_id,
    raw.subject_id || '-CXR-' || raw.study_id    AS source_note_id,
    tm.study_datetime                           AS study_datetime,
    tm.study_dates                              AS study_dates,
    LOWER(TO_HEX(SHA256(note_normalize(raw.text)))) AS full_hash,
    CASE WHEN length(note_body(raw.text)) >= 40 THEN
        LOWER(TO_HEX(SHA256(note_body(raw.text))))
    END                                         AS body_hash,
    REGEXP_CONTAINS(raw.text, r'(?i)addendum|amended report|_{10}')    AS has_addendum
FROM
    @modality_project.@modality_dataset.cxr_reports raw
LEFT JOIN (
    SELECT subject_id, study_id, min(study_datetime) AS study_datetime,
        count(DISTINCT DATE(study_datetime)) AS study_dates
    FROM src_cxr_time GROUP BY subject_id, study_id
) tm
    ON tm.subject_id = CAST(raw.subject_id AS INT64)
   AND tm.study_id = CAST(raw.study_id AS INT64)
;

-- -------------------------------------------------------------------
-- src_cxr_radiology
-- Full-text matches are strongest. A clinical-body match also requires
-- a chest radiograph exam and acquisition/chart times within one hour.
-- -------------------------------------------------------------------

CREATE TEMP TABLE src_cxr_exam AS
SELECT DISTINCT note_id
FROM @note_project.@note_dataset.radiology_detail
WHERE field_name = 'exam_name'
    AND REGEXP_CONTAINS(field_value, r'(?i)CHEST|CXR')
    AND NOT REGEXP_CONTAINS(field_value, r'(?i)(^|[^A-Z])(CT|CTA|MR|MRI|US|PET)([^A-Z]|$)|ANGIO')
;
CREATE TEMP TABLE src_cxr_radiology AS
SELECT
    raw.note_id                                 AS source_note_id,
    raw.subject_id                              AS subject_id,
    CAST(raw.charttime AS DATETIME)             AS charttime,
    raw.note_type                               AS note_type,
    LOWER(TO_HEX(SHA256(note_normalize(raw.text)))) AS full_hash,
    CASE WHEN length(note_body(raw.text)) >= 40 THEN
        LOWER(TO_HEX(SHA256(note_body(raw.text))))
    END                                         AS body_hash,
    exam.note_id IS NOT NULL                    AS chest_exam
FROM
    @note_project.@note_dataset.radiology raw
LEFT JOIN
    src_cxr_exam exam ON exam.note_id = raw.note_id
WHERE
    EXISTS (SELECT 1 FROM src_cxr_study cxr WHERE cxr.subject_id = raw.subject_id)
;

-- -------------------------------------------------------------------
-- lk_cxr_candidate
-- Match within a patient and calendar date; require uniqueness in both
-- directions. Ambiguity and differences remain reviewable source data.
-- -------------------------------------------------------------------

CREATE TEMP TABLE lk_cxr_candidate AS
SELECT
    cxr.study_id                                AS study_id,
    rad.source_note_id                          AS radiology_note_id,
    CASE WHEN cxr.full_hash = rad.full_hash
         THEN 'FULL_TEXT_SAME_DAY'
         ELSE 'CLINICAL_BODY_CHEST_WITHIN_1H'
    END                                         AS match_method,
    count(*) OVER (PARTITION BY cxr.study_id)     AS study_candidates,
    count(*) OVER (PARTITION BY rad.source_note_id) AS report_candidates
FROM
    src_cxr_study cxr
INNER JOIN
    src_cxr_radiology rad
        ON  rad.subject_id = cxr.subject_id
        AND DATE(rad.charttime) = DATE(cxr.study_datetime)
        AND rad.note_type = 'RR'
        AND cxr.study_dates = 1
        AND (
            cxr.full_hash = rad.full_hash
            OR (cxr.body_hash = rad.body_hash AND rad.chest_exam AND NOT cxr.has_addendum
                AND ABS(DATETIME_DIFF(rad.charttime, cxr.study_datetime, MICROSECOND)) <= 3600000000)
        )
;

CREATE TABLE IF NOT EXISTS @etl_project.@etl_dataset.ext_cxr_reconciliation (
    subject_id INT64 NOT NULL, study_id INT64,
    source_note_id STRING NOT NULL, study_datetime DATETIME,
    decision STRING NOT NULL, radiology_note_id STRING, match_method STRING,
    candidate_count INT64 NOT NULL
);
CREATE TEMP TABLE lk_cxr_resolution AS
SELECT
    cxr.subject_id                              AS subject_id,
    cxr.study_id                                AS study_id,
    cxr.source_note_id                          AS source_note_id,
    cxr.study_datetime                          AS study_datetime,
    CASE
        WHEN cxr.study_datetime IS NULL OR cxr.study_dates <> 1 THEN 'REVIEW_STUDY_TIME'
        WHEN match.radiology_note_id IS NOT NULL THEN 'MATCHED_RADIOLOGY'
        WHEN NOT EXISTS (SELECT 1 FROM src_cxr_radiology r WHERE r.subject_id = cxr.subject_id)
            THEN 'NEW_NO_PATIENT_RADIOLOGY'
        ELSE 'REVIEW_OVERLAP'
    END                                         AS decision,
    match.radiology_note_id                      AS radiology_note_id,
    match.match_method                          AS match_method,
    (SELECT count(*) FROM lk_cxr_candidate c WHERE c.study_id = cxr.study_id) AS candidate_count
FROM
    src_cxr_study cxr
LEFT JOIN
    lk_cxr_candidate match
        ON match.study_id = cxr.study_id
        AND match.study_candidates = 1 AND match.report_candidates = 1
;
ASSERT (SELECT count(*) FROM lk_cxr_resolution) = (SELECT count(*) FROM src_cxr_study)
    AS 'Reconciliation join fanout';
ASSERT NOT EXISTS (
    SELECT 1 FROM @etl_project.@etl_dataset.ext_cxr_reconciliation old
    LEFT JOIN lk_cxr_resolution new USING (study_id)
    WHERE old.decision IN ('MATCHED_RADIOLOGY', 'NEW_NO_PATIENT_RADIOLOGY')
      AND (new.study_id IS NULL OR TO_JSON_STRING(STRUCT(old.decision, old.radiology_note_id))
              != TO_JSON_STRING(STRUCT(new.decision, new.radiology_note_id)))
) AS 'Changed existing CXR resolution requires reconciliation';
BEGIN TRANSACTION;
DELETE FROM @etl_project.@etl_dataset.ext_cxr_reconciliation WHERE TRUE;
INSERT INTO @etl_project.@etl_dataset.ext_cxr_reconciliation SELECT * FROM lk_cxr_resolution;
COMMIT TRANSACTION;

-- -------------------------------------------------------------------
-- Canonical source views for the existing NOTE mapping
-- note_seq = 0 means unavailable, not a chronological study sequence.
-- -------------------------------------------------------------------

CREATE OR REPLACE VIEW @modality_project.@modality_dataset.cxr AS
SELECT
    link.source_note_id                         AS note_id,
    link.subject_id                             AS subject_id,
    CAST(NULL AS INT64)                       AS hadm_id,
    'CXR'                                 AS note_type,
    0                                           AS note_seq,
    link.study_datetime                         AS charttime,
    CAST(NULL AS DATETIME)                     AS storetime,
    raw.text                                    AS text
FROM
    @modality_project.@modality_dataset.cxr_reports raw
INNER JOIN
    @etl_project.@etl_dataset.ext_cxr_reconciliation link ON link.study_id = CAST(raw.study_id AS INT64)
WHERE
    link.decision = 'NEW_NO_PATIENT_RADIOLOGY'
;
CREATE OR REPLACE VIEW @modality_project.@modality_dataset.cxr_detail AS
SELECT
    src.note_id                                 AS note_id,
    src.subject_id                              AS subject_id,
    'study_id'                            AS field_name,
    raw.study_id                                AS field_value,
    1                                           AS field_ordinal
FROM @modality_project.@modality_dataset.cxr src
INNER JOIN @modality_project.@modality_dataset.cxr_reports raw
    ON src.note_id = raw.subject_id || '-CXR-' || raw.study_id
;

CREATE OR REPLACE VIEW @modality_project.@modality_dataset.ecg_machine AS
SELECT
    subject_id || '-ECG_MACHINE-' || study_id     AS note_id,
    CAST(subject_id AS INT64)                 AS subject_id,
    CAST(NULL AS INT64)                       AS hadm_id,
    'ECG_MACHINE'                         AS note_type,
    0                                           AS note_seq,
    CAST(ecg_time AS DATETIME)                 AS charttime,
    CAST(NULL AS DATETIME)                     AS storetime,
    ARRAY_TO_STRING([
        COALESCE(report_0,''), COALESCE(report_1,''), COALESCE(report_2,''),
        COALESCE(report_3,''), COALESCE(report_4,''), COALESCE(report_5,''),
        COALESCE(report_6,''), COALESCE(report_7,''), COALESCE(report_8,''),
        COALESCE(report_9,''), COALESCE(report_10,''), COALESCE(report_11,''),
        COALESCE(report_12,''), COALESCE(report_13,''), COALESCE(report_14,''),
        COALESCE(report_15,''), COALESCE(report_16,''), COALESCE(report_17,'')
    ], '\n')                                   AS text
FROM
    @modality_project.@modality_dataset.ecg_machine_measurements
WHERE
    TRIM(CONCAT(coalesce(report_0,''),coalesce(report_1,''),coalesce(report_2,''),coalesce(report_3,''),coalesce(report_4,''),coalesce(report_5,''),
        coalesce(report_6,''),coalesce(report_7,''),coalesce(report_8,''),coalesce(report_9,''),coalesce(report_10,''),coalesce(report_11,''),coalesce(report_12,''),
        coalesce(report_13,''),coalesce(report_14,''),coalesce(report_15,''),coalesce(report_16,''),coalesce(report_17,'')), ' \t\r\n') <> ''
;
CREATE OR REPLACE VIEW @modality_project.@modality_dataset.ecg_machine_detail AS
SELECT
    src.note_id                                 AS note_id,
    src.subject_id                              AS subject_id,
    'study_id'                            AS field_name,
    raw.study_id                                AS field_value,
    1                                           AS field_ordinal
FROM @modality_project.@modality_dataset.ecg_machine src
INNER JOIN @modality_project.@modality_dataset.ecg_machine_measurements raw
    ON src.note_id = raw.subject_id || '-ECG_MACHINE-' || raw.study_id
;

SELECT decision, count(*) FROM @etl_project.@etl_dataset.ext_cxr_reconciliation GROUP BY decision ORDER BY decision;
