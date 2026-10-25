-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- Prepare canonical NOTE inputs and reconcile CXR documents
--
-- Dependencies: verified modality raw tables and complete raw radiology.
-- Run before note.sql for cxr and ecg_machine, then ext_modality_note.sql.
-- -------------------------------------------------------------------

\set ON_ERROR_STOP on
\if :{?source_schema}
\else
\set source_schema mimiciv_modality
\endif
\if :{?radiology_schema}
\else
\set radiology_schema mimiciv_note
\endif
\if :{?etl_schema}
\else
\set etl_schema mimiciv_note_etl
\endif

BEGIN ISOLATION LEVEL REPEATABLE READ;
SET LOCAL TimeZone = 'UTC';
SET LOCAL lock_timeout = '30s';
SELECT pg_advisory_xact_lock(hashtext(current_database()), hashtext(:'etl_schema' || '.modality')) \gset
CREATE SCHEMA IF NOT EXISTS :"etl_schema";
CREATE TEMP TABLE modality_checks (
    check_name text PRIMARY KEY, passed boolean NOT NULL CHECK (passed)
) ON COMMIT DROP;
INSERT INTO modality_checks VALUES
    ('verified_non_echo_sources', (
        SELECT count(*) = 8 AND bool_and(checksum_status = 'VERIFIED')
        FROM :"source_schema".source_file WHERE table_name <> 'echo_study_list'
    )),
    ('radiology_available', (SELECT count(*) > 0 FROM :"radiology_schema".radiology)),
    ('cxr_study_inventory', NOT EXISTS (
        (SELECT subject_id, study_id FROM :"source_schema".cxr_reports
         EXCEPT ALL SELECT subject_id, study_id FROM :"source_schema".cxr_study_list)
        UNION ALL
        (SELECT subject_id, study_id FROM :"source_schema".cxr_study_list
         EXCEPT ALL SELECT subject_id, study_id FROM :"source_schema".cxr_reports)
    ));

-- Comparison only: raw text and the NOTE text are never normalized.
CREATE FUNCTION pg_temp.note_normalize(value text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT btrim(regexp_replace(
        regexp_replace(value, '^[[:space:]]*FINAL REPORT[[:space:]]*', ''),
        '[[:space:]]+', ' ', 'g'))
$$;
CREATE FUNCTION pg_temp.note_body(value text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT pg_temp.note_normalize(substring(value FROM
        '(?is)((?:FINDINGS(?: AND IMPRESSION)?|IMPRESSION):.*)'))
$$;

-- -------------------------------------------------------------------
-- src_cxr_study
-- One acquisition timestamp per study, retaining all raw image metadata
-- -------------------------------------------------------------------

CREATE TEMP TABLE src_cxr_time ON COMMIT DROP AS
SELECT
    CAST(subject_id AS integer)                 AS subject_id,
    CAST(study_id AS integer)                   AS study_id,
    CASE WHEN studydate <> '' AND studytime <> '' THEN
        CAST(studydate AS date) + CAST(
            substr(lpad(split_part(studytime, '.', 1), 6, '0'), 1, 2) || ':' ||
            substr(lpad(split_part(studytime, '.', 1), 6, '0'), 3, 2) || ':' ||
            substr(lpad(split_part(studytime, '.', 1), 6, '0'), 5, 2) ||
            CASE WHEN strpos(studytime, '.') > 0 THEN '.' || split_part(studytime, '.', 2) ELSE '' END
        AS time)
    END                                         AS study_datetime
FROM
    :"source_schema".cxr_metadata
;
CREATE TEMP TABLE src_cxr_study ON COMMIT DROP AS
SELECT
    CAST(raw.subject_id AS integer)             AS subject_id,
    CAST(raw.study_id AS integer)               AS study_id,
    raw.subject_id || '-CXR-' || raw.study_id    AS source_note_id,
    tm.study_datetime                           AS study_datetime,
    tm.study_dates                              AS study_dates,
    encode(sha256(convert_to(pg_temp.note_normalize(raw.text), 'UTF8')), 'hex') AS full_hash,
    CASE WHEN length(pg_temp.note_body(raw.text)) >= 40 THEN
        encode(sha256(convert_to(pg_temp.note_body(raw.text), 'UTF8')), 'hex')
    END                                         AS body_hash,
    raw.text ~* 'addendum|amended report|_{10}'    AS has_addendum
FROM
    :"source_schema".cxr_reports raw
LEFT JOIN (
    SELECT subject_id, study_id, min(study_datetime) AS study_datetime,
        count(DISTINCT study_datetime::date) AS study_dates
    FROM src_cxr_time GROUP BY subject_id, study_id
) tm
    ON tm.subject_id = CAST(raw.subject_id AS integer)
   AND tm.study_id = CAST(raw.study_id AS integer)
;
CREATE UNIQUE INDEX ON src_cxr_study(study_id);
CREATE INDEX ON src_cxr_study(subject_id);
ANALYZE src_cxr_study;

-- -------------------------------------------------------------------
-- src_cxr_radiology
-- Full-text matches are strongest. A clinical-body match also requires
-- a chest radiograph exam and acquisition/chart times within one hour.
-- -------------------------------------------------------------------

CREATE TEMP TABLE src_cxr_exam ON COMMIT DROP AS
SELECT DISTINCT note_id
FROM :"radiology_schema".radiology_detail
WHERE field_name = 'exam_name'
    AND field_value ~* 'CHEST|CXR'
    AND field_value !~* '(^|[^A-Z])(CT|CTA|MR|MRI|US|PET)([^A-Z]|$)|ANGIO'
;
CREATE UNIQUE INDEX ON src_cxr_exam(note_id);
CREATE TEMP TABLE src_cxr_radiology ON COMMIT DROP AS
SELECT
    raw.note_id                                 AS source_note_id,
    raw.subject_id                              AS subject_id,
    raw.charttime                               AS charttime,
    raw.note_type                               AS note_type,
    encode(sha256(convert_to(pg_temp.note_normalize(raw.text), 'UTF8')), 'hex') AS full_hash,
    CASE WHEN length(pg_temp.note_body(raw.text)) >= 40 THEN
        encode(sha256(convert_to(pg_temp.note_body(raw.text), 'UTF8')), 'hex')
    END                                         AS body_hash,
    exam.note_id IS NOT NULL                    AS chest_exam
FROM
    :"radiology_schema".radiology raw
LEFT JOIN
    src_cxr_exam exam ON exam.note_id = raw.note_id
WHERE
    EXISTS (SELECT 1 FROM src_cxr_study cxr WHERE cxr.subject_id = raw.subject_id)
;
CREATE INDEX ON src_cxr_radiology(subject_id, (charttime::date));
CREATE INDEX ON src_cxr_radiology(subject_id, full_hash);
ANALYZE src_cxr_radiology;

-- -------------------------------------------------------------------
-- lk_cxr_candidate
-- Match within a patient and calendar date; require uniqueness in both
-- directions. Ambiguity and differences remain reviewable source data.
-- -------------------------------------------------------------------

CREATE TEMP TABLE lk_cxr_candidate ON COMMIT DROP AS
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
        AND rad.charttime::date = cxr.study_datetime::date
        AND rad.note_type = 'RR'
        AND cxr.study_dates = 1
        AND (
            cxr.full_hash = rad.full_hash
            OR (cxr.body_hash = rad.body_hash AND rad.chest_exam AND NOT cxr.has_addendum
                AND abs(extract(epoch FROM rad.charttime - cxr.study_datetime)) <= 3600)
        )
;
CREATE INDEX ON lk_cxr_candidate(study_id);
ANALYZE lk_cxr_candidate;

CREATE TABLE IF NOT EXISTS :"etl_schema".cxr_reconciliation (
    subject_id integer NOT NULL, study_id integer PRIMARY KEY,
    source_note_id text NOT NULL, study_datetime timestamp,
    decision text NOT NULL, radiology_note_id text, match_method text,
    candidate_count bigint NOT NULL
);
CREATE TEMP TABLE lk_cxr_resolution ON COMMIT DROP AS
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
INSERT INTO modality_checks VALUES
    ('reconciliation_no_fanout', (SELECT count(*) FROM lk_cxr_resolution) = (SELECT count(*) FROM src_cxr_study)),
    ('stable_existing_resolution', NOT EXISTS (
        SELECT 1 FROM :"etl_schema".cxr_reconciliation old
        LEFT JOIN lk_cxr_resolution new USING (study_id)
        WHERE old.decision IN ('MATCHED_RADIOLOGY', 'NEW_NO_PATIENT_RADIOLOGY')
          AND (new.study_id IS NULL OR (old.decision, old.radiology_note_id)
              IS DISTINCT FROM (new.decision, new.radiology_note_id))
    ));
DELETE FROM :"etl_schema".cxr_reconciliation;
INSERT INTO :"etl_schema".cxr_reconciliation SELECT * FROM lk_cxr_resolution;

-- -------------------------------------------------------------------
-- Canonical source views for the existing NOTE mapping
-- note_seq = 0 means unavailable, not a chronological study sequence.
-- -------------------------------------------------------------------

CREATE OR REPLACE VIEW :"source_schema".cxr AS
SELECT
    link.source_note_id                         AS note_id,
    link.subject_id                             AS subject_id,
    CAST(NULL AS integer)                       AS hadm_id,
    'CXR'::text                                 AS note_type,
    0                                           AS note_seq,
    link.study_datetime                         AS charttime,
    CAST(NULL AS timestamp)                     AS storetime,
    raw.text                                    AS text
FROM
    :"source_schema".cxr_reports raw
INNER JOIN
    :"etl_schema".cxr_reconciliation link ON link.study_id = CAST(raw.study_id AS integer)
WHERE
    link.decision = 'NEW_NO_PATIENT_RADIOLOGY'
;
CREATE OR REPLACE VIEW :"source_schema".cxr_detail AS
SELECT
    src.note_id                                 AS note_id,
    src.subject_id                              AS subject_id,
    'study_id'::text                            AS field_name,
    raw.study_id                                AS field_value,
    1                                           AS field_ordinal
FROM :"source_schema".cxr src
INNER JOIN :"source_schema".cxr_reports raw
    ON src.note_id = raw.subject_id || '-CXR-' || raw.study_id
;

CREATE OR REPLACE VIEW :"source_schema".ecg_machine AS
SELECT
    subject_id || '-ECG_MACHINE-' || study_id     AS note_id,
    CAST(subject_id AS integer)                 AS subject_id,
    CAST(NULL AS integer)                       AS hadm_id,
    'ECG_MACHINE'::text                         AS note_type,
    0                                           AS note_seq,
    CAST(ecg_time AS timestamp)                 AS charttime,
    CAST(NULL AS timestamp)                     AS storetime,
    array_to_string(ARRAY[
        coalesce(report_0,''), coalesce(report_1,''), coalesce(report_2,''),
        coalesce(report_3,''), coalesce(report_4,''), coalesce(report_5,''),
        coalesce(report_6,''), coalesce(report_7,''), coalesce(report_8,''),
        coalesce(report_9,''), coalesce(report_10,''), coalesce(report_11,''),
        coalesce(report_12,''), coalesce(report_13,''), coalesce(report_14,''),
        coalesce(report_15,''), coalesce(report_16,''), coalesce(report_17,'')
    ], E'\n')                                   AS text
FROM
    :"source_schema".ecg_machine_measurements
WHERE
    btrim(concat(report_0,report_1,report_2,report_3,report_4,report_5,
        report_6,report_7,report_8,report_9,report_10,report_11,report_12,
        report_13,report_14,report_15,report_16,report_17), E' \t\r\n') <> ''
;
CREATE OR REPLACE VIEW :"source_schema".ecg_machine_detail AS
SELECT
    src.note_id                                 AS note_id,
    src.subject_id                              AS subject_id,
    'study_id'::text                            AS field_name,
    raw.study_id                                AS field_value,
    1                                           AS field_ordinal
FROM :"source_schema".ecg_machine src
INNER JOIN :"source_schema".ecg_machine_measurements raw
    ON src.note_id = raw.subject_id || '-ECG_MACHINE-' || raw.study_id
;

COMMIT;
SELECT decision, count(*) FROM :"etl_schema".cxr_reconciliation GROUP BY decision ORDER BY decision;
