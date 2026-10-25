-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- NOTE-only ETL: source -> lookup -> INSERT / visit UPDATE -> validation
-- No persistent staging, CDM extensions, type migrations or other CDM DML.
-- Run with psql -X; audit_file receives metadata, never note text.
-- -------------------------------------------------------------------
\set ON_ERROR_STOP on
\if :{?source_schema}
\else
\set source_schema mimiciv_note
\endif
\if :{?radiology_schema}
\else
\set radiology_schema mimiciv_note
\endif
\if :{?cdm_schema}
\else
\set cdm_schema mimiciv
\endif
\if :{?source_table}
\else
\set source_table discharge
\endif
BEGIN ISOLATION LEVEL REPEATABLE READ;
SET LOCAL client_encoding = 'UTF8';
SET LOCAL TimeZone = 'UTC';
SET LOCAL lock_timeout = '30s';
SELECT pg_advisory_xact_lock(hashtext(current_database()), hashtext(:'cdm_schema' || '.note')) \gset
CREATE TEMP TABLE note_checks (
    check_name text PRIMARY KEY, passed boolean NOT NULL CHECK (passed)
) ON COMMIT DROP;
INSERT INTO note_checks VALUES
    ('supported_source', :'source_table' IN ('discharge','radiology','cxr','ecg_machine')),
    ('existing_bigint_identifiers', (
        SELECT count(*) = 4 FROM information_schema.columns
        WHERE table_schema = :'cdm_schema' AND table_name = 'note'
          AND column_name IN ('note_id','person_id','visit_occurrence_id','visit_detail_id')
          AND data_type = 'bigint'));
CREATE FUNCTION pg_temp.note_key(dataset text, source_table text, source_note_id text)
RETURNS bigint LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT ('x' || substr(encode(sha256(convert_to(
        dataset || '|' || source_table || '|' || source_note_id, 'UTF8')), 'hex'), 1, 15))::bit(60)::bigint
$$;

-- -------------------------------------------------------------------
-- src_note_text: adapters expose the same read-only source contract
-- -------------------------------------------------------------------
SELECT :'source_table' = 'cxr' AS is_cxr, :'source_table' = 'ecg_machine' AS is_ecg \gset
\if :is_cxr
\ir st_cxr_note.sql
\elif :is_ecg
\ir st_ecg_note.sql
\else
CREATE TEMP VIEW src_note_text AS
SELECT note_id::text AS source_note_id, subject_id, hadm_id, note_type,
    charttime, storetime, text AS note_text,
    NULL::text AS study_id, NULL::bigint AS reuse_note_id,
    'DOCUMENT_CHARTTIME'::text AS timestamp_basis, 'REPORT'::text AS component_kind,
    NULL::text AS match_method
FROM :"source_schema".:"source_table";
\endif
CREATE TEMP TABLE src_note ON COMMIT DROP AS
SELECT
    :'source_table'::text                        AS source_table,
    s.source_note_id                            AS source_note_id,
    pg_temp.note_key(CASE :'source_table'
        WHEN 'cxr' THEN 'mimic-cxr' WHEN 'ecg_machine' THEN 'mimic-iv-ecg'
        ELSE 'mimic-iv-note' END, :'source_table', s.source_note_id) AS note_id,
    s.subject_id, s.hadm_id, s.note_type, s.charttime, s.storetime,
    s.study_id, s.reuse_note_id, s.timestamp_basis, s.component_kind, s.match_method,
    encode(sha256(convert_to(s.note_text,'UTF8')),'hex') AS text_sha256,
    octet_length(s.note_text)::bigint            AS text_bytes,
    btrim(s.note_text, E' \t\r\n') <> ''         AS has_text
FROM src_note_text s;
CREATE UNIQUE INDEX ON src_note(source_note_id);
CREATE UNIQUE INDEX ON src_note(note_id);
ANALYZE src_note;
INSERT INTO note_checks VALUES ('source_identity', (
        SELECT count(*) = 0 FROM src_note WHERE source_note_id IS NULL OR source_note_id = '' OR subject_id IS NULL));

-- -------------------------------------------------------------------
-- lk_note_type / lk_note_person / lk_note_visit
-- Admission keys are numeric. subject|date pseudo-visits are excluded.
-- An explicit but unmatched hadm_id never falls back to another visit.
-- -------------------------------------------------------------------
CREATE TEMP TABLE lk_note_type (
    source_table text, note_type text PRIMARY KEY, loinc_code text, note_title text
) ON COMMIT DROP;
INSERT INTO lk_note_type VALUES
    ('discharge','DS','18842-5','Discharge summary'),
    ('discharge','AD','18842-5','Discharge summary addendum'),
    ('radiology','RR','68604-8','Radiology report'),
    ('radiology','AR','68604-8','Radiology report addendum'),
    ('cxr','CXR','68604-8','Chest radiograph report'),
    ('cxr','CXR_SUPPLEMENT','68604-8','Chest radiograph supplementary report (preliminary/addendum)'),
    ('cxr','CXR_VARIANT','68604-8','Chest radiograph source report (possible duplicate or variant)'),
    ('ecg_machine','ECG_MACHINE','75425-9','ECG machine interpretation');
CREATE TEMP TABLE lk_note_person ON COMMIT DROP AS
SELECT s.*, p.person_id, old.note_id IS NOT NULL AS already_loaded,
    old.note_date AS existing_note_date, old.visit_occurrence_id AS existing_visit_id,
    old.visit_detail_id AS existing_visit_detail_id,
    reused.note_date AS reused_note_date, reused.note_datetime AS reused_note_datetime,
    reused.visit_occurrence_id AS reused_visit_id
FROM src_note s
LEFT JOIN :"cdm_schema".person p ON p.person_source_value = s.subject_id::text
LEFT JOIN :"cdm_schema".note old ON old.note_id = s.note_id
LEFT JOIN :"cdm_schema".note reused ON reused.note_id = s.reuse_note_id;
CREATE UNIQUE INDEX ON lk_note_person(source_note_id);
ANALYZE lk_note_person;
CREATE TEMP TABLE lk_note_visit ON COMMIT DROP AS
SELECT v.* FROM :"cdm_schema".visit_occurrence v
WHERE split_part(v.visit_source_value,'|',2) ~ '^[0-9]+$';
CREATE UNIQUE INDEX ON lk_note_visit(visit_occurrence_id);
CREATE INDEX ON lk_note_visit(person_id, visit_source_value);
CREATE INDEX ON lk_note_visit(person_id, visit_start_datetime, visit_end_datetime);
ANALYZE lk_note_visit;
CREATE TEMP TABLE lk_note_visit_match ON COMMIT DROP AS
SELECT s.note_id, count(v.visit_occurrence_id) AS candidate_count,
    CASE WHEN count(v.visit_occurrence_id) = 1 THEN min(v.visit_occurrence_id) END AS visit_occurrence_id
FROM lk_note_person s
LEFT JOIN lk_note_visit v ON v.person_id = s.person_id AND (
    (s.hadm_id IS NOT NULL AND v.visit_source_value = s.subject_id::text || '|' || s.hadm_id::text)
    OR (s.hadm_id IS NULL AND s.timestamp_basis = 'DOCUMENT_CHARTTIME'
        AND s.charttime BETWEEN v.visit_start_datetime AND v.visit_end_datetime))
GROUP BY s.note_id;
CREATE UNIQUE INDEX ON lk_note_visit_match(note_id);
CREATE TEMP TABLE lk_note_mapped ON COMMIT DROP AS
SELECT
    s.*,
    c.concept_id                                AS note_class_concept_id,
    m.note_title                               AS note_title,
    CASE WHEN s.source_table = 'cxr' THEN coalesce(s.existing_visit_id,s.reused_visit_id)
         WHEN s.hadm_id IS NOT NULL THEN v.visit_occurrence_id
         WHEN s.existing_visit_id IS NOT NULL THEN s.existing_visit_id
         ELSE v.visit_occurrence_id END         AS visit_occurrence_id,
    v.candidate_count                          AS visit_candidate_count,
    CASE
        WHEN s.person_id IS NULL THEN 'MISSING_PERSON'
        WHEN s.has_text IS NOT TRUE THEN 'EMPTY_TEXT'
        WHEN s.already_loaded AND s.source_table = 'cxr' THEN 'LEGACY_DATE_PROXY_RETAINED'
        WHEN s.reuse_note_id IS NOT NULL THEN 'REUSED_EXISTING_RADIOLOGY'
        WHEN s.charttime IS NULL THEN 'MISSING_DOCUMENT_DATE'
        ELSE 'LOADED'
    END                                         AS load_status,
    CASE
        WHEN s.source_table = 'cxr' AND s.reuse_note_id IS NOT NULL THEN 'REUSED_RADIOLOGY_VISIT'
        WHEN s.source_table = 'cxr' THEN 'NO_DOCUMENT_TIME'
        WHEN s.hadm_id IS NOT NULL AND v.candidate_count = 1 THEN 'EXPLICIT_HADM'
        WHEN s.hadm_id IS NOT NULL THEN 'UNMATCHED_EXPLICIT_HADM'
        WHEN s.existing_visit_id IS NOT NULL THEN 'PRESERVED_EXISTING_VISIT'
        WHEN s.timestamp_basis = 'DEVICE_CLOCK_UNSYNCHRONIZED' THEN 'UNSYNCHRONIZED_DEVICE_CLOCK'
        WHEN v.candidate_count = 1 THEN 'UNIQUE_ADMISSION_INTERVAL'
        WHEN v.candidate_count > 1 THEN 'AMBIGUOUS_ADMISSION_INTERVAL'
        ELSE 'NO_ADMISSION_INTERVAL'
    END                                         AS visit_status
FROM lk_note_person s
LEFT JOIN lk_note_type m ON m.source_table = s.source_table AND m.note_type = s.note_type
LEFT JOIN :"cdm_schema".concept c ON c.vocabulary_id = 'LOINC'
    AND c.concept_code = m.loinc_code AND c.domain_id = 'Note'
    AND c.standard_concept = 'S' AND c.invalid_reason IS NULL
    AND CURRENT_DATE BETWEEN c.valid_start_date AND c.valid_end_date
INNER JOIN lk_note_visit_match v USING (note_id);
CREATE UNIQUE INDEX ON lk_note_mapped(note_id);
ANALYZE lk_note_mapped;

-- Existing identity and text are immutable. Changed sources need review.
-- Count the complete population: EXISTS startup plans can repeatedly probe
-- millions of rows when the expected number of violations is zero.
INSERT INTO note_checks VALUES
    ('no_unaccounted_existing_source', (
        SELECT count(*) = 0 FROM :"cdm_schema".note n
        WHERE n.note_source_value IN (SELECT note_type FROM lk_note_type WHERE source_table = :'source_table')
          AND NOT EXISTS (SELECT 1 FROM src_note s WHERE s.note_id = n.note_id))),
    ('document_class_mapping', (
        SELECT count(*) = 0 FROM lk_note_mapped WHERE note_class_concept_id IS NULL)),
    ('constant_concepts', (SELECT count(*) = 3 FROM :"cdm_schema".concept
        WHERE ((concept_id = 32817 AND domain_id = 'Type Concept')
            OR (concept_id = 32678 AND domain_id = 'Metadata')
            OR (concept_id = 4180186 AND domain_id = 'Language'))
        AND standard_concept = 'S' AND invalid_reason IS NULL
        AND CURRENT_DATE BETWEEN valid_start_date AND valid_end_date)),
    ('unambiguous_explicit_hadm', (
        SELECT count(*) = 0 FROM lk_note_mapped WHERE hadm_id IS NOT NULL AND visit_candidate_count > 1)),
    ('existing_identity_and_text', (
        SELECT count(*) = 0 FROM lk_note_mapped s INNER JOIN :"cdm_schema".note n USING (note_id)
        WHERE n.person_id IS DISTINCT FROM s.person_id OR n.note_source_value IS DISTINCT FROM s.note_type
          OR encode(sha256(convert_to(n.note_text,'UTF8')),'hex') IS DISTINCT FROM s.text_sha256
          OR (s.source_table <> 'cxr' AND (n.note_date IS DISTINCT FROM s.charttime::date
              OR n.note_datetime IS DISTINCT FROM s.charttime))
          OR s.has_text IS NOT TRUE)),
    ('existing_visit_consistent', (
        SELECT count(*) = 0 FROM lk_note_mapped s
        LEFT JOIN :"cdm_schema".visit_occurrence v ON v.visit_occurrence_id = s.existing_visit_id
        WHERE s.existing_visit_id IS NOT NULL AND (v.person_id IS DISTINCT FROM s.person_id
          OR (s.hadm_id IS NOT NULL AND s.existing_visit_id IS DISTINCT FROM s.visit_occurrence_id)))),
    ('reused_report_same_person', (
        SELECT count(*) = 0 FROM lk_note_mapped s LEFT JOIN :"cdm_schema".note n ON n.note_id = s.reuse_note_id
        WHERE s.reuse_note_id IS NOT NULL AND n.person_id IS DISTINCT FROM s.person_id));

-- -------------------------------------------------------------------
-- note: only these two statements modify permanent clinical data
-- -------------------------------------------------------------------
CREATE TEMP TABLE note_changes (note_id bigint PRIMARY KEY, action text) ON COMMIT DROP;
WITH inserted AS (
    INSERT INTO :"cdm_schema".note (
        note_id, person_id, note_date, note_datetime, note_type_concept_id,
        note_class_concept_id, note_title, note_text, encoding_concept_id,
        language_concept_id, provider_id, visit_occurrence_id, visit_detail_id, note_source_value
    )
    SELECT
        s.note_id                               AS note_id,
        s.person_id                             AS person_id,
        s.charttime::date                       AS note_date,
        s.charttime                             AS note_datetime,
        32817                                   AS note_type_concept_id,
        s.note_class_concept_id                 AS note_class_concept_id,
        s.note_title                            AS note_title,
        raw.note_text                           AS note_text,
        32678                                   AS encoding_concept_id,
        4180186                                 AS language_concept_id,
        NULL::bigint                            AS provider_id,
        s.visit_occurrence_id                   AS visit_occurrence_id,
        NULL::bigint                            AS visit_detail_id,
        s.note_type                             AS note_source_value
    FROM lk_note_mapped s
    INNER JOIN src_note_text raw USING (source_note_id)
    WHERE s.load_status = 'LOADED' AND NOT s.already_loaded
    RETURNING note_id
)
INSERT INTO note_changes SELECT note_id, 'INSERT' FROM inserted;
WITH updated AS (
    UPDATE :"cdm_schema".note n SET visit_occurrence_id = s.visit_occurrence_id
    FROM lk_note_mapped s
    WHERE n.note_id = s.note_id AND s.load_status = 'LOADED'
      AND n.visit_occurrence_id IS NULL AND s.visit_occurrence_id IS NOT NULL
    RETURNING n.note_id
)
INSERT INTO note_changes SELECT note_id, 'VISIT_UPDATE' FROM updated;
INSERT INTO note_checks VALUES
    ('loaded_rows_match_source', (
        SELECT count(*) = 0 FROM lk_note_mapped s LEFT JOIN :"cdm_schema".note n USING (note_id)
        WHERE s.load_status IN ('LOADED','LEGACY_DATE_PROXY_RETAINED') AND (
            n.note_id IS NULL OR n.person_id IS DISTINCT FROM s.person_id
            OR n.note_source_value IS DISTINCT FROM s.note_type
            OR encode(sha256(convert_to(n.note_text,'UTF8')),'hex') IS DISTINCT FROM s.text_sha256
            OR n.visit_occurrence_id IS DISTINCT FROM s.visit_occurrence_id
            OR (s.load_status = 'LOADED' AND (
                n.note_date IS DISTINCT FROM s.charttime::date OR n.note_datetime IS DISTINCT FROM s.charttime
                OR n.note_type_concept_id <> 32817 OR n.note_class_concept_id IS DISTINCT FROM s.note_class_concept_id
                OR n.note_title IS DISTINCT FROM s.note_title OR n.encoding_concept_id <> 32678
                OR n.language_concept_id <> 4180186 OR n.provider_id IS NOT NULL
                OR n.visit_detail_id IS DISTINCT FROM s.existing_visit_detail_id))))),
    ('only_note_permanent_rows_changed', (
        SELECT count(*) = 0 FROM pg_stat_xact_user_tables WHERE schemaname NOT LIKE 'pg_temp_%'
          AND (schemaname, relname) <> (:'cdm_schema', 'note')
          AND n_tup_ins + n_tup_upd + n_tup_del > 0));
\if :{?audit_file}
COPY (SELECT s.source_table,s.source_note_id,s.study_id,s.component_kind,s.subject_id,s.hadm_id,
    s.note_id,s.reuse_note_id,s.person_id,s.charttime,s.storetime,s.timestamp_basis,
    s.text_sha256,s.text_bytes,s.load_status,s.visit_status,s.visit_candidate_count,
    s.existing_visit_id,s.visit_occurrence_id,s.existing_note_date,s.reused_note_date,s.reused_note_datetime,s.match_method,c.action,
    CASE WHEN s.load_status = 'REUSED_EXISTING_RADIOLOGY' THEN s.reuse_note_id
         WHEN s.load_status IN ('LOADED','LEGACY_DATE_PROXY_RETAINED') THEN s.note_id END AS target_note_id
    FROM lk_note_mapped s LEFT JOIN note_changes c USING (note_id) ORDER BY s.source_note_id)
TO STDOUT WITH (FORMAT CSV, HEADER) \g :audit_file
\endif
SELECT jsonb_build_object(
    'source_table', :'source_table', 'source_rows', (SELECT count(*) FROM src_note),
    'inserted_rows', (SELECT count(*) FROM note_changes WHERE action = 'INSERT'),
    'visit_updated_rows', (SELECT count(*) FROM note_changes WHERE action = 'VISIT_UPDATE'),
    'changed_note_rows', (SELECT count(*) FROM note_changes),
    'load_status', (SELECT jsonb_object_agg(load_status,n) FROM (SELECT load_status,count(*) n FROM lk_note_mapped GROUP BY 1) x),
    'visit_status', (SELECT jsonb_object_agg(visit_status,n) FROM (SELECT visit_status,count(*) n FROM lk_note_mapped GROUP BY 1) x),
    'checks_passed', (SELECT count(*) FROM note_checks)
)::text AS note_etl_report \gset
COMMIT;
\echo NOTE_ETL_REPORT :note_etl_report
