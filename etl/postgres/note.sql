-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- Populate NOTE in an existing PostgreSQL CDM
--
-- Same rules as st_note.sql, lk_note.sql, cdm_note.sql and ext_note.sql.
-- Execute with psql -X -v ON_ERROR_STOP=1; see docs/note_etl.md.
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Connection-independent parameters
-- -------------------------------------------------------------------

\set ON_ERROR_STOP on
\if :{?source_schema}
\else
\set source_schema mimiciv_note
\endif
\if :{?cdm_schema}
\else
\set cdm_schema mimiciv
\endif
\if :{?etl_schema}
\else
\set etl_schema mimiciv_note_etl
\endif
\if :{?source_table}
\else
\set source_table discharge
\endif
\if :{?source_version}
\else
\set source_version 2.2
\endif
\if :{?source_sha256}
\else
\set source_sha256 ''
\endif

-- -------------------------------------------------------------------
-- Start a source-specific transaction
-- -------------------------------------------------------------------

BEGIN ISOLATION LEVEL REPEATABLE READ;
SET LOCAL client_encoding = 'UTF8';
SET LOCAL lock_timeout = '30s';
SELECT pg_advisory_xact_lock(hashtext(current_database()), hashtext(:'cdm_schema' || '.note')) \gset
CREATE TEMP TABLE note_checks (
    check_name text PRIMARY KEY,
    passed boolean NOT NULL CHECK (passed)
) ON COMMIT DROP;
INSERT INTO note_checks VALUES
    ('supported_source', :'source_table' IN ('discharge', 'radiology'));
SELECT :'source_table' || '_detail' AS detail_table \gset

-- -------------------------------------------------------------------
-- lk_note_type
-- Rules 1-2: discharge summaries and radiology reports
-- -------------------------------------------------------------------

CREATE TEMP TABLE lk_note_type (
    source_table text, note_type text, loinc_code text, note_title text
) ON COMMIT DROP;
INSERT INTO lk_note_type VALUES
    ('discharge', 'DS', '18842-5', 'Discharge summary'),
    ('discharge', 'AD', '18842-5', 'Discharge summary addendum'),
    ('radiology', 'RR', '68604-8', 'Radiology report'),
    ('radiology', 'AR', '68604-8', 'Radiology report addendum');

-- -------------------------------------------------------------------
-- src_note
-- -------------------------------------------------------------------

-- The positive 60-bit SHA-256 prefix is identical to the BigQuery expression.
-- Source version is provenance, not part of a document's stable identity.
CREATE TEMP TABLE src_note ON COMMIT DROP AS
SELECT
    :'source_table'::text AS source_table,
    note_id::text AS source_note_id,
    ('x' || substr(encode(sha256(convert_to(
        'mimic-iv-note|' || :'source_table' || '|' || note_id, 'UTF8'
    )), 'hex'), 1, 15))::bit(60)::bigint AS note_id,
    subject_id                              AS subject_id,
    hadm_id                                 AS hadm_id,
    note_type                               AS note_type,
    note_seq                                AS note_seq,
    charttime                               AS charttime,
    storetime                               AS storetime,
    encode(sha256(convert_to(text, 'UTF8')), 'hex') AS text_sha256,
    octet_length(convert_to(text, 'UTF8'))::bigint AS text_bytes
FROM
    :"source_schema".:"source_table"
;
CREATE UNIQUE INDEX ON src_note(source_note_id);
CREATE UNIQUE INDEX ON src_note(note_id);
ANALYZE src_note;

INSERT INTO note_checks VALUES
    ('nonempty_source', (SELECT count(*) > 0 FROM src_note)),
    ('source_required_fields', NOT EXISTS (
        SELECT 1 FROM src_note WHERE source_note_id IS NULL OR source_note_id = ''
        OR subject_id IS NULL OR charttime IS NULL OR text_sha256 IS NULL OR text_bytes = 0
    )),
    ('supported_note_types', NOT EXISTS (
        SELECT 1 FROM src_note s LEFT JOIN lk_note_type m
        USING (source_table, note_type) WHERE m.note_type IS NULL
    )),
    ('constant_concepts', (
        SELECT count(*) = 3 FROM :"cdm_schema".concept
        WHERE ((concept_id = 32817 AND domain_id = 'Type Concept')
            OR (concept_id = 32678 AND domain_id = 'Metadata')
            OR (concept_id = 4180186 AND domain_id = 'Language'))
          AND standard_concept = 'S' AND invalid_reason IS NULL
          AND CURRENT_DATE BETWEEN valid_start_date AND valid_end_date
    ));

-- -------------------------------------------------------------------
-- lk_note_mapped
-- Keep existing Core person and visit identifiers
-- -------------------------------------------------------------------

CREATE TEMP TABLE lk_note_mapped ON COMMIT DROP AS
SELECT
    src.*,
    per.person_id                               AS person_id,
    vis.visit_occurrence_id                     AS visit_occurrence_id,
    vc.concept_id                               AS note_class_concept_id,
    map.note_title                              AS note_title,
    CASE
        WHEN per.person_id IS NULL THEN 'MISSING_PERSON'
        ELSE 'LOADED'
    END                                         AS load_status,
    CASE
        WHEN per.person_id IS NULL THEN 'MISSING_PERSON'
        WHEN src.hadm_id IS NULL THEN 'NO_HADM_ID'
        WHEN vis.visit_occurrence_id IS NULL THEN 'MISSING_VISIT'
        ELSE 'MATCHED'
    END                                         AS visit_status
FROM
    src_note src
INNER JOIN
    lk_note_type map
        ON  map.source_table = src.source_table
        AND map.note_type = src.note_type
LEFT JOIN
    :"cdm_schema".person per
        ON per.person_source_value = CAST(src.subject_id AS text)
LEFT JOIN
    :"cdm_schema".visit_occurrence vis
        ON  vis.person_id = per.person_id
        AND src.hadm_id IS NOT NULL
        AND vis.visit_source_value =
            CAST(src.subject_id AS text) || '|' || CAST(src.hadm_id AS text)
LEFT JOIN
    :"cdm_schema".concept vc
        ON  vc.vocabulary_id = 'LOINC'
        AND vc.concept_code = map.loinc_code
        AND vc.domain_id = 'Note'
        AND vc.standard_concept = 'S'
        AND vc.invalid_reason IS NULL
        AND CURRENT_DATE BETWEEN vc.valid_start_date AND vc.valid_end_date
;

INSERT INTO note_checks VALUES
    ('no_join_fanout', (SELECT count(*) FROM lk_note_mapped) = (SELECT count(*) FROM src_note)),
    ('document_class_mapping', NOT EXISTS (
        SELECT 1 FROM lk_note_mapped WHERE note_class_concept_id IS NULL
    )),
    ('detail_parent_and_subject', NOT EXISTS (
        SELECT 1 FROM :"source_schema".:"detail_table" d
        INNER JOIN src_note s ON s.source_note_id = d.note_id
        WHERE s.subject_id IS DISTINCT FROM d.subject_id
    ));
CREATE UNIQUE INDEX ON lk_note_mapped(source_note_id);
CREATE UNIQUE INDEX ON lk_note_mapped(note_id);
ANALYZE lk_note_mapped;

-- -------------------------------------------------------------------
-- Persistent NOTE provenance tables
-- -------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS :"etl_schema";
CREATE TABLE IF NOT EXISTS :"etl_schema".note_id_map (
    source_table text NOT NULL,
    source_note_id text NOT NULL,
    note_id bigint NOT NULL UNIQUE,
    PRIMARY KEY (source_table, source_note_id)
);
CREATE TABLE IF NOT EXISTS :"etl_schema".note_metadata (
    source_table text NOT NULL,
    source_note_id text NOT NULL,
    note_id bigint NOT NULL,
    source_version text NOT NULL,
    source_sha256 text,
    subject_id integer NOT NULL,
    hadm_id integer,
    note_type text NOT NULL,
    note_seq integer NOT NULL,
    charttime timestamp NOT NULL,
    storetime timestamp,
    person_id bigint,
    visit_occurrence_id bigint,
    text_sha256 text NOT NULL,
    text_bytes bigint NOT NULL,
    load_status text NOT NULL,
    visit_status text NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (source_table, source_note_id)
);
CREATE TABLE IF NOT EXISTS :"etl_schema".note_detail (
    source_table text NOT NULL,
    source_note_id text NOT NULL,
    note_id bigint,
    subject_id integer NOT NULL,
    field_name text NOT NULL,
    field_value text NOT NULL,
    field_ordinal integer NOT NULL
);
ALTER TABLE :"etl_schema".note_detail ALTER COLUMN note_id DROP NOT NULL;
CREATE INDEX IF NOT EXISTS note_detail_note_id_idx ON :"etl_schema".note_detail(note_id);
CREATE TABLE IF NOT EXISTS :"etl_schema".note_etl_run (
    run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    finished_at timestamptz NOT NULL,
    source_table text NOT NULL,
    source_version text NOT NULL,
    source_sha256 text,
    source_rows bigint NOT NULL,
    loaded_rows bigint NOT NULL,
    rejected_person_rows bigint NOT NULL,
    missing_visit_rows bigint NOT NULL,
    no_hadm_rows bigint NOT NULL,
    detail_rows bigint NOT NULL,
    changed_note_rows bigint NOT NULL,
    text_mismatches bigint NOT NULL,
    passed_checks bigint NOT NULL,
    unmatched_detail_rows bigint NOT NULL DEFAULT 0
);
ALTER TABLE :"etl_schema".note_etl_run
    ADD COLUMN IF NOT EXISTS unmatched_detail_rows bigint NOT NULL DEFAULT 0;

-- -------------------------------------------------------------------
-- Check ownership of existing note identifiers
-- -------------------------------------------------------------------

INSERT INTO note_checks VALUES
    ('stable_id_map', NOT EXISTS (
        SELECT 1 FROM lk_note_mapped w JOIN :"etl_schema".note_id_map m
          ON (m.source_table = w.source_table AND m.source_note_id = w.source_note_id)
          OR m.note_id = w.note_id
        WHERE (m.source_table, m.source_note_id, m.note_id)
           IS DISTINCT FROM (w.source_table, w.source_note_id, w.note_id)
    )),
    ('no_unowned_target_id_collision', NOT EXISTS (
        SELECT 1 FROM lk_note_mapped w JOIN :"cdm_schema".note n ON n.note_id = w.note_id
        LEFT JOIN :"etl_schema".note_id_map m ON m.note_id = n.note_id
        WHERE m.note_id IS NULL
    )),
    ('no_implicit_source_deletions', NOT EXISTS (
        SELECT 1 FROM :"etl_schema".note_id_map m JOIN :"cdm_schema".note n USING (note_id)
        LEFT JOIN lk_note_mapped w ON w.note_id = m.note_id AND w.person_id IS NOT NULL
        WHERE m.source_table = :'source_table' AND w.note_id IS NULL
    ));

-- -------------------------------------------------------------------
-- Match existing Core ID types
-- -------------------------------------------------------------------

-- Core IDs are preserved verbatim; concept IDs remain ordinary integers.
-- Skip columns already widened by an earlier load: PostgreSQL rejects even
-- an unchanged TYPE when a view depends on the column.
SELECT format(
    'ALTER TABLE %I.%I ALTER COLUMN %I TYPE bigint',
    :'cdm_schema', ids.table_name, ids.column_name
)
FROM (VALUES
    ('note', 'note_id'),
    ('note', 'person_id'),
    ('note', 'visit_occurrence_id'),
    ('note', 'visit_detail_id'),
    ('note_nlp', 'note_id')
) AS ids(table_name, column_name)
INNER JOIN pg_attribute attr
    ON  attr.attrelid = format('%I.%I', :'cdm_schema', ids.table_name)::regclass
    AND attr.attname = ids.column_name
    AND NOT attr.attisdropped
WHERE attr.atttypid <> 'bigint'::regtype
ORDER BY ids.table_name, ids.column_name
\gexec

-- -------------------------------------------------------------------
-- cdm_note
-- Rules 1-2: discharge summaries and radiology reports
-- -------------------------------------------------------------------

-- PostgreSQL upserts in place to retain constraints and avoid rewriting
-- unchanged text. note_changed records only inserted or modified rows.
CREATE TEMP TABLE note_changed ON COMMIT DROP AS
WITH changed AS (
    INSERT INTO :"cdm_schema".note AS n (
        note_id, person_id, note_date, note_datetime, note_type_concept_id,
        note_class_concept_id, note_title, note_text, encoding_concept_id,
        language_concept_id, provider_id, visit_occurrence_id, visit_detail_id, note_source_value
    )
    SELECT
        src.note_id                             AS note_id,
        src.person_id                           AS person_id,
        CAST(src.charttime AS date)             AS note_date,
        src.charttime                           AS note_datetime,
        32817                                   AS note_type_concept_id, -- EHR
        src.note_class_concept_id                AS note_class_concept_id,
        src.note_title                          AS note_title,
        raw.text                                AS note_text,
        32678                                   AS encoding_concept_id, -- UTF-8
        4180186                                 AS language_concept_id, -- English
        CAST(NULL AS integer)                  AS provider_id,
        src.visit_occurrence_id                 AS visit_occurrence_id,
        CAST(NULL AS bigint)                   AS visit_detail_id,
        src.note_type                           AS note_source_value
    FROM
        lk_note_mapped src
    INNER JOIN
        :"source_schema".:"source_table" raw
            ON raw.note_id = src.source_note_id
    WHERE
        src.person_id IS NOT NULL
    ON CONFLICT (note_id) DO UPDATE SET
        person_id = EXCLUDED.person_id, note_date = EXCLUDED.note_date,
        note_datetime = EXCLUDED.note_datetime, note_type_concept_id = EXCLUDED.note_type_concept_id,
        note_class_concept_id = EXCLUDED.note_class_concept_id, note_title = EXCLUDED.note_title,
        note_text = EXCLUDED.note_text, encoding_concept_id = EXCLUDED.encoding_concept_id,
        language_concept_id = EXCLUDED.language_concept_id, provider_id = EXCLUDED.provider_id,
        visit_occurrence_id = EXCLUDED.visit_occurrence_id, visit_detail_id = EXCLUDED.visit_detail_id,
        note_source_value = EXCLUDED.note_source_value
    WHERE (n.person_id, n.note_date, n.note_datetime, n.note_type_concept_id,
           n.note_class_concept_id, n.note_title, n.note_text, n.encoding_concept_id,
           n.language_concept_id, n.provider_id, n.visit_occurrence_id,
           n.visit_detail_id, n.note_source_value)
       IS DISTINCT FROM
          (EXCLUDED.person_id, EXCLUDED.note_date, EXCLUDED.note_datetime, EXCLUDED.note_type_concept_id,
           EXCLUDED.note_class_concept_id, EXCLUDED.note_title, EXCLUDED.note_text,
           EXCLUDED.encoding_concept_id, EXCLUDED.language_concept_id, EXCLUDED.provider_id,
           EXCLUDED.visit_occurrence_id, EXCLUDED.visit_detail_id, EXCLUDED.note_source_value)
    RETURNING note_id
)
SELECT note_id FROM changed;

-- -------------------------------------------------------------------
-- NOTE source identifiers and metadata
-- -------------------------------------------------------------------

INSERT INTO :"etl_schema".note_id_map
SELECT
    src.source_table                            AS source_table,
    src.source_note_id                          AS source_note_id,
    src.note_id                                 AS note_id
FROM
    lk_note_mapped src
ON CONFLICT (source_table, source_note_id) DO NOTHING
;

DELETE FROM :"etl_schema".note_metadata WHERE source_table = :'source_table';
INSERT INTO :"etl_schema".note_metadata
SELECT
    src.source_table                            AS source_table,
    src.source_note_id                          AS source_note_id,
    src.note_id                                 AS note_id,
    :'source_version'                           AS source_version,
    NULLIF(:'source_sha256', '')                AS source_sha256,
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    src.note_type                               AS note_type,
    src.note_seq                                AS note_seq,
    src.charttime                               AS charttime,
    src.storetime                               AS storetime,
    src.person_id                               AS person_id,
    src.visit_occurrence_id                     AS visit_occurrence_id,
    src.text_sha256                             AS text_sha256,
    src.text_bytes                              AS text_bytes,
    src.load_status                             AS load_status,
    src.visit_status                            AS visit_status,
    CURRENT_TIMESTAMP                          AS updated_at
FROM
    lk_note_mapped src
;

-- -------------------------------------------------------------------
-- NOTE detail
-- Keep field ordinal and duplicate multiplicity
-- -------------------------------------------------------------------

DELETE FROM :"etl_schema".note_detail WHERE source_table = :'source_table';

INSERT INTO :"etl_schema".note_detail
SELECT
    :'source_table'::text                        AS source_table,
    det.note_id                                 AS source_note_id,
    src.note_id                                 AS note_id,
    det.subject_id                              AS subject_id,
    det.field_name                              AS field_name,
    det.field_value                             AS field_value,
    det.field_ordinal                           AS field_ordinal
FROM
    :"source_schema".:"detail_table" det
LEFT JOIN
    lk_note_mapped src
        ON src.source_note_id = det.note_id
;

-- -------------------------------------------------------------------
-- Validate every mapped NOTE field and all detail rows
-- -------------------------------------------------------------------

CREATE TEMP TABLE note_differences ON COMMIT DROP AS
SELECT w.source_note_id
FROM lk_note_mapped w LEFT JOIN :"cdm_schema".note n ON n.note_id = w.note_id
WHERE w.person_id IS NOT NULL AND (
    n.note_id IS NULL
    OR (n.person_id, n.note_date, n.note_datetime, n.note_type_concept_id,
        n.note_class_concept_id, n.note_title, n.encoding_concept_id,
        n.language_concept_id, n.provider_id, n.visit_occurrence_id,
        n.visit_detail_id, n.note_source_value,
        encode(sha256(convert_to(n.note_text, 'UTF8')), 'hex'),
        octet_length(convert_to(n.note_text, 'UTF8'))::bigint)
    IS DISTINCT FROM
       (w.person_id, w.charttime::date, w.charttime, 32817,
        w.note_class_concept_id, w.note_title, 32678, 4180186, NULL::integer,
        w.visit_occurrence_id, NULL::bigint, w.note_type, w.text_sha256, w.text_bytes)
);
INSERT INTO note_checks VALUES
    ('every_loaded_field_matches', NOT EXISTS (SELECT 1 FROM note_differences)),
    ('source_accounting', (SELECT count(*) FROM src_note) = (
        SELECT count(*) FROM lk_note_mapped WHERE load_status IN ('LOADED', 'MISSING_PERSON')
    )),
    ('owned_target_row_count', (
        SELECT count(*) FROM :"cdm_schema".note n
        JOIN :"etl_schema".note_id_map m USING (note_id) WHERE m.source_table = :'source_table'
    ) = (SELECT count(*) FROM lk_note_mapped WHERE person_id IS NOT NULL)),
    ('detail_note_links_match', NOT EXISTS (
        SELECT 1 FROM :"etl_schema".note_detail d
        LEFT JOIN lk_note_mapped s ON s.source_note_id = d.source_note_id
        WHERE d.source_table = :'source_table'
          AND d.note_id IS DISTINCT FROM s.note_id
    )),
    ('all_detail_rows_preserved', NOT EXISTS (
        (SELECT note_id, subject_id, field_name, field_value, field_ordinal
         FROM :"source_schema".:"detail_table"
         EXCEPT ALL
         SELECT source_note_id, subject_id, field_name, field_value, field_ordinal
         FROM :"etl_schema".note_detail WHERE source_table = :'source_table')
        UNION ALL
        (SELECT source_note_id, subject_id, field_name, field_value, field_ordinal
         FROM :"etl_schema".note_detail WHERE source_table = :'source_table'
         EXCEPT ALL
         SELECT note_id, subject_id, field_name, field_value, field_ordinal
         FROM :"source_schema".:"detail_table")
    ));

-- -------------------------------------------------------------------
-- Record a successful run and commit
-- -------------------------------------------------------------------

INSERT INTO :"etl_schema".note_etl_run (
    finished_at, source_table, source_version, source_sha256, source_rows, loaded_rows,
    rejected_person_rows, missing_visit_rows, no_hadm_rows, detail_rows, changed_note_rows,
    text_mismatches, passed_checks, unmatched_detail_rows
)
SELECT clock_timestamp(), :'source_table', :'source_version', NULLIF(:'source_sha256', ''),
    count(*), count(*) FILTER (WHERE load_status = 'LOADED'),
    count(*) FILTER (WHERE load_status = 'MISSING_PERSON'),
    count(*) FILTER (WHERE visit_status = 'MISSING_VISIT'),
    count(*) FILTER (WHERE visit_status = 'NO_HADM_ID'),
    (SELECT count(*) FROM :"source_schema".:"detail_table"),
    (SELECT count(*) FROM note_changed), (SELECT count(*) FROM note_differences),
    (SELECT count(*) FROM note_checks),
    (SELECT count(*) FROM :"etl_schema".note_detail
     WHERE source_table = :'source_table' AND note_id IS NULL)
FROM lk_note_mapped
RETURNING run_id \gset
ANALYZE :"cdm_schema".note;
COMMIT;

SELECT 'NOTE_ETL_REPORT ' || row_to_json(r)::text
FROM :"etl_schema".note_etl_run r WHERE run_id = :run_id;
