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
CREATE FUNCTION pg_temp.note_mask(value text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT CASE WHEN value ~ '[[:alnum:]]_{3,}|_{3,}[[:alnum:]]' THEN NULL
        ELSE pg_temp.note_normalize(regexp_replace(value, '_{3,}', '', 'g')) END
$$;
CREATE FUNCTION pg_temp.note_addendum(value text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT pg_temp.note_normalize(regexp_replace(regexp_replace(regexp_replace(
        value, '(?n)^[ \t]*_{10,}[ \t]*\r?$', '', 'g'),
        '(?is)^[[:space:]]*FINAL ADDENDUM[[:space:]]*', ''),
        '(?is)^[[:space:]]*ADDENDUM[[:space:]]*:?[[:space:]]*', ''))
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
CREATE INDEX ON src_cxr_time(study_id);
CREATE TEMP TABLE src_cxr_text ON COMMIT DROP AS
SELECT
    raw.*,
    coalesce(substring(raw.text FROM '(?s)^(.*\n)[ \t]*FINAL REPORT[ \t]*\r?\n'), '') AS prefix_text
FROM :"source_schema".cxr_reports raw;
CREATE TEMP TABLE src_cxr_study ON COMMIT DROP AS
WITH normalized AS MATERIALIZED (
    SELECT
        CAST(raw.subject_id AS integer) AS subject_id,
        CAST(raw.study_id AS integer) AS study_id,
        raw.subject_id || '-CXR-' || raw.study_id AS source_note_id,
        length(raw.text) AS text_length,
        CASE WHEN pg_temp.note_normalize(raw.prefix_text) <> '' THEN length(raw.prefix_text) ELSE 0 END AS prefix_length,
        pg_temp.note_normalize(raw.text) AS full_text,
        pg_temp.note_normalize(substr(raw.text, length(raw.prefix_text) + 1)) AS final_text,
        pg_temp.note_body(raw.text) AS original_body,
        pg_temp.note_body(substr(raw.text, length(raw.prefix_text) + 1)) AS final_body,
        pg_temp.note_addendum(raw.prefix_text) AS prefix_addendum,
        raw.text ~* 'addendum|amended report|_{10}' AS has_addendum
    FROM src_cxr_text raw
)
SELECT
    raw.subject_id                              AS subject_id,
    raw.study_id                                AS study_id,
    raw.source_note_id                          AS source_note_id,
    tm.study_datetime                           AS study_datetime,
    tm.study_dates                              AS study_dates,
    tm.study_end                                AS study_end,
    tm.missing_times                            AS missing_times,
    raw.text_length                             AS text_length,
    raw.prefix_length                           AS prefix_length,
    encode(sha256(convert_to(raw.final_text, 'UTF8')), 'hex') AS final_hash,
    CASE WHEN length(raw.final_body) >= 40 THEN
        encode(sha256(convert_to(raw.final_body, 'UTF8')), 'hex')
    END                                         AS final_body_hash,
    CASE WHEN length(pg_temp.note_mask(raw.final_body)) >= 80 THEN
        encode(sha256(convert_to(pg_temp.note_mask(raw.final_body), 'UTF8')), 'hex')
    END                                         AS mask_body_hash,
    encode(sha256(convert_to(raw.prefix_addendum, 'UTF8')), 'hex') AS prefix_addendum_hash,
    encode(sha256(convert_to(raw.full_text, 'UTF8')), 'hex') AS full_hash,
    CASE WHEN length(raw.original_body) >= 40 THEN
        encode(sha256(convert_to(raw.original_body, 'UTF8')), 'hex')
    END                                         AS body_hash,
    raw.has_addendum                            AS has_addendum
FROM normalized raw
LEFT JOIN (
    SELECT subject_id, study_id, min(study_datetime) AS study_datetime,
        count(DISTINCT study_datetime::date) AS study_dates,
        max(study_datetime) AS study_end,
        count(*) - count(study_datetime) AS missing_times
    FROM src_cxr_time GROUP BY subject_id, study_id
) tm ON tm.subject_id = raw.subject_id AND tm.study_id = raw.study_id
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
WITH normalized AS MATERIALIZED (
    SELECT
        raw.note_id AS source_note_id,
        raw.subject_id,
        raw.charttime,
        raw.note_type,
        pg_temp.note_normalize(raw.text) AS full_text,
        pg_temp.note_body(raw.text) AS clinical_body,
        CASE WHEN raw.note_type = 'AR' THEN pg_temp.note_addendum(raw.text) END AS addendum_text
    FROM :"radiology_schema".radiology raw
    WHERE EXISTS (SELECT 1 FROM src_cxr_study cxr WHERE cxr.subject_id = raw.subject_id)
)
SELECT
    raw.source_note_id                          AS source_note_id,
    raw.subject_id                              AS subject_id,
    raw.charttime                               AS charttime,
    raw.note_type                               AS note_type,
    encode(sha256(convert_to(raw.full_text, 'UTF8')), 'hex') AS full_hash,
    CASE WHEN length(raw.clinical_body) >= 40 THEN
        encode(sha256(convert_to(raw.clinical_body, 'UTF8')), 'hex')
    END                                         AS body_hash,
    CASE WHEN length(pg_temp.note_mask(raw.clinical_body)) >= 80 THEN
        encode(sha256(convert_to(pg_temp.note_mask(raw.clinical_body), 'UTF8')), 'hex')
    END                                         AS mask_body_hash,
    encode(sha256(convert_to(raw.addendum_text, 'UTF8')), 'hex') AS addendum_hash,
    exam.note_id IS NOT NULL                    AS chest_exam
FROM normalized raw
LEFT JOIN src_cxr_exam exam ON exam.note_id = raw.source_note_id
;
CREATE INDEX ON src_cxr_radiology(subject_id, (charttime::date));
CREATE INDEX ON src_cxr_radiology(subject_id, full_hash);
CREATE INDEX ON src_cxr_radiology(subject_id, charttime);
CREATE INDEX ON src_cxr_radiology(subject_id, body_hash);
CREATE INDEX ON src_cxr_radiology(subject_id, mask_body_hash);
CREATE UNIQUE INDEX ON src_cxr_radiology(source_note_id);
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

-- -------------------------------------------------------------------
-- lk_cxr_extended_candidate
-- Keep legacy matches first. New matches cross midnight and use every
-- image time. Mask comparison removes underscores only, never words,
-- numbers or punctuation. Fuzzy similarity is not an identity rule.
-- -------------------------------------------------------------------

CREATE TEMP TABLE lk_cxr_legacy_match ON COMMIT DROP AS
SELECT * FROM lk_cxr_candidate WHERE study_candidates = 1 AND report_candidates = 1;
CREATE UNIQUE INDEX ON lk_cxr_legacy_match(study_id);
CREATE UNIQUE INDEX ON lk_cxr_legacy_match(radiology_note_id);
CREATE TEMP TABLE lk_cxr_extended_pair ON COMMIT DROP AS
SELECT
    cxr.study_id                                AS study_id,
    rad.source_note_id                          AS radiology_note_id,
    CASE
        WHEN cxr.full_hash = rad.full_hash THEN 1
        WHEN cxr.final_hash = rad.full_hash THEN 2
        WHEN cxr.final_body_hash = rad.body_hash THEN 3
        ELSE 4
    END                                         AS match_priority,
    CASE
        WHEN cxr.full_hash = rad.full_hash THEN 'FULL_TEXT_WITHIN_6H'
        WHEN cxr.final_hash = rad.full_hash THEN 'FINAL_TEXT_WITHIN_6H'
        WHEN cxr.final_body_hash = rad.body_hash THEN 'FINAL_BODY_CHEST_WITHIN_6H'
        ELSE 'MASK_ONLY_BODY_CHEST_WITHIN_1H'
    END                                         AS match_method
FROM src_cxr_study cxr
INNER JOIN src_cxr_radiology rad
    ON rad.subject_id = cxr.subject_id AND rad.note_type = 'RR'
    AND rad.charttime BETWEEN cxr.study_datetime - interval '6 hours' AND cxr.study_end + interval '6 hours'
    AND (cxr.full_hash = rad.full_hash OR cxr.final_hash = rad.full_hash
         OR (rad.chest_exam AND cxr.final_body_hash = rad.body_hash)
         OR (rad.chest_exam AND cxr.mask_body_hash = rad.mask_body_hash))
WHERE cxr.missing_times = 0 AND cxr.study_end - cxr.study_datetime <= interval '6 hours'
    AND NOT EXISTS (SELECT 1 FROM lk_cxr_legacy_match old WHERE old.study_id = cxr.study_id)
    AND NOT EXISTS (SELECT 1 FROM lk_cxr_legacy_match old WHERE old.radiology_note_id = rad.source_note_id)
    AND EXISTS (
        SELECT 1 FROM src_cxr_time tm WHERE tm.study_id = cxr.study_id
          AND abs(extract(epoch FROM rad.charttime - tm.study_datetime)) <=
              CASE WHEN cxr.full_hash = rad.full_hash OR cxr.final_hash = rad.full_hash
                        OR (rad.chest_exam AND cxr.final_body_hash = rad.body_hash) THEN 21600 ELSE 3600 END
    )
;
CREATE TEMP TABLE lk_cxr_extended_candidate ON COMMIT DROP AS
SELECT
    pair.*,
    count(*) OVER (PARTITION BY pair.study_id) AS study_candidates,
    count(*) OVER (PARTITION BY pair.radiology_note_id) AS report_candidates
FROM lk_cxr_extended_pair pair
INNER JOIN (
    SELECT study_id, min(match_priority) AS match_priority FROM lk_cxr_extended_pair GROUP BY study_id
) best USING (study_id, match_priority);
CREATE INDEX ON lk_cxr_extended_candidate(study_id);
CREATE TEMP TABLE lk_cxr_match ON COMMIT DROP AS
SELECT study_id, radiology_note_id, match_method FROM lk_cxr_legacy_match
UNION ALL
SELECT study_id, radiology_note_id, match_method FROM lk_cxr_extended_candidate
WHERE study_candidates = 1 AND report_candidates = 1;
CREATE UNIQUE INDEX ON lk_cxr_match(study_id);
CREATE UNIQUE INDEX ON lk_cxr_match(radiology_note_id);

-- Exact whole-document equality can bridge delayed report charting. A
-- wider window is never used for body-only or masked-text comparison.
-- Previously resolved documents keep ownership; both directions must
-- be unique among the remaining studies and reports.
CREATE TEMP TABLE lk_cxr_late_candidate ON COMMIT DROP AS
SELECT
    cxr.study_id                                AS study_id,
    rad.source_note_id                          AS radiology_note_id,
    count(*) OVER (PARTITION BY cxr.study_id)     AS study_candidates,
    count(*) OVER (PARTITION BY rad.source_note_id) AS report_candidates
FROM src_cxr_study cxr
INNER JOIN src_cxr_radiology rad
    ON rad.subject_id = cxr.subject_id AND rad.note_type = 'RR'
    AND rad.full_hash = cxr.full_hash
    AND rad.charttime BETWEEN cxr.study_datetime - interval '48 hours' AND cxr.study_end + interval '48 hours'
WHERE cxr.missing_times = 0 AND cxr.study_end - cxr.study_datetime <= interval '6 hours'
    AND NOT EXISTS (SELECT 1 FROM lk_cxr_match old WHERE old.study_id = cxr.study_id)
    AND NOT EXISTS (SELECT 1 FROM lk_cxr_match old WHERE old.radiology_note_id = rad.source_note_id)
    AND EXISTS (
        SELECT 1 FROM src_cxr_time tm WHERE tm.study_id = cxr.study_id
          AND abs(extract(epoch FROM rad.charttime - tm.study_datetime)) <= 172800
    )
;
CREATE INDEX ON lk_cxr_late_candidate(study_id);
INSERT INTO lk_cxr_match
SELECT study_id, radiology_note_id, 'FULL_TEXT_WITHIN_48H'
FROM lk_cxr_late_candidate WHERE study_candidates = 1 AND report_candidates = 1;
ANALYZE lk_cxr_match;

-- Absence of a nearby report alone is insufficient: a known exact full,
-- final, clinical-body or mask-only match anywhere in the patient blocks
-- insertion. Seven days is deliberately wider than the matching window.
CREATE TEMP TABLE lk_cxr_novel ON COMMIT DROP AS
SELECT cxr.study_id
FROM src_cxr_study cxr
WHERE NOT EXISTS (SELECT 1 FROM lk_cxr_match m WHERE m.study_id = cxr.study_id)
  AND NOT EXISTS (
      SELECT 1 FROM src_cxr_radiology r WHERE r.subject_id = cxr.subject_id
        AND r.charttime BETWEEN cxr.study_datetime - interval '7 days' AND cxr.study_end + interval '7 days'
  )
  AND NOT EXISTS (
      SELECT 1 FROM src_cxr_radiology r WHERE r.subject_id = cxr.subject_id
        AND (r.full_hash IN (cxr.full_hash, cxr.final_hash)
             OR r.body_hash = cxr.final_body_hash OR r.mask_body_hash = cxr.mask_body_hash)
  );
CREATE UNIQUE INDEX ON lk_cxr_novel(study_id);

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
        WHEN legacy.study_id IS NOT NULL THEN 'MATCHED_RADIOLOGY'
        WHEN cxr.study_datetime IS NULL OR cxr.missing_times <> 0
             OR cxr.study_end - cxr.study_datetime > interval '6 hours' THEN 'REVIEW_STUDY_TIME'
        WHEN match.radiology_note_id IS NOT NULL THEN 'MATCHED_RADIOLOGY'
        WHEN NOT EXISTS (SELECT 1 FROM src_cxr_radiology r WHERE r.subject_id = cxr.subject_id)
            THEN 'NEW_NO_PATIENT_RADIOLOGY'
        WHEN novel.study_id IS NOT NULL THEN 'NEW_NO_NEARBY_RADIOLOGY'
        ELSE 'NEW_SOURCE_VARIANT'
    END                                         AS decision,
    match.radiology_note_id                      AS radiology_note_id,
    match.match_method                          AS match_method,
    CASE WHEN legacy.study_id IS NOT NULL THEN 1 ELSE greatest(
        (SELECT count(*) FROM lk_cxr_extended_candidate c WHERE c.study_id = cxr.study_id),
        (SELECT count(*) FROM lk_cxr_late_candidate c WHERE c.study_id = cxr.study_id))
    END                                         AS candidate_count
FROM
    src_cxr_study cxr
LEFT JOIN
    lk_cxr_match match ON match.study_id = cxr.study_id
LEFT JOIN lk_cxr_legacy_match legacy ON legacy.study_id = cxr.study_id
LEFT JOIN lk_cxr_novel novel ON novel.study_id = cxr.study_id
;
CREATE UNIQUE INDEX ON lk_cxr_resolution(study_id);
ANALYZE lk_cxr_resolution;
INSERT INTO modality_checks VALUES
    ('reconciliation_no_fanout', (SELECT count(*) FROM lk_cxr_resolution) = (SELECT count(*) FROM src_cxr_study)),
    ('stable_existing_resolution', NOT EXISTS (
        SELECT 1 FROM :"etl_schema".cxr_reconciliation old
        LEFT JOIN lk_cxr_resolution new USING (study_id)
        WHERE old.decision IN ('MATCHED_RADIOLOGY', 'NEW_NO_PATIENT_RADIOLOGY', 'NEW_NO_NEARBY_RADIOLOGY', 'NEW_SOURCE_VARIANT')
          AND (new.study_id IS NULL OR (old.decision, old.radiology_note_id)
              IS DISTINCT FROM (new.decision, new.radiology_note_id))
    ));
DELETE FROM :"etl_schema".cxr_reconciliation;
INSERT INTO :"etl_schema".cxr_reconciliation SELECT * FROM lk_cxr_resolution;

-- -------------------------------------------------------------------
-- lk_cxr_overlap_candidate
-- Retain unresolved source versions as NOTE documents, without claiming
-- that a nearby/fuzzy candidate is the same document or a new event.
-- Candidate links below are evidence only; they never allocate an alias.
-- Whole-text candidates are retained even outside the identity window.
-- -------------------------------------------------------------------

CREATE TEMP TABLE lk_cxr_overlap_candidate ON COMMIT DROP AS
SELECT
    cxr.subject_id                              AS subject_id,
    cxr.study_id                                AS study_id,
    rad.source_note_id                          AS radiology_note_id,
    rad.charttime                               AS radiology_datetime,
    CASE
        WHEN cxr.full_hash = rad.full_hash THEN 'FULL_TEXT'
        WHEN cxr.final_hash = rad.full_hash THEN 'FINAL_TEXT'
        WHEN cxr.final_body_hash = rad.body_hash THEN 'CLINICAL_BODY'
        WHEN cxr.mask_body_hash = rad.mask_body_hash THEN 'MASK_ONLY_BODY'
        ELSE 'NEARBY_CHEST_REPORT'
    END                                         AS comparison_basis,
    CAST((SELECT min(abs(extract(epoch FROM rad.charttime - tm.study_datetime)))
        FROM src_cxr_time tm WHERE tm.study_id = cxr.study_id) AS double precision) AS time_gap_seconds
FROM src_cxr_study cxr
INNER JOIN lk_cxr_resolution res USING (study_id)
INNER JOIN src_cxr_radiology rad
    ON rad.subject_id = cxr.subject_id AND rad.note_type = 'RR'
    AND (rad.full_hash IN (cxr.full_hash, cxr.final_hash)
         OR rad.body_hash = cxr.final_body_hash OR rad.mask_body_hash = cxr.mask_body_hash
         OR (rad.chest_exam AND rad.charttime BETWEEN
             cxr.study_datetime - interval '48 hours' AND cxr.study_end + interval '48 hours'))
WHERE res.decision = 'NEW_SOURCE_VARIANT'
;
CREATE UNIQUE INDEX ON lk_cxr_overlap_candidate(study_id, radiology_note_id);
ANALYZE lk_cxr_overlap_candidate;
CREATE TABLE IF NOT EXISTS :"etl_schema".cxr_overlap_candidate (
    subject_id integer NOT NULL, study_id integer NOT NULL, radiology_note_id text NOT NULL,
    radiology_datetime timestamp, comparison_basis text NOT NULL, time_gap_seconds double precision,
    PRIMARY KEY (study_id, radiology_note_id)
);
DELETE FROM :"etl_schema".cxr_overlap_candidate;
INSERT INTO :"etl_schema".cxr_overlap_candidate SELECT * FROM lk_cxr_overlap_candidate;

-- -------------------------------------------------------------------
-- lk_cxr_component
-- A matched final report can have an additional source prefix containing
-- wet reads, addenda and versions. Keep the prefix byte-for-byte, or reuse
-- an exact existing AR explicitly linked to the same parent RR.
-- -------------------------------------------------------------------

CREATE TEMP TABLE src_cxr_parent ON COMMIT DROP AS
SELECT DISTINCT note_id AS parent_note_id, field_value AS addendum_note_id
FROM :"radiology_schema".radiology_detail WHERE field_name = 'addendum_note_id'
UNION
SELECT DISTINCT field_value AS parent_note_id, note_id AS addendum_note_id
FROM :"radiology_schema".radiology_detail WHERE field_name = 'parent_note_id';
CREATE INDEX ON src_cxr_parent(parent_note_id);
CREATE TEMP TABLE lk_cxr_split ON COMMIT DROP AS
SELECT cxr.study_id, cxr.prefix_length
FROM src_cxr_study cxr
INNER JOIN lk_cxr_resolution res USING (study_id)
INNER JOIN src_cxr_radiology rad ON rad.source_note_id = res.radiology_note_id
WHERE res.decision = 'MATCHED_RADIOLOGY' AND cxr.prefix_length > 0 AND cxr.full_hash <> rad.full_hash;
CREATE UNIQUE INDEX ON lk_cxr_split(study_id);
CREATE TEMP TABLE lk_cxr_addendum_candidate ON COMMIT DROP AS
SELECT
    cxr.study_id, ar.source_note_id AS radiology_note_id,
    count(*) OVER (PARTITION BY cxr.study_id) AS study_candidates,
    count(*) OVER (PARTITION BY ar.source_note_id) AS report_candidates
FROM src_cxr_study cxr
INNER JOIN lk_cxr_split split USING (study_id)
INNER JOIN lk_cxr_resolution res USING (study_id)
INNER JOIN src_cxr_parent parent ON parent.parent_note_id = res.radiology_note_id
INNER JOIN src_cxr_radiology ar ON ar.source_note_id = parent.addendum_note_id
    AND ar.subject_id = cxr.subject_id AND ar.note_type = 'AR'
    AND ar.addendum_hash = cxr.prefix_addendum_hash;
CREATE TEMP TABLE lk_cxr_component ON COMMIT DROP AS
SELECT
    cxr.subject_id, cxr.study_id,
    CASE WHEN split.study_id IS NULL THEN 'REPORT' ELSE 'FINAL' END AS component_kind,
    cxr.source_note_id,
    coalesce(split.prefix_length, 0) + 1 AS source_start,
    cxr.text_length - coalesce(split.prefix_length, 0) AS source_length,
    res.decision, res.radiology_note_id, res.match_method, res.candidate_count
FROM src_cxr_study cxr
INNER JOIN lk_cxr_resolution res USING (study_id)
LEFT JOIN lk_cxr_split split USING (study_id)
UNION ALL
SELECT
    cxr.subject_id, cxr.study_id, 'PREFIX', cxr.source_note_id || '-PREFIX',
    1, split.prefix_length,
    CASE WHEN ar.radiology_note_id IS NULL THEN 'NEW_SUPPLEMENT' ELSE 'MATCHED_RADIOLOGY' END,
    ar.radiology_note_id,
    CASE WHEN ar.radiology_note_id IS NULL THEN 'PRESERVED_SOURCE_PREFIX' ELSE 'EXACT_PARENT_LINKED_ADDENDUM' END,
    (SELECT count(*) FROM lk_cxr_addendum_candidate c WHERE c.study_id = cxr.study_id)
FROM src_cxr_study cxr
INNER JOIN lk_cxr_split split USING (study_id)
LEFT JOIN lk_cxr_addendum_candidate ar ON ar.study_id = cxr.study_id
    AND ar.study_candidates = 1 AND ar.report_candidates = 1;
CREATE UNIQUE INDEX ON lk_cxr_component(study_id, component_kind);
CREATE UNIQUE INDEX ON lk_cxr_component(source_note_id);
CREATE TABLE IF NOT EXISTS :"etl_schema".cxr_component (
    subject_id integer NOT NULL, study_id integer NOT NULL, component_kind text NOT NULL,
    source_note_id text NOT NULL UNIQUE, source_start integer NOT NULL, source_length integer NOT NULL,
    decision text NOT NULL, radiology_note_id text, match_method text, candidate_count bigint NOT NULL,
    PRIMARY KEY (study_id, component_kind)
);
-- Check the whole corpus. COUNT avoids an EXISTS plan optimized to find
-- the first mismatch, which does not exist when the reconstruction passes.
INSERT INTO modality_checks VALUES
    ('component_source_reconstruction', (
        SELECT count(*) = 0 FROM :"source_schema".cxr_reports raw
        LEFT JOIN (
            SELECT comp.study_id,
                string_agg(substr(src.text, comp.source_start, comp.source_length), '' ORDER BY comp.source_start) AS text
            FROM lk_cxr_component comp
            INNER JOIN :"source_schema".cxr_reports src ON CAST(src.study_id AS integer) = comp.study_id
            GROUP BY comp.study_id
        ) parts ON parts.study_id = CAST(raw.study_id AS integer)
        WHERE raw.text IS DISTINCT FROM parts.text
    )),
    ('stable_existing_components', NOT EXISTS (
        SELECT 1 FROM :"etl_schema".cxr_component old LEFT JOIN lk_cxr_component new USING (study_id, component_kind)
        WHERE old.decision IN ('MATCHED_RADIOLOGY','NEW_NO_PATIENT_RADIOLOGY','NEW_NO_NEARBY_RADIOLOGY','NEW_SUPPLEMENT','NEW_SOURCE_VARIANT')
          AND (new.study_id IS NULL OR (old.source_note_id,old.source_start,old.source_length,old.decision,old.radiology_note_id)
              IS DISTINCT FROM (new.source_note_id,new.source_start,new.source_length,new.decision,new.radiology_note_id))
    ));
DELETE FROM :"etl_schema".cxr_component;
INSERT INTO :"etl_schema".cxr_component SELECT * FROM lk_cxr_component;

-- -------------------------------------------------------------------
-- Canonical source views for the existing NOTE mapping
-- note_seq = 0 means unavailable, not a chronological study sequence.
-- -------------------------------------------------------------------

CREATE OR REPLACE VIEW :"source_schema".cxr AS
SELECT
    comp.source_note_id                         AS note_id,
    link.subject_id                             AS subject_id,
    CAST(NULL AS integer)                       AS hadm_id,
    CASE WHEN comp.component_kind = 'PREFIX' THEN 'CXR_SUPPLEMENT'
         WHEN comp.decision = 'NEW_SOURCE_VARIANT' THEN 'CXR_VARIANT'
         ELSE 'CXR' END                          AS note_type,
    0                                           AS note_seq,
    link.study_datetime                         AS charttime,
    CAST(NULL AS timestamp)                     AS storetime,
    substr(raw.text, comp.source_start, comp.source_length) AS text
FROM
    :"source_schema".cxr_reports raw
INNER JOIN
    :"etl_schema".cxr_reconciliation link ON link.study_id = CAST(raw.study_id AS integer)
INNER JOIN
    :"etl_schema".cxr_component comp ON comp.study_id = link.study_id
WHERE
    comp.decision IN ('NEW_NO_PATIENT_RADIOLOGY','NEW_NO_NEARBY_RADIOLOGY','NEW_SUPPLEMENT','NEW_SOURCE_VARIANT')
;
CREATE OR REPLACE VIEW :"source_schema".cxr_detail AS
SELECT
    src.note_id                                 AS note_id,
    src.subject_id                              AS subject_id,
    'study_id'::text                            AS field_name,
    raw.study_id                                AS field_value,
    1                                           AS field_ordinal
FROM :"source_schema".cxr src
INNER JOIN :"etl_schema".cxr_component comp ON comp.source_note_id = src.note_id
INNER JOIN :"source_schema".cxr_reports raw ON CAST(raw.study_id AS integer) = comp.study_id
UNION ALL
SELECT src.note_id, src.subject_id, 'timestamp_basis', 'STUDY_ACQUISITION_TIME_NOT_DOCUMENT_AVAILABILITY', 1
FROM :"source_schema".cxr src
UNION ALL
SELECT src.note_id, src.subject_id, 'duplicate_status', 'UNRESOLVED_POSSIBLE_DUPLICATE', 1
FROM :"source_schema".cxr src WHERE src.note_type = 'CXR_VARIANT'
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
