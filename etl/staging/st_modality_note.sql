-- MIMIC IV CDM Conversion
-- Same CXR component and matching rules as the PostgreSQL query.

CREATE TABLE IF NOT EXISTS @etl_project.@etl_dataset.ext_cxr_reconciliation (
    subject_id INT64 NOT NULL, study_id INT64,
    source_note_id STRING NOT NULL, study_datetime DATETIME,
    decision STRING NOT NULL, radiology_note_id STRING, match_method STRING,
    candidate_count INT64 NOT NULL
);
CREATE TABLE IF NOT EXISTS @etl_project.@etl_dataset.ext_cxr_overlap_candidate (
    subject_id INT64 NOT NULL, study_id INT64 NOT NULL, radiology_note_id STRING NOT NULL,
    radiology_datetime DATETIME, comparison_basis STRING NOT NULL, time_gap_seconds FLOAT64
);
CREATE TABLE IF NOT EXISTS @etl_project.@etl_dataset.ext_cxr_component (
    subject_id INT64 NOT NULL, study_id INT64 NOT NULL, component_kind STRING NOT NULL,
    source_note_id STRING NOT NULL, source_start INT64 NOT NULL, source_length INT64 NOT NULL,
    decision STRING NOT NULL, radiology_note_id STRING, match_method STRING, candidate_count INT64 NOT NULL
);

ASSERT ((
        SELECT count(*) = 8 AND LOGICAL_AND(checksum_status = 'VERIFIED')
        FROM @modality_project.@modality_dataset.source_file WHERE table_name <> 'echo_study_list'
    )) AS 'verified_non_echo_sources';
ASSERT ((SELECT count(*) > 0 FROM @note_project.@note_dataset.radiology)) AS 'radiology_available';
ASSERT (NOT EXISTS (
        (SELECT subject_id, study_id FROM @modality_project.@modality_dataset.cxr_reports
         EXCEPT DISTINCT SELECT subject_id, study_id FROM @modality_project.@modality_dataset.cxr_study_list)
        UNION ALL
        (SELECT subject_id, study_id FROM @modality_project.@modality_dataset.cxr_study_list
         EXCEPT DISTINCT SELECT subject_id, study_id FROM @modality_project.@modality_dataset.cxr_reports)
    )) AS 'cxr_study_inventory';
-- Comparison only: raw text and the NOTE text are never normalized.
CREATE TEMP FUNCTION note_normalize(value STRING) AS (
    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(value,
        r'^[[:space:]]*FINAL REPORT[[:space:]]*', ''), r'[[:space:]]+', ' '))
);
CREATE TEMP FUNCTION note_body(value STRING) AS (
    note_normalize(REGEXP_EXTRACT(value, r'(?is)((?:FINDINGS(?: AND IMPRESSION)?|IMPRESSION):.*)'))
);
CREATE TEMP FUNCTION note_mask(value STRING) AS (
    CASE WHEN REGEXP_CONTAINS(value, r'[[:alnum:]]_{3,}|_{3,}[[:alnum:]]') THEN NULL
        ELSE note_normalize(REGEXP_REPLACE(value, r'_{3,}', '')) END
);
CREATE TEMP FUNCTION note_addendum(value STRING) AS (
    note_normalize(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(value,
        r'(?m)^[ \t]*_{10,}[ \t]*\r?$', ''),
        r'(?is)^[[:space:]]*FINAL ADDENDUM[[:space:]]*', ''),
        r'(?is)^[[:space:]]*ADDENDUM[[:space:]]*:?[[:space:]]*', ''))
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
        DATETIME_ADD(PARSE_DATETIME('%Y%m%d%H%M%S', CONCAT(studydate,
            LPAD(SPLIT(studytime, '.')[OFFSET(0)], 6, '0'))),
            INTERVAL CAST(ROUND(CAST(CONCAT('0.', COALESCE(SPLIT(studytime, '.')[SAFE_OFFSET(1)], '0'))
                AS BIGNUMERIC) * 1000000, 0, 'ROUND_HALF_EVEN') AS INT64) MICROSECOND)
    END                                         AS study_datetime
FROM
    @modality_project.@modality_dataset.cxr_metadata
;
CREATE TEMP TABLE src_cxr_text AS
SELECT
    raw.*,
    coalesce(REGEXP_EXTRACT(raw.text, r'(?s)^(.*\n)[ \t]*FINAL REPORT[ \t]*\r?\n'), '') AS prefix_text
FROM @modality_project.@modality_dataset.cxr_reports raw;
CREATE TEMP TABLE src_cxr_study AS
WITH normalized AS (
    SELECT
        CAST(raw.subject_id AS INT64) AS subject_id,
        CAST(raw.study_id AS INT64) AS study_id,
        raw.subject_id || '-CXR-' || raw.study_id AS source_note_id,
        length(raw.text) AS text_length,
        CASE WHEN note_normalize(raw.prefix_text) <> '' THEN length(raw.prefix_text) ELSE 0 END AS prefix_length,
        note_normalize(raw.text) AS full_text,
        note_normalize(substr(raw.text, length(raw.prefix_text) + 1)) AS final_text,
        note_body(raw.text) AS original_body,
        note_body(substr(raw.text, length(raw.prefix_text) + 1)) AS final_body,
        note_addendum(raw.prefix_text) AS prefix_addendum,
        REGEXP_CONTAINS(raw.text, r'(?i)addendum|amended report|_{10}') AS has_addendum
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
    LOWER(TO_HEX(SHA256(raw.final_text))) AS final_hash,
    CASE WHEN length(raw.final_body) >= 40 THEN
        LOWER(TO_HEX(SHA256(raw.final_body)))
    END                                         AS final_body_hash,
    CASE WHEN length(note_mask(raw.final_body)) >= 80 THEN
        LOWER(TO_HEX(SHA256(note_mask(raw.final_body))))
    END                                         AS mask_body_hash,
    LOWER(TO_HEX(SHA256(raw.prefix_addendum))) AS prefix_addendum_hash,
    LOWER(TO_HEX(SHA256(raw.full_text))) AS full_hash,
    CASE WHEN length(raw.original_body) >= 40 THEN
        LOWER(TO_HEX(SHA256(raw.original_body)))
    END                                         AS body_hash,
    raw.has_addendum                            AS has_addendum
FROM normalized raw
LEFT JOIN (
    SELECT subject_id, study_id, min(study_datetime) AS study_datetime,
        count(DISTINCT DATE(study_datetime)) AS study_dates,
        max(study_datetime) AS study_end,
        count(*) - count(study_datetime) AS missing_times
    FROM src_cxr_time GROUP BY subject_id, study_id
) tm ON tm.subject_id = raw.subject_id AND tm.study_id = raw.study_id
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
WITH normalized AS (
    SELECT
        raw.note_id AS source_note_id,
        raw.subject_id,
        raw.charttime,
        raw.note_type,
        note_normalize(raw.text) AS full_text,
        note_body(raw.text) AS clinical_body,
        CASE WHEN raw.note_type = 'AR' THEN note_addendum(raw.text) END AS addendum_text
    FROM @note_project.@note_dataset.radiology raw
    WHERE EXISTS (SELECT 1 FROM src_cxr_study cxr WHERE cxr.subject_id = raw.subject_id)
)
SELECT
    raw.source_note_id                          AS source_note_id,
    raw.subject_id                              AS subject_id,
    CAST(raw.charttime AS DATETIME)             AS charttime,
    raw.note_type                               AS note_type,
    LOWER(TO_HEX(SHA256(raw.full_text))) AS full_hash,
    CASE WHEN length(raw.clinical_body) >= 40 THEN
        LOWER(TO_HEX(SHA256(raw.clinical_body)))
    END                                         AS body_hash,
    CASE WHEN length(note_mask(raw.clinical_body)) >= 80 THEN
        LOWER(TO_HEX(SHA256(note_mask(raw.clinical_body))))
    END                                         AS mask_body_hash,
    LOWER(TO_HEX(SHA256(raw.addendum_text))) AS addendum_hash,
    exam.note_id IS NOT NULL                    AS chest_exam
FROM normalized raw
LEFT JOIN src_cxr_exam exam ON exam.note_id = raw.source_note_id
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
                AND (ABS(DATETIME_DIFF(rad.charttime, cxr.study_datetime, MICROSECOND)) / 1000000.0) <= 3600)
        )
;

-- -------------------------------------------------------------------
-- lk_cxr_extended_candidate
-- Keep legacy matches first. New matches cross midnight and use every
-- image time. Mask comparison removes underscores only, never words,
-- numbers or punctuation. Fuzzy similarity is not an identity rule.
-- -------------------------------------------------------------------

CREATE TEMP TABLE lk_cxr_legacy_match AS
SELECT * FROM lk_cxr_candidate WHERE study_candidates = 1 AND report_candidates = 1;
CREATE TEMP TABLE lk_cxr_extended_pair AS
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
    AND rad.charttime BETWEEN DATETIME_SUB(cxr.study_datetime, INTERVAL 6 HOUR) AND DATETIME_ADD(cxr.study_end, INTERVAL 6 HOUR)
    AND (cxr.full_hash = rad.full_hash OR cxr.final_hash = rad.full_hash
         OR (rad.chest_exam AND cxr.final_body_hash = rad.body_hash)
         OR (rad.chest_exam AND cxr.mask_body_hash = rad.mask_body_hash))
WHERE cxr.missing_times = 0 AND DATETIME_DIFF(cxr.study_end, cxr.study_datetime, MICROSECOND) <= 21600000000
    AND NOT EXISTS (SELECT 1 FROM lk_cxr_legacy_match old WHERE old.study_id = cxr.study_id)
    AND NOT EXISTS (SELECT 1 FROM lk_cxr_legacy_match old WHERE old.radiology_note_id = rad.source_note_id)
    AND EXISTS (
        SELECT 1 FROM src_cxr_time tm WHERE tm.study_id = cxr.study_id
          AND (ABS(DATETIME_DIFF(rad.charttime, tm.study_datetime, MICROSECOND)) / 1000000.0) <=
              CASE WHEN cxr.full_hash = rad.full_hash OR cxr.final_hash = rad.full_hash
                        OR (rad.chest_exam AND cxr.final_body_hash = rad.body_hash) THEN 21600 ELSE 3600 END
    )
;
CREATE TEMP TABLE lk_cxr_extended_candidate AS
SELECT
    pair.*,
    count(*) OVER (PARTITION BY pair.study_id) AS study_candidates,
    count(*) OVER (PARTITION BY pair.radiology_note_id) AS report_candidates
FROM lk_cxr_extended_pair pair
INNER JOIN (
    SELECT study_id, min(match_priority) AS match_priority FROM lk_cxr_extended_pair GROUP BY study_id
) best USING (study_id, match_priority);
CREATE TEMP TABLE lk_cxr_match AS
SELECT study_id, radiology_note_id, match_method FROM lk_cxr_legacy_match
UNION ALL
SELECT study_id, radiology_note_id, match_method FROM lk_cxr_extended_candidate
WHERE study_candidates = 1 AND report_candidates = 1;

-- Exact whole-document equality can bridge delayed report charting. A
-- wider window is never used for body-only or masked-text comparison.
-- Previously resolved documents keep ownership; both directions must
-- be unique among the remaining studies and reports.
CREATE TEMP TABLE lk_cxr_late_candidate AS
SELECT
    cxr.study_id                                AS study_id,
    rad.source_note_id                          AS radiology_note_id,
    count(*) OVER (PARTITION BY cxr.study_id)     AS study_candidates,
    count(*) OVER (PARTITION BY rad.source_note_id) AS report_candidates
FROM src_cxr_study cxr
INNER JOIN src_cxr_radiology rad
    ON rad.subject_id = cxr.subject_id AND rad.note_type = 'RR'
    AND rad.full_hash = cxr.full_hash
    AND rad.charttime BETWEEN DATETIME_SUB(cxr.study_datetime, INTERVAL 48 HOUR) AND DATETIME_ADD(cxr.study_end, INTERVAL 48 HOUR)
WHERE cxr.missing_times = 0 AND DATETIME_DIFF(cxr.study_end, cxr.study_datetime, MICROSECOND) <= 21600000000
    AND NOT EXISTS (SELECT 1 FROM lk_cxr_match old WHERE old.study_id = cxr.study_id)
    AND NOT EXISTS (SELECT 1 FROM lk_cxr_match old WHERE old.radiology_note_id = rad.source_note_id)
    AND EXISTS (
        SELECT 1 FROM src_cxr_time tm WHERE tm.study_id = cxr.study_id
          AND (ABS(DATETIME_DIFF(rad.charttime, tm.study_datetime, MICROSECOND)) / 1000000.0) <= 172800
    )
;
INSERT INTO lk_cxr_match
SELECT study_id, radiology_note_id, 'FULL_TEXT_WITHIN_48H'
FROM lk_cxr_late_candidate WHERE study_candidates = 1 AND report_candidates = 1;

-- Absence of a nearby report alone is insufficient: a known exact full,
-- final, clinical-body or mask-only match anywhere in the patient blocks
-- insertion. Seven days is deliberately wider than the matching window.
CREATE TEMP TABLE lk_cxr_novel AS
SELECT cxr.study_id
FROM src_cxr_study cxr
WHERE NOT EXISTS (SELECT 1 FROM lk_cxr_match m WHERE m.study_id = cxr.study_id)
  AND NOT EXISTS (
      SELECT 1 FROM src_cxr_radiology r WHERE r.subject_id = cxr.subject_id
        AND r.charttime BETWEEN DATETIME_SUB(cxr.study_datetime, INTERVAL 7 DAY) AND DATETIME_ADD(cxr.study_end, INTERVAL 7 DAY)
  )
  AND NOT EXISTS (
      SELECT 1 FROM src_cxr_radiology r WHERE r.subject_id = cxr.subject_id
        AND (r.full_hash IN (cxr.full_hash, cxr.final_hash)
             OR r.body_hash = cxr.final_body_hash OR r.mask_body_hash = cxr.mask_body_hash)
  );

CREATE TEMP TABLE lk_cxr_resolution AS
SELECT
    cxr.subject_id                              AS subject_id,
    cxr.study_id                                AS study_id,
    cxr.source_note_id                          AS source_note_id,
    cxr.study_datetime                          AS study_datetime,
    CASE
        WHEN legacy.study_id IS NOT NULL THEN 'MATCHED_RADIOLOGY'
        WHEN cxr.study_datetime IS NULL OR cxr.missing_times <> 0
             OR DATETIME_DIFF(cxr.study_end, cxr.study_datetime, MICROSECOND) > 21600000000 THEN 'REVIEW_STUDY_TIME'
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
ASSERT ((SELECT count(*) FROM lk_cxr_resolution) = (SELECT count(*) FROM src_cxr_study)) AS 'reconciliation_no_fanout';
ASSERT (NOT EXISTS (
        SELECT 1 FROM @etl_project.@etl_dataset.ext_cxr_reconciliation old
        LEFT JOIN lk_cxr_resolution new USING (study_id)
        WHERE old.decision IN ('MATCHED_RADIOLOGY', 'NEW_NO_PATIENT_RADIOLOGY', 'NEW_NO_NEARBY_RADIOLOGY', 'NEW_SOURCE_VARIANT')
          AND (new.study_id IS NULL OR TO_JSON_STRING(STRUCT(old.decision, old.radiology_note_id)) IS DISTINCT FROM TO_JSON_STRING(STRUCT(new.decision, new.radiology_note_id)))
    )) AS 'stable_existing_resolution';

-- -------------------------------------------------------------------
-- lk_cxr_overlap_candidate
-- Retain unresolved source versions as NOTE documents, without claiming
-- that a nearby/fuzzy candidate is the same document or a new event.
-- Candidate links below are evidence only; they never allocate an alias.
-- Whole-text candidates are retained even outside the identity window.
-- -------------------------------------------------------------------

CREATE TEMP TABLE lk_cxr_overlap_candidate AS
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
    CAST((SELECT min((ABS(DATETIME_DIFF(rad.charttime, tm.study_datetime, MICROSECOND)) / 1000000.0))
        FROM src_cxr_time tm WHERE tm.study_id = cxr.study_id) AS FLOAT64) AS time_gap_seconds
FROM src_cxr_study cxr
INNER JOIN lk_cxr_resolution res USING (study_id)
INNER JOIN src_cxr_radiology rad
    ON rad.subject_id = cxr.subject_id AND rad.note_type = 'RR'
    AND (rad.full_hash IN (cxr.full_hash, cxr.final_hash)
         OR rad.body_hash = cxr.final_body_hash OR rad.mask_body_hash = cxr.mask_body_hash
         OR (rad.chest_exam AND rad.charttime BETWEEN
             DATETIME_SUB(cxr.study_datetime, INTERVAL 48 HOUR) AND DATETIME_ADD(cxr.study_end, INTERVAL 48 HOUR)))
WHERE res.decision = 'NEW_SOURCE_VARIANT'
;

-- -------------------------------------------------------------------
-- lk_cxr_component
-- A matched final report can have an additional source prefix containing
-- wet reads, addenda and versions. Keep the prefix byte-for-byte, or reuse
-- an exact existing AR explicitly linked to the same parent RR.
-- -------------------------------------------------------------------

CREATE TEMP TABLE src_cxr_parent AS
SELECT DISTINCT note_id AS parent_note_id, field_value AS addendum_note_id
FROM @note_project.@note_dataset.radiology_detail WHERE field_name = 'addendum_note_id'
UNION DISTINCT
SELECT DISTINCT field_value AS parent_note_id, note_id AS addendum_note_id
FROM @note_project.@note_dataset.radiology_detail WHERE field_name = 'parent_note_id';
CREATE TEMP TABLE lk_cxr_split AS
SELECT cxr.study_id, cxr.prefix_length
FROM src_cxr_study cxr
INNER JOIN lk_cxr_resolution res USING (study_id)
INNER JOIN src_cxr_radiology rad ON rad.source_note_id = res.radiology_note_id
WHERE res.decision = 'MATCHED_RADIOLOGY' AND cxr.prefix_length > 0 AND cxr.full_hash <> rad.full_hash;
CREATE TEMP TABLE lk_cxr_addendum_candidate AS
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
CREATE TEMP TABLE lk_cxr_component AS
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
-- Check the whole corpus. COUNT avoids an EXISTS plan optimized to find
-- the first mismatch, which does not exist when the reconstruction passes.
ASSERT ((
        SELECT count(*) = 0 FROM @modality_project.@modality_dataset.cxr_reports raw
        LEFT JOIN (
            SELECT comp.study_id,
                string_agg(substr(src.text, comp.source_start, comp.source_length), '' ORDER BY comp.source_start) AS text
            FROM lk_cxr_component comp
            INNER JOIN @modality_project.@modality_dataset.cxr_reports src ON CAST(src.study_id AS INT64) = comp.study_id
            GROUP BY comp.study_id
        ) parts ON parts.study_id = CAST(raw.study_id AS INT64)
        WHERE raw.text IS DISTINCT FROM parts.text
    )) AS 'component_source_reconstruction';
ASSERT (NOT EXISTS (
        SELECT 1 FROM @etl_project.@etl_dataset.ext_cxr_component old LEFT JOIN lk_cxr_component new USING (study_id, component_kind)
        WHERE old.decision IN ('MATCHED_RADIOLOGY','NEW_NO_PATIENT_RADIOLOGY','NEW_NO_NEARBY_RADIOLOGY','NEW_SUPPLEMENT','NEW_SOURCE_VARIANT')
          AND (new.study_id IS NULL OR TO_JSON_STRING(STRUCT(old.source_note_id,old.source_start,old.source_length,old.decision,old.radiology_note_id)) IS DISTINCT FROM TO_JSON_STRING(STRUCT(new.source_note_id,new.source_start,new.source_length,new.decision,new.radiology_note_id)))
    )) AS 'stable_existing_components';

-- -------------------------------------------------------------------
-- Canonical source views for the existing NOTE mapping
-- note_seq = 0 means unavailable, not a chronological study sequence.
-- -------------------------------------------------------------------


ASSERT (SELECT count(*) = count(DISTINCT study_id) FROM src_cxr_study) AS 'Duplicate CXR study';
ASSERT (SELECT count(*) = count(DISTINCT study_id) AND count(*) = count(DISTINCT radiology_note_id)
    FROM lk_cxr_match) AS 'CXR match fanout';
ASSERT (SELECT count(*) = count(DISTINCT source_note_id)
    AND count(*) = count(DISTINCT TO_JSON_STRING(STRUCT(study_id,component_kind))) FROM lk_cxr_component)
    AS 'Component fanout';

BEGIN TRANSACTION;
DELETE FROM @etl_project.@etl_dataset.ext_cxr_reconciliation;
INSERT INTO @etl_project.@etl_dataset.ext_cxr_reconciliation SELECT * FROM lk_cxr_resolution;
DELETE FROM @etl_project.@etl_dataset.ext_cxr_overlap_candidate;
INSERT INTO @etl_project.@etl_dataset.ext_cxr_overlap_candidate SELECT * FROM lk_cxr_overlap_candidate;
DELETE FROM @etl_project.@etl_dataset.ext_cxr_component;
INSERT INTO @etl_project.@etl_dataset.ext_cxr_component SELECT * FROM lk_cxr_component;
COMMIT TRANSACTION;

CREATE OR REPLACE VIEW @modality_project.@modality_dataset.cxr AS
SELECT
    comp.source_note_id                         AS note_id,
    link.subject_id                             AS subject_id,
    CAST(NULL AS INT64)                       AS hadm_id,
    CASE WHEN comp.component_kind = 'PREFIX' THEN 'CXR_SUPPLEMENT'
         WHEN comp.decision = 'NEW_SOURCE_VARIANT' THEN 'CXR_VARIANT'
         ELSE 'CXR' END                          AS note_type,
    0                                           AS note_seq,
    link.study_datetime                         AS charttime,
    CAST(NULL AS DATETIME)                     AS storetime,
    substr(raw.text, comp.source_start, comp.source_length) AS text
FROM
    @modality_project.@modality_dataset.cxr_reports raw
INNER JOIN
    @etl_project.@etl_dataset.ext_cxr_reconciliation link ON link.study_id = CAST(raw.study_id AS INT64)
INNER JOIN
    @etl_project.@etl_dataset.ext_cxr_component comp ON comp.study_id = link.study_id
WHERE
    comp.decision IN ('NEW_NO_PATIENT_RADIOLOGY','NEW_NO_NEARBY_RADIOLOGY','NEW_SUPPLEMENT','NEW_SOURCE_VARIANT')
;
CREATE OR REPLACE VIEW @modality_project.@modality_dataset.cxr_detail AS
SELECT
    src.note_id                                 AS note_id,
    src.subject_id                              AS subject_id,
    'study_id'                            AS field_name,
    raw.study_id                                AS field_value,
    1                                           AS field_ordinal
FROM @modality_project.@modality_dataset.cxr src
INNER JOIN @etl_project.@etl_dataset.ext_cxr_component comp ON comp.source_note_id = src.note_id
INNER JOIN @modality_project.@modality_dataset.cxr_reports raw ON CAST(raw.study_id AS INT64) = comp.study_id
UNION ALL
SELECT src.note_id, src.subject_id, 'timestamp_basis', 'STUDY_ACQUISITION_TIME_NOT_DOCUMENT_AVAILABILITY', 1
FROM @modality_project.@modality_dataset.cxr src
UNION ALL
SELECT src.note_id, src.subject_id, 'duplicate_status', 'UNRESOLVED_POSSIBLE_DUPLICATE', 1
FROM @modality_project.@modality_dataset.cxr src WHERE src.note_type = 'CXR_VARIANT'
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
        coalesce(coalesce(report_0,''),''), coalesce(coalesce(report_1,''),''), coalesce(coalesce(report_2,''),''),
        coalesce(coalesce(report_3,''),''), coalesce(coalesce(report_4,''),''), coalesce(coalesce(report_5,''),''),
        coalesce(coalesce(report_6,''),''), coalesce(coalesce(report_7,''),''), coalesce(coalesce(report_8,''),''),
        coalesce(coalesce(report_9,''),''), coalesce(coalesce(report_10,''),''), coalesce(coalesce(report_11,''),''),
        coalesce(coalesce(report_12,''),''), coalesce(coalesce(report_13,''),''), coalesce(coalesce(report_14,''),''),
        coalesce(coalesce(report_15,''),''), coalesce(coalesce(report_16,''),''), coalesce(coalesce(report_17,''),'')
    ], '\n')                                   AS text
FROM
    @modality_project.@modality_dataset.ecg_machine_measurements
WHERE
    TRIM(CONCAT(coalesce(report_0,''),coalesce(report_1,''),coalesce(report_2,''),coalesce(report_3,''),coalesce(report_4,''),coalesce(report_5,''),
        report_6,coalesce(report_7,''),coalesce(report_8,''),coalesce(report_9,''),coalesce(report_10,''),coalesce(report_11,''),coalesce(report_12,''),
        report_13,coalesce(report_14,''),coalesce(report_15,''),coalesce(report_16,''),coalesce(report_17,'')), ' \t\r\n') <> ''
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
