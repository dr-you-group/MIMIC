-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- Populate cdm_note table
--
-- Dependencies: lk_note.sql, ddl_note_extensions.sql
-- -------------------------------------------------------------------

-- Run through workflow_note.conf. ext_note.sql and qa_note.sql complete
-- this transaction in the same BigQuery job.
-- The target DDL is already present. Replace only this source's rows so a
-- later radiology load retains discharge notes and their stable identifiers.

-- -------------------------------------------------------------------
-- Check ownership of existing note identifiers
-- -------------------------------------------------------------------

BEGIN TRANSACTION;

ASSERT (
    SELECT COUNT(*) = COUNT(DISTINCT note_id)
        AND COUNT(*) = COUNT(DISTINCT TO_JSON_STRING(STRUCT(source_table, source_note_id)))
    FROM @etl_project.@etl_dataset.ext_note_id_map
) AS 'Duplicate permanent NOTE map'
;
ASSERT NOT EXISTS (
    SELECT 1
    FROM
        @etl_project.@etl_dataset.lk_note_mapped src
    INNER JOIN
        @etl_project.@etl_dataset.ext_note_id_map map
            ON  (map.source_table = src.source_table AND map.source_note_id = src.source_note_id)
            OR  map.note_id = src.note_id
    WHERE
        map.source_table != src.source_table
        OR map.source_note_id != src.source_note_id
        OR map.note_id != src.note_id
) AS 'NOTE ID collision or changed identity rule'
;
ASSERT NOT EXISTS (
    SELECT 1
    FROM
        @etl_project.@etl_dataset.lk_note_mapped src
    INNER JOIN
        @etl_project.@etl_dataset.cdm_note nt
            ON nt.note_id = src.note_id
    LEFT JOIN
        @etl_project.@etl_dataset.ext_note_id_map map
            ON map.note_id = nt.note_id
    WHERE
        map.note_id IS NULL
) AS 'NOTE ID already belongs to another ETL'
;
ASSERT NOT EXISTS (
    SELECT 1
    FROM
        @etl_project.@etl_dataset.ext_note_id_map map
    INNER JOIN
        @etl_project.@etl_dataset.cdm_note nt
            ON nt.note_id = map.note_id
    LEFT JOIN
        @etl_project.@etl_dataset.lk_note_mapped src
            ON  src.note_id = map.note_id
            AND src.person_id IS NOT NULL
    WHERE
        map.source_table = '@note_source_table'
        AND src.note_id IS NULL
) AS 'Source deletion or changed Core cohort requires explicit reconciliation'
;

-- -------------------------------------------------------------------
-- cdm_note
-- Rules 1-2: discharge summaries and radiology reports
-- -------------------------------------------------------------------

DELETE FROM @etl_project.@etl_dataset.cdm_note
WHERE note_id IN (
    SELECT note_id
    FROM @etl_project.@etl_dataset.ext_note_id_map
    WHERE source_table = '@note_source_table'
)
;

INSERT INTO @etl_project.@etl_dataset.cdm_note
(
    note_id,
    person_id,
    note_date,
    note_datetime,
    note_type_concept_id,
    note_class_concept_id,
    note_title,
    note_text,
    encoding_concept_id,
    language_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    note_source_value
)
SELECT
    src.note_id                                 AS note_id,
    src.person_id                               AS person_id,
    CAST(src.charttime AS DATE)                 AS note_date,
    -- CXR supplies acquisition time, not document recording time.
    CASE WHEN src.source_table = 'cxr' THEN CAST(NULL AS DATETIME)
         ELSE src.charttime END                 AS note_datetime,
    32817                                       AS note_type_concept_id, -- EHR
    src.note_class_concept_id                   AS note_class_concept_id,
    src.note_title                              AS note_title,
    src.note_text                               AS note_text,
    32678                                       AS encoding_concept_id, -- UTF-8
    4180186                                     AS language_concept_id, -- English
    CAST(NULL AS INT64)                         AS provider_id,
    src.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INT64)                         AS visit_detail_id,
    src.note_type                               AS note_source_value
FROM
    @etl_project.@etl_dataset.lk_note_mapped src
WHERE
    src.person_id IS NOT NULL
;
