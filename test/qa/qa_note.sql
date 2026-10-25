-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- Validate NOTE and commit the source-specific load
--
-- Dependencies: cdm_note.sql, ext_note.sql, in the same BigQuery job
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Compare every mapped NOTE field and original text
-- -------------------------------------------------------------------

ASSERT NOT EXISTS (
    SELECT 1 FROM @etl_project.@etl_dataset.lk_note_mapped w LEFT JOIN @etl_project.@etl_dataset.cdm_note n USING (note_id)
    WHERE w.person_id IS NOT NULL AND (
        n.note_id IS NULL OR
        TO_JSON_STRING(STRUCT(n.person_id, n.note_date, n.note_datetime, n.note_type_concept_id,
            n.note_class_concept_id, n.note_title, n.encoding_concept_id, n.language_concept_id,
            n.provider_id, n.visit_occurrence_id, n.visit_detail_id, n.note_source_value,
            LOWER(TO_HEX(SHA256(n.note_text))) AS text_sha256, BYTE_LENGTH(n.note_text) AS text_bytes))
        != TO_JSON_STRING(STRUCT(w.person_id, DATE(w.charttime) AS note_date,
            CASE WHEN w.source_table = 'cxr' THEN CAST(NULL AS DATETIME)
                 ELSE w.charttime END AS note_datetime, 32817 AS note_type_concept_id, w.note_class_concept_id,
            w.note_title, 32678 AS encoding_concept_id, 4180186 AS language_concept_id,
            CAST(NULL AS INT64) AS provider_id, w.visit_occurrence_id,
            CAST(NULL AS INT64) AS visit_detail_id, w.note_type AS note_source_value,
            w.text_sha256, w.text_bytes))
    )
) AS 'Loaded NOTE differs from source or mapping';
ASSERT (
    SELECT COUNT(*) FROM @etl_project.@etl_dataset.cdm_note n
    JOIN @etl_project.@etl_dataset.ext_note_id_map m USING (note_id)
    WHERE m.source_table = '@note_source_table'
) = (SELECT COUNT(*) FROM @etl_project.@etl_dataset.lk_note_mapped WHERE person_id IS NOT NULL) AS 'Owned NOTE row count mismatch';

-- -------------------------------------------------------------------
-- Compare all detail fields, including duplicate rows
-- -------------------------------------------------------------------
ASSERT NOT EXISTS (
    SELECT 1 FROM @etl_project.@etl_dataset.ext_note_detail det
    LEFT JOIN @etl_project.@etl_dataset.lk_note_mapped src
        ON src.source_note_id = det.source_note_id
    WHERE det.source_table = '@note_source_table'
      AND det.note_id IS DISTINCT FROM src.note_id
) AS 'Incorrect detail NOTE link';

ASSERT NOT EXISTS (
    WITH source_groups AS (
        SELECT source_note_id, subject_id, field_name, field_value, field_ordinal, COUNT(*) AS n
        FROM @etl_project.@etl_dataset.src_note_detail GROUP BY 1,2,3,4,5
    ), target_groups AS (
        SELECT source_note_id, subject_id, field_name, field_value, field_ordinal, COUNT(*) AS n
        FROM @etl_project.@etl_dataset.ext_note_detail
        WHERE source_table = '@note_source_table' GROUP BY 1,2,3,4,5
    )
    SELECT * FROM source_groups EXCEPT DISTINCT SELECT * FROM target_groups
) AS 'Missing or changed detail rows';
ASSERT NOT EXISTS (
    WITH source_groups AS (
        SELECT source_note_id, subject_id, field_name, field_value, field_ordinal, COUNT(*) AS n
        FROM @etl_project.@etl_dataset.src_note_detail GROUP BY 1,2,3,4,5
    ), target_groups AS (
        SELECT source_note_id, subject_id, field_name, field_value, field_ordinal, COUNT(*) AS n
        FROM @etl_project.@etl_dataset.ext_note_detail
        WHERE source_table = '@note_source_table' GROUP BY 1,2,3,4,5
    )
    SELECT * FROM target_groups EXCEPT DISTINCT SELECT * FROM source_groups
) AS 'Extra detail rows';

-- -------------------------------------------------------------------
-- Record a successful run and commit
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.ext_note_etl_run
(
    run_id, finished_at, source_table, source_version, source_sha256,
    source_rows, loaded_rows, rejected_person_rows, missing_visit_rows,
    no_hadm_rows, detail_rows, text_mismatches, unmatched_detail_rows
)
SELECT GENERATE_UUID(), CURRENT_TIMESTAMP(), '@note_source_table', '@note_source_version',
    NULLIF('@note_source_sha256', ''), COUNT(*), COUNTIF(load_status = 'LOADED'),
    COUNTIF(load_status = 'MISSING_PERSON'), COUNTIF(visit_status = 'MISSING_VISIT'),
    COUNTIF(visit_status = 'NO_HADM_ID'),
    (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_note_detail), 0,
    (SELECT COUNT(*) FROM @etl_project.@etl_dataset.ext_note_detail
     WHERE source_table = '@note_source_table' AND note_id IS NULL)
FROM @etl_project.@etl_dataset.lk_note_mapped;
COMMIT TRANSACTION;

SELECT * FROM @etl_project.@etl_dataset.ext_note_etl_run
WHERE source_table = '@note_source_table' ORDER BY finished_at DESC LIMIT 1;
