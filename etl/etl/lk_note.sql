-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- Populate lookups for cdm_note
--
-- Dependencies: st_note.sql, finalized cdm_person, cdm_visit_occurrence
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- lk_note_type
-- Rules 1-2: discharge summaries and radiology reports
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_note_type AS
SELECT
    'discharge'                                 AS source_table,
    'DS'                                        AS note_type,
    '18842-5'                                   AS loinc_code,
    'Discharge summary'                         AS note_title
UNION ALL
SELECT 'discharge', 'AD', '18842-5', 'Discharge summary addendum'
UNION ALL
SELECT 'radiology', 'RR', '68604-8', 'Radiology report'
UNION ALL
SELECT 'radiology', 'AR', '68604-8', 'Radiology report addendum'
UNION ALL
SELECT 'cxr', 'CXR', '68604-8', 'Chest radiograph report'
UNION ALL
SELECT 'cxr', 'CXR_SUPPLEMENT', '68604-8', 'Chest radiograph supplementary report (preliminary/addendum)'
UNION ALL
SELECT 'ecg_machine', 'ECG_MACHINE', '75425-9', 'ECG machine interpretation'
;

-- -------------------------------------------------------------------
-- lk_note_mapped
-- Keep existing Core person and visit identifiers
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_note_mapped AS
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
    @etl_project.@etl_dataset.src_note src
INNER JOIN
    @etl_project.@etl_dataset.lk_note_type map
        ON  map.source_table = src.source_table
        AND map.note_type = src.note_type
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person per
        ON per.person_source_value = CAST(src.subject_id AS STRING)
LEFT JOIN
    @etl_project.@etl_dataset.cdm_visit_occurrence vis
        ON  vis.person_id = per.person_id
        AND src.hadm_id IS NOT NULL
        AND vis.visit_source_value =
            CONCAT(CAST(src.subject_id AS STRING), '|', CAST(src.hadm_id AS STRING))
LEFT JOIN
    @etl_project.@etl_dataset.voc_concept vc
        ON  vc.vocabulary_id = 'LOINC'
        AND vc.concept_code = map.loinc_code
        AND vc.domain_id = 'Note'
        AND vc.standard_concept = 'S'
        AND vc.invalid_reason IS NULL
        AND CURRENT_DATE() BETWEEN vc.valid_start_date AND vc.valid_end_date
;

-- -------------------------------------------------------------------
-- Validate mappings before changing the CDM
-- -------------------------------------------------------------------

ASSERT NOT EXISTS (
    SELECT 1 FROM @etl_project.@etl_dataset.src_note
    WHERE source_table != '@note_source_table'
) AS 'Staging contains another source; run NOTE workflows sequentially'
;
ASSERT (
    SELECT COUNT(*) = 3
    FROM @etl_project.@etl_dataset.voc_concept
    WHERE ((concept_id = 32817 AND domain_id = 'Type Concept')
        OR (concept_id = 32678 AND domain_id = 'Metadata')
        OR (concept_id = 4180186 AND domain_id = 'Language'))
        AND standard_concept = 'S'
        AND invalid_reason IS NULL
        AND CURRENT_DATE() BETWEEN valid_start_date AND valid_end_date
) AS 'Missing or invalid NOTE constant concepts'
;
ASSERT NOT EXISTS (
    SELECT 1
    FROM @etl_project.@etl_dataset.src_note src
    LEFT JOIN @etl_project.@etl_dataset.lk_note_type map
        ON map.source_table = src.source_table AND map.note_type = src.note_type
    WHERE map.note_type IS NULL
) AS 'Unsupported note type'
;
ASSERT NOT EXISTS (
    SELECT 1
    FROM @etl_project.@etl_dataset.src_note_detail det
    INNER JOIN @etl_project.@etl_dataset.src_note src
        ON src.source_note_id = det.source_note_id
    WHERE src.subject_id IS DISTINCT FROM det.subject_id
) AS 'Detail subject mismatch for an available parent'
;
ASSERT (SELECT COUNT(*) FROM @etl_project.@etl_dataset.lk_note_mapped) =
    (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_note)
                                                AS 'NOTE join fanout'
;
ASSERT NOT EXISTS (
    SELECT 1 FROM @etl_project.@etl_dataset.lk_note_mapped
    WHERE note_class_concept_id IS NULL
) AS 'Missing document class mapping'
;
