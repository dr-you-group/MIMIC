-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- NOTE-only mapping; same rules as etl/postgres/note.sql.
-- Temporary work tables only. Run in one workflow_note.conf script.
-- -------------------------------------------------------------------

CREATE TEMP TABLE note_changes AS
SELECT note_id, CASE WHEN already_loaded THEN 'VISIT_UPDATE' ELSE 'INSERT' END AS action
FROM lk_note_mapped WHERE load_status = 'LOADED'
    AND (NOT already_loaded OR (existing_visit_id IS NULL AND visit_occurrence_id IS NOT NULL));

INSERT INTO @etl_project.@etl_dataset.cdm_note (
    note_id, person_id, note_date, note_datetime, note_type_concept_id,
    note_class_concept_id, note_title, note_text, encoding_concept_id,
    language_concept_id, provider_id, visit_occurrence_id, visit_detail_id, note_source_value
)
SELECT
    src.note_id                                 AS note_id,
    src.person_id                               AS person_id,
    DATE(src.charttime)                         AS note_date,
    src.charttime                               AS note_datetime,
    32817                                       AS note_type_concept_id,
    src.note_class_concept_id                   AS note_class_concept_id,
    src.note_title                              AS note_title,
    raw.note_text                               AS note_text,
    32678                                       AS encoding_concept_id,
    4180186                                     AS language_concept_id,
    CAST(NULL AS INT64)                         AS provider_id,
    src.visit_occurrence_id                     AS visit_occurrence_id,
    CAST(NULL AS INT64)                         AS visit_detail_id,
    src.note_type                               AS note_source_value
FROM lk_note_mapped src INNER JOIN src_note_text raw USING (source_note_id)
WHERE src.load_status = 'LOADED' AND NOT src.already_loaded;

UPDATE @etl_project.@etl_dataset.cdm_note nt
SET visit_occurrence_id = src.visit_occurrence_id
FROM lk_note_mapped src
WHERE nt.note_id = src.note_id AND src.load_status = 'LOADED'
    AND nt.visit_occurrence_id IS NULL AND src.visit_occurrence_id IS NOT NULL;
