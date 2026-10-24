-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- Preserve NOTE source identifiers and metadata
--
-- Dependencies: cdm_note.sql, in the same transaction
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- ext_note_id_map
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.ext_note_id_map
SELECT
    src.source_table                            AS source_table,
    src.source_note_id                          AS source_note_id,
    src.note_id                                 AS note_id
FROM
    @etl_project.@etl_dataset.lk_note_mapped src
LEFT JOIN
    @etl_project.@etl_dataset.ext_note_id_map map
        ON  map.source_table = src.source_table
        AND map.source_note_id = src.source_note_id
WHERE
    map.note_id IS NULL
;

-- -------------------------------------------------------------------
-- ext_note_metadata
-- Includes documents without a matching CDM person
-- -------------------------------------------------------------------

DELETE FROM @etl_project.@etl_dataset.ext_note_metadata
WHERE source_table = '@note_source_table'
;

INSERT INTO @etl_project.@etl_dataset.ext_note_metadata
SELECT
    src.source_table                            AS source_table,
    src.source_note_id                          AS source_note_id,
    src.note_id                                 AS note_id,
    '@note_source_version'                      AS source_version,
    NULLIF('@note_source_sha256', '')           AS source_sha256,
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
    CURRENT_TIMESTAMP()                         AS updated_at
FROM
    @etl_project.@etl_dataset.lk_note_mapped src
;

-- -------------------------------------------------------------------
-- ext_note_detail
-- Keep field ordinal and duplicate multiplicity
-- -------------------------------------------------------------------

DELETE FROM @etl_project.@etl_dataset.ext_note_detail
WHERE source_table = '@note_source_table'
;

INSERT INTO @etl_project.@etl_dataset.ext_note_detail
SELECT
    '@note_source_table'                         AS source_table,
    det.source_note_id                          AS source_note_id,
    src.note_id                                 AS note_id,
    det.subject_id                              AS subject_id,
    det.field_name                              AS field_name,
    det.field_value                             AS field_value,
    det.field_ordinal                           AS field_ordinal
FROM
    @etl_project.@etl_dataset.src_note_detail det
LEFT JOIN
    @etl_project.@etl_dataset.lk_note_mapped src
        ON src.source_note_id = det.source_note_id
;
