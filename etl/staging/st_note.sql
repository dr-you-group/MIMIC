-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- Populate NOTE staging tables
--
-- Dependencies: complete note and matching detail source tables
-- -------------------------------------------------------------------

-- Run one source at a time: discharge or radiology.
ASSERT '@note_source_table' IN ('discharge', 'radiology')
                                                AS 'Unsupported NOTE source'
;
ASSERT '@note_detail_table' = CONCAT('@note_source_table', '_detail')
                                                AS 'Wrong detail table'
;

-- -------------------------------------------------------------------
-- src_note
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_note AS
SELECT
    '@note_source_table'                        AS source_table,
    note_id                                     AS source_note_id,
    CAST(CONCAT('0x', SUBSTR(TO_HEX(SHA256(CONCAT(
        'mimic-iv-note|', '@note_source_table', '|', note_id
    ))), 1, 15)) AS INT64)                       AS note_id,
    subject_id                                  AS subject_id,
    hadm_id                                     AS hadm_id,
    note_type                                   AS note_type,
    note_seq                                    AS note_seq,
    CAST(charttime AS DATETIME)                  AS charttime,
    CAST(storetime AS DATETIME)                  AS storetime,
    text                                        AS note_text,
    LOWER(TO_HEX(SHA256(text)))                 AS text_sha256,
    BYTE_LENGTH(text)                           AS text_bytes
FROM
    @note_project.@note_dataset.@note_source_table
;

-- -------------------------------------------------------------------
-- src_note_detail
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_note_detail AS
SELECT
    note_id                                     AS source_note_id,
    subject_id                                  AS subject_id,
    field_name                                  AS field_name,
    field_value                                 AS field_value,
    field_ordinal                               AS field_ordinal
FROM
    @note_project.@note_dataset.@note_detail_table
;

-- -------------------------------------------------------------------
-- Validate the source before mapping
-- -------------------------------------------------------------------

ASSERT (
    SELECT COUNT(*) > 0
    FROM @etl_project.@etl_dataset.src_note
) AS 'Empty NOTE source'
;
ASSERT (
    SELECT COUNT(*) = COUNT(DISTINCT source_note_id)
        AND COUNT(*) = COUNT(DISTINCT note_id)
    FROM @etl_project.@etl_dataset.src_note
) AS 'Duplicate source ID or generated ID collision'
;
ASSERT NOT EXISTS (
    SELECT 1
    FROM @etl_project.@etl_dataset.src_note
    WHERE source_note_id IS NULL OR source_note_id = '' OR subject_id IS NULL
        OR charttime IS NULL OR note_text IS NULL OR text_bytes = 0
) AS 'Missing NOTE required fields'
;
