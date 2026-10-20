-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- Create persistent NOTE provenance tables
--
-- Dependencies: existing CDM DDL
-- -------------------------------------------------------------------

-- These tables survive source-specific reruns. Do not CREATE OR REPLACE.

CREATE TABLE IF NOT EXISTS @etl_project.@etl_dataset.ext_note_id_map
(
    source_table                STRING       NOT NULL,
    source_note_id              STRING       NOT NULL,
    note_id                     INT64        NOT NULL
)
;
CREATE TABLE IF NOT EXISTS @etl_project.@etl_dataset.ext_note_metadata
(
    source_table                STRING       NOT NULL,
    source_note_id              STRING       NOT NULL,
    note_id                     INT64        NOT NULL,
    source_version              STRING       NOT NULL,
    source_sha256               STRING      ,
    subject_id                  INT64        NOT NULL,
    hadm_id                     INT64       ,
    note_type                   STRING       NOT NULL,
    note_seq                    INT64        NOT NULL,
    charttime                   DATETIME     NOT NULL,
    storetime                   DATETIME    ,
    person_id                   INT64       ,
    visit_occurrence_id         INT64       ,
    text_sha256                 STRING       NOT NULL,
    text_bytes                  INT64        NOT NULL,
    load_status                 STRING       NOT NULL,
    visit_status                STRING       NOT NULL,
    updated_at                  TIMESTAMP    NOT NULL
)
;
CREATE TABLE IF NOT EXISTS @etl_project.@etl_dataset.ext_note_detail
(
    source_table                STRING       NOT NULL,
    source_note_id              STRING       NOT NULL,
    note_id                     INT64        NOT NULL,
    subject_id                  INT64        NOT NULL,
    field_name                  STRING       NOT NULL,
    field_value                 STRING       NOT NULL,
    field_ordinal               INT64        NOT NULL
)
;
CREATE TABLE IF NOT EXISTS @etl_project.@etl_dataset.ext_note_etl_run
(
    run_id                      STRING       NOT NULL,
    finished_at                 TIMESTAMP    NOT NULL,
    source_table                STRING       NOT NULL,
    source_version              STRING       NOT NULL,
    source_sha256               STRING      ,
    source_rows                 INT64        NOT NULL,
    loaded_rows                 INT64        NOT NULL,
    rejected_person_rows        INT64        NOT NULL,
    missing_visit_rows          INT64        NOT NULL,
    no_hadm_rows                INT64        NOT NULL,
    detail_rows                 INT64        NOT NULL,
    text_mismatches             INT64        NOT NULL
)
;
