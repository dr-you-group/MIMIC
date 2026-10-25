-- MIMIC IV CDM Conversion
-- Same CXR component and matching rules as the PostgreSQL query.

CREATE TABLE IF NOT EXISTS @etl_project.@etl_dataset.ext_modality_note_link (
    source_table STRING NOT NULL, subject_id INT64 NOT NULL, study_id INT64 NOT NULL,
    source_note_id STRING NOT NULL, external_note_id STRING, person_id INT64, note_id INT64,
    source_datetime DATETIME, timestamp_basis STRING NOT NULL,
    reconciliation_status STRING NOT NULL, load_status STRING NOT NULL,
    match_method STRING, candidate_count INT64 NOT NULL,
    source_version STRING NOT NULL, source_sha256 STRING NOT NULL, checksum_status STRING NOT NULL
);
CREATE TABLE IF NOT EXISTS @etl_project.@etl_dataset.ext_cxr_note_component (
    subject_id INT64 NOT NULL, study_id INT64 NOT NULL, component_kind STRING NOT NULL,
    source_note_id STRING NOT NULL, source_start INT64 NOT NULL, source_length INT64 NOT NULL,
    text_sha256 STRING NOT NULL, person_id INT64, note_id INT64, parent_note_id INT64, external_note_id STRING,
    decision STRING NOT NULL, match_method STRING, candidate_count INT64 NOT NULL, load_status STRING NOT NULL,
    source_datetime DATETIME, timestamp_basis STRING NOT NULL,
    source_version STRING NOT NULL, source_sha256 STRING NOT NULL, checksum_status STRING NOT NULL
);

-- -------------------------------------------------------------------
-- lk_modality_note_link
-- Alias links do not enter the one-document/one-owner NOTE ID map.
-- -------------------------------------------------------------------

CREATE TEMP TABLE lk_modality_note_link AS
SELECT
    'cxr'                                 AS source_table,
    link.subject_id                             AS subject_id,
    link.study_id                               AS study_id,
    link.source_note_id                         AS source_note_id,
    link.radiology_note_id                       AS external_note_id,
    per.person_id                               AS person_id,
    nt.note_id                                  AS note_id,
    link.study_datetime                         AS source_datetime,
    'STUDY_ACQUISITION_TIME'               AS timestamp_basis,
    link.decision                               AS reconciliation_status,
    CASE
        WHEN per.person_id IS NULL THEN 'MISSING_PERSON'
        WHEN link.decision = 'MATCHED_RADIOLOGY' AND nt.note_id IS NOT NULL THEN 'LINKED_EXISTING'
        WHEN link.decision IN ('NEW_NO_PATIENT_RADIOLOGY','NEW_NO_NEARBY_RADIOLOGY') AND nt.note_id IS NOT NULL THEN 'LOADED'
        WHEN link.decision IN ('MATCHED_RADIOLOGY','NEW_NO_PATIENT_RADIOLOGY','NEW_NO_NEARBY_RADIOLOGY') THEN 'ERROR_NOTE_NOT_LOADED'
        ELSE link.decision
    END                                         AS load_status,
    link.match_method                           AS match_method,
    link.candidate_count                        AS candidate_count,
    file.source_version                         AS source_version,
    file.source_sha256                          AS source_sha256,
    file.checksum_status                        AS checksum_status
FROM
    @etl_project.@etl_dataset.ext_cxr_reconciliation link
CROSS JOIN
    @modality_project.@modality_dataset.source_file file
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person per ON per.person_source_value = CAST(link.subject_id AS STRING)
LEFT JOIN
    @etl_project.@etl_dataset.ext_note_id_map map
        ON map.source_table = CASE WHEN link.decision = 'MATCHED_RADIOLOGY' THEN 'radiology' ELSE 'cxr' END
        AND map.source_note_id = CASE WHEN link.decision = 'MATCHED_RADIOLOGY'
            THEN link.radiology_note_id ELSE link.source_note_id END
LEFT JOIN
    @etl_project.@etl_dataset.cdm_note nt ON nt.note_id = map.note_id
WHERE
    file.table_name = 'cxr_reports'
UNION ALL
SELECT
    'ecg_machine'                               AS source_table,
    CAST(raw.subject_id AS INT64)             AS subject_id,
    CAST(raw.study_id AS INT64)               AS study_id,
    raw.subject_id || '-ECG_MACHINE-' || raw.study_id AS source_note_id,
    CAST(NULL AS STRING)                          AS external_note_id,
    per.person_id                               AS person_id,
    nt.note_id                                  AS note_id,
    CAST(raw.ecg_time AS DATETIME)             AS source_datetime,
    'UNSYNCHRONIZED_ECG_MACHINE_CLOCK'          AS timestamp_basis,
    'MACHINE_INTERPRETATION'                    AS reconciliation_status,
    CASE
        WHEN src.note_id IS NULL THEN 'EMPTY_TEXT'
        WHEN per.person_id IS NULL THEN 'MISSING_PERSON'
        WHEN nt.note_id IS NULL THEN 'ERROR_NOTE_NOT_LOADED'
        ELSE 'LOADED'
    END                                         AS load_status,
    CAST(NULL AS STRING)                          AS match_method,
    0                                   AS candidate_count,
    file.source_version                         AS source_version,
    file.source_sha256                          AS source_sha256,
    file.checksum_status                        AS checksum_status
FROM
    @modality_project.@modality_dataset.ecg_machine_measurements raw
CROSS JOIN
    @modality_project.@modality_dataset.source_file file
LEFT JOIN
    @modality_project.@modality_dataset.ecg_machine src
        ON src.note_id = raw.subject_id || '-ECG_MACHINE-' || raw.study_id
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person per ON per.person_source_value = raw.subject_id
LEFT JOIN
    @etl_project.@etl_dataset.ext_note_id_map map ON map.source_table = 'ecg_machine' AND map.source_note_id = src.note_id
LEFT JOIN
    @etl_project.@etl_dataset.cdm_note nt ON nt.note_id = map.note_id
WHERE
    file.table_name = 'ecg_machine_measurements'
UNION ALL
SELECT
    'echo'                                      AS source_table,
    CAST(raw.subject_id AS INT64)             AS subject_id,
    CAST(raw.study_id AS INT64)               AS study_id,
    raw.subject_id || '-ECHO_STUDY-' || raw.study_id AS source_note_id,
    NULLIF(raw.note_id, '')                     AS external_note_id,
    per.person_id                               AS person_id,
    CAST(NULL AS INT64)                        AS note_id,
    CAST(NULLIF(raw.study_datetime, '') AS DATETIME) AS source_datetime,
    'STUDY_TIME_WITH_TEMPORAL_REPORT_LINK'       AS timestamp_basis,
    'LINK_METADATA_ONLY'                       AS reconciliation_status,
    'NO_REPORT_BODY'                            AS load_status,
    'PUBLISHED_TEMPORAL_LINK_WITHIN_2_DAYS'      AS match_method,
    0                                   AS candidate_count,
    file.source_version                         AS source_version,
    file.source_sha256                          AS source_sha256,
    file.checksum_status                        AS checksum_status
FROM
    @modality_project.@modality_dataset.echo_study_list raw
CROSS JOIN
    @modality_project.@modality_dataset.source_file file
LEFT JOIN
    @etl_project.@etl_dataset.cdm_person per ON per.person_source_value = raw.subject_id
WHERE
    file.table_name = 'echo_study_list'
;

-- -------------------------------------------------------------------
-- ext_modality_note_link
-- Every source study remains accounted for, including empty/unavailable
-- text, missing persons, ambiguous overlaps and unverified Echo metadata.
-- -------------------------------------------------------------------

ASSERT (NOT EXISTS (SELECT 1 FROM lk_modality_note_link WHERE load_status = 'ERROR_NOTE_NOT_LOADED')) AS 'all_eligible_notes_loaded';
ASSERT ((SELECT count(*) FROM lk_modality_note_link) =
        (SELECT count(*) FROM @modality_project.@modality_dataset.cxr_reports) +
        (SELECT count(*) FROM @modality_project.@modality_dataset.ecg_machine_measurements) +
        (SELECT count(*) FROM @modality_project.@modality_dataset.echo_study_list)) AS 'source_accounting';
ASSERT (NOT EXISTS (
        SELECT 1 FROM lk_modality_note_link link INNER JOIN @etl_project.@etl_dataset.cdm_note nt USING (note_id)
        WHERE nt.person_id IS DISTINCT FROM link.person_id
    )) AS 'same_person_note_links';
ASSERT (NOT EXISTS (SELECT 1 FROM lk_modality_note_link WHERE source_table = 'echo' AND note_id IS NOT NULL)) AS 'no_fabricated_echo_notes';
ASSERT (NOT EXISTS (
        SELECT 1 FROM @etl_project.@etl_dataset.ext_modality_note_link old LEFT JOIN lk_modality_note_link new
        USING (source_table, study_id)
        WHERE old.note_id IS NOT NULL AND old.note_id IS DISTINCT FROM new.note_id
    )) AS 'no_changed_or_deleted_existing_link';

-- -------------------------------------------------------------------
-- lk_cxr_note_component
-- One study may reference a final NOTE and a supplementary NOTE or AR.
-- Offsets and hashes also preserve unmatched and missing-person text.
-- -------------------------------------------------------------------

CREATE TEMP TABLE lk_cxr_note_component AS
SELECT
    comp.subject_id, comp.study_id, comp.component_kind, comp.source_note_id,
    comp.source_start, comp.source_length,
    LOWER(TO_HEX(SHA256(substr(raw.text, comp.source_start, comp.source_length)))) AS text_sha256,
    study.person_id,
    nt.note_id,
    CASE WHEN comp.component_kind = 'PREFIX' THEN study.note_id END AS parent_note_id,
    comp.radiology_note_id AS external_note_id,
    comp.decision, comp.match_method, comp.candidate_count,
    CASE
        WHEN study.person_id IS NULL THEN 'MISSING_PERSON'
        WHEN comp.decision = 'MATCHED_RADIOLOGY' AND nt.note_id IS NOT NULL THEN 'LINKED_EXISTING'
        WHEN comp.decision IN ('NEW_NO_PATIENT_RADIOLOGY','NEW_NO_NEARBY_RADIOLOGY','NEW_SUPPLEMENT')
            AND nt.note_id IS NOT NULL THEN 'LOADED'
        WHEN comp.decision IN ('MATCHED_RADIOLOGY','NEW_NO_PATIENT_RADIOLOGY','NEW_NO_NEARBY_RADIOLOGY','NEW_SUPPLEMENT')
            THEN 'ERROR_NOTE_NOT_LOADED'
        ELSE comp.decision
    END AS load_status,
    study.source_datetime,
    'STUDY_ACQUISITION_TIME_NOT_DOCUMENT_AVAILABILITY' AS timestamp_basis,
    study.source_version, study.source_sha256, study.checksum_status
FROM @etl_project.@etl_dataset.ext_cxr_component comp
INNER JOIN @modality_project.@modality_dataset.cxr_reports raw ON CAST(raw.study_id AS INT64) = comp.study_id
INNER JOIN lk_modality_note_link study ON study.source_table = 'cxr' AND study.study_id = comp.study_id
LEFT JOIN @etl_project.@etl_dataset.ext_note_id_map map
    ON map.source_table = CASE WHEN comp.decision = 'MATCHED_RADIOLOGY' THEN 'radiology' ELSE 'cxr' END
    AND map.source_note_id = CASE WHEN comp.decision = 'MATCHED_RADIOLOGY' THEN comp.radiology_note_id ELSE comp.source_note_id END
LEFT JOIN @etl_project.@etl_dataset.cdm_note nt ON nt.note_id = map.note_id;
ASSERT (NOT EXISTS (SELECT 1 FROM lk_cxr_note_component WHERE load_status = 'ERROR_NOTE_NOT_LOADED')) AS 'all_eligible_components_loaded';
ASSERT (NOT EXISTS (
        SELECT 1 FROM lk_cxr_note_component c INNER JOIN @etl_project.@etl_dataset.cdm_note n USING (note_id)
        WHERE c.person_id IS DISTINCT FROM n.person_id
    )) AS 'same_person_component_links';
ASSERT (NOT EXISTS (
        SELECT 1 FROM lk_cxr_note_component c INNER JOIN @etl_project.@etl_dataset.cdm_note n USING (note_id)
        WHERE c.load_status = 'LOADED' AND c.text_sha256 <> LOWER(TO_HEX(SHA256(n.note_text)))
    )) AS 'loaded_component_text_unchanged';
ASSERT (NOT EXISTS (
        SELECT 1 FROM @etl_project.@etl_dataset.ext_cxr_note_component old LEFT JOIN lk_cxr_note_component new USING (study_id, component_kind)
        WHERE old.note_id IS NOT NULL AND old.note_id IS DISTINCT FROM new.note_id
    )) AS 'stable_existing_component_links';
ASSERT (SELECT count(*) = count(DISTINCT TO_JSON_STRING(STRUCT(source_table,study_id)))
    FROM lk_modality_note_link) AS 'Modality link fanout';
ASSERT (SELECT count(*) = count(DISTINCT TO_JSON_STRING(STRUCT(study_id,component_kind)))
    FROM lk_cxr_note_component) AS 'Component link fanout';

BEGIN TRANSACTION;
DELETE FROM @etl_project.@etl_dataset.ext_modality_note_link WHERE source_table IN ('cxr', 'ecg_machine', 'echo');
INSERT INTO @etl_project.@etl_dataset.ext_modality_note_link SELECT * FROM lk_modality_note_link;
DELETE FROM @etl_project.@etl_dataset.ext_cxr_note_component;
INSERT INTO @etl_project.@etl_dataset.ext_cxr_note_component SELECT * FROM lk_cxr_note_component;
ASSERT (NOT EXISTS (
        (SELECT * FROM lk_modality_note_link EXCEPT DISTINCT SELECT * FROM @etl_project.@etl_dataset.ext_modality_note_link
         WHERE source_table IN ('cxr', 'ecg_machine', 'echo'))
        UNION ALL
        (SELECT * FROM @etl_project.@etl_dataset.ext_modality_note_link WHERE source_table IN ('cxr', 'ecg_machine', 'echo')
         EXCEPT DISTINCT SELECT * FROM lk_modality_note_link)
    )) AS 'all_link_fields_preserved';
COMMIT TRANSACTION;

CREATE OR REPLACE VIEW @etl_project.@etl_dataset.ext_cxr_component_text AS
SELECT comp.*, substr(raw.text, comp.source_start, comp.source_length) AS source_text
FROM @etl_project.@etl_dataset.ext_cxr_note_component comp
INNER JOIN @modality_project.@modality_dataset.cxr_reports raw ON CAST(raw.study_id AS INT64) = comp.study_id;

SELECT source_table, load_status, checksum_status, count(*) AS source_rows
FROM @etl_project.@etl_dataset.ext_modality_note_link
GROUP BY source_table, load_status, checksum_status
ORDER BY source_table, load_status, checksum_status;
