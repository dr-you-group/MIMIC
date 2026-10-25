-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- Preserve modality study links and explicit non-load decisions
--
-- Dependencies: st_modality_note.sql; note.sql for cxr and ecg_machine.
-- Echo is linkage-only. No source narrative exists in Echo 1.0.1.
-- -------------------------------------------------------------------

\set ON_ERROR_STOP on
\if :{?source_schema}
\else
\set source_schema mimiciv_modality
\endif
\if :{?cdm_schema}
\else
\set cdm_schema mimiciv
\endif
\if :{?etl_schema}
\else
\set etl_schema mimiciv_note_etl
\endif

BEGIN ISOLATION LEVEL REPEATABLE READ;
SET LOCAL lock_timeout = '30s';
SELECT pg_advisory_xact_lock(hashtext(current_database()), hashtext(:'cdm_schema' || '.note')) \gset
CREATE TEMP TABLE modality_checks (
    check_name text PRIMARY KEY, passed boolean NOT NULL CHECK (passed)
) ON COMMIT DROP;

-- -------------------------------------------------------------------
-- lk_modality_note_link
-- Alias links do not enter the one-document/one-owner NOTE ID map.
-- -------------------------------------------------------------------

CREATE TEMP TABLE lk_modality_note_link ON COMMIT DROP AS
SELECT
    'cxr'::text                                 AS source_table,
    link.subject_id                             AS subject_id,
    link.study_id                               AS study_id,
    link.source_note_id                         AS source_note_id,
    link.radiology_note_id                       AS external_note_id,
    per.person_id                               AS person_id,
    nt.note_id                                  AS note_id,
    link.study_datetime                         AS source_datetime,
    'STUDY_ACQUISITION_TIME'::text               AS timestamp_basis,
    link.decision                               AS reconciliation_status,
    CASE
        WHEN per.person_id IS NULL THEN 'MISSING_PERSON'
        WHEN link.decision = 'MATCHED_RADIOLOGY' AND nt.note_id IS NOT NULL THEN 'LINKED_EXISTING'
        WHEN link.decision = 'NEW_NO_PATIENT_RADIOLOGY' AND nt.note_id IS NOT NULL THEN 'LOADED'
        WHEN link.decision IN ('MATCHED_RADIOLOGY', 'NEW_NO_PATIENT_RADIOLOGY') THEN 'ERROR_NOTE_NOT_LOADED'
        ELSE link.decision
    END                                         AS load_status,
    link.match_method                           AS match_method,
    link.candidate_count                        AS candidate_count,
    file.source_version                         AS source_version,
    file.source_sha256                          AS source_sha256,
    file.checksum_status                        AS checksum_status
FROM
    :"etl_schema".cxr_reconciliation link
CROSS JOIN
    :"source_schema".source_file file
LEFT JOIN
    :"cdm_schema".person per ON per.person_source_value = CAST(link.subject_id AS text)
LEFT JOIN
    :"etl_schema".note_id_map map
        ON map.source_table = CASE WHEN link.decision = 'MATCHED_RADIOLOGY' THEN 'radiology' ELSE 'cxr' END
        AND map.source_note_id = CASE WHEN link.decision = 'MATCHED_RADIOLOGY'
            THEN link.radiology_note_id ELSE link.source_note_id END
LEFT JOIN
    :"cdm_schema".note nt ON nt.note_id = map.note_id
WHERE
    file.table_name = 'cxr_reports'
UNION ALL
SELECT
    'ecg_machine'                               AS source_table,
    CAST(raw.subject_id AS integer)             AS subject_id,
    CAST(raw.study_id AS integer)               AS study_id,
    raw.subject_id || '-ECG_MACHINE-' || raw.study_id AS source_note_id,
    CAST(NULL AS text)                          AS external_note_id,
    per.person_id                               AS person_id,
    nt.note_id                                  AS note_id,
    CAST(raw.ecg_time AS timestamp)             AS source_datetime,
    'UNSYNCHRONIZED_ECG_MACHINE_CLOCK'          AS timestamp_basis,
    'MACHINE_INTERPRETATION'                    AS reconciliation_status,
    CASE
        WHEN src.note_id IS NULL THEN 'EMPTY_TEXT'
        WHEN per.person_id IS NULL THEN 'MISSING_PERSON'
        WHEN nt.note_id IS NULL THEN 'ERROR_NOTE_NOT_LOADED'
        ELSE 'LOADED'
    END                                         AS load_status,
    CAST(NULL AS text)                          AS match_method,
    0::bigint                                   AS candidate_count,
    file.source_version                         AS source_version,
    file.source_sha256                          AS source_sha256,
    file.checksum_status                        AS checksum_status
FROM
    :"source_schema".ecg_machine_measurements raw
CROSS JOIN
    :"source_schema".source_file file
LEFT JOIN
    :"source_schema".ecg_machine src
        ON src.note_id = raw.subject_id || '-ECG_MACHINE-' || raw.study_id
LEFT JOIN
    :"cdm_schema".person per ON per.person_source_value = raw.subject_id
LEFT JOIN
    :"etl_schema".note_id_map map ON map.source_table = 'ecg_machine' AND map.source_note_id = src.note_id
LEFT JOIN
    :"cdm_schema".note nt ON nt.note_id = map.note_id
WHERE
    file.table_name = 'ecg_machine_measurements'
UNION ALL
SELECT
    'echo'                                      AS source_table,
    CAST(raw.subject_id AS integer)             AS subject_id,
    CAST(raw.study_id AS integer)               AS study_id,
    raw.subject_id || '-ECHO_STUDY-' || raw.study_id AS source_note_id,
    NULLIF(raw.note_id, '')                     AS external_note_id,
    per.person_id                               AS person_id,
    CAST(NULL AS bigint)                        AS note_id,
    CAST(NULLIF(raw.study_datetime, '') AS timestamp) AS source_datetime,
    'STUDY_TIME_WITH_TEMPORAL_REPORT_LINK'       AS timestamp_basis,
    'LINK_METADATA_ONLY'                       AS reconciliation_status,
    'NO_REPORT_BODY'                            AS load_status,
    'PUBLISHED_TEMPORAL_LINK_WITHIN_2_DAYS'      AS match_method,
    0::bigint                                   AS candidate_count,
    file.source_version                         AS source_version,
    file.source_sha256                          AS source_sha256,
    file.checksum_status                        AS checksum_status
FROM
    :"source_schema".echo_study_list raw
CROSS JOIN
    :"source_schema".source_file file
LEFT JOIN
    :"cdm_schema".person per ON per.person_source_value = raw.subject_id
WHERE
    file.table_name = 'echo_study_list'
;
CREATE UNIQUE INDEX ON lk_modality_note_link(source_table, study_id);
ANALYZE lk_modality_note_link;

-- -------------------------------------------------------------------
-- ext_modality_note_link
-- Every source study remains accounted for, including empty/unavailable
-- text, missing persons, ambiguous overlaps and unverified Echo metadata.
-- -------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS :"etl_schema".modality_note_link (
    source_table text NOT NULL, subject_id integer NOT NULL, study_id integer NOT NULL,
    source_note_id text NOT NULL, external_note_id text, person_id bigint, note_id bigint,
    source_datetime timestamp, timestamp_basis text NOT NULL,
    reconciliation_status text NOT NULL, load_status text NOT NULL,
    match_method text, candidate_count bigint NOT NULL,
    source_version text NOT NULL, source_sha256 text NOT NULL, checksum_status text NOT NULL,
    PRIMARY KEY (source_table, study_id)
);
CREATE INDEX IF NOT EXISTS modality_note_link_note_id_idx ON :"etl_schema".modality_note_link(note_id);
INSERT INTO modality_checks VALUES
    ('all_eligible_notes_loaded', NOT EXISTS (SELECT 1 FROM lk_modality_note_link WHERE load_status = 'ERROR_NOTE_NOT_LOADED')),
    ('source_accounting', (SELECT count(*) FROM lk_modality_note_link) =
        (SELECT count(*) FROM :"source_schema".cxr_reports) +
        (SELECT count(*) FROM :"source_schema".ecg_machine_measurements) +
        (SELECT count(*) FROM :"source_schema".echo_study_list)),
    ('same_person_note_links', NOT EXISTS (
        SELECT 1 FROM lk_modality_note_link link INNER JOIN :"cdm_schema".note nt USING (note_id)
        WHERE nt.person_id IS DISTINCT FROM link.person_id
    )),
    ('no_fabricated_echo_notes', NOT EXISTS (SELECT 1 FROM lk_modality_note_link WHERE source_table = 'echo' AND note_id IS NOT NULL)),
    ('no_changed_or_deleted_existing_link', NOT EXISTS (
        SELECT 1 FROM :"etl_schema".modality_note_link old LEFT JOIN lk_modality_note_link new
        USING (source_table, study_id)
        WHERE old.note_id IS NOT NULL AND old.note_id IS DISTINCT FROM new.note_id
    ));
DELETE FROM :"etl_schema".modality_note_link WHERE source_table IN ('cxr', 'ecg_machine', 'echo');
INSERT INTO :"etl_schema".modality_note_link SELECT * FROM lk_modality_note_link;
INSERT INTO modality_checks VALUES
    ('all_link_fields_preserved', NOT EXISTS (
        (SELECT * FROM lk_modality_note_link EXCEPT ALL SELECT * FROM :"etl_schema".modality_note_link
         WHERE source_table IN ('cxr', 'ecg_machine', 'echo'))
        UNION ALL
        (SELECT * FROM :"etl_schema".modality_note_link WHERE source_table IN ('cxr', 'ecg_machine', 'echo')
         EXCEPT ALL SELECT * FROM lk_modality_note_link)
    ));
COMMIT;

SELECT source_table, load_status, checksum_status, count(*) AS source_rows
FROM :"etl_schema".modality_note_link
GROUP BY source_table, load_status, checksum_status
ORDER BY source_table, load_status, checksum_status;
