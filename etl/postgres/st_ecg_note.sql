-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion: machine ECG text, one document per study
-- All 18 positions (including blanks) are retained. No waveform import.
-- Device time is not trusted for encounter matching. Called by note.sql.
-- -------------------------------------------------------------------
INSERT INTO note_checks VALUES ('verified_ecg_source', (
    SELECT count(*) = 1 AND bool_and(checksum_status = 'VERIFIED')
    FROM :"source_schema".source_file WHERE table_name = 'ecg_machine_measurements'));
CREATE TEMP VIEW src_note_text AS
SELECT
    subject_id || '-ECG_MACHINE-' || study_id     AS source_note_id,
    subject_id::integer                         AS subject_id,
    NULL::integer                              AS hadm_id,
    'ECG_MACHINE'::text                         AS note_type,
    nullif(ecg_time,'')::timestamp               AS charttime,
    NULL::timestamp                            AS storetime,
    array_to_string(ARRAY[
        coalesce(report_0,''), coalesce(report_1,''), coalesce(report_2,''),
        coalesce(report_3,''), coalesce(report_4,''), coalesce(report_5,''),
        coalesce(report_6,''), coalesce(report_7,''), coalesce(report_8,''),
        coalesce(report_9,''), coalesce(report_10,''), coalesce(report_11,''),
        coalesce(report_12,''), coalesce(report_13,''), coalesce(report_14,''),
        coalesce(report_15,''), coalesce(report_16,''), coalesce(report_17,'')
    ], E'\n')                                   AS note_text,
    study_id::text                              AS study_id,
    NULL::bigint                                AS reuse_note_id,
    'DEVICE_CLOCK_UNSYNCHRONIZED'::text           AS timestamp_basis,
    'REPORT'::text                              AS component_kind,
    NULL::text                                  AS match_method
FROM :"source_schema".ecg_machine_measurements;
