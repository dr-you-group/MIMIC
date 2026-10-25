-- Echo supplies study / measurement / note references, not narrative text.
-- Retain the source and export the reason for withholding each study.
-- A checksum mismatch independently quarantines this release.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
\if :{?audit_file}
COPY (SELECT e.subject_id,e.study_id,e.note_id AS source_note_id,e.note_charttime,
    p.person_id,'NO_REPORT_BODY' AS load_status,f.checksum_status
    FROM :"source_schema".echo_study_list e
    LEFT JOIN :"cdm_schema".person p ON p.person_source_value = e.subject_id
    CROSS JOIN (SELECT checksum_status FROM :"source_schema".source_file WHERE table_name = 'echo_study_list') f
    ORDER BY e.study_id)
TO STDOUT WITH (FORMAT CSV, HEADER) \g :audit_file
\endif
SELECT jsonb_build_object('source_table','echo','source_rows',count(*),
    'inserted_rows',0,'visit_updated_rows',0,'changed_note_rows',0,
    'missing_person_rows',count(*) FILTER (WHERE p.person_id IS NULL),
    'load_status',jsonb_build_object('NO_REPORT_BODY',count(*)),
    'checksum_status',(SELECT checksum_status FROM :"source_schema".source_file WHERE table_name = 'echo_study_list')
)::text AS note_etl_report
FROM :"source_schema".echo_study_list e
LEFT JOIN :"cdm_schema".person p ON p.person_source_value = e.subject_id \gset
COMMIT;
\echo NOTE_ETL_REPORT :note_etl_report
