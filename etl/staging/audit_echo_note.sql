-- Echo references are not narrative reports. No NOTE or other CDM DML.
ASSERT (SELECT COUNT(*) = 1 FROM @note_project.@note_dataset.source_file
    WHERE table_name = 'echo_study_list') AS 'echo_source_provenance_present';
SELECT
    e.subject_id, e.study_id, e.note_id AS source_note_id, e.note_charttime,
    p.person_id, 'NO_REPORT_BODY' AS load_status, f.checksum_status
FROM @note_project.@note_dataset.echo_study_list e
LEFT JOIN @etl_project.@etl_dataset.cdm_person p
    ON p.person_source_value = CAST(e.subject_id AS STRING)
CROSS JOIN (
    SELECT checksum_status FROM @note_project.@note_dataset.source_file
    WHERE table_name = 'echo_study_list'
) f
ORDER BY e.study_id;
