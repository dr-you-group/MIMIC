-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- Final NOTE conventions audit across all source modalities
--
-- Run after all source-specific note.sql checks and modality extensions.
-- Targets the shared 14 NOTE fields in CDM 5.3; does not migrate Core to 5.4.
-- Source-specific ETL additionally verifies raw text and every detail row.
-- -------------------------------------------------------------------

\set ON_ERROR_STOP on
\if :{?cdm_schema}
\else
\set cdm_schema mimiciv
\endif
\if :{?etl_schema}
\else
\set etl_schema mimiciv_note_etl
\endif

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TimeZone = 'UTC';
-- Aggregate the whole population. Do not rely on a sample or on the
-- presence of declared database constraints in an imported CDM.
WITH lk_note_convention_counts AS MATERIALIZED (
SELECT
    count(*) FILTER (WHERE n.note_id IS NULL OR n.person_id IS NULL OR n.note_date IS NULL
        OR n.note_type_concept_id IS NULL OR n.note_class_concept_id IS NULL
        OR n.note_text IS NULL OR n.encoding_concept_id IS NULL OR n.language_concept_id IS NULL)
        AS missing_required_fields,
    count(*) FILTER (WHERE btrim(n.note_text, E' \t\r\n') = '') AS empty_note_text,
    count(*) FILTER (WHERE p.person_id IS NULL) AS person_fk,
    count(*) FILTER (WHERE n.visit_occurrence_id IS NOT NULL
        AND (v.visit_occurrence_id IS NULL OR v.person_id IS DISTINCT FROM n.person_id)) AS visit_fk_or_person,
    count(*) FILTER (WHERE n.visit_detail_id IS NOT NULL
        AND (vd.visit_detail_id IS NULL OR vd.person_id IS DISTINCT FROM n.person_id)) AS visit_detail_fk_or_person,
    count(*) FILTER (WHERE n.provider_id IS NOT NULL AND pr.provider_id IS NULL) AS provider_fk,
    count(*) FILTER (WHERE doc.concept_id IS NULL) AS document_concept,
    count(*) FILTER (WHERE typ.concept_id IS NULL) AS type_concept,
    count(*) FILTER (WHERE enc.concept_id IS NULL) AS encoding_concept,
    count(*) FILTER (WHERE lang.concept_id IS NULL) AS language_concept,
    count(*) FILTER (WHERE length(n.note_title) > 250 OR length(n.note_source_value) > 50) AS field_width,
    count(*) FILTER (WHERE n.note_datetime IS NOT NULL AND n.note_date <> n.note_datetime::date) AS date_datetime,
    count(*) FILTER (WHERE n.note_source_value IN ('CXR','CXR_SUPPLEMENT','CXR_VARIANT')
        AND n.note_datetime IS NOT NULL) AS invented_cxr_document_time,
    count(*) FILTER (WHERE n.note_source_value IN ('CXR','CXR_SUPPLEMENT','CXR_VARIANT','ECG_MACHINE')
        AND (n.visit_occurrence_id IS NOT NULL OR n.visit_detail_id IS NOT NULL OR n.provider_id IS NOT NULL)) AS invented_modality_link
FROM :"cdm_schema".note n
LEFT JOIN :"cdm_schema".person p USING (person_id)
LEFT JOIN :"cdm_schema".visit_occurrence v ON v.visit_occurrence_id = n.visit_occurrence_id
LEFT JOIN :"cdm_schema".visit_detail vd ON vd.visit_detail_id = n.visit_detail_id
LEFT JOIN :"cdm_schema".provider pr ON pr.provider_id = n.provider_id
LEFT JOIN :"cdm_schema".concept doc ON doc.concept_id = n.note_class_concept_id
    AND doc.domain_id = 'Note' AND doc.vocabulary_id = 'LOINC'
    AND doc.standard_concept = 'S' AND doc.invalid_reason IS NULL
    AND CURRENT_DATE BETWEEN doc.valid_start_date AND doc.valid_end_date
LEFT JOIN :"cdm_schema".concept typ ON typ.concept_id = n.note_type_concept_id
    AND typ.domain_id = 'Type Concept' AND typ.standard_concept = 'S' AND typ.invalid_reason IS NULL
    AND CURRENT_DATE BETWEEN typ.valid_start_date AND typ.valid_end_date
LEFT JOIN :"cdm_schema".concept enc ON enc.concept_id = n.encoding_concept_id
    AND enc.concept_id = 32678 AND enc.standard_concept = 'S' AND enc.invalid_reason IS NULL
    AND CURRENT_DATE BETWEEN enc.valid_start_date AND enc.valid_end_date
LEFT JOIN :"cdm_schema".concept lang ON lang.concept_id = n.language_concept_id
    AND lang.domain_id = 'Language' AND lang.standard_concept = 'S' AND lang.invalid_reason IS NULL
    AND CURRENT_DATE BETWEEN lang.valid_start_date AND lang.valid_end_date
), note_convention_checks(check_name, violations) AS MATERIALIZED (
SELECT key, value::bigint FROM lk_note_convention_counts c, jsonb_each_text(to_jsonb(c))
UNION ALL
VALUES
    ('duplicate_note_id', (SELECT count(*) - count(DISTINCT note_id) FROM :"cdm_schema".note)),
    ('unowned_note', (SELECT count(*) FROM :"cdm_schema".note n
        LEFT JOIN :"etl_schema".note_id_map m USING (note_id) WHERE m.note_id IS NULL)),
    ('source_person_and_visit_mapping', (
        SELECT count(*) FROM :"etl_schema".note_metadata m
        INNER JOIN :"cdm_schema".note n USING (note_id)
        INNER JOIN :"cdm_schema".person p ON p.person_id = n.person_id
        LEFT JOIN :"cdm_schema".visit_occurrence v ON v.visit_occurrence_id = n.visit_occurrence_id
        WHERE p.person_source_value IS DISTINCT FROM CAST(m.subject_id AS text)
          OR n.person_id IS DISTINCT FROM m.person_id
          OR n.visit_occurrence_id IS DISTINCT FROM m.visit_occurrence_id
          OR (n.visit_occurrence_id IS NOT NULL AND (m.hadm_id IS NULL
              OR v.visit_source_value IS DISTINCT FROM CAST(m.subject_id AS text) || '|' || CAST(m.hadm_id AS text)))
    )),
    ('eligible_cxr_unloaded', (SELECT count(*) FROM :"etl_schema".modality_note_link
        WHERE source_table = 'cxr' AND person_id IS NOT NULL AND note_id IS NULL)),
    ('eligible_ecg_unloaded', (SELECT count(*) FROM :"etl_schema".modality_note_link
        WHERE source_table = 'ecg_machine' AND person_id IS NOT NULL AND load_status <> 'EMPTY_TEXT' AND note_id IS NULL)),
    ('fabricated_echo_note', (SELECT count(*) FROM :"etl_schema".modality_note_link
        WHERE source_table = 'echo' AND note_id IS NOT NULL)),
    ('variant_flag_missing', (
        SELECT count(*) FROM :"cdm_schema".note n WHERE n.note_source_value = 'CXR_VARIANT'
          AND NOT EXISTS (SELECT 1 FROM :"etl_schema".note_detail d WHERE d.note_id = n.note_id
              AND d.field_name = 'duplicate_status' AND d.field_value = 'UNRESOLVED_POSSIBLE_DUPLICATE')
    ))
)

SELECT jsonb_build_object(
    'status', CASE WHEN (SELECT bool_and(violations = 0) FROM note_convention_checks) THEN 'PASS' ELSE 'FAIL' END,
    'checks', (SELECT jsonb_object_agg(check_name, violations) FROM note_convention_checks),
    'note_counts', (SELECT jsonb_object_agg(note_source_value, rows)
        FROM (SELECT note_source_value, count(*) AS rows FROM :"cdm_schema".note GROUP BY 1) s),
    'source_accounting', (SELECT jsonb_agg(s) FROM (
        SELECT source_table, load_status, visit_status, count(*) AS rows
        FROM :"etl_schema".note_metadata GROUP BY 1,2,3 ORDER BY 1,2,3
    ) s)
)::text AS note_convention_report,
    (SELECT bool_and(violations = 0) FROM note_convention_checks) AS note_convention_pass
\gset
\echo NOTE_CONVENTION_REPORT :note_convention_report
\if :note_convention_pass
COMMIT;
\else
ROLLBACK;
\echo NOTE convention violations are listed in NOTE_CONVENTION_REPORT.
SELECT 1 / 0 AS note_convention_check_failed;
\endif
