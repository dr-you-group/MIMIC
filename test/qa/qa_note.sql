-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- NOTE-only mapping; same rules as etl/postgres/note.sql.
-- Temporary work tables only. Run in one workflow_note.conf script.
-- -------------------------------------------------------------------

ASSERT ((
  SELECT
    COUNT(*) = 0
  FROM lk_note_mapped AS s
  LEFT JOIN @etl_project.@etl_dataset.cdm_note AS n
    USING (note_id)
  WHERE
    s.load_status IN ('LOADED', 'LEGACY_DATE_PROXY_RETAINED')
    AND (
      n.note_id IS NULL
      OR n.person_id IS DISTINCT FROM s.person_id
      OR n.note_source_value IS DISTINCT FROM s.note_type
      OR LOWER(TO_HEX(SHA256(n.note_text))) IS DISTINCT FROM s.text_sha256
      OR n.visit_occurrence_id IS DISTINCT FROM s.visit_occurrence_id
      OR (
        s.load_status = 'LOADED'
        AND (
          n.note_date IS DISTINCT FROM CAST(s.charttime AS DATE)
          OR n.note_datetime IS DISTINCT FROM s.charttime
          OR n.note_type_concept_id <> 32817
          OR n.note_class_concept_id IS DISTINCT FROM s.note_class_concept_id
          OR n.note_title IS DISTINCT FROM s.note_title
          OR n.encoding_concept_id <> 32678
          OR n.language_concept_id <> 4180186
          OR n.provider_id IS NOT NULL
          OR n.visit_detail_id IS DISTINCT FROM s.existing_visit_detail_id
        )
      )
    )
)) AS 'loaded_rows_match_source';

COMMIT TRANSACTION;

-- Capture these child-job query results outside the CDM for source identity,
-- withheld rows, legacy date exceptions and every changed visit.
SELECT s.source_table,s.source_note_id,s.study_id,s.component_kind,s.subject_id,s.hadm_id,
    s.note_id,s.reuse_note_id,s.person_id,s.charttime,s.storetime,s.timestamp_basis,
    s.text_sha256,s.text_bytes,s.load_status,s.visit_status,s.visit_candidate_count,
    s.existing_visit_id,s.visit_occurrence_id,s.existing_note_date,s.reused_note_date,s.reused_note_datetime,s.match_method,c.action,
    CASE WHEN s.load_status = 'REUSED_EXISTING_RADIOLOGY' THEN s.reuse_note_id
         WHEN s.load_status IN ('LOADED','LEGACY_DATE_PROXY_RETAINED') THEN s.note_id END AS target_note_id
FROM lk_note_mapped s LEFT JOIN note_changes c USING (note_id)
ORDER BY s.source_note_id;
SELECT source_table,load_status,visit_status,COUNT(*) AS rows
FROM lk_note_mapped GROUP BY 1,2,3 ORDER BY 1,2,3;
