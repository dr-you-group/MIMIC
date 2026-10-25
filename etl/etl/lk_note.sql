-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- NOTE-only mapping; same rules as etl/postgres/note.sql.
-- Temporary work tables only. Run in one workflow_note.conf script.
-- -------------------------------------------------------------------

CREATE TEMPORARY TABLE src_note AS
SELECT
  CAST('@note_source_table' AS STRING) AS source_table,
  s.source_note_id AS source_note_id,
  note_key(
    CASE '@note_source_table'
      WHEN 'cxr'
      THEN 'mimic-cxr'
      WHEN 'ecg_machine'
      THEN 'mimic-iv-ecg'
      ELSE 'mimic-iv-note'
    END,
    '@note_source_table',
    s.source_note_id
  ) AS note_id,
  s.subject_id,
  s.hadm_id,
  s.note_type,
  s.charttime,
  s.storetime,
  s.study_id,
  s.reuse_note_id,
  s.timestamp_basis,
  s.component_kind,
  s.match_method,
  LOWER(TO_HEX(SHA256(s.note_text))) AS text_sha256,
  CAST(BYTE_LENGTH(s.note_text) AS INT64) AS text_bytes,
  REGEXP_CONTAINS(s.note_text, r'[^ \t\r\n]') AS has_text
FROM src_note_text AS s;

ASSERT ((
  SELECT
    COUNT(*) = 0
  FROM src_note
  WHERE
    source_note_id IS NULL OR source_note_id = '' OR subject_id IS NULL
)) AS 'source_identity';

/* ------------------------------------------------------------------- */
/* lk_note_type / lk_note_person / lk_note_visit */
/* Admission keys are numeric. subject|date pseudo-visits are excluded. */
/* An explicit but unmatched hadm_id never falls back to another visit. */
/* ------------------------------------------------------------------- */
CREATE TEMPORARY TABLE lk_note_type (
  source_table STRING,
  note_type STRING,
  loinc_code STRING,
  note_title STRING
);

INSERT INTO lk_note_type
VALUES
  ('discharge', 'DS', '18842-5', 'Discharge summary'),
  ('discharge', 'AD', '18842-5', 'Discharge summary addendum'),
  ('radiology', 'RR', '68604-8', 'Radiology report'),
  ('radiology', 'AR', '68604-8', 'Radiology report addendum'),
  ('cxr', 'CXR', '68604-8', 'Chest radiograph report'),
  (
    'cxr',
    'CXR_SUPPLEMENT',
    '68604-8',
    'Chest radiograph supplementary report (preliminary/addendum)'
  ),
  (
    'cxr',
    'CXR_VARIANT',
    '68604-8',
    'Chest radiograph source report (possible duplicate or variant)'
  ),
  ('ecg_machine', 'ECG_MACHINE', '75425-9', 'ECG machine interpretation');

CREATE TEMPORARY TABLE lk_note_person AS
SELECT
  s.*,
  p.person_id,
  old.note_id IS NOT NULL AS already_loaded,
  old.note_date AS existing_note_date,
  old.visit_occurrence_id AS existing_visit_id,
  old.visit_detail_id AS existing_visit_detail_id,
  reused.note_date AS reused_note_date,
  reused.note_datetime AS reused_note_datetime,
  reused.visit_occurrence_id AS reused_visit_id
FROM src_note AS s
LEFT JOIN @etl_project.@etl_dataset.cdm_person AS p
  ON p.person_source_value = CAST(s.subject_id AS STRING)
LEFT JOIN @etl_project.@etl_dataset.cdm_note AS old
  ON old.note_id = s.note_id
LEFT JOIN @etl_project.@etl_dataset.cdm_note AS reused
  ON reused.note_id = s.reuse_note_id;

CREATE TEMPORARY TABLE lk_note_visit AS
SELECT
  v.*
FROM @etl_project.@etl_dataset.cdm_visit_occurrence AS v
WHERE
  REGEXP_CONTAINS(SPLIT(v.visit_source_value, '|')[SAFE_OFFSET(1)], '^[0-9]+$');

CREATE TEMPORARY TABLE lk_note_visit_match AS
SELECT
  s.note_id,
  COUNT(v.visit_occurrence_id) AS candidate_count,
  CASE WHEN COUNT(v.visit_occurrence_id) = 1 THEN MIN(v.visit_occurrence_id) END AS visit_occurrence_id
FROM lk_note_person AS s
LEFT JOIN lk_note_visit AS v
  ON v.person_id = s.person_id
  AND (
    (
      s.hadm_id IS NOT NULL
      AND v.visit_source_value = CAST(s.subject_id AS STRING) || '|' || CAST(s.hadm_id AS STRING)
    )
    OR (
      s.hadm_id IS NULL
      AND s.timestamp_basis = 'DOCUMENT_CHARTTIME'
      AND s.charttime BETWEEN v.visit_start_datetime AND v.visit_end_datetime
    )
  )
GROUP BY
  s.note_id;

CREATE TEMPORARY TABLE lk_note_mapped AS
SELECT
  s.*,
  c.concept_id AS note_class_concept_id,
  m.note_title AS note_title,
  CASE
    WHEN s.source_table = 'cxr'
    THEN COALESCE(s.existing_visit_id, s.reused_visit_id)
    WHEN s.hadm_id IS NOT NULL
    THEN v.visit_occurrence_id
    WHEN s.existing_visit_id IS NOT NULL
    THEN s.existing_visit_id
    ELSE v.visit_occurrence_id
  END AS visit_occurrence_id,
  v.candidate_count AS visit_candidate_count,
  CASE
    WHEN s.person_id IS NULL
    THEN 'MISSING_PERSON'
    WHEN NOT s.has_text IS TRUE
    THEN 'EMPTY_TEXT'
    WHEN s.already_loaded AND s.source_table = 'cxr'
    THEN 'LEGACY_DATE_PROXY_RETAINED'
    WHEN s.reuse_note_id IS NOT NULL
    THEN 'REUSED_EXISTING_RADIOLOGY'
    WHEN s.charttime IS NULL
    THEN 'MISSING_DOCUMENT_DATE'
    ELSE 'LOADED'
  END AS load_status,
  CASE
    WHEN s.source_table = 'cxr' AND s.reuse_note_id IS NOT NULL
    THEN 'REUSED_RADIOLOGY_VISIT'
    WHEN s.source_table = 'cxr'
    THEN 'NO_DOCUMENT_TIME'
    WHEN s.hadm_id IS NOT NULL AND v.candidate_count = 1
    THEN 'EXPLICIT_HADM'
    WHEN s.hadm_id IS NOT NULL
    THEN 'UNMATCHED_EXPLICIT_HADM'
    WHEN s.existing_visit_id IS NOT NULL
    THEN 'PRESERVED_EXISTING_VISIT'
    WHEN s.timestamp_basis = 'DEVICE_CLOCK_UNSYNCHRONIZED'
    THEN 'UNSYNCHRONIZED_DEVICE_CLOCK'
    WHEN v.candidate_count = 1
    THEN 'UNIQUE_ADMISSION_INTERVAL'
    WHEN v.candidate_count > 1
    THEN 'AMBIGUOUS_ADMISSION_INTERVAL'
    ELSE 'NO_ADMISSION_INTERVAL'
  END AS visit_status
FROM lk_note_person AS s
LEFT JOIN lk_note_type AS m
  ON m.source_table = s.source_table AND m.note_type = s.note_type
LEFT JOIN @etl_project.@etl_dataset.voc_concept AS c
  ON c.vocabulary_id = 'LOINC'
  AND c.concept_code = m.loinc_code
  AND c.domain_id = 'Note'
  AND c.standard_concept = 'S'
  AND c.invalid_reason IS NULL
  AND CURRENT_DATE BETWEEN c.valid_start_date AND c.valid_end_date
INNER JOIN lk_note_visit_match AS v
  USING (note_id);

ASSERT ((
  SELECT
    COUNT(*) = 0
  FROM @etl_project.@etl_dataset.cdm_note AS n
  WHERE
    n.note_source_value IN (
      SELECT
        note_type
      FROM lk_note_type
      WHERE
        source_table = '@note_source_table'
    )
    AND NOT EXISTS(
      SELECT
        1
      FROM src_note AS s
      WHERE
        s.note_id = n.note_id
    )
)) AS 'no_unaccounted_existing_source';

ASSERT ((
  SELECT
    COUNT(*) = 0
  FROM lk_note_mapped
  WHERE
    note_class_concept_id IS NULL
)) AS 'document_class_mapping';

ASSERT ((
  SELECT
    COUNT(*) = 3
  FROM @etl_project.@etl_dataset.voc_concept
  WHERE
    (
      (
        concept_id = 32817 AND domain_id = 'Type Concept'
      )
      OR (
        concept_id = 32678 AND domain_id = 'Metadata'
      )
      OR (
        concept_id = 4180186 AND domain_id = 'Language'
      )
    )
    AND standard_concept = 'S'
    AND invalid_reason IS NULL
    AND CURRENT_DATE BETWEEN valid_start_date AND valid_end_date
)) AS 'constant_concepts';

ASSERT ((
  SELECT
    COUNT(*) = 0
  FROM lk_note_mapped
  WHERE
    hadm_id IS NOT NULL AND visit_candidate_count > 1
)) AS 'unambiguous_explicit_hadm';

ASSERT ((
  SELECT
    COUNT(*) = 0
  FROM lk_note_mapped AS s
  INNER JOIN @etl_project.@etl_dataset.cdm_note AS n
    USING (note_id)
  WHERE
    n.person_id IS DISTINCT FROM s.person_id
    OR n.note_source_value IS DISTINCT FROM s.note_type
    OR LOWER(TO_HEX(SHA256(n.note_text))) IS DISTINCT FROM s.text_sha256
    OR (
      s.source_table <> 'cxr'
      AND (
        n.note_date IS DISTINCT FROM CAST(s.charttime AS DATE)
        OR n.note_datetime IS DISTINCT FROM s.charttime
      )
    )
    OR NOT s.has_text IS TRUE
)) AS 'existing_identity_and_text';

ASSERT ((
  SELECT
    COUNT(*) = 0
  FROM lk_note_mapped AS s
  LEFT JOIN @etl_project.@etl_dataset.cdm_visit_occurrence AS v
    ON v.visit_occurrence_id = s.existing_visit_id
  WHERE
    s.existing_visit_id IS NOT NULL
    AND (
      v.person_id IS DISTINCT FROM s.person_id
      OR (
        s.hadm_id IS NOT NULL
        AND s.existing_visit_id IS DISTINCT FROM s.visit_occurrence_id
      )
    )
)) AS 'existing_visit_consistent';

ASSERT ((
  SELECT
    COUNT(*) = 0
  FROM lk_note_mapped AS s
  LEFT JOIN @etl_project.@etl_dataset.cdm_note AS n
    ON n.note_id = s.reuse_note_id
  WHERE
    s.reuse_note_id IS NOT NULL AND n.person_id IS DISTINCT FROM s.person_id
)) AS 'reused_report_same_person';

ASSERT (SELECT COUNT(*) = COUNT(DISTINCT source_note_id) AND COUNT(*) = COUNT(DISTINCT note_id)
    FROM src_note) AS 'unique_source_note_identity';
ASSERT (SELECT COUNT(*) FROM lk_note_person) = (SELECT COUNT(*) FROM src_note) AS 'unique_person_mapping';
ASSERT (SELECT COUNT(*) FROM lk_note_mapped) = (SELECT COUNT(*) FROM src_note) AS 'unique_document_mapping';
ASSERT (SELECT COUNT(*) = COUNT(DISTINCT note_id) FROM @etl_project.@etl_dataset.cdm_note) AS 'unique_target_note_identity';
