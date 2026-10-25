-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion
-- NOTE-only mapping; same rules as etl/postgres/note.sql.
-- Temporary work tables only. Run in one workflow_note.conf script.
-- -------------------------------------------------------------------

IF '@note_source_table' = 'cxr' THEN
ASSERT (SELECT COUNT(*) = 2 AND LOGICAL_AND(checksum_status = 'VERIFIED') FROM @note_project.@note_dataset.source_file WHERE table_name IN ('cxr_reports','cxr_metadata')) AS 'verified_cxr_sources';
CREATE TEMPORARY TABLE src_cxr_time AS
SELECT
  CAST(subject_id AS INT64) AS subject_id,
  CAST(study_id AS INT64) AS study_id,
  CASE
    WHEN studydate <> '' AND studytime <> ''
    THEN PARSE_DATETIME(
      '%Y%m%d%H%M%E*S',
      CONCAT(
        COALESCE(studydate, ''),
        COALESCE(LPAD(SPLIT(studytime, '.')[SAFE_OFFSET(0)], 6, '0'), ''),
        COALESCE(
          CASE
            WHEN INSTR(studytime, '.') > 0
            THEN CONCAT('.', COALESCE(SPLIT(studytime, '.')[SAFE_OFFSET(1)], ''))
            ELSE ''
          END,
          ''
        )
      )
    )
  END AS acquisition_time
FROM @note_project.@note_dataset.cxr_metadata;

CREATE TEMPORARY TABLE src_cxr_study AS
WITH src AS (
  SELECT
    raw.*,
    COALESCE(REGEXP_EXTRACT(raw.text, '(?s)^(.*\\n)[ \\t]*FINAL REPORT[ \\t]*\\r?\\n'), '') AS prefix
  FROM @note_project.@note_dataset.cxr_reports AS raw
)
SELECT
  CAST(src.subject_id AS INT64) AS subject_id,
  CAST(src.study_id AS INT64) AS study_id,
  src.subject_id || '-CXR-' || src.study_id AS source_note_id,
  LENGTH(src.text) AS text_length,
  LENGTH(src.prefix) AS prefix_length,
  TO_HEX(MD5(note_normalize(src.text))) AS full_hash,
  TO_HEX(MD5(note_normalize(SUBSTRING(src.text, LENGTH(src.prefix) + 1)))) AS final_hash,
  TO_HEX(MD5(note_addendum(src.prefix))) AS prefix_hash,
  tm.acquisition_start,
  tm.acquisition_end,
  tm.missing_times
FROM src
LEFT JOIN (
  SELECT
    subject_id,
    study_id,
    MIN(acquisition_time) AS acquisition_start,
    MAX(acquisition_time) AS acquisition_end,
    COUNT(*) - COUNT(acquisition_time) AS missing_times
  FROM src_cxr_time
  GROUP BY
    1,
    2
) AS tm
  ON tm.subject_id = CAST(src.subject_id AS INT64)
  AND tm.study_id = CAST(src.study_id AS INT64);

CREATE TEMPORARY TABLE src_cxr_radiology AS
SELECT
  r.note_id AS source_note_id,
  r.subject_id,
  r.note_type,
  CAST(r.charttime AS DATETIME) AS charttime,
  n.note_id,
  TO_HEX(MD5(note_normalize(r.text))) AS full_hash,
  CASE WHEN r.note_type = 'AR' THEN TO_HEX(MD5(note_addendum(r.text))) END AS addendum_hash
FROM @radiology_project.@radiology_dataset.radiology AS r
INNER JOIN @etl_project.@etl_dataset.cdm_note AS n
  ON n.note_id = note_key('mimic-iv-note', 'radiology', r.note_id)
  AND n.note_text = r.text
WHERE
  EXISTS(
    SELECT
      1
    FROM src_cxr_study AS s
    WHERE
      s.subject_id = r.subject_id
  );

/* Hashes only accelerate candidate search. Compare complete normalized */
/* strings again; collisions cannot establish document identity. */
CREATE TEMPORARY TABLE lk_cxr_pair AS
SELECT
  s.study_id,
  r.source_note_id AS radiology_note_id,
  r.note_id,
  CASE WHEN s.full_hash = r.full_hash THEN 1 ELSE 2 END AS priority
FROM src_cxr_study AS s
INNER JOIN src_cxr_radiology AS r
  ON r.subject_id = s.subject_id
  AND r.note_type = 'RR'
  AND r.full_hash IN (s.full_hash, s.final_hash)
  AND r.charttime BETWEEN DATETIME_SUB(s.acquisition_start, INTERVAL 48 HOUR) AND DATETIME_ADD(s.acquisition_end, INTERVAL 48 HOUR)
INNER JOIN @note_project.@note_dataset.cxr_reports AS raw
  ON CAST(raw.study_id AS INT64) = s.study_id
INNER JOIN @radiology_project.@radiology_dataset.radiology AS rad
  ON rad.note_id = r.source_note_id
WHERE
  s.missing_times = 0
  AND DATETIME_DIFF(s.acquisition_end, s.acquisition_start, MICROSECOND) <= 21600000000
  AND (
    note_normalize(raw.text) = note_normalize(rad.text)
    OR (
      s.prefix_length > 0
      AND note_normalize(SUBSTRING(raw.text, s.prefix_length + 1)) = note_normalize(rad.text)
    )
  );

CREATE TEMPORARY TABLE lk_cxr_match AS
WITH best AS (
  SELECT
    p.*,
    MIN(priority) OVER (PARTITION BY study_id) AS best_priority
  FROM lk_cxr_pair AS p
), unique_pair AS (
  SELECT
    b.*,
    COUNT(*) OVER (PARTITION BY study_id) AS study_candidates,
    COUNT(*) OVER (PARTITION BY radiology_note_id) AS report_candidates
  FROM best AS b
  WHERE
    priority = best_priority
)
SELECT
  *
FROM unique_pair
WHERE
  study_candidates = 1 AND report_candidates = 1;

CREATE TEMPORARY TABLE src_cxr_parent AS
SELECT
  note_id AS parent_note_id,
  field_value AS addendum_note_id
FROM @radiology_project.@radiology_dataset.radiology_detail
WHERE
  field_name = 'addendum_note_id'
UNION DISTINCT
SELECT
  field_value,
  note_id
FROM @radiology_project.@radiology_dataset.radiology_detail
WHERE
  field_name = 'parent_note_id';

CREATE TEMPORARY TABLE lk_cxr_addendum AS
WITH candidates AS (
  SELECT
    s.study_id,
    r.note_id,
    COUNT(*) OVER (PARTITION BY s.study_id) AS study_candidates,
    COUNT(*) OVER (PARTITION BY r.note_id) AS report_candidates
  FROM src_cxr_study AS s
  INNER JOIN lk_cxr_match AS m
    USING (study_id)
  INNER JOIN src_cxr_parent AS p
    ON p.parent_note_id = m.radiology_note_id
  INNER JOIN src_cxr_radiology AS r
    ON r.source_note_id = p.addendum_note_id
    AND r.subject_id = s.subject_id
    AND r.note_type = 'AR'
    AND r.addendum_hash = s.prefix_hash
  INNER JOIN @note_project.@note_dataset.cxr_reports AS raw
    ON CAST(raw.study_id AS INT64) = s.study_id
  INNER JOIN @radiology_project.@radiology_dataset.radiology AS rad
    ON rad.note_id = r.source_note_id
  WHERE
    m.priority = 2
    AND s.prefix_length > 0
    AND note_addendum(SUBSTRING(raw.text, 1, s.prefix_length)) = note_addendum(rad.text)
)
SELECT
  study_id,
  note_id
FROM candidates
WHERE
  study_candidates = 1 AND report_candidates = 1;

/* Existing whole/prefix documents retain their IDs and source text even */
/* when the stricter reconciliation no longer confirms a former alias. */
CREATE TEMPORARY TABLE lk_cxr_component AS
SELECT
  s.*,
  s.source_note_id AS component_id,
  CAST('REPORT' AS STRING) AS component_kind,
  CASE WHEN old.note_id IS NULL AND m.priority = 2 THEN s.prefix_length + 1 ELSE 1 END AS source_start,
  CASE
    WHEN old.note_id IS NULL AND m.priority = 2
    THEN s.text_length - s.prefix_length
    ELSE s.text_length
  END AS source_length,
  COALESCE(
    old.note_source_value,
    CASE
      WHEN NOT EXISTS(
        SELECT
          1
        FROM src_cxr_radiology AS r
        WHERE
          r.subject_id = s.subject_id
      )
      THEN 'CXR'
      ELSE 'CXR_VARIANT'
    END
  ) AS note_type,
  CASE WHEN old.note_id IS NULL THEN m.note_id END AS reuse_note_id,
  CASE
    WHEN m.priority = 1
    THEN 'FULL_DOCUMENT_EQUAL_WITHIN_48H'
    WHEN m.priority = 2
    THEN 'FINAL_SECTION_EQUAL_WITHIN_48H'
  END AS match_method
FROM src_cxr_study AS s
LEFT JOIN lk_cxr_match AS m
  USING (study_id)
LEFT JOIN @etl_project.@etl_dataset.cdm_note AS old
  ON old.note_id = note_key('mimic-cxr', 'cxr', s.source_note_id)
UNION ALL
SELECT
  s.*,
  s.source_note_id || '-PREFIX',
  'PREFIX',
  1,
  s.prefix_length,
  'CXR_SUPPLEMENT',
  CASE WHEN old.note_id IS NULL THEN ar.note_id END,
  CASE
    WHEN ar.note_id IS NOT NULL
    THEN 'EXACT_PARENT_LINKED_ADDENDUM'
    ELSE 'PRESERVED_SOURCE_PREFIX'
  END
FROM src_cxr_study AS s
LEFT JOIN lk_cxr_match AS m
  USING (study_id)
LEFT JOIN lk_cxr_addendum AS ar
  USING (study_id)
LEFT JOIN @etl_project.@etl_dataset.cdm_note AS old
  ON old.note_id = note_key('mimic-cxr', 'cxr', s.source_note_id || '-PREFIX')
/* A pre-existing whole document already preserves its prefix. Keep an */
/* old standalone prefix, but do not create a redundant new component. */
WHERE
  s.prefix_length > 0
  AND (
    old.note_id IS NOT NULL
    OR (
      m.priority = 2
      AND NOT EXISTS(
        SELECT
          1
        FROM @etl_project.@etl_dataset.cdm_note AS whole
        WHERE
          whole.note_id = note_key('mimic-cxr', 'cxr', s.source_note_id)
      )
    )
  );

CREATE TEMPORARY TABLE src_note_text AS
SELECT
  c.component_id AS source_note_id,
  c.subject_id,
  CAST(NULL AS INT64) AS hadm_id,
  c.note_type,
  CAST(NULL AS DATETIME) AS charttime,
  CAST(NULL AS DATETIME) AS storetime,
  SUBSTRING(raw.text, c.source_start, c.source_length) AS note_text,
  CAST(c.study_id AS STRING) AS study_id,
  c.reuse_note_id,
  CAST('NO_DOCUMENT_DATE_ACQUISITION_ONLY' AS STRING) AS timestamp_basis,
  c.component_kind,
  c.match_method
FROM lk_cxr_component AS c
INNER JOIN @note_project.@note_dataset.cxr_reports AS raw
  ON CAST(raw.study_id AS INT64) = c.study_id;
END IF;
