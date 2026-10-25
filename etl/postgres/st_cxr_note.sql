-- -------------------------------------------------------------------
-- MIMIC IV CDM Conversion: conservative CXR document reconciliation
-- Called by note.sql. Full document / final-section equality only;
-- body-only, masked-text and proximity-only matches never discard text.
-- Acquisition time supports identity review, never NOTE date or visit.
-- -------------------------------------------------------------------
INSERT INTO note_checks VALUES ('verified_cxr_sources', (
    SELECT count(*) = 2 AND bool_and(checksum_status = 'VERIFIED')
    FROM :"source_schema".source_file WHERE table_name IN ('cxr_reports','cxr_metadata')));
CREATE FUNCTION pg_temp.note_normalize(value text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT btrim(regexp_replace(regexp_replace(value,
        '^[[:space:]]*FINAL REPORT[[:space:]]*', ''), '[[:space:]]+', ' ', 'g'))
$$;
CREATE FUNCTION pg_temp.note_addendum(value text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT pg_temp.note_normalize(regexp_replace(regexp_replace(regexp_replace(
        value, '(?n)^[ \t]*_{10,}[ \t]*\r?$', '', 'g'),
        '(?is)^[[:space:]]*FINAL ADDENDUM[[:space:]]*', ''),
        '(?is)^[[:space:]]*ADDENDUM[[:space:]]*:?[[:space:]]*', ''))
$$;
CREATE TEMP TABLE src_cxr_time ON COMMIT DROP AS
SELECT subject_id::integer AS subject_id, study_id::integer AS study_id,
    CASE WHEN studydate <> '' AND studytime <> '' THEN studydate::date + (
        substr(lpad(split_part(studytime,'.',1),6,'0'),1,2) || ':' ||
        substr(lpad(split_part(studytime,'.',1),6,'0'),3,2) || ':' ||
        substr(lpad(split_part(studytime,'.',1),6,'0'),5,2) ||
        CASE WHEN strpos(studytime,'.') > 0 THEN '.' || split_part(studytime,'.',2) ELSE '' END
    )::time END AS acquisition_time
FROM :"source_schema".cxr_metadata;
CREATE TEMP TABLE src_cxr_study ON COMMIT DROP AS
WITH src AS MATERIALIZED (
    SELECT raw.*, coalesce(substring(raw.text FROM '(?s)^(.*\n)[ \t]*FINAL REPORT[ \t]*\r?\n'),'') AS prefix
    FROM :"source_schema".cxr_reports raw
)
SELECT src.subject_id::integer AS subject_id, src.study_id::integer AS study_id,
    src.subject_id || '-CXR-' || src.study_id AS source_note_id,
    length(src.text) AS text_length, length(src.prefix) AS prefix_length,
    md5(pg_temp.note_normalize(src.text)) AS full_hash,
    md5(pg_temp.note_normalize(substr(src.text,length(src.prefix)+1))) AS final_hash,
    md5(pg_temp.note_addendum(src.prefix)) AS prefix_hash,
    tm.acquisition_start, tm.acquisition_end,
    tm.missing_times
FROM src LEFT JOIN (
    SELECT subject_id,study_id,min(acquisition_time) AS acquisition_start,
        max(acquisition_time) AS acquisition_end,
        count(*) - count(acquisition_time) AS missing_times
    FROM src_cxr_time GROUP BY 1,2
) tm ON tm.subject_id = src.subject_id::integer AND tm.study_id = src.study_id::integer;
CREATE UNIQUE INDEX ON src_cxr_study(study_id);
CREATE INDEX ON src_cxr_study(subject_id);
ANALYZE src_cxr_study;
CREATE TEMP TABLE src_cxr_radiology ON COMMIT DROP AS
SELECT r.note_id AS source_note_id, r.subject_id, r.note_type, r.charttime,
    n.note_id, md5(pg_temp.note_normalize(r.text)) AS full_hash,
    CASE WHEN r.note_type = 'AR' THEN md5(pg_temp.note_addendum(r.text)) END AS addendum_hash
FROM :"radiology_schema".radiology r
INNER JOIN :"cdm_schema".note n
    ON n.note_id = pg_temp.note_key('mimic-iv-note','radiology',r.note_id)
    AND n.note_text = r.text
WHERE EXISTS (SELECT 1 FROM src_cxr_study s WHERE s.subject_id = r.subject_id);
CREATE INDEX ON src_cxr_radiology(subject_id,full_hash);
CREATE UNIQUE INDEX ON src_cxr_radiology(source_note_id);
ANALYZE src_cxr_radiology;

-- Hashes only accelerate candidate search. Compare complete normalized
-- strings again; collisions cannot establish document identity.
CREATE TEMP TABLE lk_cxr_pair ON COMMIT DROP AS
SELECT s.study_id, r.source_note_id AS radiology_note_id, r.note_id,
    CASE WHEN s.full_hash = r.full_hash THEN 1 ELSE 2 END AS priority
FROM src_cxr_study s
INNER JOIN src_cxr_radiology r ON r.subject_id = s.subject_id AND r.note_type = 'RR'
    AND r.full_hash IN (s.full_hash,s.final_hash)
    AND r.charttime BETWEEN s.acquisition_start - interval '48 hours' AND s.acquisition_end + interval '48 hours'
INNER JOIN :"source_schema".cxr_reports raw ON raw.study_id::integer = s.study_id
INNER JOIN :"radiology_schema".radiology rad ON rad.note_id = r.source_note_id
WHERE s.missing_times = 0 AND s.acquisition_end - s.acquisition_start <= interval '6 hours'
    AND (pg_temp.note_normalize(raw.text) = pg_temp.note_normalize(rad.text)
      OR (s.prefix_length > 0 AND pg_temp.note_normalize(substr(raw.text,s.prefix_length+1)) = pg_temp.note_normalize(rad.text)));
CREATE TEMP TABLE lk_cxr_match ON COMMIT DROP AS
WITH best AS (
    SELECT p.*, min(priority) OVER (PARTITION BY study_id) AS best_priority FROM lk_cxr_pair p
), unique_pair AS (
    SELECT b.*,count(*) OVER (PARTITION BY study_id) AS study_candidates,
        count(*) OVER (PARTITION BY radiology_note_id) AS report_candidates
    FROM best b WHERE priority = best_priority
)
SELECT * FROM unique_pair WHERE study_candidates = 1 AND report_candidates = 1;
CREATE UNIQUE INDEX ON lk_cxr_match(study_id);
CREATE TEMP TABLE src_cxr_parent ON COMMIT DROP AS
SELECT note_id AS parent_note_id, field_value AS addendum_note_id
FROM :"radiology_schema".radiology_detail WHERE field_name = 'addendum_note_id'
UNION
SELECT field_value, note_id FROM :"radiology_schema".radiology_detail WHERE field_name = 'parent_note_id';
CREATE INDEX ON src_cxr_parent(parent_note_id);
CREATE TEMP TABLE lk_cxr_addendum ON COMMIT DROP AS
WITH candidates AS (
    SELECT s.study_id,r.note_id,
        count(*) OVER (PARTITION BY s.study_id) AS study_candidates,
        count(*) OVER (PARTITION BY r.note_id) AS report_candidates
    FROM src_cxr_study s INNER JOIN lk_cxr_match m USING (study_id)
    INNER JOIN src_cxr_parent p ON p.parent_note_id = m.radiology_note_id
    INNER JOIN src_cxr_radiology r ON r.source_note_id = p.addendum_note_id
        AND r.subject_id = s.subject_id AND r.note_type = 'AR' AND r.addendum_hash = s.prefix_hash
    INNER JOIN :"source_schema".cxr_reports raw ON raw.study_id::integer = s.study_id
    INNER JOIN :"radiology_schema".radiology rad ON rad.note_id = r.source_note_id
    WHERE m.priority = 2 AND s.prefix_length > 0
      AND pg_temp.note_addendum(substr(raw.text,1,s.prefix_length)) = pg_temp.note_addendum(rad.text)
)
SELECT study_id,note_id FROM candidates WHERE study_candidates = 1 AND report_candidates = 1;
CREATE UNIQUE INDEX ON lk_cxr_addendum(study_id);

-- Existing whole/prefix documents retain their IDs and source text even
-- when the stricter reconciliation no longer confirms a former alias.
CREATE TEMP TABLE lk_cxr_component ON COMMIT DROP AS
SELECT s.*, s.source_note_id AS component_id, 'REPORT'::text AS component_kind,
    CASE WHEN old.note_id IS NULL AND m.priority = 2 THEN s.prefix_length + 1 ELSE 1 END AS source_start,
    CASE WHEN old.note_id IS NULL AND m.priority = 2 THEN s.text_length - s.prefix_length ELSE s.text_length END AS source_length,
    coalesce(old.note_source_value,CASE WHEN NOT EXISTS (
        SELECT 1 FROM src_cxr_radiology r WHERE r.subject_id = s.subject_id
    ) THEN 'CXR' ELSE 'CXR_VARIANT' END) AS note_type,
    CASE WHEN old.note_id IS NULL THEN m.note_id END AS reuse_note_id,
    CASE WHEN m.priority = 1 THEN 'FULL_DOCUMENT_EQUAL_WITHIN_48H'
         WHEN m.priority = 2 THEN 'FINAL_SECTION_EQUAL_WITHIN_48H' END AS match_method
FROM src_cxr_study s LEFT JOIN lk_cxr_match m USING (study_id)
LEFT JOIN :"cdm_schema".note old ON old.note_id = pg_temp.note_key('mimic-cxr','cxr',s.source_note_id)
UNION ALL
SELECT s.*, s.source_note_id || '-PREFIX','PREFIX',1,s.prefix_length,
    'CXR_SUPPLEMENT',CASE WHEN old.note_id IS NULL THEN ar.note_id END,
    CASE WHEN ar.note_id IS NOT NULL THEN 'EXACT_PARENT_LINKED_ADDENDUM' ELSE 'PRESERVED_SOURCE_PREFIX' END
FROM src_cxr_study s LEFT JOIN lk_cxr_match m USING (study_id)
LEFT JOIN lk_cxr_addendum ar USING (study_id)
LEFT JOIN :"cdm_schema".note old ON old.note_id = pg_temp.note_key('mimic-cxr','cxr',s.source_note_id || '-PREFIX')
-- A pre-existing whole document already preserves its prefix. Keep an
-- old standalone prefix, but do not create a redundant new component.
WHERE s.prefix_length > 0 AND (old.note_id IS NOT NULL OR (m.priority = 2 AND NOT EXISTS (
    SELECT 1 FROM :"cdm_schema".note whole
    WHERE whole.note_id = pg_temp.note_key('mimic-cxr','cxr',s.source_note_id)
)));
CREATE UNIQUE INDEX ON lk_cxr_component(component_id);
CREATE TEMP VIEW src_note_text AS
SELECT c.component_id AS source_note_id, c.subject_id, NULL::integer AS hadm_id,
    c.note_type, NULL::timestamp AS charttime, NULL::timestamp AS storetime,
    substr(raw.text,c.source_start,c.source_length) AS note_text,
    c.study_id::text AS study_id,c.reuse_note_id,
    'NO_DOCUMENT_DATE_ACQUISITION_ONLY'::text AS timestamp_basis,c.component_kind,c.match_method
FROM lk_cxr_component c
INNER JOIN :"source_schema".cxr_reports raw ON raw.study_id::integer = c.study_id;
