-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------
-- -------------------------------------------------------------------
-- Populate lookups for cdm_observation table
-- 
-- Dependencies: run after 
--      st_core.sql,
--      st_hosp.sql
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize create or replace
--
-- loaded custom mapping:
--      gcpt_insurance_to_concept -> mimiciv_obs_insurance
--      gcpt_marital_status_to_concept -> mimiciv_obs_marital
--      gcpt_drgcode_to_concept -> mimiciv_obs_drgcodes
--          source_code = gcpt.description
-- Cost containment drgcode should be in cost table apparently.... 
--      http://forums.ohdsi.org/t/most-appropriate-omop-table-to-house-drg-information/1591/9,
-- observation.proc.* (Achilless Heel report)
--      value_as_string IS NULL AND value_as_number IS NULL AND COALESCE(value_as_concept_id, 0) = 0
--      review custom mapping. if ok, use value_as_concept_id = 4188539 'Yes'?
-- -------------------------------------------------------------------

-- on demo: 1585 rows
-- -------------------------------------------------------------------
-- lk_observation_clean from admissions
-- rules 1-3
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_observation_clean AS
-- rule 1, insurance
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    'Insurance'                     AS source_code,
    46235654                        AS target_concept_id, -- Primary insurance,
    src.admittime                   AS start_datetime,
    src.insurance                   AS value_as_string,
    'mimiciv_obs_insurance'         AS source_vocabulary_id,
    --
    'admissions.insurance'          AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.src_admissions src -- adm
WHERE
    src.insurance IS NOT NULL

UNION ALL
-- rule 2, marital_status
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    'Marital status'                AS source_code,
    40766231                        AS target_concept_id, -- Marital status,
    src.admittime                   AS start_datetime,
    src.marital_status              AS value_as_string,
    'mimiciv_obs_marital'           AS source_vocabulary_id,
    --
    'admissions.marital_status'     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.src_admissions src -- adm
WHERE
    src.marital_status IS NOT NULL

UNION ALL
-- rule 3, language
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    'Language'                      AS source_code,
    40758030                        AS target_concept_id, -- Preferred language
    src.admittime                   AS start_datetime,
    src.language                    AS value_as_string,
    'mimiciv_obs_language'          AS source_vocabulary_id,
    --
    'admissions.language'           AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.src_admissions src -- adm
WHERE
    src.language IS NOT NULL
;

-- -------------------------------------------------------------------
-- lk_observation_clean
-- Rule 4, drgcodes
-- -------------------------------------------------------------------

INSERT INTO @etl_project.@etl_dataset.lk_observation_clean
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    -- 'DRG code' AS source_code,
    src.drg_code                    AS source_code,
    4296248                         AS target_concept_id, -- Cost containment
    COALESCE(adm.edregtime, adm.admittime)  AS start_datetime,
    src.description                 AS value_as_string,
    'mimiciv_obs_drgcodes'          AS source_vocabulary_id,
    --
    'drgcodes.description'          AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.src_drgcodes src -- drg
INNER JOIN
    @etl_project.@etl_dataset.src_admissions adm
        ON src.hadm_id = adm.hadm_id
WHERE
    src.description IS NOT NULL
;


-- on demo: 270 rows
-- -------------------------------------------------------------------
-- lk_obs_admissions_concept
-- Rules 1-4
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_obs_admissions_concept AS
WITH src_codes AS (
  SELECT DISTINCT
    src.source_code,
    src.source_vocabulary_id,
    src.value_as_string
  FROM @etl_project.@etl_dataset.lk_observation_clean src
),

vc_ranked AS (
  SELECT
    vc.*,
    ROW_NUMBER() OVER (
      PARTITION BY vc.vocabulary_id, vc.domain_id, vc.concept_code
      ORDER BY
        (vc.invalid_reason IS NULL) DESC,
        (vc.standard_concept = 'S') DESC,
        vc.valid_end_date DESC,
        vc.concept_id DESC
    ) AS rn
  FROM @etl_project.@etl_dataset.voc_concept vc
),

vc_pick AS (
  SELECT *
  FROM vc_ranked
  WHERE rn = 1
),

mapped AS (
  SELECT
    s.source_code,
    s.value_as_string AS source_value,
    s.source_vocabulary_id,

    vc.domain_id  AS source_domain_id,
    vc.concept_id AS source_concept_id,

    CASE
      WHEN vc.standard_concept = 'S' AND vc.invalid_reason IS NULL THEN vc.domain_id
      ELSE vc2.domain_id
    END AS target_domain_id,

    CASE
      WHEN vc.standard_concept = 'S' AND vc.invalid_reason IS NULL THEN vc.concept_id
      ELSE vc2.concept_id
    END AS target_concept_id

  FROM src_codes s

  LEFT JOIN vc_pick vc
    ON (
      (s.source_vocabulary_id = 'mimiciv_obs_drgcodes'
      AND vc.domain_id = 'Observation'
      AND vc.vocabulary_id = 'DRG'
      AND s.source_code = vc.concept_code)
    OR (s.source_vocabulary_id = 'mimiciv_obs_language'
      AND (vc.vocabulary_id in ('Language', 'Race', 'Ethnicity'))
      AND s.value_as_string = vc.concept_name)
    OR (s.source_vocabulary_id = 'mimiciv_obs_marital'
      AND vc.vocabulary_id = 'mimiciv_obs_marital'
      AND s.value_as_string = vc.concept_name)
    OR (s.source_vocabulary_id = 'mimiciv_obs_insurance'
      AND vc.vocabulary_id = 'mimiciv_obs_insurance'
      AND s.value_as_string = vc.concept_name)
   )

  LEFT JOIN @etl_project.@etl_dataset.voc_concept_relationship vcr
    ON vc.concept_id = vcr.concept_id_1
   AND vcr.relationship_id = 'Maps to'
   AND vcr.invalid_reason IS NULL

  LEFT JOIN @etl_project.@etl_dataset.voc_concept vc2
    ON vc2.concept_id = vcr.concept_id_2
   AND vc2.standard_concept = 'S'
   AND vc2.invalid_reason IS NULL
)

SELECT DISTINCT
  source_code,
  source_value,
  source_vocabulary_id,
  source_domain_id,
  source_concept_id,
  target_domain_id,
  target_concept_id
FROM mapped;

-- -------------------------------------------------------------------
-- lk_observation_mapped
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_observation_mapped AS
SELECT
    src.hadm_id                             AS hadm_id, -- to visit
    src.subject_id                          AS subject_id, -- to person
    COALESCE(src.target_concept_id, 0)      AS target_concept_id,
    src.start_datetime                      AS start_datetime,
    32817                                   AS type_concept_id, -- OMOP4976890 EHR, -- Rules 1-4
    src.source_code                         AS source_code,
    0                                       AS source_concept_id,
    src.value_as_string                     AS value_as_string,
    lc.target_concept_id                    AS value_as_concept_id,
    'Observation'                           AS target_domain_id, -- to join on src.target_concept_id?
    --
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    @etl_project.@etl_dataset.lk_observation_clean src
LEFT JOIN
    @etl_project.@etl_dataset.lk_obs_admissions_concept lc
    ON (
        src.source_vocabulary_id = 'mimiciv_obs_drgcodes'
        AND lc.source_vocabulary_id = 'mimiciv_obs_drgcodes'
        AND src.source_code = lc.source_code
        )
    OR (
        src.source_vocabulary_id = 'mimiciv_obs_language'
        AND lc.source_vocabulary_id = 'mimiciv_obs_language'
        AND src.value_as_string = lc.source_value
        )
    OR (
        src.source_vocabulary_id = 'mimiciv_obs_marital'
        AND lc.source_vocabulary_id = 'mimiciv_obs_marital'
        AND src.value_as_string = lc.source_value
        )
    OR (
        src.source_vocabulary_id = 'mimiciv_obs_insurance'
        AND lc.source_vocabulary_id = 'mimiciv_obs_insurance'
        AND src.value_as_string = lc.source_value
        )
;
