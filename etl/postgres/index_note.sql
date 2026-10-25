-- Physical indexes on existing NOTE columns; no CDM schema extension.
-- Run outside a transaction after NOTE validation.
\set ON_ERROR_STOP on
CREATE INDEX CONCURRENTLY IF NOT EXISTS note_person_date_idx
    ON :"cdm_schema".note (person_id, note_date, note_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS note_visit_date_idx
    ON :"cdm_schema".note (visit_occurrence_id, note_date, note_id)
    WHERE visit_occurrence_id IS NOT NULL;
