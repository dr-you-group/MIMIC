# NOTE-only ETL

This optional add-on loads MIMIC-IV discharge/radiology and machine ECG text
into an **existing** OMOP CDM NOTE table. CXR documents are reconciled with
radiology; new CXR documents require a genuine document date. Echo study
references currently have no report body and are audited without insertion.

The PostgreSQL target is the existing CDM 5.3.1 schema, with its existing
64-bit note/person/visit identifiers. The BigQuery target is `cdm_note` in the
original OHDSI/MIMIC ETL dataset. This add-on does not run the Core pipeline.

## Scope and execution contract

Only NOTE receives permanent DML. PERSON, VISIT_OCCURRENCE, VISIT_DETAIL,
PROCEDURE_OCCURRENCE, MEASUREMENT, NOTE_NLP, CONCEPT and raw sources are read-only.
There are no new CDM columns, model migrations, permanent mapping tables,
views, or extension tables. Historical extensions from previous releases are
left untouched and are not dependencies of the current ETL. The former
extension-writing workflows have been removed; do not run old copies.

Working relations use temporary tables/views. The PostgreSQL runner writes
source mappings, rejected/withheld rows, visit decisions, SQL hashes and
verification results to an external audit directory. These files include
restricted source identifiers and dates; keep them with the licensed data,
not in Git. Original raw `_detail`, linkage and study files preserve metadata
that has no standard NOTE column. They are not joined directly into NOTE.

A current source ID deterministically maps to a positive 60-bit SHA-256 prefix:

| Source | Identity input |
|---|---|
| Discharge / radiology | `mimic-iv-note\|<source_table>\|<source_note_id>` |
| CXR | `mimic-cxr\|cxr\|<subject_id>-CXR-<study_id>` |
| CXR prefix | Same key with `-PREFIX` appended to the source ID |
| ECG machine | `mimic-iv-ecg\|ecg_machine\|<subject_id>-ECG_MACHINE-<study_id>` |

Version is provenance, not identity. Duplicate IDs, join fanout and conflicting
existing patient/text identities abort the transaction. Existing NOTE IDs,
text, dates, titles, concepts and other descriptive fields are immutable.
The only permitted existing-row change is filling a NULL visit with a verified
mapping. Source changes/deletions require review; there is no automatic DELETE.
Each source commits separately, so a failure rolls back that source; completed
sources can be safely rerun in a new audit directory.

## Shared mapping

| NOTE column | Rule |
|---|---|
| `person_id` | Existing PERSON whose `person_source_value = subject_id::text` |
| `note_date`, `note_datetime` | Source document `charttime`; machine ECG uses its supplied device time |
| `note_type_concept_id` | 32817 (EHR) |
| `note_class_concept_id` | Installed, valid, standard LOINC concept in the Note domain |
| `note_title` | Document type, explicitly identifying machine ECG or CXR variants |
| `note_text` | Original text; ECG concatenates all 18 ordered report slots |
| `encoding_concept_id`, `language_concept_id` | 32678 (UTF-8), 4180186 (English) |
| `provider_id` | NULL; no inferred provider |
| `visit_occurrence_id` | Existing visit, according to the rules below |
| `visit_detail_id` | NULL for new rows; existing validated values retained |
| `note_source_value` | DS, AD, RR, AR, CXR, CXR_SUPPLEMENT, CXR_VARIANT, ECG_MACHINE |

LOINC codes are 18842-5 for DS/AD, 68604-8 for radiology/CXR, and 75425-9 for
machine ECG. All constants and document concepts are checked against the
installed vocabulary; the ETL never inserts vocabulary concepts. Missing
PERSON, empty text or absent required document date causes withholding and
an audit record, not an invented value. A missing visit does not prevent NOTE.

`study_id` is not an OMOP NOTE identifier and is not written into
`note_source_value` or the narrative. It remains in the raw inputs and the
external source-to-NOTE audit. Under this NOTE-only scope, an OMOP-only query
cannot reconstruct all study/image/waveform links. No procedure or imaging
extension is created to imply otherwise.

## Existing visit linkage

1. With an explicit `hadm_id`, require both the same PERSON and
   `visit_source_value = subject_id || '|' || hadm_id`. The source association
   remains valid even if document charting occurs outside the stay interval.
   An unmatched explicit ID stays NULL; there is no fallback to another visit.
2. Preserve an existing consistent visit. A CXR alias uses the existing
   radiology NOTE and therefore its actual document date and visit.
3. Without `hadm_id`, only a reliable **document** timestamp may match a
   single numeric-admission visit whose start/end contain that timestamp.
   All admission-derived visit types are eligible; the rule is not limited
   to concept 9201. Overlapping candidates stay NULL. This is a documented
   inference about the document encounter, not proof of the study encounter.
4. `subject_id|YYYY-MM-DD` synthetic visits are excluded from time fallback.
   No nearest-visit choice, expanded time window, acquisition-time assignment,
   or unsynchronized-ECG-clock assignment is used.

CXR acquisition time and ECG device time cannot by themselves establish a
reliable visit. No suitable study-to-encounter key is currently supplied for
new CXR or machine ECG documents. These visits therefore remain NULL.

## PostgreSQL

Set the standard `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` and
`PGSSLMODE` environment variables securely. Docker `postgres:16` provides psql.
No credentials belong in source files or the audit.

```sh
python scripts/run_modality_note_pg.py \
  --audit /private/note-audit/new-run \
  --source-schema mimiciv_modality \
  --radiology-schema mimiciv_note --cdm-schema mimiciv \
  --sources discharge radiology cxr ecg_machine echo --indexes
```

Use a new audit directory on every run. Sources can be selected independently;
load radiology before reconciling CXR. The runner reads verified raw tables,
not the retired canonical modality views. It does not download or load raw
files. `source_file` provenance must mark CXR/ECG inputs VERIFIED; Echo's
checksum state is reported independently of its missing body.

Each source runs `etl/postgres/note.sql`, which includes the applicable CXR
or ECG adapter. This retains the sibling convention:
`src_* -> lk_* -> column-by-column INSERT SELECT -> validation`.
Existing visit updates are a separate, small UPDATE. Echo runs the read-only
`audit_echo_note.sql` instead.

Source-level validation runs **before COMMIT** and checks actual per-transaction
PostgreSQL write counters: any non-temporary table other than NOTE receiving
inserts, updates or deletes causes rollback, including writes from triggers.
A full before/after fingerprint independently verifies every pre-existing
NOTE field except explicitly audited NULL-to-confirmed visit updates.
`qa_note.sql` checks required fields, vocabulary, patient/visit/provider FKs,
field lengths, date consistency and duplicates across the whole NOTE table.

`--indexes` creates two physical indexes on existing NOTE columns:
`(person_id, note_date, note_id)` and `(visit_occurrence_id, note_date, note_id)`
for non-NULL visits. This does not add CDM columns or clinical rows.

## BigQuery

Copy the appropriate `conf/note*.etlconf` to a private configuration and set
its source/radiology/ETL datasets. Use the same existing Core identifiers.

```sh
python scripts/run_workflow.py -e /private/note_radiology.etlconf -c conf/workflow_note.conf
python scripts/run_workflow.py -e /private/note_cxr.etlconf -c conf/workflow_note.conf
python scripts/run_workflow.py -e /private/note_ecg_machine.etlconf -c conf/workflow_note.conf
python scripts/run_workflow.py -e /private/note_echo.etlconf -c conf/workflow_note_echo.conf
```

The NOTE workflow is one BigQuery script:
`st_note.sql -> st_cxr_note.sql -> lk_note.sql -> cdm_note.sql -> qa_note.sql`.
All working tables are temporary. Only `cdm_note` has permanent INSERT/UPDATE.
The final source-accounting query and the preceding detailed audit query are
available as child-job results; retain/export them outside the CDM with the
job ID. Echo's workflow is read-only. PostgreSQL execution does not constitute
a live BigQuery execution or validation of a user's BigQuery dataset types.

## Verification

```sh
python -m unittest discover -s test/note -v
```

Integration tests start an isolated PostgreSQL container with synthetic text.
They exercise visit ambiguity, explicit IDs, pseudo-visits, ECG slots and
clock restrictions, CXR complete-text identity and date withholding, legacy
preservation, vocabulary failures, reruns, and rollback of other-table writes.
See [modality_notes.md](modality_notes.md) for modality-specific limits.

Aggregate execution results: [2026-10-26 validation](note_validation_20261026.md).
