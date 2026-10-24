# MIMIC-IV-Note ETL

This optional add-on loads MIMIC-IV-Note into an existing OMOP CDM. It uses the
completed Core ETL's person and visit identifiers. Run `discharge` first if that
is the available source; run `radiology` later without rebuilding Core or
replacing discharge notes.

The BigQuery implementation follows this repository's ETL dataset convention.
The PostgreSQL implementation executes equivalent mapping rules directly
against an existing database. It does not run or translate the full BigQuery
pipeline.

## Mapping contract

| NOTE field | Rule |
| --- | --- |
| `note_id` | Positive integer from the first 15 hexadecimal digits of SHA-256 of UTF-8 `mimic-iv-note\|<source_table>\|<source_note_id>` |
| `person_id` | Existing `person_source_value = CAST(subject_id AS string)` mapping |
| `note_date`, `note_datetime` | Date of `charttime`, and unchanged `charttime` |
| `note_type_concept_id` | 32817, EHR |
| `note_class_concept_id` | Resolve the source-type LOINC code in the installed Vocabulary |
| `note_title` | Discharge summary / Radiology report, with an addendum suffix for AD / AR |
| `note_text` | Original text, without trimming, section extraction, or newline normalization |
| `encoding_concept_id` | 32678, UTF-8 |
| `language_concept_id` | 4180186, English |
| `provider_id`, `visit_detail_id` | NULL |
| `visit_occurrence_id` | Existing visit with the same person and `visit_source_value = subject_id\|hadm_id` |
| `note_source_value` | Original DS, AD, RR, or AR |

| Source | Types | LOINC document code |
| --- | --- | --- |
| discharge | DS, AD | 18842-5, Discharge summary |
| radiology | RR, AR | 68604-8, Radiology Diagnostic Study Note |

Document concepts must be valid, standard LOINC concepts in the Note domain.
The installed Vocabulary determines their concept IDs. The three constant
concepts are also validated before loading. LOINC 18748-4 is not used as the
document class: in the reviewed Vocabulary it represents an Observation.

The identical SHA-256 expression in both dialects avoids dependency on
BigQuery's FARM_FINGERPRINT in PostgreSQL. A permanent ID map records every
source document, including rejected documents. Generated-ID collisions,
changed mapping rules, and collisions with notes owned by another ETL stop
the run. The hash is not assumed to be collision-free. Source version and
file checksum are provenance fields and do not change document identity.

## Missing links and metadata

Missing persons are recorded as `MISSING_PERSON` in the metadata table and
excluded from NOTE. Their raw notes and detail rows remain available. Do not
create replacement persons.

A note with a known person and an unmatched non-NULL `hadm_id` is loaded with
a NULL visit and `visit_status = 'MISSING_VISIT'`. This is a review flag, not
proof that the note has no encounter. Notes with no `hadm_id` use the separate
`NO_HADM_ID` status. No time-based encounter guessing is performed.

Metadata preserves the raw note ID, subject, admission, type, sequence,
charttime, storetime, text SHA-256 and byte length, source version/checksum,
link status, and mapped IDs. The detail table preserves all five original
fields and duplicate multiplicity. It is not joined into NOTE to create
additional documents. Anonymous author placeholders are not provider IDs.

Some published detail rows refer to a source document whose body is absent.
Retain all five raw fields and their duplicate multiplicity with a NULL
`note_detail.note_id` for those rows. Do not create a NOTE, person, or document
ID-map entry from a detail reference alone. `unmatched_detail_rows` records
the count for each run. A subject mismatch for an available parent remains
a validation failure. QA checks both raw-field equality and document linkage.

PostgreSQL audit tables live in the selected `etl_schema`:

- `note_id_map`: permanent raw-to-CDM document identifier map.
- `note_metadata`: source metadata, person rejections, and visit review flags.
- `note_detail`: complete source detail rows, including rejected documents.
- `note_etl_run`: successful run counts and integrity-check results.

The corresponding BigQuery tables have the `ext_` prefix and remain in the
ETL dataset. Retain that dataset when publishing the final CDM.

## SQL layout

The NOTE files use the repository's `src_*` staging and `lk_*` lookup naming,
section comments, and column-by-column `INSERT INTO ... SELECT` mapping.
Read them in the order configured in `conf/workflow_note.conf`:

| File | Responsibility |
| --- | --- |
| `etl/staging/st_note.sql` | Read one complete source and its detail table; generate stable note IDs |
| `etl/etl/lk_note.sql` | Map document concepts and existing person/visit IDs; validate joins |
| `etl/ddl/ddl_note_extensions.sql` | Create persistent provenance tables if absent |
| `etl/etl/cdm_note.sql` | Begin the transaction, check ID ownership, and insert the 14 NOTE fields |
| `etl/etl/ext_note.sql` | Preserve source ID mappings, metadata, and all detail rows |
| `test/qa/qa_note.sql` | Compare all output fields and detail rows; record counts and commit |

`cdm_note.sql` uses the existing CDM NOTE DDL. Within the transaction it
replaces only the selected source's owned rows using `DELETE` and
`INSERT ... SELECT`. Other sources and note identifiers are retained.
Run the complete workflow; `cdm_note.sql` alone intentionally does not commit.

`etl/postgres/note.sql` expresses the same stages and field mappings as one
psql script. It uses temporary `src_note` and `lk_note_mapped` tables and an
in-place PostgreSQL upsert, which also retains existing constraints and avoids
rewriting unchanged text. It executes directly against the existing CDM.

## BigQuery

Finish Core ETL, including `cdm_finalize_person`, and vocabulary refresh first.
The existing `cdm_note` DDL must have been created. Do not rerun the Core DDL or
Core ETL merely to add notes: Core uses generated identifiers.

Copy `conf/note.etlconf` to a local configuration and set the source and ETL
project/dataset names. Choose one `@note_source_table` and the matching
`@note_detail_table`. Both selected source tables must be complete.
`@note_source_sha256` is optional when using a managed BigQuery source; leave
it empty rather than claiming a file checksum that has not been verified.

```sh
python scripts/run_workflow.py \
  -e /path/to/local-note.etlconf \
  -c conf/workflow_note.conf
```

For radiology, change those source settings to `radiology` and
`radiology_detail` and run again. Run NOTE workflows sequentially within the
same ETL dataset because the staging table names are shared. Downloads may
run concurrently.

The workflow opts into `script_mode`, which reads all listed SQL files in
order and sends their combined contents to one `bq query` job through stdin.
The transaction spans `cdm_note.sql`, `ext_note.sql`, and `qa_note.sql`;
failed validation rolls back all target and provenance changes. Staging,
lookup, and extension DDL run before the transaction. Existing workflows
retain statement-by-statement execution. SQL errors propagate to the
workflow's exit status.

The existing `etl/unload/unload_to_atlas_gen.sql` already publishes `cdm_note`
and `cdm_note_nlp` to the final dataset. Run the normal unload after the NOTE
workflow, or publish those two tables as part of the established release
process. NOTE_NLP remains empty unless a separate NLP pipeline populates it.

## PostgreSQL

Requires PostgreSQL 14+ and `psql`. The target must already contain the OMOP
`note`, `note_nlp`, `person`, `visit_occurrence`, and `concept` tables.
The source must contain the selected note and detail tables.

Use normal PostgreSQL connection settings or a protected password file.
Do not put passwords into SQL, configuration files committed to Git, or
command-line connection URLs.

```sh
psql -X -v ON_ERROR_STOP=1 \
  -v source_schema=mimiciv_note \
  -v cdm_schema=mimiciv \
  -v etl_schema=mimiciv_note_etl \
  -v source_table=discharge \
  -v source_version=2.2 \
  -f etl/postgres/note.sql
```

If available, pass the verified source file checksum using
`-v source_sha256=<sha256>`. Change only `source_table` to `radiology` for a
later radiology load; its detail table name is derived automatically.

The transaction widens NOTE's document, person, visit, and visit-detail IDs
to BIGINT and widens NOTE_NLP's note reference to BIGINT. Columns already using
BIGINT are skipped, so incremental loads retain views over the widened tables.
For an initial type conversion, dependent views must first be preserved and
recreated with their original definitions and privileges in the same transaction.
Existing constraints are retained. Concept IDs remain ordinary integers.
A database advisory lock serializes this add-on's runs for the same target schema. Repeatable-read
isolation keeps the source and Core mapping snapshot consistent.

The implementation upserts its own notes. An identical rerun changes zero
NOTE rows, preserves IDs, and keeps other sources. It refuses implicit
deletions caused by a reduced source snapshot or a changed Core cohort.
Such changes require explicit reconciliation. If source text is revised,
downstream NLP or embeddings require reprocessing.

## Validation

The load fails on missing required source fields, unsupported note types,
missing vocabulary mappings, person/visit join fanout, incorrect detail
parents, identifier collisions, or output differences. Before commit it
compares every loaded NOTE field, exact UTF-8 text SHA-256 and byte length,
and the full detail multiset. Missing-person and missing-visit counts are
reported separately; a nonzero count is not hidden by an inner join.
PostgreSQL records `finished_at` using wall-clock time at the successful audit
insert, immediately before ANALYZE and commit, rather than transaction start time.

Dates are preserved even if `storetime < charttime` or storetime is absent.
Charttime is not a guarantee that the final document was available for a
prediction at that time. Define research availability rules separately.

```sh
python -m unittest discover -s test/note -v
```

Tests run in an isolated PostgreSQL Docker container with synthetic notes.
They cover large/signed Core IDs, DS/AD/RR/AR mapping, unknown patients,
unmatched visits, absent admission IDs, exact Unicode/newline preservation,
duplicate detail rows, adding radiology after discharge, idempotence,
collision rejection, and rollback after an integrity failure. Runner tests
use a fake `bq` executable to verify the combined script order, transaction
boundaries, and error propagation.
They do not constitute execution of the SQL on the BigQuery service.

Availability and proposed mappings for CXR, ECG, and Echo are documented in
[Modality notes](modality_notes.md). That assessment does not enable additional
source types in the current discharge/radiology workflow.

References: [OMOP NOTE](https://ohdsi.github.io/CommonDataModel/cdm54.html#note),
[MIMIC-IV-Note](https://mimic.mit.edu/docs/iv/modules/note/),
[LOINC 18842-5](https://loinc.org/18842-5/),
[LOINC 68604-8](https://loinc.org/68604-8/).
