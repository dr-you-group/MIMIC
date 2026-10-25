# Modality NOTE add-on

CXR and ECG use canonical SQL views and the existing NOTE mapping: `src_*`,
`lk_*`, column-by-column `INSERT ... SELECT`, permanent IDs, concept checks,
exact text validation, and source-specific transactions. PostgreSQL executes
these queries directly against the existing CDM. Existing Core IDs are retained.

| Source | Implemented action | NOTE source / LOINC |
| --- | --- | --- |
| CXR 2.1.0 | Reuse resolved radiology documents; add reports from subjects with no raw radiology; retain other cases for review | CXR / 68604-8 |
| ECG 1.0 | Construct machine interpretation documents from 18 original report fields | ECG_MACHINE / 75425-9 |
| Echo 1.0.1 | Preserve study/report linkage and validation status; no NOTE without narrative | NO_REPORT_BODY |

## Inputs and provenance

`prepare_modality_inputs.py` verifies files against `DOWNLOAD_MANIFEST.json`,
extracts CXR UTF-8 text unchanged, and preserves CSV values including empty
strings. It records the complete row multiset hash, retaining duplicates.
Python performs raw ingestion and verification; mapping is in SQL.

Raw tables: `cxr_reports`, `cxr_study_list`, `cxr_record_list`,
`cxr_provider_list`, `cxr_metadata`, `ecg_machine_measurements`, `ecg_record_list`,
`ecg_waveform_note_links`, `echo_study_list`, plus `source_file` provenance.
Images, videos, waveforms, and numeric Echo measurements are not required.

The known Echo copy differs from its published checksum. Two independent
copies agree, which does not establish a published checksum match. Explicit
`--retain-unverified-echo-links` retains this copy in raw quarantine as
`CHECKSUM_MISMATCH`. It becomes neither a verified input nor a NOTE. Other
checksum failures stop preparation. Reconcile Echo with the source before
using its linkage as validated data.

## CXR rules

A study may have several images; do not create one NOTE per image. `study_id`
is not `hadm_id`. The earliest image-metadata study time is an acquisition
time, not a report signing time. Multiple study dates require review.

Candidates must have the same patient and calendar date:

- `FULL_TEXT_SAME_DAY`: complete text matches after whitespace normalization
  and removal of a leading `FINAL REPORT` label.
- `CLINICAL_BODY_CHEST_WITHIN_1H`: normalized FINDINGS/IMPRESSION-to-end text
  matches exactly, is at least 40 characters, the raw exam is a chest radiograph,
  and acquisition/chart times differ by at most one hour. CXR addendum,
  amended-report, and long underscore separator markers exclude this route.
  It addresses header/deidentification differences and is recorded separately.
- Both study and radiology document must have exactly one candidate. Repeated
  generic reports and ambiguous pairs remain in review.

These are deterministic matching rules, not clinical adjudication. Comparison
normalization never changes stored text. `cxr_reconciliation` records method,
count, and decision. Resolved reports reuse the existing radiology NOTE and
its person, visit, document time, and text. `modality_note_link` stores the
study alias without creating a second owner in the unique NOTE ID map.

The default new-document rule is narrow: `NEW_NO_PATIENT_RADIOLOGY` requires
no raw radiology document at all for that subject. Other unmatched documents,
including unresolved addenda, are `REVIEW_OVERLAP`. A missing hash match is
not proof of novelty. Complete originals remain available for reconciliation.

## ECG and Echo rules

The `ecg_machine` view joins `report_0` through `report_17` with 17 newline
separators, retaining empty and trailing positions. Original fields stay in
raw data. Empty/whitespace-only reports become `EMPTY_TEXT` and produce no
NOTE. Numeric measurements are not converted into prose.

Title `ECG machine interpretation` and source `ECG_MACHINE` identify machine
authorship; provider is NULL. These are not physician-confirmed diagnoses.
`ecg_time` is used with the recorded timestamp basis
`UNSYNCHRONIZED_ECG_MACHINE_CLOCK`. `EK` IDs in `waveform_note_links` identify
physician reports, not machine text, and are never used as machine NOTE IDs.
ECG 1.0 does not provide those physician narrative bodies.

Echo supplies linkage fields, not narrative. Every study and available `EC`
reference is preserved with `NO_REPORT_BODY` and NULL OMOP note ID. The official
within-two-day temporal association is not an admission link. Future narratives
must be keyed by actual document ID because multiple studies can reference one
document. EF and other numeric results require a separate MEASUREMENT ETL.

## Shared mapping

Reuse `person_source_value = subject_id`; missing persons remain in raw/audit
records. New modalities have no reliable `hadm_id`, so new NOTE visits are
NULL. `note_seq = 0` means unavailable; random study IDs are not chronological
sequences. Canonical detail views store study IDs; raw tables retain all other
original metadata. Existing constants, vocabulary and collision checks apply.

The positive 60-bit SHA-256 prefix uses these UTF-8 inputs:

| Source | Stable identity input |
| --- | --- |
| discharge/radiology, unchanged | `mimic-iv-note\|<source_table>\|<source_note_id>` |
| New CXR | `mimic-cxr\|cxr\|<subject_id>-CXR-<study_id>` |
| ECG machine | `mimic-iv-ecg\|ecg_machine\|<subject_id>-ECG_MACHINE-<study_id>` |

Version/checksum are provenance, not identity. No new NOTE ID is allocated
for Echo or unresolved aliases. NOTE_NLP remains a separate workflow.

## PostgreSQL

Set standard PG connection variables securely in the execution environment.
Helpers use Docker postgres:16. Core, vocabulary, NOTE, and the completed
radiology/discharge add-on must exist. Keep data and credentials outside Git.

```sh
python scripts/prepare_modality_inputs.py \
  --data /private/modality_notes --output /private/prepared \
  --retain-unverified-echo-links
python scripts/load_modality_raw.py \
  --input /private/prepared --schema mimiciv_modality \
  --report /private/audit/raw_verification.json
python scripts/run_modality_note_pg.py \
  --input /private/prepared --raw-report /private/audit/raw_verification.json \
  --audit /private/audit/note
```

The loader verifies every raw field at the destination. The runner executes
`etl/postgres/st_modality_note.sql`, existing `note.sql` for `cxr` and
`ecg_machine`, then `etl/postgres/ext_modality_note.sql`. Each stage has its own
transaction and can be resumed. Changed established resolutions require
explicit reconciliation. Source notes are not silently deleted; zero new CXR
notes is valid. The runner records SQL hashes and checks all 14 mapped fields
of every pre-existing NOTE before and after. Compare final NOTE fingerprints
and modality links between databases running the same revision.

## BigQuery

The preparer emits `*.schema.json` and `source_file.csv`. Load every prepared
CSV and provenance, preserving empty strings and quoted newlines, for example:

```sh
bq load --source_format=CSV --skip_leading_rows=1 --allow_quoted_newlines \
  --null_marker='\N' PROJECT:MODALITY_DATASET.cxr_reports \
  /private/prepared/cxr_reports.csv /private/prepared/cxr_reports.schema.json
```

Copy the three configuration templates to private files and set project,
dataset and verified source-checksum values. Run sequentially:

```sh
python scripts/run_workflow.py -e /private/modality_note.etlconf -c conf/workflow_modality_note_staging.conf
python scripts/run_workflow.py -e /private/note_cxr.etlconf -c conf/workflow_note.conf
python scripts/run_workflow.py -e /private/note_ecg_machine.etlconf -c conf/workflow_note.conf
python scripts/run_workflow.py -e /private/modality_note.etlconf -c conf/workflow_modality_note_links.conf
```

Persistent extensions have the `ext_` prefix. The 14 NOTE field rules still use
`st_note.sql -> lk_note.sql -> cdm_note.sql -> ext_note.sql -> qa_note.sql`.

## Validation and references

`python -m unittest discover -s test/note -v` runs isolated PostgreSQL integration
and BigQuery runner/rendering tests, covering existing-note preservation, stable
IDs, CXR ambiguity/body matching, machine line positions, missing persons, Echo
quarantine, empty CXR output, rollback, and idempotence. Rendering and parsing
are not execution on the BigQuery service.

Official sources: [CXR](https://physionet.org/content/mimic-cxr/2.1.0/),
[ECG](https://physionet.org/content/mimic-iv-ecg/1.0/),
[Echo](https://physionet.org/content/mimic-iv-echo/1.0.1/),
[MIMIC-IV-Note](https://physionet.org/content/mimic-iv-note/2.2/).
