# Modality NOTE add-on

CXR and ECG use canonical SQL views and the existing NOTE mapping: `src_*`,
`lk_*`, column-by-column `INSERT ... SELECT`, permanent IDs, concept checks,
exact text validation, and source-specific transactions. PostgreSQL executes
these queries directly against the existing CDM. Existing Core IDs are retained.

| Source | Implemented action | NOTE source / LOINC |
| --- | --- | --- |
| CXR 2.1.0 | Reuse resolved radiology documents and add non-overlapping reports and supplementary source text | CXR, CXR_SUPPLEMENT / 68604-8 |
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
is not `hadm_id`. New CXR notes use the earliest image acquisition time as an
explicit proxy. It is **not the authoring, signing, or availability time** of a
final, preliminary, or supplementary report. `storetime` stays NULL. Do not use
this proxy alone to determine whether a document was available for prediction.

### Match the final report

All candidates require the same patient. Existing unambiguous legacy matches
are evaluated first and retain their NOTE IDs, including their original time
rule. Newly resolved studies must have complete image times spanning at most
six hours. A study can cross midnight; every image time is considered.

| Priority | Matching rule for previously unresolved studies |
| --- | --- |
| 1 | Entire normalized source report matches within six hours of an image |
| 2 | Final report after the last standalone `FINAL REPORT` line matches within six hours |
| 3 | FINDINGS/IMPRESSION-to-end matches exactly, has at least 40 characters, and the target is a chest radiograph within six hours |
| 4 | Body of at least 80 characters differs only in standalone `___` mask runs and resulting whitespace, with a chest radiograph within one hour |

The fourth rule never removes or replaces words, numbers, or punctuation.
Masks touching letters or numbers are ineligible. For example, `No effusion`
and `___ effusion`, or `3.1 cm` and `3.2 cm`, do **not** match. Neither fuzzy
similarity nor an inferred reconstruction of masked text is an identity rule.

Select the best available priority for each study, then require uniqueness in
both directions across those candidates. Already resolved legacy reports cannot
be claimed by a new study. Ambiguous or time-distant matches remain reviewable.
`cxr_reconciliation` records the method, count, and decision. Existing NOTE text,
patient, visit, and document time stay unchanged. Original CXR text is retained
even when its header or masks differ from the linked radiology document.

### Preserve preliminary reports and addenda

A CXR file can contain `WET READ`, provisional findings, addenda, and their
versions before its final report. When the final report has a resolved RR link,
partition the original at the last standalone `FINAL REPORT` heading:

- `FINAL` includes that heading and the rest of the report, and reuses the RR.
- `PREFIX` retains every preceding character, including original headers,
  line breaks, separator lines, timestamps and version markers.
- If the entire normalized prefix is an exact existing AR linked to that RR
  through `parent_note_id`/`addendum_note_id`, reuse the AR as well. Only wrapper
  headings and standalone separator lines are removed for that comparison.
- Otherwise load the prefix unchanged as `CXR_SUPPLEMENT`, with a title that
  explicitly identifies supplementary preliminary/addendum text. Its source ID
  is `<subject_id>-CXR-<study_id>-PREFIX`. It is a source document component,
  not an assertion of a new clinical event or a final physician diagnosis.

A prefix can contain multiple versions; they are preserved together. Do not
invent signing times or a sequence of newly dated clinical events. A prefix
that differs from an existing AR remains a separately identified source
variant, not a silent replacement of that AR.

For an eligible new CXR report, preserve the **whole original file** as one
`REPORT`, including any prefix. Unresolved bundles are likewise retained whole
in the extension; their uncertain final report is not automatically inserted.

### Admit additional source reports

`NEW_NO_PATIENT_RADIOLOGY` retains the original no-radiology-for-patient rule.
`NEW_NO_NEARBY_RADIOLOGY` also admits an unmatched report if:

1. Every image timestamp is available and the image span is at most six hours.
2. No raw RR or AR for that patient falls between seven days before the first
   image and seven days after the last image.
3. No full-text, final-text, body, or mask-only-body match exists anywhere in
   that patient's radiology history, including outside the time window.

This is a reproducible source-coverage rule, not proof that no corresponding
clinical document exists elsewhere. The wider exclusion window reduces
insertion of delayed versions of existing reports. Mere lack of an exact hash
match does not qualify a report that overlaps an existing episode.

### Account for every character and link

`cxr_component` stores character offsets, component kind, and decisions.
Concatenating all components in source order must reproduce every original CXR
file exactly; staging fails if any character is missing or duplicated.
`cxr_note_component` adds component hashes, person/NOTE IDs, the final parent
NOTE for a prefix, external RR/AR ID, time basis, and source checksums. The view
`cxr_component_text` exposes the original component text, including unresolved
and missing-person cases, without duplicating the entire raw text in storage.
BigQuery uses the same names with the `ext_` prefix.

`modality_note_link` remains one row per study and points to the principal
report. Join `cxr_note_component` by `study_id` to retrieve supplements; counting
only the principal study link omits those additional notes. Missing CDM persons
remain in raw/extension records. Established NOTE and component IDs cannot be
silently changed or removed. The common NOTE ETL checks the complete text hash
and all mapped fields of every new source component.

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
| New whole CXR | `mimic-cxr\|cxr\|<subject_id>-CXR-<study_id>` |
| CXR supplement | `mimic-cxr\|cxr\|<subject_id>-CXR-<study_id>-PREFIX` |
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
and modality/component links between databases running the same revision.
Use `--sources cxr` to update only CXR after ECG has already been loaded;
the existing ECG and Echo linkage is still verified.

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
IDs, CXR ambiguity/body/mask matching, midnight boundaries, prefix reconstruction,
existing AR reuse, negation/numeric differences, machine line positions, missing persons, Echo
quarantine, empty CXR output, rollback, and idempotence. Rendering and parsing
are not execution on the BigQuery service.

Official sources: [CXR](https://physionet.org/content/mimic-cxr/2.1.0/),
[ECG](https://physionet.org/content/mimic-iv-ecg/1.0/),
[Echo](https://physionet.org/content/mimic-iv-echo/1.0.1/),
[MIMIC-IV-Note](https://physionet.org/content/mimic-iv-note/2.2/).
