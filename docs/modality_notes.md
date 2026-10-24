# Modality notes: availability and ETL feasibility

Checked against the official releases on 2026-10-25. The implemented NOTE
workflow currently supports MIMIC-IV-Note discharge and radiology. The adapters
below are feasibility proposals; they have not been executed by this workflow.

| Dataset | Text available in the current release | Feasible destination |
| --- | --- | --- |
| MIMIC-CXR 2.1.0 | Radiologist reports in study-level `.txt` files; a reports-only archive is also documented | NOTE, after checking overlap with existing radiology notes |
| MIMIC-IV-ECG 1.0 | Machine interpretation lines in `machine_measurements.csv`, `report_0` through `report_17` | A separately identified machine-generated NOTE source |
| MIMIC-IV-ECG 1.0, cardiologist interpretation | `waveform_note_links.csv` supplies document identifiers, not the report body | No direct physician-report NOTE load from this release |
| MIMIC-IV-Echo 1.0.1 | Structured measurements and report-link metadata; physician narratives are described as a future release | MEASUREMENT/appropriate structured domains now; physician NOTE after the actual narrative is available |

## CXR

The [official CXR description](https://physionet.org/content/mimic-cxr/2.1.0/)
defines one report per study, even when several images belong to that study.
Use `subject_id`, `study_id`, and the report path as provenance. A study ID is
not an admission ID. Retrieve a reliable report/study timestamp from the
corresponding metadata and document what that timestamp represents.

First reconcile reports with MIMIC-IV-Note radiology. Possible overlap is a
deduplication question, not a reason to assume identical text or identifiers.
Reuse an existing NOTE when document identity is established, and preserve a
study-to-note crosswalk. Otherwise retain the unresolved candidate relationship
for review. Do not create one NOTE for every DICOM image.

## ECG

The [current ECG release](https://physionet.org/content/mimic-iv-ecg/1.0/)
explicitly removed physician report text; older 0.3 documentation mentioning
`reports.csv` does not establish its availability in 1.0. Public link identifiers
cannot substitute for missing text. MIMIC-IV-Note 2.2 does not contain an ECG
report table.

For machine text, retain all 18 original fields and their order, including empty
positions, before constructing a derived document. Record machine authorship
and a distinct source namespace. Keep numeric ECG parameters in structured
tables. The release cautions that ECG machine clocks may be unsynchronized;
do not infer admissions from those timestamps alone.

## Echo

The [current Echo release](https://physionet.org/content/mimic-iv-echo/1.0.1/)
contains `structured_measurement.csv` and study/record lists. It states that
cardiologist narratives will be released later. `note_id`, `note_seq`, and
`note_charttime` in the study list are linkage metadata. A two-day temporal link
is not proof of a hospitalization or an available report body.

Structured measurement mapping is a separate ETL extension. Do not manufacture
a physician narrative from measurement columns or load a document containing
only a link identifier into NOTE.

## Shared mapping and validation requirements

Reuse existing PERSON identifiers through `person_source_value = subject_id`.
Keep visit identifiers NULL unless a reliable source admission/crosswalk exists;
never interpret a modality study ID as `hadm_id`. Missing persons remain in raw
data and rejection metadata.

Use a distinct source namespace and permanent document crosswalk when extending
the stable NOTE identifier rule. Validate a suitable standard Note-domain
document concept in the installed vocabulary. Preserve raw UTF-8 text/fields,
source checksums, document counts, missing links, and duplicate relationships.
Run the same field-level QA on both database targets.

The [MIMIC-IV-Note 2.2 release](https://physionet.org/content/mimic-iv-note/2.2/)
contains only discharge, discharge_detail, radiology, and radiology_detail.
Link tables from another modality do not add a report body to those tables.
Dataset access must be established for the chosen release before execution.
