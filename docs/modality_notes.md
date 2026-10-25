# Modality NOTE rules

The active implementation uses the same shared NOTE mapping for discharge,
radiology, CXR and machine ECG. Only the source adapters differ. See
[note_etl.md](note_etl.md) for execution and visit rules.

| Modality | Source | NOTE handling |
|---|---|---|
| Discharge | `discharge.text`, original `note_id`, `hadm_id`, `charttime` | One DS/AD document per original note; explicit admission linkage |
| Radiology | `radiology.text`, original `note_id`, `hadm_id`, `charttime` | One RR/AR document per original note; explicit admission or unique document-time interval |
| CXR | `cxr_reports`, image acquisition metadata, raw radiology and detail | Reuse a confirmed existing report; retain original differences; require a true document date for any new NOTE |
| Machine ECG | `ecg_machine_measurements.report_0` … `report_17`, `ecg_time` | One machine interpretation per study, including all ordered blank slots |
| Echo | `echo_study_list` | References only; no report body, so no NOTE insertion |

## CXR

The adapter uses the entire report or its complete final section. It strips
only a leading `FINAL REPORT` marker and collapses formatting whitespace for
comparison. It compares the full normalized strings after hash lookup;
clinical-body similarity and removed deidentification masks are **not** identity
rules. Candidate pairs must be for the same patient within 48 hours of the
study acquisition interval (the interval itself must be complete and no longer
than six hours), and unique in both directions at the strongest matching level.
Time alone never establishes identity. The existing NOTE text must also equal
the raw radiology text before it can be reused.

When a complete final section matches an RR, preceding source text remains a
separate prefix. Only an identical addendum explicitly linked to that same RR
can reuse an AR. Addendum comparison also ignores its leading heading and
separator-only underscore lines; it preserves all clinical content for comparison
and never changes the stored source text. Otherwise retain the complete prefix
for a CXR_SUPPLEMENT candidate.
A source with no known radiology report for that patient is a CXR candidate.
Other unmatched/ambiguous source text remains a CXR_VARIANT candidate; there is no
unmasking, summarization, body-only merge or fuzzy text deletion. Existing
whole CXR documents and prefixes keep their previous CXR/CXR_VARIANT/
CXR_SUPPLEMENT identity, body and title even if the stricter rule changes a
former duplicate decision.

**Required document date:** CXR metadata supplies image acquisition time, not
report recording time. The current CXR input has no reliable standalone
document date. Therefore a newly unmatched whole report or prefix is audited
as `MISSING_DOCUMENT_DATE`, with its original text still in the raw table;
it is not inserted using an acquisition date. A confirmed radiology alias
uses the existing RR/AR NOTE's actual date and visit. A final report's date
cannot be inherited by an unmatched preliminary/addendum prefix.

There are 28,136 historical standalone CXR NOTE rows in the deployment being
reconciled. Their acquisition-date proxies and NULL `note_datetime` are kept
under `LEGACY_DATE_PROXY_RETAINED`, not silently declared valid document dates
or deleted. These rows remain a known data-quality exception. The current
strict workflow, when run on an empty NOTE target, intentionally cannot
recreate those rows from acquisition metadata alone.

All current decisions and target NOTE IDs are available in the external audit.
`note_id` in that file is the deterministic candidate ID; **`target_note_id`**
is populated only for an existing/loaded NOTE or confirmed RR/AR alias. A
withheld candidate is not a loaded NOTE. This crosswalk is a private ETL
artifact, not an OMOP extension table. Old permanent mapping tables are
historical snapshots and should not be used as current-run results.

## Machine ECG

Concatenate `report_0` through `report_17` in order, substituting an empty
string for NULL and separating every position with a newline. Keep all 18
positions, including blank and trailing slots; do not trim or summarize the
result. Different studies with identical text remain distinct documents.
A report with no non-whitespace text is accounted for without creating NOTE.

Use the supplied `ecg_time` as the source machine interpretation time and
identify its basis as `DEVICE_CLOCK_UNSYNCHRONIZED` in the audit. Do not shift
that time to force a visit match. Neither a unique time overlap nor a nearby
ICD/procedure timestamp proves the encounter. Machine ECG currently has no
verified encounter key, so new notes have NULL visits and provider IDs.

`waveform_note_links.note_id` identifies physician EK notes, not these machine
interpretations. The links do not provide physician narrative text or make
the machine and physician notes interchangeable. No waveform or quantitative
measurement is loaded by this NOTE workflow.

## Echo

The available study list has study, measurement and EC-note references but no
narrative report body. Do not synthesize prose from ejection fraction or
create an empty placeholder NOTE. The current downloaded list additionally
has `CHECKSUM_MISMATCH`, so its references are quarantined metadata. The audit
reports both conditions separately; approving or fixing a checksum alone
would still not supply a report body.

A future Echo adapter needs verified original text, actual document date and
an existing PERSON. Use the original EC note ID as document identity, so
multiple studies referencing one report do not create duplicate notes.
Only explicit encounter linkage or a reliable document-time rule may assign
a visit. Supporting that future source is a separate input-contract change;
this release makes no fabricated Echo notes and adds no other CDM rows.

## Information preservation and limits

Missing PERSON prevents insertion without creating synthetic patients. Missing
visit alone never removes an otherwise valid NOTE. Raw reports, all `_detail`
rows, study IDs, provider metadata, source times, original report references
and waveform/image identifiers remain available in their original tables/files.
They are not embedded as metadata in the clinical narrative.

Under the requested NOTE-only scope, OMOP has no dedicated study ID column or
new study/procedure rows. Study/image/waveform traversal uses the retained raw
sources and external audit. Standard NOTE patient/date and visit/date indexes
support searches through existing PERSON and VISIT_OCCURRENCE. This preserves
usable text and lineage without claiming that all source studies can be
represented fully inside the unextended NOTE table.
