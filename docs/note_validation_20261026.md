# NOTE-only validation, 2026-10-26 (KST)

Executed the PostgreSQL NOTE-only queries on the existing local CDM and its
AWS RDS copy. This report contains aggregate results only; private source
identifiers, note text and credentials are not included in the repository.

## Clinical rows and linkage

No new NOTE rows were required: all previously loaded documents remain.
Only 78,786 existing radiology notes received a previously NULL visit link
(78,190 RR and 596 AR), using a unique admission interval and actual document
time. Explicit unmatched admission IDs and ambiguous intervals remain NULL.
No rows in other CDM tables were inserted, updated or deleted by the ETL.

| NOTE source | Rows | With an existing visit |
|---|---:|---:|
| AR | 25,312 | 9,594 |
| CXR | 3,360 | 0 |
| CXR_SUPPLEMENT | 16,330 | 0 |
| CXR_VARIANT | 8,446 | 0 |
| DS | 331,761 | 331,732 |
| ECG_MACHINE | 796,616 | 0 |
| RR | 2,265,110 | 1,213,584 |

Total: **3,446,935 NOTE rows** on each destination. Every pre-existing ID,
text, date, title, concept and other descriptive field was preserved.
The local replay changed **0 rows**. Final source-to-NOTE mappings agree
between the destinations; execution-state fields such as a visit's prior
value differ between an initial update and a replay as expected.

## Remaining source limitations

- CXR: 123,218 report/prefix components reuse existing radiology notes;
  28,136 historical standalone CXR notes remain as known acquisition-date
  exceptions. Their `note_datetime` remains NULL. These exceptions are not
  certified as genuine document dates.
- CXR: 80,239 candidates lack an actual document date and remain in the raw
  source, without new NOTE insertion. Another 12,829 studies lack a matching
  PERSON. Two redundant prefix candidates already covered by existing whole
  documents are excluded from the withheld count.
- Machine ECG: 796,616 notes retained; 3,418 source studies lack PERSON and
  one report has no non-whitespace text. Unsynchronized device time alone
  does not assign a visit.
- Echo: 7,228 metadata rows audited, with no narrative body and a source
  checksum mismatch. No Echo NOTE was fabricated.

## Verification

- 19 isolated integration/runner tests passed.
- All 15 full-table NOTE structural, vocabulary and FK checks passed with
  zero violations on both databases. CXR date exceptions are reported above
  separately from these checks.
- Per-transaction PostgreSQL counters confirmed that NOTE was the only
  permanent table receiving DML.
- Existing column contracts are unchanged. Both NOTE patient/date and
  visit/date indexes exist and are valid.
- The complete ordered NOTE fingerprints match, including the final visit
  values and hashes of unmodified note text:

```
b2b50e8d41c27d9d8d8f39b4dc9ca2960b9221252bc425819a2d8bab21318150
```

BigQuery workflows passed rendering tests and SQL statement/assertion parsing;
only `cdm_note` is a permanent DML target. They were **not executed on a live
BigQuery dataset**. Production execution evidence here applies to PostgreSQL.
