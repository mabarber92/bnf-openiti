# BNF–OpenITI Matching Pipeline

Match digitised Arabic-script manuscripts in the Bibliothèque nationale de France (BNF) Gallica catalogue against work-level records in the OpenITI corpus, producing candidate pairs for annotator review.

The pipeline also addresses a research question: how much of the OpenITI corpus is represented in BNF holdings, and how much of the BNF Arabic-script collection has no OpenITI text to compare against?

---

## Data sources

### BNF (Gallica OAI-PMH, Dublin Core XML)
- ~32,000 Arabic-script manuscript records (Arabic, Persian, Ottoman Turkish, other)
- Key fields: `dc:title`, `dc:creator`, `dc:description` (multiple, unstructured), `dc:subject`, `dc:source` (shelfmark), `dc:date` (manuscript copy date)
- Both Arabic-script and ALA-LC transliteration appear across fields, sometimes in the same record
- Metadata quality varies significantly: some records are fully described, others have only a title or author
- Composite manuscripts (majāmiʿ) are common: one BNF record may contain multiple works → one BNF ID can legitimately match multiple OpenITI URIs

### OpenITI (custom YAML metadata)
- ~9,000 book-level records; this pipeline targets the **book YML** only (not author or version YMLs, except to pull WorldCat links from version YMLs)
- URI structure encodes the primary matching signal: `{death_year_AH}{AuthorSlug}.{TitleSlug}` — camel-case, no diacritics, emphatics merged with plain equivalents (ص → s, ض → d, etc.)
- Metadata completeness is variable: many records have only the URI; TITLEA/TITLEB and external IDs are populated inconsistently
- Wikidata IDs are reliably populated for authors who died before 400 AH; sparse thereafter
- No VIAF links or manuscript links exist in current data
- Book relations (RELATED field) are partially populated — used only where clearly present

---

## Matching approach

### Key design principles
- **Every field is Optional.** No record is dropped for missing data; it receives fewer signals and a lower confidence ceiling.
- **URI decomposition is the primary structured signal.** Even for records with no additional metadata, the URI encodes death year, author slug, and title slug.
- **Two-script fuzzy matching.** Latin-track (normalised ALA-LC vs camel-split URI) and Arabic-track run independently; best score wins.
- **Output is many-to-many.** One BNF record can legitimately match multiple OpenITI books (composite manuscripts / majāmiʿ); one OpenITI URI can match multiple BNF records (multiple manuscript copies of the same work).

### Pipeline stages

#### Stages 1–2 — OpenITI preparation (one-off per corpus version)
Parse OpenITI YML files into structured JSON (`data/openiti_<version>.json`), then enrich with Wikidata metadata for authors who died before 400 AH.

#### Stages 3–5 — Manuscript library preparation
Survey the BNF corpus to build a boilerplate map and diacritic conversion table (Stage 3), apply manual review of the boilerplate list (Stage 4), then parse all BNF XML into `outputs/bnf_parsed.json` (Stage 5).

#### Stage 6 — Fuzzy matching pipeline
Three-stage filtered fuzzy matching with token-level IDF weighting:

1. **Author matching** — BNF creator fields vs OpenITI author name variants. Two-tier IDF: permissive entry threshold (allows common-name authors through for title disambiguation), precision scoring (rare tokens only, so common-name-only matches fail Stage 3).
2. **Title matching** — BNF title fields vs OpenITI book title variants. Continuous IDF boost on rare title tokens.
3. **Combined scoring** — forms (author, book) pairs and applies floor checks + normalised weighted threshold (`0.3 × author_norm + 0.7 × title_norm ≥ 0.94`).

Validated at **100% precision** on the correspondence test set. See [PIPELINE.md Stage 6](PIPELINE.md#stage-6--matching-pipeline) and [matching/README.md](matching/README.md) for full design documentation.

#### Stage 7 — Clustering and resolution
Not yet implemented. Will handle multi-match resolution and manual review assignment.

---

## Module architecture

```
bnf_openiti/
├── parsers/
│   ├── openiti.py          OpenITI YML → typed dataclasses (author/book/version)
│   └── bnf.py              BNF OAI-PMH XML → BNFRecord dataclass
├── matching/
│   ├── config.py           All tunable thresholds and parameters
│   ├── pipeline.py         Stage orchestration
│   ├── author_matcher.py   Stage 1: two-tier IDF author matching
│   ├── title_matcher.py    Stage 2: continuous IDF title matching
│   ├── combined_matcher.py Stage 3: normalised weighted combined scoring
│   ├── classifier.py       Stage 4: confidence tier assignment
│   ├── normalize.py        Shared normalisation (diacritics, CamelCase, conversions)
│   └── scripts/            Validation and debugging scripts
├── utils/
│   ├── parse_openiti.py    Stage 1 runner
│   ├── enrich_wikidata.py  Stage 2 runner
│   ├── survey_bnf.py       Stage 3 runner
│   └── parse_bnf.py        Stage 5 runner
└── run_matching_pipeline.py  Stage 6 CLI entry point
```

### Parser classes

**`parsers/openiti.py`** — parses a single OpenITI YML file, detects type (author / book / version), and returns the appropriate typed dataclass. All fields are `Optional`; placeholder template text is treated as absent.

**`parsers/bnf.py`** — parses a BNF OAI-PMH XML file and returns a `BNFRecord` dataclass. Arabic-script and Latin-script content are separated by Unicode block into distinct fields. A record may carry the same data in multiple fields (e.g. title in both `dc:title` and `dc:description`) — downstream stages decide what to use.

### Normaliser (`matching/normalize.py`)

`normalize_for_matching(text, split_camelcase, is_openiti)` — the single entry point used by all matching stages:
1. Apply table-driven diacritic conversions (if `USE_DIACRITIC_CONVERSION_TABLE = True`)
2. Split CamelCase if `split_camelcase=True` (used for OpenITI URI slugs, not BNF fields)
3. NFD Unicode decomposition + remove combining characters
4. Lowercase and collapse whitespace

---

## Status

Stages 1–6 are complete. The matching pipeline (Stage 6) has been validated on a curated test set of 16 BNF–OpenITI pairs (including composite manuscripts) achieving 100% precision, and runs on the full BNF corpus (~7,800 records) in ~20s with parallelization.

Stage 7 (clustering and resolution of multi-match and ambiguous cases) is not yet implemented.

## Next steps

- [ ] Run Stage 6 on the full BNF corpus and review output distribution
- [ ] Manual spot-check of `matches_high_confidence.json` for systematic error patterns
- [ ] Implement Stage 7: cluster multi-match records, route ambiguous cases to review
- [ ] Investigate embedding-based retrieval as a recall-improvement layer for author-only records

---

## Configuration

Copy `config_template.yml` to `config.yml` and populate paths:

```yaml
bnf_data_path: /path/to/bnf/xml/directory
openiti_data_path: /path/to/openiti/yml/directory
```

`config.yml` is gitignored.
