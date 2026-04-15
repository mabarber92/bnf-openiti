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
- **Two-script fuzzy matching.** Latin-track (normalised ALA-LC vs camel-split URI) and Arabic-track (URI converted to consonantal Arabic vs normalised BNF Arabic fields) run independently; best score wins.
- **Embeddings are for retrieval, not clustering.** The embedding stage generates candidates; fuzzy matching and human review resolve them.
- **Output is many-to-many.** One BNF record may produce multiple output rows (composite manuscripts); one OpenITI URI may match multiple BNF records (multiple manuscript copies of the same work).

### Pipeline stages

#### Stage 0 — Parse and structure
Parse BNF XML and OpenITI YML files into structured objects with consistent `None` for missing fields. Separate Arabic-script content from Latin-script content by Unicode block. Flag probable composite manuscripts heuristically (multiple `dc:creator` entries, enumerated works in description). Collect WorldCat links from version YMLs and attach to the parent book record.

#### Stage 1 — Wikidata bridge (pre-400 AH records only)
Where OpenITI author records carry a Wikidata ID and the BNF linked data carries a matching Wikidata property, use this as a high-confidence author-level link. Within confirmed author clusters, match by title.

#### Stage 2 — Two-track fuzzy matching
For every candidate pair, run both tracks and take the maximum score:
- **Latin track:** camel-split URI components → compare against diacritic-stripped BNF transliteration using token sort ratio
- **Arabic track:** URI components → best-effort consonantal Arabic (character map, digraphs first) → `normalize_ara_heavy` on both sides → token sort ratio

Output: scored candidate pairs with evidence flags indicating which fields contributed and which tracks fired.

#### Stage 3 — Embedding retrieval
Build text representations for both sides and retrieve top-k BNF candidates per OpenITI book using cosine similarity. Four representations are compared experimentally:
- (a) All fields concatenated
- (b) Latin-script fields only
- (c) Arabic-script fields only
- (d) URI decomposition only (the minimal-metadata case)

Primary model: **BGE-M3** (multilingual, strong Arabic support). Comparison model: `paraphrase-multilingual-mpnet-base-v2`. Embeddings are cached after first computation. At 9k × 32k scale, brute-force cosine is sufficient; FAISS Flat index used for learning the API.

Evaluation metric: **Mean Reciprocal Rank (MRR@10)** against known pairs from `data_samplers/correspondence.json`.

#### Stage 4 — Score fusion and output
Merge candidates from Stages 1–3, deduplicate, and emit a row per (BNF record, OpenITI URI) pair:

```
bnf_id          str     — filename stem of the BNF XML
openiti_uri     str     — book-level URI (e.g. 0685NasirDinBaydawi.AnwarTanzil)
confidence      float   — fused score, 0–1
match_source    list    — stages and tracks that contributed
evidence        dict    — per-field signal flags
signal_count    int     — number of fields that provided signal
relation_type   str     — "direct" | "partial_commentary" | "parent_of_matched"
requires_review bool    — flagged for human validation
```

---

## Module architecture

The pipeline is built from independent, reusable modules. Parsers in particular are designed to be bolted into other pipelines without modification.

```
bnf_openiti/
├── parsers/
│   ├── openiti.py      OpenITIYml, OpenITIMetaYmls + data classes
│   └── bnf.py          BNFXml, BNFMetadata + BNFRecord dataclass
├── normalize/
│   ├── latin.py        normalize_latin(), camel_split()
│   └── arabic.py       normalize_arabic(), uri_to_arabic()
├── match/
│   └── fuzzy.py        FuzzyMatcher
├── embed/
│   └── embedder.py     Embedder, EmbeddingIndex
└── pipeline.py         Orchestration
```

### Parser classes

**`OpenITIYml(path)`** — parses a single OpenITI YML file. Detects its type (author / book / version) from the number of dot-separated components in the filename stem, then returns the appropriate typed dataclass (`OpenITIAuthorData`, `OpenITIBookData`, `OpenITIVersionData`). All fields are `Optional`; placeholder template text is treated as absent.

**`BNFXml(path)`** — parses a single BNF OAI-PMH XML file and returns a `BNFRecord` dataclass. Arabic-script and Latin-script content are separated by Unicode block regex and stored in distinct fields. Some data will be stored in multiple fields (e.g. a title appearing in both `dc:title` and `dc:description`) — this is intentional; downstream stages decide what to use.

**`OpenITIMetaYmls(directory)`** — walks a directory, instantiates `OpenITIYml` for each file, and indexes results into three dicts keyed by URI: `.authors`, `.books`, `.versions`. Exposes helper methods: `get_book(uri)`, `get_author_for_book(book_uri)`, `get_worldcat_links(book_uri)`.

**`BNFMetadata(directory)`** — walks a directory tree of XML files, instantiates `BNFXml` for each, and indexes as `.records` keyed by BNF ID. Exposes `get(bnf_id)` and iteration.

### Normaliser functions (stateless, importable independently)

- `normalize_latin(text)` — strips ALA-LC diacritics (NFD decomposition → remove combining characters), lowercases, collapses whitespace
- `camel_split(slug)` — splits a URI camel-case component into word tokens (`NasirDinBaydawi` → `["Nasir", "Din", "Baydawi"]`)
- `normalize_arabic(text)` — wraps `openiti.helper.ara.normalize_ara_heavy`: strips harakat, normalises alef variants, normalises hamza, normalises tāʾ marbūṭa
- `uri_to_arabic(tokens)` — maps camel-split URI tokens to consonantal Arabic using a character-level table (digraphs: Sh→ش, Th→ث, Kh→خ, Dh→ذ; singles: B→ب, D→د, etc.). Emphatic ambiguity (ص/س, ض/د) is absorbed by fuzzy matching downstream.

### Match and embed classes
Defined in their respective modules; take parsed collection objects as inputs. See module docstrings for interface.

---

## TODO

### Phase 1 — Parsers and normalisers
- [ ] Implement `OpenITIYml`: type detection, raw YML parser (check `openiti.helper.yml` first), typed dataclass output, placeholder-value filtering
- [ ] Implement `BNFXml`: XML parsing, field extraction with `None` fallback, Arabic/Latin Unicode split, composite manuscript heuristic, `signal_count` computation
- [ ] Implement `OpenITIMetaYmls`: directory walk, indexing, helper methods, WorldCat link attachment from version YMLs
- [ ] Implement `BNFMetadata`: directory walk, XML discovery, indexing
- [ ] Implement `normalize_latin()` and `camel_split()` in `normalize/latin.py`
- [ ] Implement `normalize_arabic()` wrapper and `uri_to_arabic()` character map in `normalize/arabic.py`
- [ ] Validate all parsers and normalisers against the known pair in `data_samplers/`

### Phase 2 — Fuzzy matching
- [ ] Implement `FuzzyMatcher`: two-track scoring (Latin + Arabic), evidence dict, max-score fusion
- [ ] Tune score thresholds against known pair; extend ground-truth set if possible
- [ ] Implement Wikidata bridge for pre-400 AH records

### Phase 3 — Embeddings
- [ ] Set up BGE-M3 embedding pipeline with output caching (avoid recomputation)
- [ ] Define and build the four text representations per side (all-fields, Latin-only, Arabic-only, URI-only)
- [ ] Implement cosine similarity retrieval (brute-force numpy + FAISS Flat for comparison)
- [ ] Implement MRR@10 evaluation against known pairs
- [ ] Run paraphrase-multilingual-mpnet as comparison model; record results

### Phase 4 — Pipeline and output
- [ ] Implement score fusion across stages with provenance tracking
- [ ] Handle composite manuscript expansion (one BNF ID → multiple output rows)
- [ ] Implement output serialisation to correspondence format
- [ ] End-to-end pipeline run on full dataset
- [ ] Manual review of sample output; threshold adjustment

---

## Configuration

Copy `config_template.yml` to `config.yml` and populate paths:

```yaml
bnf_data_path: /path/to/bnf/xml/directory
openiti_data_path: /path/to/openiti/yml/directory
```

`config.yml` is gitignored.
