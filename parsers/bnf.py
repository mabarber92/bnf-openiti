"""
parsers/bnf.py

Parse BNF Gallica OAI-PMH XML records (Dublin Core) into structured data objects.

Classes:
    BNFRecord    — dataclass holding all extracted fields (all Optional except bnf_id)
    BNFXml       — parses a single OAI_*.xml file into a BNFRecord
    BNFMetadata  — walks a directory, loads all OAI_*.xml files, indexes by ID

Design notes (informed by surveying the full 7,825-record dataset):

dc:title
    - Not always single: max 4 per record, avg 1.16.
    - Raw strings are stored as-is in title_lat / title_ar — no structural
      transformation is applied at parse time. Many values follow the pattern
      "AUTHOR_NAME. Title text"; BNFRecord.matching_data() splits these on
      '. ' to surface all parts as separate matching candidates.
    - Arabic and Latin versions of the same title appear as separate dc:title
      elements, so the same split-by-script treatment as dc:creator applies.

dc:creator
    - Only 74.3% coverage — never assume it is present.
    - Names often include parenthetical dates: "Name (1100?-1165?). Role"
    - Max 89 per record (composite manuscripts).
    - Dates and role suffixes are stripped from all entries.

dc:description
    - Always present (100%), avg 6 per record, max 146.
    - 34.9% of records have mixed-script descriptions.
    - Short strings (≤ TITLE_FROM_DESC_MAX_LEN chars) where fewer than
      MIN_REMAINING_TOKENS tokens survive after marking boilerplate n-gram
      coverage are excluded.  Use load_boilerplate_file() (production) or
      load_boilerplate_ngrams() (experimentation) to populate BOILERPLATE_NGRAMS
      before bulk parsing.

matching_data()
    - BNFRecord.matching_data() returns {"lat": [...], "ar": [...]} —
      deduplicated lists of strings ready to query against OpenITI, keyed by
      script so downstream stages can apply the appropriate matching strategy
      (Latin fuzzy / Arabic fuzzy / embedding).  Includes: all title parts
      (split on '. '), creator names, and description_candidates.

dc:subject
    - 60.8% coverage, multi-value (avg 1.54, max 24) — stored as list.

dc:contributor
    - 38.7% coverage, avg 2.23 per record, max 29.
    - Encodes previous owners and other non-author contributors.
    - All sampled values carry a role suffix ("Ancien possesseur" in every
      sample); the same _ROLE_RE / _DATES_RE cleaning applied to dc:creator
      is reused here.
    - 87.5% Latin-only, 11.7% mixed — split by script like creator.
    - Not counted in signal_count (provenance metadata, not text-matching
      signal).

dc:format
    - 90.8% coverage, avg 1.0 per record, max 2.
    - Contains physical description: script style (Naskhi, Maghribi, …),
      material, leaf count, dimensions, decoration notes.
    - 99.8% Latin-only; stored as a raw list without further parsing.
    - Not counted in signal_count.

dc:coverage
    - Rare (0.8%, 59 records) — captured so it is not silently discarded.

Relation detection
    - BNFXml accepts an optional relation_terms dict mapping search patterns
      to relation type labels (e.g. {"commentaire sur": "commentary"}).
    - When a pattern is found in any description or title text, the matched
      term and a short context window are stored in detected_relations.
    - Default is an empty dict — populate after surveying the corpus for
      actual patterns (n-gram analysis will surface them).
    - Matching is case-insensitive across both Latin and Arabic script terms.

Composite detection
    - Flagged when more than one distinct Latin-script creator is present.
    - Pattern-based heuristics are not used until confirmed by survey data.
"""

from __future__ import annotations

import json as _json
import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)

# Ensure project root is on sys.path when parsers/ is used outside the package.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.tokens import (  # noqa: E402
    has_arabic   as _has_arabic,
    tokenize_lat as _tokenize_lat,
    tokenize_ar  as _tokenize_ar,
)

# ---------------------------------------------------------------------------
# XML namespace map
# ---------------------------------------------------------------------------
NS = {
    "oai":    "http://www.openarchives.org/OAI/2.0/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "dc":     "http://purl.org/dc/elements/1.1/",
}

# ---------------------------------------------------------------------------
# Creator name cleaning
# Role suffixes and parenthetical dates are stripped to leave the name only.
# ---------------------------------------------------------------------------
_ROLE_RE = re.compile(
    r"\.\s*("
    r"Auteur du texte|Copiste|Ancien possesseur|Traducteur"
    r"|Enlumineur|Relieur|Annotateur|Libraire|Destinataire"
    r")\s*$",
    re.IGNORECASE,
)
_DATES_RE = re.compile(r"\s*\(\d{3,4}\??(?:\s*[-\u2013]\s*\d{3,4}\??)?\)\s*")

# ---------------------------------------------------------------------------
# Description → title promotion
# ---------------------------------------------------------------------------

# Maximum length (chars) for a description string to be considered a title
# candidate.
TITLE_FROM_DESC_MAX_LEN: int = 100

# Minimum uncovered tokens required for a description string to be included
# as a matching candidate after boilerplate n-grams are marked out.
MIN_REMAINING_TOKENS: int = 3

# N-grams (any size 2–4) whose presence marks a description string as
# structural boilerplate.  Populated by calling load_boilerplate_ngrams()
# or load_boilerplate_file() before bulk parsing; empty by default (all
# short descriptions promoted, no filtering).
BOILERPLATE_NGRAMS: frozenset[str] = frozenset()


def load_boilerplate_ngrams(
    ngrams_path: str,
    min_doc_freq_pct: float = 5.0,
    max_repeats_per_doc: float = 1.4,
) -> frozenset[str]:
    """Derive a boilerplate n-gram set from a raw vocabulary file (ngrams.json).

    Applies two criteria in intersection:
      - doc_freq / files_parsed >= min_doc_freq_pct / 100
        (n-gram appears in enough records to be structural)
      - term_freq / doc_freq <= max_repeats_per_doc
        (n-gram appears roughly once per record — template text, not content)

    True boilerplate (digitisation notices) has doc_freq_pct ~100 % and
    repeats_per_doc ~1.0.  Name fragments that slip through have higher
    repeats_per_doc values (same author appears many times per record).

    Parameters
    ----------
    ngrams_path : str
        Path to ngrams.json produced by ``survey_bnf.py build``.
    min_doc_freq_pct : float
        Minimum percentage of records an n-gram must appear in.
    max_repeats_per_doc : float
        Maximum average occurrences per record.

    Typical use before bulk parsing::

        import parsers.bnf as bnf
        bnf.BOILERPLATE_NGRAMS = bnf.load_boilerplate_ngrams("outputs/bnf_survey/ngrams.json")
        metadata = BNFMetadata(directory)
    """
    with open(ngrams_path, encoding="utf-8") as fh:
        data = _json.load(fh)

    n_docs = data.get("files_parsed", 0)
    if n_docs == 0:
        return frozenset()

    boilerplate: set[str] = set()
    for script_data in data["ngrams"].values():
        for size_data in script_data.values():
            for row in size_data["by_doc_freq"]:
                df      = row["doc_freq"]
                tf      = row["term_freq"]
                df_pct  = 100 * df / n_docs
                repeats = tf / df if df > 0 else float("inf")
                if df_pct >= min_doc_freq_pct and repeats <= max_repeats_per_doc:
                    boilerplate.add(row["ngram"])

    return frozenset(boilerplate)


def load_boilerplate_file(boilerplate_path: str) -> frozenset[str]:
    """Load a curated boilerplate list from boilerplate.json.

    boilerplate.json is a flat JSON array of strings produced by
    ``survey_bnf.py apply-review`` after manual review of the CSV.
    Use this in production runs; use load_boilerplate_ngrams() for
    threshold experimentation.

    Typical use::

        import parsers.bnf as bnf
        bnf.BOILERPLATE_NGRAMS = bnf.load_boilerplate_file(
            "outputs/bnf_survey/boilerplate.json"
        )
        metadata = BNFMetadata(directory)
    """
    with open(boilerplate_path, encoding="utf-8") as fh:
        items = _json.load(fh)
    return frozenset(items)


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _clean_creator(text: str) -> str:
    """Strip role suffix and parenthetical dates from a dc:creator value."""
    text = _ROLE_RE.sub("", text)
    text = _DATES_RE.sub(" ", text)
    return text.strip().rstrip(",").strip()


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class DetectedRelation:
    """A single relation detected in the record's text fields.

    Populated when a pattern from relation_terms matches any description
    or title element. context gives a short window of surrounding text
    so downstream stages can attempt to extract the related work's name.
    """
    relation_type: str           # e.g. "commentary", "abridgement"
    matched_term:  str           # the pattern that triggered the match
    context:       str           # surrounding text (up to 120 chars)
    source_field:  str           # which DC field the match was found in


@dataclass
class BNFRecord:
    """All fields extracted from a single BNF OAI-PMH XML record.

    Fields that appear in multiple scripts are stored as separate Latin and
    Arabic lists. All fields except bnf_id are Optional or default to empty
    lists. signal_count is a rough confidence weight for downstream stages.
    """
    bnf_id:             str                          # e.g. "OAI_11000434"

    # URLs
    gallica_url:        Optional[str] = None
    catalogue_url:      Optional[str] = None         # archivesetmanuscrits link

    # Title — raw dc:title strings, split by script
    title_lat:          list[str] = field(default_factory=list)
    title_ar:           list[str] = field(default_factory=list)

    # Creator (authors of the text), dates and role suffixes stripped
    creator_lat:        list[str] = field(default_factory=list)
    creator_ar:         list[str] = field(default_factory=list)

    # Description, split by script (full text, some boilerplate)
    description_lat:    list[str] = field(default_factory=list)
    description_ar:     list[str] = field(default_factory=list)

    # Short non-boilerplate description strings promoted as matching candidates
    # (potential titles or author names embedded in dc:description)
    description_candidates: list[str] = field(default_factory=list)

    # Contributor (previous owners, copyists, etc.), dates and role suffixes stripped
    contributor_lat:    list[str] = field(default_factory=list)
    contributor_ar:     list[str] = field(default_factory=list)

    # Physical description (script style, material, dimensions, leaf count)
    format_raw:         list[str] = field(default_factory=list)

    # Other fields
    subject:            list[str] = field(default_factory=list)
    coverage:           list[str] = field(default_factory=list)
    shelfmark:          Optional[str] = None
    copy_date_raw:      Optional[str] = None   # original dc:date string
    date_from:          Optional[int] = None   # parsed year range start
    date_to:            Optional[int] = None   # parsed year range end (None = single year)
    language:           list[str] = field(default_factory=list)

    # Flags and derived data
    is_composite:       bool = False
    detected_relations: list[DetectedRelation] = field(default_factory=list)
    signal_count:       int = 0

    def matching_data(self) -> dict[str, list[str]]:
        """Return all strings useful for OpenITI matching, keyed by script.

        Returns {"lat": [...], "ar": [...]} — deduplicated lists ready to
        query or embed against OpenITI data.

        Each list includes:
        - All parts of dc:title strings, split on '. ' (surfaces both the
          embedded author-name prefix and the actual title as separate items)
        - dc:creator names (dates and role suffixes already stripped)
        - description_candidates (short non-boilerplate description strings)

        Use "lat" for Latin-script fuzzy / embedding matching and "ar" for
        Arabic-script matching.
        """
        lat: list[str] = []
        ar:  list[str] = []
        seen_lat: set[str] = set()
        seen_ar:  set[str] = set()

        def add(s: str, dest: list[str], seen: set[str]) -> None:
            s = s.strip().rstrip(".")
            if s and s not in seen:
                seen.add(s)
                dest.append(s)

        for title in self.title_lat:
            for part in title.split(". "):
                add(part, lat, seen_lat)

        for title in self.title_ar:
            for part in title.split(". "):
                add(part, ar, seen_ar)

        for name in self.creator_lat:
            add(name, lat, seen_lat)

        for name in self.creator_ar:
            add(name, ar, seen_ar)

        for cand in self.description_candidates:
            if _has_arabic(cand):
                add(cand, ar, seen_ar)
            else:
                add(cand, lat, seen_lat)

        return {"lat": lat, "ar": ar}


# ---------------------------------------------------------------------------
# Single-file parser
# ---------------------------------------------------------------------------

# Characters of context to capture around a relation match
_CONTEXT_WINDOW = 120

class BNFXml:
    """Parse a single BNF OAI-PMH XML file into a BNFRecord.

    Parameters
    ----------
    path : str
        Path to the OAI_*.xml file.
    relation_terms : dict[str, str], optional
        Mapping of {search_pattern: relation_type}. Patterns are matched
        case-insensitively against all description and title text. Populate
        this from the survey n-gram results once patterns are confirmed.

        Example:
            {
                "commentaire sur":  "commentary",
                "sharh":            "commentary",
                "شرح":              "commentary",
                "ذيل":              "continuation",
                "مختصر":            "abridgement",
            }
    boilerplate_ngrams : frozenset[str], optional
        Set of n-grams (any size 2–4) that mark a description string as
        boilerplate.  Defaults to the module-level BOILERPLATE_NGRAMS (empty
        unless load_boilerplate_ngrams() or load_boilerplate_file() has been
        called).

    Usage
    -----
        record = BNFXml("OAI_11000434.xml").record
        record = BNFXml("OAI_11000434.xml", relation_terms=MY_TERMS).record
    """

    def __init__(
        self,
        path: str,
        relation_terms: dict[str, str] | None = None,
        boilerplate_ngrams: frozenset[str] | None = None,
    ) -> None:
        self.path                 = Path(path)
        self.relation_terms       = relation_terms or {}
        self._boilerplate_ngrams  = (
            boilerplate_ngrams if boilerplate_ngrams is not None
            else BOILERPLATE_NGRAMS
        )
        self.record: BNFRecord = self._parse()

    def _parse(self) -> BNFRecord:
        tree = ET.parse(self.path)
        dc = tree.find(".//oai_dc:dc", NS)
        if dc is None:
            raise ValueError(f"No oai_dc:dc element found in {self.path}")

        elements: dict[str, list[ET.Element]] = {}
        for el in dc:
            local = _strip_ns(el.tag)
            elements.setdefault(local, []).append(el)

        def texts(field_name: str) -> list[str]:
            return [
                (el.text or "").strip()
                for el in elements.get(field_name, [])
                if (el.text or "").strip()
            ]

        def first(field_name: str) -> Optional[str]:
            vals = texts(field_name)
            return vals[0] if vals else None

        # --- Identifiers ---
        gallica_url   = first("identifier")
        catalogue_url = next(
            (t for t in texts("relation") if "archivesetmanuscrits" in t),
            None,
        )

        # --- Title ---
        title_lat, title_ar = self._split_by_script(texts("title"))

        # --- Creators ---
        cleaned = [_clean_creator(t) for t in texts("creator")]
        creator_lat, creator_ar = self._split_by_script(
            [c for c in cleaned if c]
        )

        # --- Contributors (previous owners, copyists, etc.) ---
        cleaned_contrib = [_clean_creator(t) for t in texts("contributor")]
        contributor_lat, contributor_ar = self._split_by_script(
            [c for c in cleaned_contrib if c]
        )

        # --- Descriptions ---
        desc_lat, desc_ar = self._split_by_script(texts("description"))

        # Promote short, non-boilerplate description strings as matching
        # candidates (potential titles / author names buried in dc:description).
        existing = set(title_lat + title_ar)
        description_candidates = self._desc_candidates(
            desc_lat + desc_ar, existing
        )

        # --- Other fields ---
        format_raw = texts("format")
        subject    = texts("subject")
        coverage   = texts("coverage")
        shelfmark  = self._extract_shelfmark(first("source"))
        copy_date  = first("date")
        date_from, date_to = self._parse_date_range(copy_date) if copy_date else (None, None)
        language   = texts("language")

        # --- Composite heuristic ---
        # More than one distinct Latin-script creator entry indicates multiple
        # authors. Pattern-based detection is deferred until survey data
        # confirms what composite descriptions actually look like.
        is_composite = len(creator_lat) > 1

        # --- Relation detection ---
        # Run over all text fields that may contain relation language.
        search_fields = {
            "title":       title_lat + title_ar,
            "description": desc_lat + desc_ar,
        }
        detected_relations = self._detect_relations(search_fields)

        record = BNFRecord(
            bnf_id             = self.path.stem,
            gallica_url        = gallica_url,
            catalogue_url      = catalogue_url,
            title_lat          = title_lat,
            title_ar           = title_ar,
            creator_lat        = creator_lat,
            creator_ar         = creator_ar,
            description_lat         = desc_lat,
            description_ar          = desc_ar,
            description_candidates  = description_candidates,
            contributor_lat         = contributor_lat,
            contributor_ar     = contributor_ar,
            format_raw         = format_raw,
            subject            = subject,
            coverage           = coverage,
            shelfmark          = shelfmark,
            copy_date_raw      = copy_date,
            date_from          = date_from,
            date_to            = date_to,
            language           = language,
            is_composite       = is_composite,
            detected_relations = detected_relations,
        )
        record.signal_count = self._count_signals(record)
        return record

    def _desc_candidates(
        self,
        desc_texts: list[str],
        existing: set[str],
    ) -> list[str]:
        """Promote short description strings as matching candidates.

        A string is included when:
          1. Its length is within TITLE_FROM_DESC_MAX_LEN chars.
          2. After marking every token position covered by any boilerplate
             n-gram (sizes 2–4), at least MIN_REMAINING_TOKENS uncovered
             tokens remain.

        Token coverage uses the same normalisation as the survey (tokenize_lat
        with keep_abbrev_dots=True, or tokenize_ar for Arabic text) so that
        boilerplate n-grams match reliably on both sides of the comparison.

        Strings already in *existing* are deduplicated against title_lat /
        title_ar to avoid redundant matching candidates.
        """
        boilerplate = self._boilerplate_ngrams
        seen = set(existing)
        candidates: list[str] = []

        for text in desc_texts:
            if len(text) > TITLE_FROM_DESC_MAX_LEN:
                continue

            if boilerplate:
                is_ar  = _has_arabic(text)
                tokens = _tokenize_ar(text) if is_ar else _tokenize_lat(text, keep_abbrev_dots=True)
                n_tok  = len(tokens)
                covered = [False] * n_tok

                for i in range(n_tok):
                    for size in range(2, 5):   # check bigrams, trigrams, quadgrams
                        end = i + size
                        if end > n_tok:
                            break
                        if " ".join(tokens[i:end]) in boilerplate:
                            for j in range(i, end):
                                covered[j] = True

                if sum(1 for c in covered if not c) < MIN_REMAINING_TOKENS:
                    continue

            if text not in seen:
                candidates.append(text)
                seen.add(text)

        return candidates

    @staticmethod
    def _parse_date_range(date_str: str) -> tuple[Optional[int], Optional[int]]:
        """Parse 'YYYY' or 'YYYY-YYYY' from a dc:date value.

        Returns (date_from, date_to). For a single year date_to is None.
        Unparseable strings return (None, None).
        """
        m = re.match(r"(\d{4})(?:-(\d{4}))?", date_str.strip())
        if not m:
            return None, None
        date_from = int(m.group(1))
        date_to   = int(m.group(2)) if m.group(2) else None
        return date_from, date_to

    def _detect_relations(
        self, fields: dict[str, list[str]]
    ) -> list[DetectedRelation]:
        """Search all text fields for configured relation patterns.

        Returns one DetectedRelation per match. The same term can match
        multiple times across different fields or elements.
        """
        if not self.relation_terms:
            return []

        results: list[DetectedRelation] = []
        for field_name, values in fields.items():
            full_text = " ".join(values)
            for pattern, relation_type in self.relation_terms.items():
                for m in re.finditer(re.escape(pattern), full_text, re.IGNORECASE):
                    start = max(0, m.start() - 40)
                    end   = min(len(full_text), m.end() + 80)
                    context = full_text[start:end].strip()
                    results.append(DetectedRelation(
                        relation_type = relation_type,
                        matched_term  = pattern,
                        context       = context,
                        source_field  = field_name,
                    ))
        return results

    @staticmethod
    def _split_by_script(values: list[str]) -> tuple[list[str], list[str]]:
        """Split strings into (latin_list, arabic_list) by script content.

        Mixed-script strings go to the Arabic list — Arabic content is the
        harder-to-surface signal and takes priority.
        """
        lat, ar = [], []
        for v in values:
            (ar if _has_arabic(v) else lat).append(v)
        return lat, ar

    @staticmethod
    def _extract_shelfmark(source: Optional[str]) -> Optional[str]:
        """Extract the shelfmark from dc:source.

        "Bibliothèque nationale de France. Département des Manuscrits. Arabe 631"
        → "Arabe 631"
        """
        if not source:
            return None
        parts = [p.strip() for p in source.split(".")]
        return parts[-1] if parts else None

    @staticmethod
    def _count_signals(r: BNFRecord) -> int:
        """Count populated matching-relevant fields for confidence weighting."""
        return sum([
            bool(r.creator_lat or r.creator_ar),
            bool(r.title_lat or r.title_ar),
            bool(r.description_ar),
            bool(r.description_lat),
            bool(r.subject),
            bool(r.shelfmark),
            bool(r.detected_relations),
        ])


# ---------------------------------------------------------------------------
# Collection class
# ---------------------------------------------------------------------------

class BNFMetadata:
    """Load and index all OAI_*.xml files from a BNF data directory.

    Parameters
    ----------
    directory : str
    glob : str
        File pattern relative to directory. Default matches OAI_*.xml at any depth.
    relation_terms : dict[str, str], optional
        Passed through to every BNFXml instance. Add patterns here once the
        survey n-gram analysis has confirmed what relation language exists.
    boilerplate_ngrams : frozenset[str], optional
        Passed through to every BNFXml instance.  Populate via
        load_boilerplate_file() (production) or load_boilerplate_ngrams()
        (threshold experimentation) before bulk parsing.

    Usage
    -----
        bnf = BNFMetadata("/path/to/data")
        bnf = BNFMetadata("/path/to/data", relation_terms={"commentaire sur": "commentary"})
        record = bnf.get("OAI_11000434")
        for record in bnf:
            ...
        print(bnf)
        # BNFMetadata(records=7825, failed=0)
    """

    def __init__(
        self,
        directory: str,
        glob: str = "**/OAI_*.xml",
        relation_terms: dict[str, str] | None = None,
        boilerplate_ngrams: frozenset[str] | None = None,
    ) -> None:
        self.relation_terms      = relation_terms or {}
        self._boilerplate_ngrams = boilerplate_ngrams
        self.records: dict[str, BNFRecord] = {}
        self.failed:  list[dict]           = []
        self._load(directory, glob)

    def _load(self, directory: str, glob: str) -> None:
        for path in sorted(Path(directory).glob(glob)):
            try:
                xml = BNFXml(
                    str(path),
                    relation_terms=self.relation_terms,
                    boilerplate_ngrams=self._boilerplate_ngrams,
                )
                self.records[xml.record.bnf_id] = xml.record
            except Exception as exc:
                self.failed.append({"path": str(path), "error": str(exc)})
                logger.warning("Failed to parse %s: %s", path, exc)

    def get(self, bnf_id: str) -> Optional[BNFRecord]:
        return self.records.get(bnf_id)

    def __iter__(self):
        return iter(self.records.values())

    def __len__(self) -> int:
        return len(self.records)

    def __repr__(self) -> str:
        return f"BNFMetadata(records={len(self.records)}, failed={len(self.failed)})"
