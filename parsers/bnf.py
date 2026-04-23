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
    - Raw strings are stored as-is in title_lat / title_ara — no structural
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
    - Boilerplate n-grams act as span masks: covered tokens are removed and
      contiguous uncovered runs are extracted as character-level segments
      (description_candidates).  Long descriptions that are partly boilerplate
      yield useful sub-strings rather than being discarded wholesale.
      Use load_boilerplate_file() (production) or load_boilerplate_ngrams()
      (experimentation) to populate BOILERPLATE_NGRAMS before bulk parsing.

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

from tqdm import tqdm

logger = logging.getLogger(__name__)

# Ensure project root is on sys.path when parsers/ is used outside the package.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.tokens import (  # noqa: E402
    has_arabic                 as _has_arabic,
    tokenize_lat_pos           as _tokenize_lat_pos,
    tokenize_ar_pos            as _tokenize_ar_pos,
    greedy_longest_match_scan  as _greedy_match,
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
# 654/7825 BNF XML files claim UTF-8 but store Š (0x8A) and š (0x9A) as CP1252
# single bytes.  Python's XML parser yields these as U+008A / U+009A (C1
# control characters).  Remap them to the correct Unicode codepoints.
_C1_FIX = str.maketrans({"\x8a": "\u0160", "\x9a": "\u0161"})  # Š / š

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
TITLE_FROM_DESC_MAX_LEN: int = 250

# Minimum uncovered tokens required for a description string to be included
# as a matching candidate after boilerplate n-grams are marked out.
MIN_REMAINING_TOKENS: int = 3

# N-grams (any size 2–4) whose presence marks a description string as
# structural boilerplate, keyed by DC field name.  Populated by calling
# load_boilerplate_ngrams() or load_boilerplate_file() before bulk parsing;
# empty by default (all short descriptions promoted, no filtering).
BOILERPLATE_NGRAMS: dict[str, frozenset[str]] = {}


def load_boilerplate_ngrams(
    ngrams_path: str,
    min_doc_freq_pct: float = 5.0,
    max_repeats_per_doc: float = 1.4,
) -> dict[str, frozenset[str]]:
    """Derive a boilerplate n-gram set from a raw vocabulary file (ngrams.json).

    Applies two criteria in intersection for every field found in the file:
      - doc_freq / files_parsed >= min_doc_freq_pct / 100
      - term_freq / doc_freq   <= max_repeats_per_doc

    Returns a dict keyed by field name (e.g. {"description": frozenset(...),
    "format": frozenset(...)}).

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
        bnf.BOILERPLATE_NGRAMS = bnf.load_boilerplate_ngrams(
            "outputs/bnf_survey/ngrams.json"
        )
        metadata = BNFMetadata(directory)
    """
    with open(ngrams_path, encoding="utf-8") as fh:
        data = _json.load(fh)

    n_docs = data.get("files_parsed", 0)
    if n_docs == 0:
        return {}

    result: dict[str, frozenset[str]] = {}

    # New per-field format: {"fields": {"description": {"latin": ..., "arabic": ...}, ...}}
    if "fields" in data:
        for fname, script_map in data["fields"].items():
            boilerplate: set[str] = set()
            for script_data in script_map.values():
                for size_data in script_data.values():
                    for row in size_data["by_doc_freq"]:
                        df      = row["doc_freq"]
                        tf      = row["term_freq"]
                        df_pct  = 100 * df / n_docs
                        repeats = tf / df if df > 0 else float("inf")
                        if df_pct >= min_doc_freq_pct and repeats <= max_repeats_per_doc:
                            boilerplate.add(row["ngram"])
            result[fname] = frozenset(boilerplate)
    else:
        # Legacy flat format: {"ngrams": {"latin": ..., "arabic": ...}}
        boilerplate = set()
        for script_data in data.get("ngrams", {}).values():
            for size_data in script_data.values():
                for row in size_data["by_doc_freq"]:
                    df      = row["doc_freq"]
                    tf      = row["term_freq"]
                    df_pct  = 100 * df / n_docs
                    repeats = tf / df if df > 0 else float("inf")
                    if df_pct >= min_doc_freq_pct and repeats <= max_repeats_per_doc:
                        boilerplate.add(row["ngram"])
        result["description"] = frozenset(boilerplate)

    return result


def load_boilerplate_file(boilerplate_path: str) -> dict[str, frozenset[str]]:
    """Load the curated boilerplate n-gram set from boilerplate.json.

    boilerplate.json is produced by ``survey_bnf.py apply-review`` after
    manual review of the CSV.  Format::

        {
            "boilerplate": [{"ngram": "numérisation effectuée", "field": "description"}, ...],
            "signals":     [{"ngram": "lieu de copie", "field": "description",
                             "signal_type": "agent:copyist"}, ...]
        }

    Returns a dict keyed by field name: {"description": frozenset(...), ...}
    containing only the ``boilerplate`` entries — phrases to strip entirely
    from the relevant field text.  Use load_signal_ngrams() separately to
    retrieve signal phrases for relation detection.

    Use this in production runs; use load_boilerplate_ngrams() for threshold
    experimentation against the raw ngrams.json vocabulary.

    Typical use::

        import parsers.bnf as bnf
        bnf.BOILERPLATE_NGRAMS = bnf.load_boilerplate_file(
            "outputs/bnf_survey/boilerplate.json"
        )
        metadata = BNFMetadata(directory)
    """
    with open(boilerplate_path, encoding="utf-8") as fh:
        data = _json.load(fh)

    # Legacy flat-list format
    if isinstance(data, list):
        return {"description": frozenset(data)}

    result: dict[str, set[str]] = {}
    for entry in data.get("boilerplate", []):
        if isinstance(entry, str):
            # Legacy: flat string list inside the dict
            result.setdefault("description", set()).add(entry)
        else:
            fname = entry.get("field", "description")
            result.setdefault(fname, set()).add(entry["ngram"])
    return {f: frozenset(s) for f, s in result.items()}


def load_signal_ngrams(boilerplate_path: str) -> list[dict]:
    """Load signal n-grams from boilerplate.json.

    Signal n-grams are phrases that mark a structural role or textual
    relationship rather than pure boilerplate.  Each entry has the form::

        {"ngram": "lieu de copie", "field": "description",
         "signal_type": "agent:copyist"}

    Valid signal_type values:
        agent:copyist         — phrase marks a copyist name
        agent:commentator     — phrase marks a commentator name
        agent:owner           — phrase marks a previous owner name
        relation:commentary   — phrase marks "this text IS a commentary on X"
        relation:abridgement  — phrase marks abridgement of source work X
        relation:continuation — phrase marks continuation of source work X
        date:copy             — phrase marks a copy date / colophon date

    These are used by the parser to populate BNFRecord.detected_relations
    and to annotate matching candidates in matching_data() with their
    evidential role.
    """
    with open(boilerplate_path, encoding="utf-8") as fh:
        data = _json.load(fh)
    if isinstance(data, list):
        return []
    return data.get("signals", [])


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
    title_ara:          list[str] = field(default_factory=list)

    # Creator (authors of the text), dates and role suffixes stripped
    creator_lat:        list[str] = field(default_factory=list)
    creator_ara:        list[str] = field(default_factory=list)

    # Description, split by script (full text, some boilerplate)
    description_lat:    list[str] = field(default_factory=list)
    description_ara:    list[str] = field(default_factory=list)

    # Short non-boilerplate description strings promoted as matching candidates
    # (potential titles or author names embedded in dc:description), split by script
    description_candidates_lat: list[str] = field(default_factory=list)
    description_candidates_ara: list[str] = field(default_factory=list)  # was: description_candidates_ar

    # Labels for each description candidate, keyed by relation type or None if unlabeled
    description_candidate_labels_lat: list[Optional[str]] = field(default_factory=list)
    description_candidate_labels_ara: list[Optional[str]] = field(default_factory=list)

    # Contributor (previous owners, copyists, etc.), dates and role suffixes stripped
    contributor_lat:    list[str] = field(default_factory=list)
    contributor_ara:    list[str] = field(default_factory=list)

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

    def matching_candidates(self, norm_strategy: str = "fuzzy") -> dict[str, list[str]]:
        """
        Extract matching candidates with optional normalization.

        Returns normalized, deduplicated candidates ready for fuzzy matching
        or embedding. Includes all title parts, creators, and description
        candidates across both scripts.

        Parameters
        ----------
        norm_strategy : str
            Normalization strategy: "fuzzy" (aggressive), "embedding" (moderate),
            or "raw" (no normalization).

        Returns
        -------
        dict[str, list[str]]
            {"lat": [...], "ara": [...]} with normalized, deduplicated candidates.
        """
        from utils.normalize import normalize

        lat: list[str] = []
        ara: list[str] = []
        seen_lat: set[str] = set()
        seen_ara: set[str] = set()

        def add(raw: str, script: str, dest: list[str], seen: set[str]) -> None:
            raw = raw.strip().rstrip(".")
            if not raw:
                return
            # Normalize based on strategy
            norm = normalize(raw, script, norm_strategy)
            if norm and norm not in seen:
                seen.add(norm)
                dest.append(norm)

        # Titles (split on '. ' to surface embedded author names)
        for title in self.title_lat:
            for part in title.split(". "):
                add(part, "lat", lat, seen_lat)

        for title in self.title_ara:
            for part in title.split(". "):
                add(part, "ara", ara, seen_ara)

        # Creators
        for name in self.creator_lat:
            add(name, "lat", lat, seen_lat)

        for name in self.creator_ara:
            add(name, "ara", ara, seen_ara)

        # Description candidates
        for cand in self.description_candidates_lat:
            add(cand, "lat", lat, seen_lat)

        for cand in self.description_candidates_ara:
            add(cand, "ara", ara, seen_ara)

        return {"lat": lat, "ara": ara}


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
        relation_terms: dict[str, tuple[str, str | None]] | None = None,
        boilerplate_ngrams: dict[str, frozenset[str]] | None = None,
        composite_min_creators: int = 2,
        max_desc_len:           int = TITLE_FROM_DESC_MAX_LEN,
        min_desc_tokens:        int = 1,
    ) -> None:
        self.path                    = Path(path)
        self.relation_terms          = relation_terms or {}
        self._boilerplate_ngrams     = (
            boilerplate_ngrams if boilerplate_ngrams is not None
            else BOILERPLATE_NGRAMS
        )
        self._composite_min_creators = composite_min_creators
        self._max_desc_len           = max_desc_len
        self._min_desc_tokens        = min_desc_tokens
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
                (el.text or "").strip().translate(_C1_FIX)
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

        # Extract non-boilerplate segments from description strings as matching
        # candidates (potential titles / author names buried in dc:description).
        existing = set(title_lat + title_ar)
        desc_cands_flat, desc_cands_labels = self._extract_desc_segments(desc_lat + desc_ar, existing)
        desc_cands_lat, desc_cands_labels_lat, desc_cands_ar, desc_cands_labels_ar = self._split_by_script_labeled(desc_cands_flat, desc_cands_labels)

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
        is_composite = len(creator_lat) >= self._composite_min_creators

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
            title_ara          = title_ar,
            creator_lat        = creator_lat,
            creator_ara        = creator_ar,
            description_lat              = desc_lat,
            description_ara              = desc_ar,
            description_candidates_lat   = desc_cands_lat,
            description_candidates_ara   = desc_cands_ar,
            description_candidate_labels_lat = desc_cands_labels_lat,
            description_candidate_labels_ara = desc_cands_labels_ar,
            contributor_lat         = contributor_lat,
            contributor_ara    = contributor_ar,
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

    def _extract_desc_segments(
        self,
        desc_texts: list[str],
        existing: set[str],
    ) -> tuple[list[str], list[Optional[str]]]:
        """Extract non-boilerplate segments from description strings.

        Uses a two-pass algorithm:
          1. Greedy longest-match-first signal matching (sizes 4→2)
          2. Greedy longest-match-first boilerplate matching on remaining uncovered tokens

        Between-phrase boundary extraction: each uncovered run uses the character
        positions of adjacent covered phrases to determine its bounds, capturing
        inter-token content (digits, punctuation) between marked phrases.

        Returns (candidates, labels) — parallel lists where labels[i] is the
        relation type for candidates[i], or None if unlabeled.
        """
        boilerplate = self._boilerplate_ngrams.get("description", frozenset())
        # Build signal phrase mapping: phrase (lowercased) → signal_type
        signal_map: dict[str, str] = {}
        for pattern, (signal_type, _) in self.relation_terms.items():
            signal_map[pattern.lower()] = signal_type
        signal_phrases = frozenset(signal_map.keys())

        seen = set(existing)
        candidates: list[str] = []
        labels: list[Optional[str]] = []

        for text in desc_texts:
            if not (boilerplate or signal_phrases):
                # No splitters available: include whole string up to max_desc_len
                if len(text) <= self._max_desc_len and text not in seen:
                    candidates.append(text)
                    labels.append(None)
                    seen.add(text)
                continue

            is_ar   = _has_arabic(text)
            tok_pos = _tokenize_ar_pos(text) if is_ar else _tokenize_lat_pos(text, keep_abbrev_dots=True)
            if not tok_pos:
                continue

            tokens  = [t for t, _s, _e in tok_pos]
            n_tok   = len(tokens)
            covered = [False] * n_tok
            signal_at: dict[int, str] = {}  # token_position → signal_type

            # Pass 1: Greedy signal matching (priority) — track signal types
            i = 0
            while i < n_tok:
                matched = False
                for size in range(4, 1, -1):
                    end = i + size
                    if end > n_tok:
                        continue
                    phrase = " ".join(tokens[i:end])
                    phrase_lower = phrase.lower()
                    if phrase_lower in signal_phrases:
                        for j in range(i, end):
                            covered[j] = True
                        signal_type = signal_map[phrase_lower]
                        signal_at[i] = signal_type
                        i = end
                        matched = True
                        break
                if not matched:
                    i += 1

            # Pass 2: Greedy boilerplate matching on remaining uncovered tokens
            # (use utility with skip_covered=True to avoid re-matching already-covered positions)
            _greedy_match(tokens, boilerplate, covered, skip_covered=True, case_sensitive=True)

            # Extract contiguous runs of uncovered tokens using between-phrase boundaries
            run_start: int | None = None
            for i, c in enumerate(covered):
                if not c and run_start is None:
                    run_start = i
                elif c and run_start is not None:
                    self._add_run(text, tok_pos, run_start, i, signal_at, seen, candidates, labels)
                    run_start = None
            if run_start is not None:
                self._add_run(text, tok_pos, run_start, n_tok, signal_at, seen, candidates, labels)

        return candidates, labels

    def _add_run(
        self,
        text: str,
        tok_pos: list[tuple[str, int, int]],
        run_start: int,
        run_end: int,
        signal_at: dict[int, str],
        seen: set[str],
        candidates: list[str],
        labels: list[Optional[str]],
    ) -> None:
        """Extract one uncovered-token run using between-phrase boundaries.

        Between-phrase boundary extraction:
          - char_start: end of preceding covered token (or 0 if no predecessor)
          - char_end: start of next covered token (or len(text) if no successor)

        Label assignment: find the nearest signal before and after the run.
        A run is labeled with a signal if it directly follows or precedes it
        (separated only by other covered tokens). When signals exist on both
        sides, both labels should be captured.
        """
        if run_end - run_start < self._min_desc_tokens:
            return

        # Find character boundaries from adjacent covered tokens
        if run_start > 0:
            char_start = tok_pos[run_start - 1][2]  # end of preceding covered token
        else:
            char_start = 0

        if run_end < len(tok_pos):
            char_end = tok_pos[run_end][1]  # start of next covered token
        else:
            char_end = len(text)

        # Strip leading/trailing whitespace
        while char_start < char_end and text[char_start].isspace():
            char_start += 1
        while char_end > char_start and text[char_end - 1].isspace():
            char_end -= 1

        segment = text[char_start:char_end]
        if not segment or len(segment) > self._max_desc_len:
            return
        if segment in seen:
            return

        # Label assignment: check for relation markers on either side of the run.
        # For linguistic agnosticism, scan both directions to capture relation data
        # regardless of word order or which side marks the relation structurally.
        label_before = None
        label_after = None

        # Look backward for the nearest signal (search from run_start-1 downward)
        for i in range(run_start - 1, -1, -1):
            if i in signal_at:
                label_before = signal_at[i]
                break

        # Look forward for the nearest signal (search from run_end upward)
        for i in range(run_end, len(tok_pos)):
            if i in signal_at:
                label_after = signal_at[i]
                break

        # Prefer preceding signal (typically marks context); fall back to following signal.
        # Both directions are always checked to ensure no relations are missed.
        label = label_before or label_after

        seen.add(segment)
        candidates.append(segment)
        labels.append(label)

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
        """Search text fields for configured relation patterns.

        relation_terms maps pattern → (signal_type, field_constraint).
        When field_constraint is set, the pattern is only searched in that
        field.  None means search all fields.
        """
        if not self.relation_terms:
            return []

        results: list[DetectedRelation] = []
        for field_name, values in fields.items():
            full_text = " ".join(values)
            for pattern, (relation_type, field_constraint) in self.relation_terms.items():
                # Only restrict when the signal came from a searchable field
                # (title/description). Signals sourced from creator/subject etc.
                # can validly appear in any text field, so don't filter them.
                if field_constraint in ("title", "description") and field_constraint != field_name:
                    continue
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
    def _split_by_script_labeled(
        values: list[str], labels: list[Optional[str]]
    ) -> tuple[list[str], list[Optional[str]], list[str], list[Optional[str]]]:
        """Partition candidates and labels into Latin and Arabic sublists.

        Returns (lat_candidates, lat_labels, ar_candidates, ar_labels).
        Mixed-script candidates go to the Arabic list (priority for harder-to-surface signal).
        """
        lat_cands: list[str] = []
        lat_labs:  list[Optional[str]] = []
        ar_cands:  list[str] = []
        ar_labs:   list[Optional[str]] = []

        for cand, label in zip(values, labels):
            if _has_arabic(cand):
                ar_cands.append(cand)
                ar_labs.append(label)
            else:
                lat_cands.append(cand)
                lat_labs.append(label)
        return lat_cands, lat_labs, ar_cands, ar_labs

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
        relation_terms: dict[str, tuple[str, str | None]] | None = None,
        boilerplate_ngrams: dict[str, frozenset[str]] | None = None,
        composite_min_creators: int = 2,
        max_desc_len:           int = TITLE_FROM_DESC_MAX_LEN,
        min_desc_tokens:        int = 1,
    ) -> None:
        self.relation_terms          = relation_terms or {}
        self._boilerplate_ngrams     = boilerplate_ngrams
        self._composite_min_creators = composite_min_creators
        self._max_desc_len           = max_desc_len
        self._min_desc_tokens        = min_desc_tokens
        self.records: dict[str, BNFRecord] = {}
        self.failed:  list[dict]           = []
        self._load(directory, glob)

    def _load(self, directory: str, glob: str) -> None:
        paths = sorted(Path(directory).glob(glob))
        for path in tqdm(paths, desc="Parsing BNF XML files", unit="file"):
            try:
                xml = BNFXml(
                    str(path),
                    relation_terms         = self.relation_terms,
                    boilerplate_ngrams     = self._boilerplate_ngrams,
                    composite_min_creators = self._composite_min_creators,
                    max_desc_len           = self._max_desc_len,
                    min_desc_tokens        = self._min_desc_tokens,
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
