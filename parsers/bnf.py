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
    - Arabic and Latin versions of the same title appear as separate dc:title
      elements, so the same split-by-script treatment as dc:creator applies.
    - Format is not uniform — no structural split is attempted at parse time.
      Full strings are stored for embedding; structure is left to match stages.

dc:creator
    - Only 74.3% coverage — never assume it is present.
    - Names often include parenthetical dates: "Name (1100?-1165?). Role"
    - Max 89 per record (composite manuscripts).
    - Dates and role suffixes are stripped from all entries.

dc:description
    - Always present (100%), avg 6 per record, max 146.
    - 34.9% of records have mixed-script descriptions.

dc:subject
    - 60.8% coverage, multi-value (avg 1.54, max 24) — stored as list.

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

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# XML namespace map
# ---------------------------------------------------------------------------
NS = {
    "oai":    "http://www.openarchives.org/OAI/2.0/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "dc":     "http://purl.org/dc/elements/1.1/",
}

# ---------------------------------------------------------------------------
# Script detection
# ---------------------------------------------------------------------------
_ARABIC_RE = re.compile(
    r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF\uFB50-\uFDFF\uFE70-\uFEFF]"
)

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


def _has_arabic(text: str) -> bool:
    return bool(_ARABIC_RE.search(text)) if text else False


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

    # Title — stored as full strings; no structural split attempted at parse time
    title_lat:          list[str] = field(default_factory=list)
    title_ar:           list[str] = field(default_factory=list)

    # Creator (authors of the text), dates and role suffixes stripped
    creator_lat:        list[str] = field(default_factory=list)
    creator_ar:         list[str] = field(default_factory=list)

    # Description, split by script
    description_lat:    list[str] = field(default_factory=list)
    description_ar:     list[str] = field(default_factory=list)

    # Other fields
    subject:            list[str] = field(default_factory=list)
    coverage:           list[str] = field(default_factory=list)
    shelfmark:          Optional[str] = None
    copy_date_raw:      Optional[str] = None
    language:           list[str] = field(default_factory=list)

    # Flags and derived data
    is_composite:       bool = False
    detected_relations: list[DetectedRelation] = field(default_factory=list)
    signal_count:       int = 0


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

    Usage
    -----
        record = BNFXml("OAI_11000434.xml").record
        record = BNFXml("OAI_11000434.xml", relation_terms=MY_TERMS).record
    """

    def __init__(
        self,
        path: str,
        relation_terms: dict[str, str] | None = None,
    ) -> None:
        self.path           = Path(path)
        self.relation_terms = relation_terms or {}
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

        # --- Descriptions ---
        desc_lat, desc_ar = self._split_by_script(texts("description"))

        # --- Other fields ---
        subject   = texts("subject")
        coverage  = texts("coverage")
        shelfmark = self._extract_shelfmark(first("source"))
        copy_date = first("date")
        language  = texts("language")

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
            description_lat    = desc_lat,
            description_ar     = desc_ar,
            subject            = subject,
            coverage           = coverage,
            shelfmark          = shelfmark,
            copy_date_raw      = copy_date,
            language           = language,
            is_composite       = is_composite,
            detected_relations = detected_relations,
        )
        record.signal_count = self._count_signals(record)
        return record

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
    ) -> None:
        self.relation_terms = relation_terms or {}
        self.records: dict[str, BNFRecord] = {}
        self.failed:  list[dict]           = []
        self._load(directory, glob)

    def _load(self, directory: str, glob: str) -> None:
        for path in sorted(Path(directory).glob(glob)):
            try:
                xml = BNFXml(str(path), relation_terms=self.relation_terms)
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
