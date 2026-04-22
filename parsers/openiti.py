"""
parsers/openiti.py

Parse OpenITI metadata YML files into structured data objects.

The OpenITI corpus uses a custom key-value format (not standard YAML).
Three file types exist, distinguished by the number of dot-separated
components in the filename stem:

    0685NasirDinBaydawi.yml                       → author (0 dots)
    0685NasirDinBaydawi.AnwarTanzil.yml            → book   (1 dot)
    0685NasirDinBaydawi.AnwarTanzil.Vers-ara1.yml  → version (2+ dots)

Class hierarchy
---------------
OpenITIYml  (abstract base — shared file reading, parsing, URI extraction)
├── OpenITIAuthorYml
├── OpenITIBookYml    ← primary record type for this pipeline
└── OpenITIVersionYml ← used only to extract WorldCat/edition links

Use parse_openiti_yml(path) to get the right subclass without needing
to know the file type in advance.

Use OpenITIMetaYmls(directory) to load an entire corpus at once.
"""

from __future__ import annotations

import csv
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Placeholder detection
#
# Unpopulated OpenITI YML files contain template text rather than real data.
# Any field value that contains one of these markers is treated as absent.
# ---------------------------------------------------------------------------
_PLACEHOLDER_MARKERS = {
    "Fulān",                        # generic Arabic name placeholder
    "permalink",                    # link fields
    "@id",                          # e.g. "viaf@id", "wikidata@id"
    "URIs from Althurayya",
    "YEAR-MON-DA",
    "YYYY-MM-DD",
    "src@keyword",
    "URI of a book from OpenITI",
    "AUTH_URI from OpenITI",
    "the name of the annotator",
    "Kitāb al-Muʾallif",
    "Risālaŧ al-Muʾallif",
    "Fars_RE_Auto",
    "comma separated",              # catches several list-placeholder patterns
    "abbreviation for relation",
}

# ---------------------------------------------------------------------------
# Raw format parsing
# ---------------------------------------------------------------------------
# Lines look like:  00#BOOK#URI######: 0685NasirDinBaydawi.AnwarTanzil
# Indented continuation lines extend the previous value.
_KEY_RE = re.compile(r"^(\d+#[A-Z]+#[A-Z]+(?:#[A-Z]*)*)#{0,}:\s*(.*)")

# Patterns for external IDs embedded in the EXTID field value
_WIKIDATA_RE = re.compile(r"wikidata@(Q\d+)")
_VIAF_RE     = re.compile(r"viaf@(\d+)")

# Extract any http/https URL from a field value
_URL_RE = re.compile(r"https?://\S+")


def _parse_raw(text: str) -> dict[str, str]:
    """Parse the OpenITI custom key-value format into a flat dict.

    Handles multi-line values (indented continuation lines) and strips
    trailing '#' padding from keys so they are easier to query.

        "00#BOOK#URI######: 0685NasirDinBaydawi.AnwarTanzil"
        → {"00#BOOK#URI": "0685NasirDinBaydawi.AnwarTanzil"}
    """
    result: dict[str, str] = {}
    current_key: Optional[str] = None

    for line in text.splitlines():
        m = _KEY_RE.match(line)
        if m:
            current_key = m.group(1)
            result[current_key] = m.group(2).strip()
        elif current_key and line.startswith("    ") and line.strip():
            result[current_key] += " " + line.strip()

    return result


def _is_placeholder(value: str) -> bool:
    """Return True if value is empty or matches a known template placeholder."""
    if not value or not value.strip():
        return True
    return any(marker in value for marker in _PLACEHOLDER_MARKERS)


def _clean(value: Optional[str]) -> Optional[str]:
    """Return the value stripped of whitespace if it is real data, else None."""
    if value is None or _is_placeholder(value):
        return None
    return value.strip()


def _decompose_uri(uri: str) -> tuple[Optional[int], Optional[str], Optional[str]]:
    """Split a book URI into (death_year_ah, author_slug, title_slug).

    "0685NasirDinBaydawi.AnwarTanzil" → (685, "NasirDinBaydawi", "AnwarTanzil")

    The leading digits of the first component are the death year in AH.
    Returns None for any component that cannot be parsed.
    """
    parts = uri.split(".", 1)
    author_part = parts[0] if parts else ""
    title_slug  = parts[1] if len(parts) > 1 else None

    death_year_ah: Optional[int] = None
    author_slug:   Optional[str] = None

    m = re.match(r"^(\d+)(.*)", author_part)
    if m:
        try:
            death_year_ah = int(m.group(1))
        except ValueError:
            pass
        author_slug = m.group(2) or None

    return death_year_ah, author_slug, title_slug


# ---------------------------------------------------------------------------
# Typed data containers
#
# These are plain dataclasses — just fields, no methods. They are the objects
# that downstream pipeline stages (fuzzy matching, embedding) work with.
# All fields except the URI are Optional to handle sparse metadata gracefully.
# ---------------------------------------------------------------------------

@dataclass
class OpenITIAuthorData:
    uri: str
    death_year_ah:  Optional[int] = None
    name_slug:      Optional[str] = None  # CamelCase slug from URI (e.g. "NasirDinBaydawi")
    # Arabic name components from YML — all may be absent
    name_shuhra_ar: Optional[str] = None  # "known as" name — most useful for matching
    name_ism_ar:    Optional[str] = None  # personal name
    name_kunya_ar:  Optional[str] = None  # teknonym (Abū...)
    name_laqab_ar:  Optional[str] = None  # honorific / epithet
    name_nasab_ar:  Optional[str] = None  # patronymic chain (b. X b. Y...)
    name_nisba_ar:  Optional[str] = None  # relational adjective (al-Baṣrī etc.)
    wikidata_id:    Optional[str] = None
    # Wikidata enrichment — populated by enrich_wikidata.py; absent until that stage runs
    wd_label_ar:    Optional[str]  = None
    wd_label_en:    Optional[str]  = None
    wd_aliases_ar:  list[str]      = field(default_factory=list)
    wd_aliases_en:  list[str]      = field(default_factory=list)
    wd_death_year:  Optional[int]  = None
    wd_fetched_at:  Optional[str]  = None
    wd_error:       Optional[str]  = None


@dataclass
class OpenITIBookData:
    uri:            str
    author_uri:     str                   # e.g. "0685NasirDinBaydawi"
    death_year_ah:  Optional[int] = None  # parsed from URI prefix
    author_slug:    Optional[str] = None  # e.g. "NasirDinBaydawi"
    title_slug:     Optional[str] = None  # e.g. "AnwarTanzil"
    title_a:        Optional[str] = None  # TITLEA (transliterated)
    title_b:        Optional[str] = None  # TITLEB
    wikidata_id:    Optional[str] = None
    viaf_id:        Optional[str] = None
    # Populated after loading by OpenITIMetaYmls._attach_version_links()
    version_links:  list[str] = field(default_factory=list)


@dataclass
class OpenITIVersionData:
    uri:      str
    book_uri: str              # parent book URI (all but last dot-component)
    links:    list[str] = field(default_factory=list)  # edition/catalogue URLs


# ---------------------------------------------------------------------------
# Abstract base class
# ---------------------------------------------------------------------------

class OpenITIYml(ABC):
    """Abstract base class for all three OpenITI metadata YML types.

    Handles everything that is common across author, book, and version files:
      - Reading the file from disk
      - Detecting the record type from the filename
      - Parsing the custom key-value format
      - Extracting the URI

    Each subclass implements to_data() to extract its own specific fields
    from self._raw and return the appropriate typed dataclass.

    Use the module-level parse_openiti_yml(path) factory function rather
    than instantiating subclasses directly.

    Why ABC and @abstractmethod?
    -----------------------------
    Marking a method @abstractmethod tells Python: "any class that inherits
    from me MUST implement this method." If a subclass forgets to, Python
    raises a TypeError at instantiation time rather than letting the bug
    surface later at runtime. It is a contract enforced by the interpreter.
    """

    def __init__(self, path: str) -> None:
        self.path     = Path(path)
        self.yml_type = self._detect_type()
        self._raw     = _parse_raw(self.path.read_text(encoding="utf-8"))
        self.uri      = self._extract_uri()

    def _detect_type(self) -> str:
        """Infer record type from the number of dots in the filename stem.

        0685NasirDinBaydawi.yml              → 0 dots → 'author'
        0685NasirDinBaydawi.AnwarTanzil.yml  → 1 dot  → 'book'
        0685...AnwarTanzil.Vers-ara1.yml     → 2 dots → 'version'
        """
        n_dots = self.path.stem.count(".")
        if n_dots == 0:
            return "author"
        if n_dots == 1:
            return "book"
        return "version"

    def _extract_uri(self) -> Optional[str]:
        """Pull the URI value from whichever field contains '#URI'."""
        for key, value in self._raw.items():
            if "#URI" in key:
                return _clean(value)
        return None

    def _get(self, *fragments: str) -> Optional[str]:
        """Look up the first field whose key contains all the given fragments.

        Useful because field keys include padding (e.g. "10#BOOK#TITLEA#AR")
        but we can find them by fragment ("TITLEA") without knowing the prefix.
        """
        for key, value in self._raw.items():
            if all(frag in key for frag in fragments):
                return _clean(value)
        return None

    @abstractmethod
    def to_data(self):
        """Extract and return the typed dataclass for this YML type.

        Implemented differently in each subclass — this is the part that
        varies between author, book, and version records.
        """
        ...


# ---------------------------------------------------------------------------
# Subclasses
# ---------------------------------------------------------------------------

class OpenITIAuthorYml(OpenITIYml):
    """Author-level YML. Extracts Arabic name components and death year."""

    def __init__(self, path: str) -> None:
        super().__init__(path)   # runs OpenITIYml.__init__ first, sets self._raw etc.
        self.data: OpenITIAuthorData = self.to_data()

    def to_data(self) -> OpenITIAuthorData:
        if not self.uri:
            raise ValueError(f"No URI found in {self.path}")

        death_year_ah, name_slug, _ = _decompose_uri(self.uri)

        extid_raw     = self._get("EXTID") or ""
        wikidata_match = _WIKIDATA_RE.search(extid_raw)

        return OpenITIAuthorData(
            uri           = self.uri,
            death_year_ah = death_year_ah,
            name_slug     = name_slug,
            name_shuhra_ar = self._get("SHUHRA"),
            name_ism_ar    = self._get("ISM"),
            name_kunya_ar  = self._get("KUNYA"),
            name_laqab_ar  = self._get("LAQAB"),
            name_nasab_ar  = self._get("NASAB"),
            name_nisba_ar  = self._get("NISBA"),
            wikidata_id    = wikidata_match.group(1) if wikidata_match else None,
        )


class OpenITIBookYml(OpenITIYml):
    """Book-level YML. Primary record type for the matching pipeline."""

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.data: OpenITIBookData = self.to_data()

    def to_data(self) -> OpenITIBookData:
        if not self.uri:
            raise ValueError(f"No URI found in {self.path}")

        death_year_ah, author_slug, title_slug = _decompose_uri(self.uri)
        # author_uri is the first dot-component of the book URI
        author_uri = self.uri.split(".")[0]

        extid_raw      = self._get("EXTID") or ""
        wikidata_match = _WIKIDATA_RE.search(extid_raw)
        viaf_match     = _VIAF_RE.search(extid_raw)

        return OpenITIBookData(
            uri           = self.uri,
            author_uri    = author_uri,
            death_year_ah = death_year_ah,
            author_slug   = author_slug,
            title_slug    = title_slug,
            title_a       = self._get("TITLEA"),
            title_b       = self._get("TITLEB"),
            wikidata_id   = wikidata_match.group(1) if wikidata_match else None,
            viaf_id       = viaf_match.group(1) if viaf_match else None,
        )


class OpenITIVersionYml(OpenITIYml):
    """Version-level YML. Captures edition/WorldCat links for the parent book."""

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.data: OpenITIVersionData = self.to_data()

    def to_data(self) -> OpenITIVersionData:
        if not self.uri:
            raise ValueError(f"No URI found in {self.path}")

        # Drop the last dot-component to get the parent book URI
        book_uri = ".".join(self.uri.split(".")[:-1]) if "." in self.uri else self.uri

        # Harvest any real URLs from link-type fields
        links: list[str] = []
        for key, value in self._raw.items():
            if any(lk in key for lk in ("LINKS", "BASED", "COLLATED")) and value:
                links.extend(_URL_RE.findall(value))

        return OpenITIVersionData(uri=self.uri, book_uri=book_uri, links=links)


# ---------------------------------------------------------------------------
# Factory function
# ---------------------------------------------------------------------------

def parse_openiti_yml(path: str) -> OpenITIYml:
    """Return the correct OpenITIYml subclass for the given file path.

    Detects the record type from the filename and instantiates the matching
    subclass. Callers never need to choose the subclass themselves.

        yml = parse_openiti_yml("0685NasirDinBaydawi.AnwarTanzil.yml")
        assert isinstance(yml, OpenITIBookYml)
        book_data = yml.data   # OpenITIBookData
    """
    n_dots = Path(path).stem.count(".")
    if n_dots == 0:
        return OpenITIAuthorYml(path)
    if n_dots == 1:
        return OpenITIBookYml(path)
    return OpenITIVersionYml(path)


# ---------------------------------------------------------------------------
# TSV parsing
# ---------------------------------------------------------------------------

class OpenITITSVRow:
    """A single row from the OpenITI compiled metadata TSV.

    The TSV is version-level (one row per text version), with denormalized
    author and book data in each row.
    """
    def __init__(self, row: dict[str, str]) -> None:
        self.version_uri = row.get("version_uri", "").strip()
        self.language = row.get("language", "").strip()
        self.author_ar = row.get("author_ar", "").strip()
        self.author_lat = row.get("author_lat", "").strip()
        self.author_lat_shuhra = row.get("author_lat_shuhra", "").strip()
        self.book = row.get("book", "").strip()
        self.title_ar = row.get("title_ar", "").strip()
        self.title_lat = row.get("title_lat", "").strip()
        self.tags = row.get("tags", "").strip()

    @property
    def author_uri(self) -> str:
        """Extract author URI from version_uri (first dot-separated component)."""
        if self.version_uri:
            return self.version_uri.split(".")[0]
        return ""

    @property
    def book_uri(self) -> str:
        """Extract book URI from version_uri (first two dot-separated components)."""
        if self.version_uri:
            parts = self.version_uri.split(".")
            if len(parts) >= 2:
                return ".".join(parts[0:2])
        return ""


class OpenITITSV:
    """Load and parse the OpenITI compiled metadata TSV.

    The TSV is at version granularity; this class aggregates to book and author
    level, producing dicts compatible with OpenITIMetaYmls output.

    Returns:
        authors: dict[str, OpenITIAuthorData]
        books: dict[str, OpenITIBookData]
        versions: dict[str, OpenITIVersionData]
    """

    def __init__(self, tsv_path: str) -> None:
        self.authors:  dict[str, OpenITIAuthorData]  = {}
        self.books:    dict[str, OpenITIBookData]    = {}
        self.versions: dict[str, OpenITIVersionData] = {}
        self._load(tsv_path)

    def _load(self, tsv_path: str) -> None:
        """Load and parse the TSV file."""
        with open(tsv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            rows = [OpenITITSVRow(row) for row in reader]

        # Index rows by book_uri and author_uri for aggregation
        books_data: dict[str, dict[str, str]] = {}  # book_uri → aggregated fields
        authors_data: dict[str, dict[str, str]] = {}  # author_uri → aggregated fields

        for row in rows:
            if not row.version_uri:
                continue

            # Create version record
            self.versions[row.version_uri] = OpenITIVersionData(
                uri=row.version_uri,
                book_uri=row.book_uri,
            )

            # Aggregate book data (use first occurrence for each field)
            if row.book_uri not in books_data:
                books_data[row.book_uri] = {
                    "title_lat": row.title_lat,
                    "title_ar": row.title_ar,
                    "author_uri": row.author_uri,
                }

            # Aggregate author data (use first occurrence for each field)
            if row.author_uri not in authors_data:
                authors_data[row.author_uri] = {
                    "author_ar": row.author_ar,
                    "author_lat": row.author_lat,
                    "author_lat_shuhra": row.author_lat_shuhra,
                }

        # Build book records from aggregated data
        for book_uri, book_fields in books_data.items():
            # Extract author and title slugs from URI
            uri_parts = book_uri.split(".")
            author_slug = uri_parts[0] if uri_parts else ""
            title_slug = uri_parts[1] if len(uri_parts) > 1 else ""

            self.books[book_uri] = OpenITIBookData(
                uri=book_uri,
                author_uri=book_fields["author_uri"],
                author_slug=author_slug,
                title_slug=title_slug,
                title_a=book_fields["title_lat"],
                title_b=book_fields["title_ar"],
            )

        # Build author records from aggregated data
        for author_uri, author_fields in authors_data.items():
            # Extract author slug from URI
            author_slug = author_uri.split(".")[0] if author_uri else ""

            self.authors[author_uri] = OpenITIAuthorData(
                uri=author_uri,
                name_slug=author_slug,
                name_shuhra_ar=author_fields["author_lat_shuhra"],  # TSV uses this for shuhra
            )

    def __repr__(self) -> str:
        return (
            f"OpenITITSV("
            f"authors={len(self.authors)}, "
            f"books={len(self.books)}, "
            f"versions={len(self.versions)})"
        )


# ---------------------------------------------------------------------------
# Collection class
# ---------------------------------------------------------------------------

class OpenITIMetaYmls:
    """Load and index an entire OpenITI metadata directory.

    Walks the directory recursively, parses every .yml file, and separates
    results into three dicts keyed by URI. After loading, version links are
    attached to their parent book records automatically.

    Usage:
        corpus = OpenITIMetaYmls("/path/to/openiti/data")
        book   = corpus.get_book("0685NasirDinBaydawi.AnwarTanzil")
        author = corpus.get_author_for_book("0685NasirDinBaydawi.AnwarTanzil")
        print(corpus)
        # OpenITIMetaYmls(authors=1500, books=9000, versions=12000, failed=3)
    """

    def __init__(self, directory: str) -> None:
        self.authors:  dict[str, OpenITIAuthorData]  = {}
        self.books:    dict[str, OpenITIBookData]    = {}
        self.versions: dict[str, OpenITIVersionData] = {}
        self.failed:   list[dict]                    = []
        self._load(directory)
        self._attach_version_links()

    def _load(self, directory: str) -> None:
        for path in sorted(Path(directory).rglob("*.yml")):
            try:
                yml = parse_openiti_yml(str(path))
                if not yml.uri:
                    continue
                if yml.yml_type == "author":
                    self.authors[yml.uri] = yml.data
                elif yml.yml_type == "book":
                    self.books[yml.uri] = yml.data
                else:
                    self.versions[yml.uri] = yml.data
            except Exception as exc:
                self.failed.append({"path": str(path), "error": str(exc)})
                logger.warning("Failed to parse %s: %s", path, exc)

    def _attach_version_links(self) -> None:
        """Copy edition/WorldCat URLs from version records into parent book records."""
        for vers_data in self.versions.values():
            book = self.books.get(vers_data.book_uri)
            if book and vers_data.links:
                book.version_links.extend(vers_data.links)

    def get_book(self, uri: str) -> Optional[OpenITIBookData]:
        return self.books.get(uri)

    def get_author_for_book(self, book_uri: str) -> Optional[OpenITIAuthorData]:
        book = self.books.get(book_uri)
        if book:
            return self.authors.get(book.author_uri)
        return None

    def __repr__(self) -> str:
        return (
            f"OpenITIMetaYmls("
            f"authors={len(self.authors)}, "
            f"books={len(self.books)}, "
            f"versions={len(self.versions)}, "
            f"failed={len(self.failed)})"
        )
