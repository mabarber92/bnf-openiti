"""
Candidate extraction for fuzzy matching.

Extracts author and title candidates from BNF and OpenITI records,
handling multiple scripts (Latin transliteration and Arabic).

Both extractors return normalized candidates as dicts with 'lat' and 'ara' keys.
"""

from typing import Dict, List


def extract_bnf_author_candidates(bnf_record: dict) -> Dict[str, List[str]]:
    """
    Extract author candidates from BNF record (Stage 1).

    Uses ALL fields where author info appears: creators, contributors,
    title parts (sometimes contain author names), description_candidates.

    Args:
        bnf_record: Parsed BNF record dict

    Returns:
        {'lat': [...], 'ara': [...]} with deduplicated candidates
    """
    candidates = {"lat": [], "ara": []}

    # Creator fields (primary)
    for creator in bnf_record.get("creator_lat", []):
        if creator and creator not in candidates["lat"]:
            candidates["lat"].append(creator)

    for creator in bnf_record.get("creator_ara", []):
        if creator and creator not in candidates["ara"]:
            candidates["ara"].append(creator)

    # Contributor fields
    for contrib in bnf_record.get("contributor_lat", []):
        if contrib and contrib not in candidates["lat"]:
            candidates["lat"].append(contrib)

    for contrib in bnf_record.get("contributor_ar", []):  # Note: BNF uses 'ar' not 'ara'
        if contrib and contrib not in candidates["ara"]:
            candidates["ara"].append(contrib)

    # Titles (sometimes contain author names)
    for title in bnf_record.get("title_lat", []):
        for part in title.split(". "):
            part = part.strip().rstrip(".")
            if part and part not in candidates["lat"]:
                candidates["lat"].append(part)

    for title in bnf_record.get("title_ara", []):
        for part in title.split(". "):
            part = part.strip().rstrip(".")
            if part and part not in candidates["ara"]:
                candidates["ara"].append(part)

    # Description candidates (author info often here, especially composites)
    for desc in bnf_record.get("description_candidates_lat", []):
        if desc and desc not in candidates["lat"]:
            candidates["lat"].append(desc)

    for desc in bnf_record.get("description_candidates_ara", []):
        if desc and desc not in candidates["ara"]:
            candidates["ara"].append(desc)

    return candidates


def extract_bnf_title_candidates(bnf_record: dict) -> Dict[str, List[str]]:
    """
    Extract title candidates from BNF record (Stage 2).

    Uses titles and description_candidates as fallback.

    Args:
        bnf_record: Parsed BNF record dict

    Returns:
        {'lat': [...], 'ara': [...]} with deduplicated candidates
    """
    candidates = {"lat": [], "ara": []}

    # Titles (primary)
    for title in bnf_record.get("title_lat", []):
        for part in title.split(". "):
            part = part.strip().rstrip(".")
            if part and part not in candidates["lat"]:
                candidates["lat"].append(part)

    for title in bnf_record.get("title_ara", []):
        for part in title.split(". "):
            part = part.strip().rstrip(".")
            if part and part not in candidates["ara"]:
                candidates["ara"].append(part)

    # Description candidates as fallback
    for desc in bnf_record.get("description_candidates_lat", []):
        if desc and desc not in candidates["lat"]:
            candidates["lat"].append(desc)

    for desc in bnf_record.get("description_candidates_ara", []):
        if desc and desc not in candidates["ara"]:
            candidates["ara"].append(desc)

    return candidates


def extract_openiti_author_candidates(author_uri: str, author_record: dict) -> Dict[str, List[str]]:
    """
    Extract all author name variants from OpenITI author record.

    Name components are stored in separate _lat (ArabicBetaCode transliteration)
    and _ara (Arabic script) fields.

    Args:
        author_uri: Author URI identifier (for context/debugging)
        author_record: Author record from OpenITI corpus

    Returns:
        {'lat': [...], 'ara': [...]} with all available name variants
    """
    candidates = {"lat": [], "ara": []}

    # Transliterated variants (Latin script)
    if author_record.get("name_slug"):
        candidates["lat"].append(author_record["name_slug"])

    if author_record.get("wd_label_en"):
        candidates["lat"].append(author_record["wd_label_en"])

    if author_record.get("wd_aliases_en"):
        aliases = author_record["wd_aliases_en"]
        if isinstance(aliases, list):
            candidates["lat"].extend(aliases)
        elif isinstance(aliases, str):
            candidates["lat"].append(aliases)

    # Structured name components (Latin transliteration)
    for field in [
        "name_shuhra_lat", "name_ism_lat", "name_kunya_lat",
        "name_laqab_lat", "name_nasab_lat", "name_nisba_lat"
    ]:
        if author_record.get(field):
            candidates["lat"].append(author_record[field])

    # Structured name components (Arabic script)
    for field in [
        "name_shuhra_ara", "name_ism_ara", "name_kunya_ara",
        "name_laqab_ara", "name_nasab_ara", "name_nisba_ara"
    ]:
        if author_record.get(field):
            candidates["ara"].append(author_record[field])

    # Arabic variants from Wikidata
    if author_record.get("wd_label_ar"):
        candidates["ara"].append(author_record["wd_label_ar"])

    if author_record.get("wd_aliases_ar"):
        aliases = author_record["wd_aliases_ar"]
        if isinstance(aliases, list):
            candidates["ara"].extend(aliases)
        elif isinstance(aliases, str):
            candidates["ara"].append(aliases)

    return candidates


def extract_openiti_title_candidates(book_uri: str, book_record: dict) -> Dict[str, List[str]]:
    """
    Extract title candidates from OpenITI book record.

    Title fields are pre-split lists (from TSV separator splitting during parsing).
    Handles both list and string formats for robustness.

    Args:
        book_uri: Book URI identifier (for context/debugging)
        book_record: Book record from OpenITI corpus

    Returns:
        {'lat': [...], 'ara': [...]} with deduplicated title candidates
    """
    candidates = {"lat": [], "ara": []}

    # Handle title_lat (should be a list after TSV splitting)
    title_lat = book_record.get("title_lat")
    if title_lat:
        if isinstance(title_lat, list):
            for part in title_lat:
                part = part.strip().rstrip(".") if part else ""
                if part and part not in candidates["lat"]:
                    candidates["lat"].append(part)
        else:
            # Fallback for string (shouldn't happen post-refactor, but be safe)
            for part in title_lat.split(". "):
                part = part.strip().rstrip(".")
                if part and part not in candidates["lat"]:
                    candidates["lat"].append(part)

    # Handle title_ara (should be a list after TSV splitting)
    title_ara = book_record.get("title_ara")
    if title_ara:
        if isinstance(title_ara, list):
            for part in title_ara:
                part = part.strip().rstrip(".") if part else ""
                if part and part not in candidates["ara"]:
                    candidates["ara"].append(part)
        else:
            # Fallback for string
            for part in title_ara.split(". "):
                part = part.strip().rstrip(".")
                if part and part not in candidates["ara"]:
                    candidates["ara"].append(part)

    return candidates
