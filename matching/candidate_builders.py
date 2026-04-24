"""Shared utilities for building candidates separated by script (lat/ara)."""


def build_author_candidates_by_script(author_data):
    """
    Extract author candidates from OpenITI author data, organized by script.

    Replicates the exact candidate extraction from test_fuzzy_with_author_comprehensive.py,
    but separates Latin/transliterated candidates from Arabic ones.

    Parameters
    ----------
    author_data : dict or dataclass
        OpenITI author object (dict or dataclass)

    Returns
    -------
    dict
        {"lat": [candidate_strings], "ara": [candidate_strings]}
    """
    candidates_lat = []
    candidates_ara = []

    is_dict = isinstance(author_data, dict)

    # Helper to get attribute (works for both dict and dataclass)
    def get_attr(obj, name):
        return obj.get(name) if is_dict else getattr(obj, name, None)

    # Latin/English candidates
    if get_attr(author_data, "name_slug"):
        candidates_lat.append(get_attr(author_data, "name_slug"))
    if get_attr(author_data, "wd_label_en"):
        candidates_lat.append(get_attr(author_data, "wd_label_en"))
    if get_attr(author_data, "wd_aliases_en"):
        aliases = get_attr(author_data, "wd_aliases_en")
        if isinstance(aliases, list):
            candidates_lat.extend(aliases)
        elif isinstance(aliases, str):
            candidates_lat.append(aliases)

    # Transliterated name components
    for field in ["name_shuhra_lat", "name_ism_lat", "name_kunya_lat", "name_laqab_lat", "name_nasab_lat", "name_nisba_lat"]:
        val = get_attr(author_data, field)
        if val:
            candidates_lat.append(val)

    # Arabic candidates
    if get_attr(author_data, "wd_label_ar"):
        candidates_ara.append(get_attr(author_data, "wd_label_ar"))
    if get_attr(author_data, "wd_aliases_ar"):
        aliases = get_attr(author_data, "wd_aliases_ar")
        if isinstance(aliases, list):
            candidates_ara.extend(aliases)
        elif isinstance(aliases, str):
            candidates_ara.append(aliases)

    return {
        "lat": candidates_lat,
        "ara": candidates_ara,
    }


def build_book_candidates_by_script(book_data):
    """
    Extract book title candidates from OpenITI book data, organized by script.

    Splits titles on ". " separator to match original test logic.

    Parameters
    ----------
    book_data : dict or dataclass
        OpenITI book object (dict or dataclass)

    Returns
    -------
    dict
        {"lat": [title_parts], "ara": [title_parts]}
    """
    candidates_lat = []
    candidates_ara = []

    is_dict = isinstance(book_data, dict)

    # Helper to get attribute (works for both dict and dataclass)
    def get_attr(obj, name):
        return obj.get(name) if is_dict else getattr(obj, name, None)

    # Latin/transliterated titles (strip trailing punctuation like benchmark)
    title_lat = get_attr(book_data, "title_lat")
    if title_lat:
        if isinstance(title_lat, list):
            for part in title_lat:
                part = part.strip().rstrip(".") if part else ""
                if part:
                    candidates_lat.append(part)
        elif isinstance(title_lat, str):
            for part in title_lat.split(". "):
                part = part.strip().rstrip(".")
                if part:
                    candidates_lat.append(part)

    # Arabic titles (strip trailing punctuation like benchmark)
    title_ara = get_attr(book_data, "title_ara")
    if title_ara:
        if isinstance(title_ara, list):
            for part in title_ara:
                part = part.strip().rstrip(".") if part else ""
                if part:
                    candidates_ara.append(part)
        elif isinstance(title_ara, str):
            for part in title_ara.split(". "):
                part = part.strip().rstrip(".")
                if part:
                    candidates_ara.append(part)

    return {
        "lat": candidates_lat,
        "ara": candidates_ara,
    }
