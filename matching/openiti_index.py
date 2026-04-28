"""
OpenITI data index for efficient fuzzy matching.

Wraps the loaded OpenITI corpus (books + authors dicts) and provides
precomputed indices for fast lookups:
- author_uri → [book_uris] (for intersection filtering in Stage 3)
- Book/author data access by URI
"""

from typing import Optional


class OpenITIIndex:
    """Index wrapper over OpenITI corpus data for matching operations."""

    def __init__(self, books: dict, authors: dict):
        """
        Initialize the index.

        Parameters
        ----------
        books : dict
            {book_uri: OpenITIBookData} from parsed OpenITI corpus
        authors : dict
            {author_uri: OpenITIAuthorData} from parsed OpenITI corpus
        """
        self.books = books
        self.authors = authors
        self._author_books = self._build_author_index()

        # Cull author data from book fields if enabled
        from matching.config import CULL_AUTHOR_DATA_FROM_BOOKS
        if CULL_AUTHOR_DATA_FROM_BOOKS:
            self._cull_author_data_from_books()

    def _build_author_index(self) -> dict[str, list[str]]:
        """Build author_uri → [book_uris] mapping (precomputed once)."""
        index: dict[str, list[str]] = {}
        for book_uri, book in self.books.items():
            # Handle both dict and dataclass access
            author_uri = book["author_uri"] if isinstance(book, dict) else book.author_uri
            if author_uri not in index:
                index[author_uri] = []
            index[author_uri].append(book_uri)
        return index

    def get_book(self, book_uri: str):
        """Retrieve a book by URI. Returns dict or dataclass object."""
        return self.books.get(book_uri)

    def get_author(self, author_uri: str):
        """Retrieve an author by URI. Returns dict or dataclass object."""
        return self.authors.get(author_uri)

    def get_books_for_authors(self, author_uris: list[str]) -> list[str]:
        """
        Get all book URIs belonging to a list of authors.

        Used in Stage 3 (intersection) to filter title matches by author.

        Parameters
        ----------
        author_uris : list[str]
            List of author URIs to query

        Returns
        -------
        list[str]
            All book URIs whose author_uri is in the input list (deduplicated)
        """
        books: set[str] = set()
        for author_uri in author_uris:
            books.update(self._author_books.get(author_uri, []))
        return list(books)

    def get_books_for_author(self, author_uri: str) -> list[str]:
        """Get all book URIs for a single author."""
        return self._author_books.get(author_uri, [])

    def book_count(self) -> int:
        """Total number of books in corpus."""
        return len(self.books)

    def author_count(self) -> int:
        """Total number of authors in corpus."""
        return len(self.authors)

    def _cull_author_data_from_books(self) -> None:
        """
        Preprocess: remove author tokens from book fields to prevent author name
        boosts during title matching.

        For each book, gets all author candidate strings (Latin + Arabic),
        normalizes and tokenizes them, then removes matching tokens from
        the book's title and description fields (in-place).

        This prevents author names from contributing to title match scores.
        """
        from matching.candidate_builders import build_author_candidates_by_script
        from matching.normalize import normalize_for_matching

        culled_count = 0
        books_modified = 0

        for book_uri, book in self.books.items():
            # Get author for this book
            author_uri = book["author_uri"] if isinstance(book, dict) else book.author_uri
            author = self.authors.get(author_uri)
            if not author:
                continue

            # Build author candidate set (both Latin and Arabic)
            candidates = build_author_candidates_by_script(author)
            if not (candidates.get("lat") or candidates.get("ara")):
                continue


            # Collect author tokens (normalized and tokenized)
            author_tokens = set()

            for lat_candidate in candidates.get("lat", []):
                if lat_candidate:
                    norm_candidate = normalize_for_matching(lat_candidate, split_camelcase=True, is_openiti=True)
                    if norm_candidate:
                        tokens = norm_candidate.lower().split()
                        author_tokens.update(tokens)

            for ara_candidate in candidates.get("ara", []):
                if ara_candidate:
                    norm_candidate = normalize_for_matching(ara_candidate, split_camelcase=True, is_openiti=True)
                    if norm_candidate:
                        tokens = norm_candidate.lower().split()
                        author_tokens.update(tokens)

            if not author_tokens:
                continue

    
            # Cull author tokens from book fields (Arabic only; Latin too generic, hurts recall)
            fields_to_cull = ["title_ara"]

            book_modified = False
            for field in fields_to_cull:
                # Handle both dict and dataclass access
                if isinstance(book, dict):
                    field_value = book.get(field)
                else:
                    field_value = getattr(book, field, None)


                if not field_value:
                    continue

                # Handle list of titles
                if isinstance(field_value, list):
                    for i, title_part in enumerate(field_value):
                        if not isinstance(title_part, str):
                            continue
                        norm_field = normalize_for_matching(title_part, split_camelcase=True, is_openiti=True)
                        if not norm_field:
                            continue
                        field_tokens = norm_field.lower().split()
                        cleaned_tokens = [t for t in field_tokens if t.lower() not in author_tokens]
                        cleaned_field = " ".join(cleaned_tokens)
                        if cleaned_field != norm_field:
                            removed = len(field_tokens) - len(cleaned_tokens)
                            field_value[i] = cleaned_field
                            book_modified = True
                            culled_count += removed
                       
                # Handle string titles (may contain ". " separators)
                elif isinstance(field_value, str):
                    norm_field = normalize_for_matching(field_value, split_camelcase=True, is_openiti=True)
                    if norm_field:
                        field_tokens = norm_field.lower().split()
                        cleaned_tokens = [t for t in field_tokens if t.lower() not in author_tokens]
                        cleaned_field = " ".join(cleaned_tokens)
                        if cleaned_field != norm_field:
                            if isinstance(book, dict):
                                book[field] = cleaned_field
                            else:
                                setattr(book, field, cleaned_field)
                            book_modified = True
                            culled_count += len(field_tokens) - len(cleaned_tokens)

            if book_modified:
                books_modified += 1

