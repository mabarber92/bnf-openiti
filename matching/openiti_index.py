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

    def _build_author_index(self) -> dict[str, list[str]]:
        """Build author_uri → [book_uris] mapping (precomputed once)."""
        index: dict[str, list[str]] = {}
        for book_uri, book in self.books.items():
            author_uri = book.author_uri
            if author_uri not in index:
                index[author_uri] = []
            index[author_uri].append(book_uri)
        return index

    def get_book(self, book_uri: str):
        """Retrieve a book by URI."""
        return self.books.get(book_uri)

    def get_author(self, author_uri: str):
        """Retrieve an author by URI."""
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
