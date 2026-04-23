"""
Smoke tests for matching pipeline infrastructure.

Validates that core components (indices, pipeline, data flow) work correctly
before scoring/matching stages are implemented.
"""

import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from matching.openiti_index import OpenITIIndex
from matching.bnf_index import BNFCandidateIndex
from matching.pipeline import MatchingPipeline
from matching.config import BNF_SAMPLE_PATH, OPENITI_CORPUS_PATH
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus


def load_test_data():
    """Load sample BNF and OpenITI data."""
    print("Loading test data...")

    # Load BNF sample (deserializes to BNFRecord dataclass objects)
    bnf_records = load_bnf_records(BNF_SAMPLE_PATH)
    print(f"  BNF records: {len(bnf_records)}")

    # Load OpenITI corpus (deserializes to dataclass objects)
    openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)
    print(f"  OpenITI books: {len(openiti_data['books'])}")
    print(f"  OpenITI authors: {len(openiti_data['authors'])}")

    return bnf_records, openiti_data


def test_openiti_index():
    """Test OpenITIIndex construction and lookups."""
    print("\n--- Test: OpenITIIndex ---")

    _, openiti_data = load_test_data()

    # Create index
    index = OpenITIIndex(openiti_data["books"], openiti_data["authors"])
    print(f"Index created: {index.book_count()} books, {index.author_count()} authors")

    # Test author lookup
    sample_author_uri = list(openiti_data["authors"].keys())[0]
    author = index.get_author(sample_author_uri)
    assert author is not None, "Author lookup failed"
    print(f"  Author lookup: {sample_author_uri} [OK]")

    # Test book lookup
    sample_book_uri = list(openiti_data["books"].keys())[0]
    book = index.get_book(sample_book_uri)
    assert book is not None, "Book lookup failed"
    print(f"  Book lookup: {sample_book_uri} [OK]")

    # Test author->books mapping
    author_uri = book["author_uri"] if isinstance(book, dict) else book.author_uri
    books_for_author = index.get_books_for_author(author_uri)
    assert len(books_for_author) > 0, f"No books found for author {author_uri}"
    assert sample_book_uri in books_for_author, "Sample book not in author's books"
    print(f"  Author->books mapping: {author_uri} has {len(books_for_author)} books [OK]")

    # Test batch lookup
    books_for_authors = index.get_books_for_authors([author_uri])
    assert len(books_for_authors) > 0, "Batch lookup failed"
    print(f"  Batch author->books: {len(books_for_authors)} books [OK]")

    print("OpenITIIndex tests PASSED [OK]")
    return True


def test_bnf_index():
    """Test BNFCandidateIndex construction and candidate extraction."""
    print("\n--- Test: BNFCandidateIndex ---")

    bnf_records, _ = load_test_data()

    # Create index
    index = BNFCandidateIndex(bnf_records, norm_strategy="fuzzy")
    print(f"Index created: {index.author_candidate_count()} author candidates, "
          f"{index.title_candidate_count()} title candidates")

    # Test iteration
    author_count = 0
    for candidate, bnf_ids in index.author_candidates_iter():
        author_count += 1
        assert isinstance(candidate, str), "Candidate should be string"
        assert isinstance(bnf_ids, list), "BNF IDs should be list"
        assert len(bnf_ids) > 0, "Should have at least one BNF ID"
        if author_count <= 3:
            print(f"  Author candidate {author_count}: {len(bnf_ids)} BNF records [OK]")

    print(f"  Total author candidates: {author_count} [OK]")

    title_count = 0
    for candidate, bnf_ids in index.title_candidates_iter():
        title_count += 1

    print(f"  Total title candidates: {title_count} [OK]")

    # Test retrieval by candidate
    first_candidate = list(index.author_index.keys())[0]
    bnf_ids = index.get_bnf_records_with_author_candidate(first_candidate)
    assert len(bnf_ids) > 0, "Should have BNF IDs for candidate"
    print(f"  Candidate lookup: {len(bnf_ids)} BNF records [OK]")

    print("BNFCandidateIndex tests PASSED [OK]")
    return True


def test_pipeline_initialization():
    """Test MatchingPipeline initialization and state management."""
    print("\n--- Test: MatchingPipeline ---")

    bnf_records, openiti_data = load_test_data()

    # Create pipeline
    pipeline = MatchingPipeline(
        bnf_records,
        openiti_data,
        run_id="smoke_test",
        norm_strategy="fuzzy",
        verbose=False
    )
    print(f"Pipeline created: {len(bnf_records)} BNF records")

    # Test state management
    test_bnf_id = list(bnf_records.keys())[0]
    test_authors = ["0123TestAuthor", "0456AnotherAuthor"]
    test_books = ["0123TestAuthor.TestBook", "0456AnotherAuthor.AnotherBook"]

    # Test Stage 1 result storage
    pipeline.set_stage1_result(test_bnf_id, test_authors)
    retrieved = pipeline.get_stage1_result(test_bnf_id)
    assert retrieved == test_authors, "Stage 1 result mismatch"
    print(f"  Stage 1 set/get: {len(test_authors)} authors [OK]")

    # Test Stage 2 result storage
    pipeline.set_stage2_result(test_bnf_id, test_books)
    retrieved = pipeline.get_stage2_result(test_bnf_id)
    assert retrieved == test_books, "Stage 2 result mismatch"
    print(f"  Stage 2 set/get: {len(test_books)} books [OK]")

    # Test Stage 3 result storage
    combined = test_books  # Simplified
    pipeline.set_stage3_result(test_bnf_id, combined)
    retrieved = pipeline.get_stage3_result(test_bnf_id)
    assert retrieved == combined, "Stage 3 result mismatch"
    print(f"  Stage 3 set/get: {len(combined)} combined matches [OK]")

    # Test classification
    pipeline.set_classification(test_bnf_id, "high_confidence")
    tier = pipeline.get_classification(test_bnf_id)
    assert tier == "high_confidence", "Classification mismatch"
    print(f"  Classification set/get: {tier} [OK]")

    print("MatchingPipeline tests PASSED [OK]")
    return True


def test_stage_registration():
    """Test pluggable stage registration."""
    print("\n--- Test: Stage Registration ---")

    bnf_records, openiti_data = load_test_data()

    # Create pipeline
    pipeline = MatchingPipeline(
        bnf_records,
        openiti_data,
        run_id="smoke_test",
        verbose=False
    )

    # Create dummy stage
    class DummyStage:
        def execute(self, pipeline):
            pass

    # Register stage
    stage = DummyStage()
    pipeline.register_stage(stage)
    assert len(pipeline.stages) == 1, "Stage registration failed"
    print(f"  Stage registered: {stage.__class__.__name__} [OK]")

    # Run pipeline (should not error even with dummy stage)
    try:
        pipeline.run()
        print(f"  Pipeline executed successfully [OK]")
    except Exception as e:
        print(f"  Pipeline execution failed: {e}")
        return False

    print("Stage registration tests PASSED [OK]")
    return True


def main():
    """Run all smoke tests."""
    print("="*60)
    print("MATCHING PIPELINE SMOKE TESTS")
    print("="*60)

    try:
        test_openiti_index()
        test_bnf_index()
        test_pipeline_initialization()
        test_stage_registration()

        print("\n" + "="*60)
        print("ALL SMOKE TESTS PASSED")
        print("="*60)
        return 0

    except Exception as e:
        print(f"\nTEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
