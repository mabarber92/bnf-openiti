"""Count OpenITI books by matched authors."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher

bnf_records = load_bnf_records(BNF_FULL_PATH)
openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)

test_record_id = "OAI_10030933"
bnf_records_test = {test_record_id: bnf_records[test_record_id]}

pipeline = MatchingPipeline(bnf_records_test, openiti_data, run_id="debug", verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.run()

matched_authors = set(pipeline.get_stage1_result(test_record_id) or [])
print(f"Matched authors: {len(matched_authors)}")

# Count books by these authors
books_by_matched = 0
for book_uri, book_data in openiti_data["books"].items():
    author_uri = book_data["author_uri"] if isinstance(book_data, dict) else book_data.author_uri
    if author_uri in matched_authors:
        books_by_matched += 1

print(f"OpenITI books by matched authors: {books_by_matched}")
print(f"Expected from original test: 789")
