"""Generate CSV report for correspondence.json test set."""

import json
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))

from matching.config import BNF_FULL_PATH, OPENITI_CORPUS_PATH
from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.classifier import Classifier

# Load data
bnf_records = load_bnf_records(BNF_FULL_PATH)
openiti_data = load_openiti_corpus(OPENITI_CORPUS_PATH)

# Load correspondence test set
with open("data_samplers/correspondence.json") as f:
    correspondence_list = json.load(f)

correspondence = {}
for mapping in correspondence_list:
    for book_uri, bnf_id in mapping.items():
        correspondence[bnf_id] = book_uri

# Run pipeline on test set
test_bnf_ids = [bid for bid in correspondence.keys() if bid in bnf_records]
bnf_records_test = {bid: bnf_records[bid] for bid in test_bnf_ids}

pipeline = MatchingPipeline(bnf_records_test, openiti_data, run_id="correspondence_report", verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(Classifier(verbose=False))

print("Running pipeline on correspondence test set...")
pipeline.run()

# Generate report
rows = []
for bnf_id in sorted(test_bnf_ids):
    expected_book = correspondence[bnf_id]
    
    # Get matches at each stage
    stage1_authors = pipeline.get_stage1_result(bnf_id) or []
    stage2_books = pipeline.get_stage2_result(bnf_id) or []
    stage3_books = pipeline.get_stage3_result(bnf_id) or []
    
    # Check if expected book is in results
    found_in_s1 = False
    found_in_s2 = expected_book in stage2_books
    found_in_s3 = expected_book in stage3_books
    
    # Check if expected book's author is in stage 1
    expected_book_data = openiti_data["books"].get(expected_book)
    if expected_book_data:
        expected_author = expected_book_data["author_uri"] if isinstance(expected_book_data, dict) else expected_book_data.author_uri
        found_in_s1 = expected_author in stage1_authors
    
    # Count books by stage 1 authors
    s1_book_count = 0
    for author_uri in stage1_authors:
        for book_uri, book_data in openiti_data["books"].items():
            author = book_data["author_uri"] if isinstance(book_data, dict) else book_data.author_uri
            if author == author_uri:
                s1_book_count += 1
    
    rows.append({
        "bnf_id": bnf_id,
        "expected_book": expected_book,
        "stage1_author_matches": len(stage1_authors),
        "stage1_book_count": s1_book_count,
        "stage2_book_matches": len(stage2_books),
        "stage3_book_matches": len(stage3_books),
        "correct_in_stage1_author": found_in_s1,
        "correct_in_stage2": found_in_s2,
        "correct_in_stage3": found_in_s3,
    })

# Write CSV
with open("correspondence_report.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "bnf_id", "expected_book", "stage1_author_matches", "stage1_book_count",
        "stage2_book_matches", "stage3_book_matches",
        "correct_in_stage1_author", "correct_in_stage2", "correct_in_stage3"
    ])
    writer.writeheader()
    writer.writerows(rows)

print(f"Report written to correspondence_report.csv")
print(f"Total records: {len(rows)}")
print(f"Correct in stage 1: {sum(1 for r in rows if r['correct_in_stage1_author'])}")
print(f"Correct in stage 2: {sum(1 for r in rows if r['correct_in_stage2'])}")
print(f"Correct in stage 3: {sum(1 for r in rows if r['correct_in_stage3'])}")
