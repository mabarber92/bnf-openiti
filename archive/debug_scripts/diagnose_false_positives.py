"""
Diagnose false positives for a single BNF record.

Traces OAI_11000520 through all three matching stages with detailed output:
- Stage 1: Which authors matched and their scores
- Stage 2: Which books matched and their scores
- Stage 3: Combined scoring logic and why pairs pass/fail
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, '.')

from parsers.bnf import load_bnf_records
from parsers.openiti import load_openiti_corpus
from matching.pipeline import MatchingPipeline
from matching.author_matcher import AuthorMatcher
from matching.title_matcher import TitleMatcher
from matching.combined_matcher import CombinedMatcher
from matching.classifier import Classifier
import matching.config as cfg

# Load data - ONLY load one test record to avoid huge logs
bnf_records = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)

with open('data_samplers/correspondence.json') as f:
    correspondences = json.load(f)

# Extract just OAI_11000520
target_bnf_id = None
for item in correspondences:
    for book_uri, bnf_id in item.items():
        if bnf_id == 'OAI_11000520':
            target_bnf_id = bnf_id
            break

if not target_bnf_id:
    print("ERROR: OAI_11000520 not found in correspondence")
    print("Available BNF IDs:")
    all_bnf = set()
    for item in correspondences:
        for book_uri, bnf_id in item.items():
            all_bnf.add(bnf_id)
    for bid in sorted(all_bnf)[:20]:
        print(f"  {bid}")
    sys.exit(1)

if target_bnf_id not in bnf_records:
    print(f"ERROR: {target_bnf_id} not in BNF records")
    sys.exit(1)

# Create single-record test set
bnf_records_test = {target_bnf_id: bnf_records[target_bnf_id]}

print("="*80)
print(f"DIAGNOSING: {target_bnf_id} (OAI_11000520)")
print("="*80)

# Show the BNF record
bnf_record = bnf_records_test[target_bnf_id]
creators = (getattr(bnf_record, 'creator_lat', []) or [])
titles = (getattr(bnf_record, 'title_lat', []) or [])

print(f"\nBNF RECORD:")
print(f"  ID: {target_bnf_id}")
for c in creators[:2]:
    c_safe = c.encode('ascii', 'replace').decode('ascii') if c else "None"
    print(f"  Creator: {c_safe[:60]}")
for t in titles[:2]:
    t_safe = t.encode('ascii', 'replace').decode('ascii') if t else "None"
    print(f"  Title: {t_safe[:60]}")

# Run pipeline
pipeline = MatchingPipeline(bnf_records_test, openiti_data, verbose=False)
pipeline.register_stage(AuthorMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=False, use_parallel=False))
pipeline.register_stage(CombinedMatcher(verbose=False))
pipeline.register_stage(Classifier(verbose=False))
pipeline.run()

# Extract results
stage1_authors = pipeline.get_stage1_result(target_bnf_id) or []
stage1_scores = pipeline.get_stage1_scores(target_bnf_id) or {}

stage2_books = pipeline.get_stage2_result(target_bnf_id) or []
stage2_scores = pipeline.get_stage2_scores(target_bnf_id) or {}

stage3_matches = pipeline.get_stage3_result(target_bnf_id) or []

print(f"\n" + "-"*80)
print("STAGE 1 RESULTS (Author Matching)")
print("-"*80)
print(f"Total matched authors: {len(stage1_authors)}\n")

# Sort all by score and find Quduri
sorted_authors = sorted(stage1_authors, key=lambda x: stage1_scores.get(x, 0), reverse=True)

# Find Quduri in the list
quduri_idx = None
for idx, author_uri in enumerate(sorted_authors):
    if 'quduri' in author_uri.lower():
        quduri_idx = idx
        break

print("TOP 15 SCORING AUTHORS:")
print(f"{'Rank':>4} {'Score':>8} {'Author URI':<25} {'Name':<40}")
print("-"*80)
for rank, author_uri in enumerate(sorted_authors[:15], 1):
    score = stage1_scores.get(author_uri, 0)
    author = pipeline.openiti_index.authors.get(author_uri)
    author_str = (author.name if hasattr(author, 'name') else "?")[:50]
    author_safe = author_str.encode('ascii', 'replace').decode('ascii')
    marker = " <-- QUDURI" if 'quduri' in author_uri.lower() else ""
    print(f"{rank:4d} {score:7.2%}  {author_uri:<25} {author_safe:<40}{marker}")

if quduri_idx and quduri_idx > 15:
    print(f"\n... ({quduri_idx - 15} authors between top 15 and Quduri) ...\n")
    print(f"{'QUDURI':>4} {stage1_scores.get(sorted_authors[quduri_idx], 0):7.2%}  {sorted_authors[quduri_idx]:<25}")

# Show score distribution
print(f"\n" + "-"*80)
print("SCORE DISTRIBUTION ANALYSIS")
print("-"*80)
scores_list = sorted([stage1_scores.get(uri, 0) for uri in stage1_authors], reverse=True)
if scores_list:
    max_score = scores_list[0]
    min_score = scores_list[-1]
    median_score = scores_list[len(scores_list)//2]
    top_10_avg = sum(scores_list[:10]) / 10 if len(scores_list) >= 10 else max_score

    print(f"Max score: {max_score:.4f}")
    print(f"Min score: {min_score:.4f}")
    print(f"Median: {median_score:.4f}")
    print(f"Top 10 average: {top_10_avg:.4f}")

    if quduri_idx is not None:
        quduri_score = stage1_scores.get(sorted_authors[quduri_idx], 0)
        print(f"\nQuduri score: {quduri_score:.4f}")
        print(f"Quduri rank: {quduri_idx + 1} / {len(sorted_authors)}")
        if scores_list[0] > 0:
            gap = (scores_list[0] - quduri_score) / scores_list[0] * 100
            print(f"Gap from top: {gap:.1f}%")
else:
    print("  (No scores)")

print(f"\n" + "-"*80)
print("STAGE 2 RESULTS (Title Matching)")
print("-"*80)
print(f"Matched books: {len(stage2_books)}")
if stage2_books:
    for book_uri in sorted(stage2_books, key=lambda x: stage2_scores.get(x, 0), reverse=True)[:10]:
        book = pipeline.openiti_index.get_book(book_uri)
        score = stage2_scores.get(book_uri, 0)
        book_title = (book.title if hasattr(book, 'title') else str(book)[:50])
        book_safe = book_title.encode('ascii', 'replace').decode('ascii') if book_title else "?"
        print(f"  {score:.2%} | {book_uri} | {book_safe[:40]}")
else:
    print("  (None)")

print(f"\n" + "-"*80)
print("STAGE 3 RESULTS (Combined Matching) - DETAILED")
print("-"*80)

if not stage1_authors or not stage2_books:
    print("(No valid author+book pairs to combine)")
else:
    from matching.config import COMBINED_THRESHOLD, COMBINED_FLOOR

    print(f"Combining with thresholds: floor={COMBINED_FLOOR:.2%}, threshold={COMBINED_THRESHOLD:.2%}\n")

    results = []
    for book_uri in stage2_books:
        book = pipeline.openiti_index.get_book(book_uri)
        if isinstance(book, dict):
            book_author_uri = book.get("author_uri")
        else:
            book_author_uri = book.author_uri

        author_score = stage1_scores.get(book_author_uri, None)
        book_score = stage2_scores.get(book_uri, None)

        # Check gates
        gate1_pass = book_author_uri in stage1_authors
        gate2_pass = author_score is not None and book_score is not None
        gate3_pass = False
        combined_score = None

        if gate1_pass and gate2_pass:
            gate3_pass = (author_score >= COMBINED_FLOOR and book_score >= COMBINED_FLOOR)
            combined_score = (author_score + book_score) / 2.0
            gate4_pass = combined_score >= COMBINED_THRESHOLD

        in_final = book_uri in stage3_matches

        results.append({
            'book_uri': book_uri,
            'author_uri': book_author_uri,
            'author_score': author_score,
            'book_score': book_score,
            'combined_score': combined_score,
            'gate1': gate1_pass,
            'gate3': gate3_pass,
            'gate4': gate4_pass if combined_score else None,
            'final': in_final
        })

    # Sort by combined score
    results = sorted(results, key=lambda x: x['combined_score'] or 0, reverse=True)

    print(f"{'Book URI':<50} {'AuthorScore':>10} {'BookScore':>10} {'Combined':>10} {'Final':>6}")
    print("-"*90)

    for r in results[:30]:  # Show top 30
        author_score_str = f"{r['author_score']:.2%}" if r['author_score'] else "None"
        book_score_str = f"{r['book_score']:.2%}" if r['book_score'] else "None"
        combined_str = f"{r['combined_score']:.2%}" if r['combined_score'] else "---"
        final_str = "PASS" if r['final'] else "FAIL"

        print(f"{r['book_uri']:<50} {author_score_str:>10} {book_score_str:>10} {combined_str:>10} {final_str:>6}")

print(f"\n" + "-"*80)
print(f"FINAL STAGE 3 RESULT: {len(stage3_matches)} match(es)")
print("-"*80)
if stage3_matches:
    for book_uri in stage3_matches:
        print(f"  {book_uri}")
