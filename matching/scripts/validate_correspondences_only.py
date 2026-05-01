"""
Validate matching on just the records in correspondences.json.

IDF is computed from full OpenITI dataset, but we only match the test records.

Ground truth conventions
------------------------
  book_uri  → expected match; correct if that URI appears in stage 3 results.
  "null"    → no match expected; correct if stage 3 returns nothing (TN).
              If stage 3 returns any result for a null record, each returned
              book counts as a false positive.

Outputs
-------
  Console  — per-record summary and aggregate precision/recall/F1.
  CSV      — one row per winning match, with author/title/combined scores,
             for deeper investigation of FPs and score components.
"""

import json
import sys
import csv
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

# ── Load ground truth ────────────────────────────────────────────────────────

print("Loading correspondences.json...")
with open('data_samplers/correspondence.json') as f:
    correspondences = json.load(f)

expected_matches = {}  # {bnf_id: set(book_uris)}  — supports multiple expected per record
for item in correspondences:
    for book_uri, bnf_id in item.items():
        expected_matches.setdefault(bnf_id, set()).add(book_uri)

null_count = sum(1 for v in expected_matches.values() if v == {'null'})
match_count = len(expected_matches) - null_count
print(f"Found {len(expected_matches)} test records ({null_count} null, {match_count} with expected match)\n")

# ── Load data ────────────────────────────────────────────────────────────────

print("Loading BNF and OpenITI data...")
all_bnf = load_bnf_records(cfg.BNF_FULL_PATH)
openiti_data = load_openiti_corpus(cfg.OPENITI_CORPUS_PATH)
test_bnf_records = {bnf_id: all_bnf[bnf_id] for bnf_id in expected_matches if bnf_id in all_bnf}
print(f"Loaded {len(test_bnf_records)} test BNF records\n")

# ── Run pipeline ─────────────────────────────────────────────────────────────

print("Running matching pipeline...")
pipeline = MatchingPipeline(test_bnf_records, openiti_data, verbose=True)
pipeline.register_stage(AuthorMatcher(verbose=True, use_parallel=False))
pipeline.register_stage(TitleMatcher(verbose=True, use_parallel=False))
pipeline.register_stage(CombinedMatcher(verbose=True))
pipeline.register_stage(Classifier(verbose=True))
pipeline.run()

# ── Evaluate ─────────────────────────────────────────────────────────────────

print("\nEvaluating results...\n")

true_positives = 0    # expected non-null match found
false_positives = 0   # wrong result returned (incl. results on null records)
false_negatives = 0   # expected non-null match not found
true_negatives = 0    # null expected, nothing returned

results_by_id = {}
csv_rows = []

for bnf_id, expected_uris in expected_matches.items():
    if bnf_id not in test_bnf_records:
        continue

    is_null_expected = (expected_uris == {'null'})
    stage3_books = pipeline.get_stage3_result(bnf_id) or []
    stage3_scores = pipeline.get_stage3_scores(bnf_id) or {}
    stage1_scores = pipeline.get_stage1_scores(bnf_id) or {}
    stage2_scores = pipeline.get_stage2_scores(bnf_id) or {}

    if is_null_expected:
        if not stage3_books:
            true_negatives += 1
            is_correct = True
        else:
            false_positives += len(stage3_books)
            is_correct = False
    else:
        returned_set = set(stage3_books)
        matched = returned_set & expected_uris          # TPs
        unexpected = returned_set - expected_uris       # FPs
        missed = expected_uris - returned_set           # FNs

        true_positives  += len(matched)
        false_positives += len(unexpected)
        false_negatives += len(missed)
        is_correct = (len(unexpected) == 0 and len(missed) == 0)

    results_by_id[bnf_id] = {
        'expected': sorted(expected_uris),
        'stage3': stage3_books,
        'num_matches': len(stage3_books),
        'is_null_expected': is_null_expected,
        'correct': is_correct,
    }

    # CSV rows — one per winning match
    expected_display = '|'.join(sorted(expected_uris))
    for book_uri in stage3_books:
        book = pipeline.openiti_index.get_book(book_uri)
        author_uri = (book.get('author_uri') if isinstance(book, dict)
                      else getattr(book, 'author_uri', None)) if book else None
        author_score = stage1_scores.get(author_uri, '') if author_uri else ''
        title_score = stage2_scores.get(book_uri, '')
        combined_score = stage3_scores.get(book_uri, '')

        csv_rows.append({
            'bnf_id': bnf_id,
            'expected_book': expected_display,
            'winning_match': book_uri,
            'is_expected_match': book_uri in expected_uris,
            'is_null_expected': is_null_expected,
            'author_uri': author_uri or '',
            'author_score': round(author_score, 4) if isinstance(author_score, float) else '',
            'title_score': round(title_score, 4) if isinstance(title_score, float) else '',
            'combined_score': round(combined_score, 4) if isinstance(combined_score, float) else '',
            'num_matches_for_record': len(stage3_books),
        })

# ── Metrics ──────────────────────────────────────────────────────────────────

total = len(results_by_id)
non_null_total = sum(1 for r in results_by_id.values() if not r['is_null_expected'])
# expected_books_total counts each distinct expected book URI (multi-expected records count > 1)
expected_books_total = sum(
    len(r['expected']) for r in results_by_id.values() if not r['is_null_expected']
)

precision = (true_positives / (true_positives + false_positives)
             if (true_positives + false_positives) > 0 else 0)
recall = (true_positives / (true_positives + false_negatives)
          if (true_positives + false_negatives) > 0 else 0)
f1 = (2 * precision * recall / (precision + recall)
      if (precision + recall) > 0 else 0)

print("=" * 100)
print("VALIDATION RESULTS - Correspondences Only")
print("=" * 100)
print(f"\nDataset: {total} records ({non_null_total} with expected match [{expected_books_total} expected books total], "
      f"{total - non_null_total} null/no-match)")
print(f"\nOutcomes:")
print(f"  True positives  (correct match found):          {true_positives}")
print(f"  False positives (wrong match returned):         {false_positives}")
print(f"  False negatives (expected match not returned):  {false_negatives}")
print(f"  True negatives  (null — correctly no result):   {true_negatives}")
print(f"\nMetrics (null TN records excluded from P/R/F1):")
print(f"  Precision: {precision:.1%}")
print(f"  Recall:    {recall:.1%}")
print(f"  F1 Score:  {f1:.1%}")

print(f"\n{'ID':<20} {'Expected':<40} {'Top result':<40} {'#':<4} {'Result'}")
print("-" * 115)

for bnf_id in sorted(expected_matches.keys()):
    if bnf_id not in results_by_id:
        continue
    r = results_by_id[bnf_id]
    expected_str = ('|'.join(r['expected']))[:39]
    top = (r['stage3'][0] if r['stage3'] else "NONE")[:39]
    num = r['num_matches']

    if r['is_null_expected']:
        status = "CORRECT (TN)" if r['correct'] else f"WRONG (FP ×{num})"
    else:
        returned_set = set(r['stage3'])
        expected_set = set(r['expected'])
        fps = returned_set - expected_set
        fns = expected_set - returned_set
        if not fps and not fns:
            status = "CORRECT"
        elif not fps and fns:
            status = f"PARTIAL ({len(expected_set - returned_set)} missing)"
        elif fps and not fns:
            status = f"CORRECT (+{len(fps)} FP)"
        else:
            status = f"WRONG ({len(fps)} FP, {len(fns)} FN)"

    print(f"{bnf_id:<20} {expected_str:<40} {top:<40} {num:<4} {status}")

# ── CSV output ───────────────────────────────────────────────────────────────

csv_path = Path('data_samplers/validation_matches.csv')
if csv_rows:
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'bnf_id', 'expected_book', 'winning_match', 'is_expected_match',
            'is_null_expected', 'author_uri', 'author_score', 'title_score',
            'combined_score', 'num_matches_for_record',
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)
    print(f"\nCSV written: {csv_path} ({len(csv_rows)} rows)")
else:
    print("\nNo winning matches to write to CSV.")

print("\n" + "=" * 100)
print("Config:")
print(f"  AUTHOR_THRESHOLD:        {cfg.AUTHOR_THRESHOLD}")
print(f"  TITLE_THRESHOLD:         {cfg.TITLE_THRESHOLD}")
print(f"  COMBINED_THRESHOLD:      {cfg.COMBINED_THRESHOLD}")
print(f"  COMBINED_FLOOR:          {cfg.COMBINED_FLOOR}")
print(f"  TITLE_FLOOR:             {cfg.TITLE_FLOOR}")
print(f"  COMBINED_AUTHOR_WEIGHT:  {cfg.COMBINED_AUTHOR_WEIGHT}  COMBINED_TITLE_WEIGHT: {cfg.COMBINED_TITLE_WEIGHT}")
print(f"  TOKEN_RARITY_THRESHOLD:  {cfg.TOKEN_RARITY_THRESHOLD}")
print(f"  AUTHOR_IDF_BOOST_SCALE:  {cfg.AUTHOR_IDF_BOOST_SCALE}  AUTHOR_MAX_BOOST: {cfg.AUTHOR_MAX_BOOST}")
print(f"  TITLE_IDF_BOOST_SCALE:   {cfg.TITLE_IDF_BOOST_SCALE}   TITLE_MAX_BOOST:  {cfg.TITLE_MAX_BOOST}")
print("=" * 100)
